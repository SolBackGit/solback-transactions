"""Durable, project-funded ALT leases. No user keys or token instructions."""
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from fastapi import HTTPException
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.hash import Hash
from solders.message import MessageV0, to_bytes_versioned
from solders.transaction import VersionedTransaction
from solders.system_program import (create_lookup_table,extend_lookup_table,
    deactivate_lookup_table,close_lookup_table)
from solders.compute_budget import set_compute_unit_limit,set_compute_unit_price

LEASE_LOCK=asyncio.Lock()
MAINTENANCE=None
LIVE=2**64-1

@contextmanager
def ledger():
    root=Path(os.environ.get('SOLBACK_WEB_STATE','/opt/solback/web_test/state'))
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    path=root/'compact_tables.sqlite3'
    db=sqlite3.connect(path,timeout=15);db.row_factory=sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS leases(
      id TEXT PRIMARY KEY,claim_token TEXT NOT NULL,wallet TEXT NOT NULL,
      address TEXT UNIQUE NOT NULL,authority TEXT NOT NULL,payer TEXT NOT NULL,
      addresses TEXT NOT NULL,rows TEXT NOT NULL,state TEXT NOT NULL,
      created REAL NOT NULL,touched REAL NOT NULL,rent_budget INTEGER NOT NULL,
      deactivation_slot INTEGER,error TEXT);
    CREATE TABLE IF NOT EXISTS operations(
      id TEXT PRIMARY KEY,lease TEXT NOT NULL,action TEXT NOT NULL,
      raw TEXT NOT NULL,signature TEXT NOT NULL,last_height INTEGER NOT NULL,
      state TEXT NOT NULL,created REAL NOT NULL,UNIQUE(lease,action));
    CREATE INDEX IF NOT EXISTS leases_claim_state ON leases(claim_token,state);
    CREATE TABLE IF NOT EXISTS operation_history AS SELECT * FROM operations WHERE 0;
    ''')
    db.commit();os.chmod(path,0o600)
    try:
        with db:yield db
    finally:db.close()

def named_tables(addresses):
    if not addresses:return []
    with ledger() as db:
        query='SELECT * FROM leases WHERE address IN ('+','.join('?' for _ in addresses)+')'
        return [dict(row,addresses=json.loads(row['addresses'])) for row in db.execute(query,addresses).fetchall()]

def all_tables():
    # Keep historical mappings even after closure: finalized receipts need them.
    with ledger() as db:
        return [dict(row,address=row['address'],addresses=json.loads(row['addresses']))
                for row in db.execute('SELECT * FROM leases').fetchall()]

def required(rows):
    return list(dict.fromkeys([r['address'] for r in rows]+[r['mint'] for r in rows if r['kind']=='balance']))

def ready_table(rows,token=None,wallet=None):
    wanted=set(required(rows))
    for table in all_tables():
        if table['state']=='ready' and (token is None or table['claim_token']==token or (wallet and table['wallet']==wallet)) and wanted<=set(table['addresses']):
            return table
    return None

def covered_rows(token,rows):
    import recovery
    wallet=recovery.claim(token)['wallet']
    for table in all_tables():
        if (table['claim_token']!=token and table['wallet']!=wallet) or table['state']!='ready':continue
        covered=set(table['addresses'])
        group=[r for r in rows if r['address'] in covered and (r['kind']!='balance' or r['mint'] in covered)]
        if group:return group[:56]
    return rows[:56]

def lease_authority(sponsor,identity):
    seed=hmac.new(bytes(sponsor),b'SolBack compact ALT authority v1\0'+identity.encode(),hashlib.sha256).digest()
    return Keypair.from_seed(seed)

def authority_for(table,sponsor):
    key=lease_authority(sponsor,table['id'])
    if str(key.pubkey())!=table['authority'] or str(sponsor.pubkey())!=table['payer']:
        raise HTTPException(503,'The lookup-table funding key changed. Restore the original project funding key before resuming.')
    return key

async def account(client,call,address,commitment='finalized'):
    return await call(client,'getAccountInfo',[address,{'encoding':'base64','commitment':commitment}])

async def transact(client,call,table,action,instructions,sponsor,wait=True):
    """Persist complete signed bytes before sending; never rebuild an uncertain send."""
    with ledger() as db:
        row=db.execute('SELECT * FROM operations WHERE lease=? AND action=?',(table['id'],action)).fetchone()
    if row and row['state']=='confirmed':return True
    if row and row['state'] in ('failed','expired'):
        with ledger() as db:
            db.execute('INSERT INTO operation_history SELECT * FROM operations WHERE id=?',(row['id'],))
            db.execute('DELETE FROM operations WHERE id=?',(row['id'],))
        row=None
    if not row:
        authority=authority_for(table,sponsor)
        block=(await call(client,'getLatestBlockhash',[{'commitment':'confirmed'}]))['value']
        ix=[set_compute_unit_limit(150000),set_compute_unit_price(20000),*instructions]
        msg=MessageV0.try_compile(sponsor.pubkey(),ix,[],Hash.from_string(block['blockhash']))
        signers={str(sponsor.pubkey()):sponsor,str(authority.pubkey()):authority}
        tx=VersionedTransaction.populate(msg,[signers[str(k)].sign_message(to_bytes_versioned(msg))
                     for k in msg.account_keys[:msg.header.num_required_signatures]])
        if len(bytes(tx))>1232 or not all(tx.verify_with_results()):raise ValueError('Invalid ALT management transaction')
        raw=base64.b64encode(bytes(tx)).decode();signature=str(tx.signatures[0])
        sim=(await call(client,'simulateTransaction',[raw,{'encoding':'base64','sigVerify':True,'commitment':'confirmed'}]))['value']
        if 'err' not in sim or sim['err'] is not None:
            raise HTTPException(503,'Lookup-table setup is temporarily unavailable. Your confirmed recovery is saved.')
        with ledger() as db:
            db.execute('INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)',
                (secrets.token_urlsafe(20),table['id'],action,raw,signature,block['lastValidBlockHeight'],'sending',time.time()))
            row=db.execute('SELECT * FROM operations WHERE lease=? AND action=?',(table['id'],action)).fetchone()
    deadline=time.monotonic()+(50 if wait else 0)
    while True:
        statuses=(await call(client,'getSignatureStatuses',[[row['signature']],{'searchTransactionHistory':True}]))['value']
        state=statuses[0]
        if state and state.get('confirmationStatus')=='finalized':
            outcome='failed' if state.get('err') is not None else 'confirmed'
            with ledger() as db:db.execute('UPDATE operations SET state=? WHERE id=?',(outcome,row['id']))
            if outcome=='failed':raise HTTPException(503,'Lookup-table setup transaction failed. Resume to retry safely.')
            return True
        if not state:
            height=await call(client,'getBlockHeight',[{'commitment':'confirmed'}])
            if height>row['last_height']+32:
                chain=await call(client,'getTransaction',[row['signature'],{'encoding':'base64','commitment':'finalized','maxSupportedTransactionVersion':0}])
                if chain:
                    outcome='failed' if chain['meta']['err'] is not None else 'confirmed'
                    with ledger() as db:db.execute('UPDATE operations SET state=? WHERE id=?',(outcome,row['id']))
                    if outcome=='confirmed':return True
                    raise HTTPException(503,'Lookup-table setup transaction failed. Resume to retry.')
                with ledger() as db:db.execute('UPDATE operations SET state="expired" WHERE id=?',(row['id'],))
                raise HTTPException(503,'Lookup-table setup expired. Resume to prepare a new setup transaction.')
            try:
                sent=await call(client,'sendTransaction',[row['raw'],{'encoding':'base64','skipPreflight':False,'preflightCommitment':'confirmed','maxRetries':3}])
                if sent!=row['signature']:raise HTTPException(503,'Unexpected lookup-table transaction response.')
            except HTTPException:
                # The exact bytes remain durable. A later check discovers the result.
                pass
        if not wait:return False
        if time.monotonic()>=deadline:
            raise HTTPException(503,'Lookup-table setup is still confirming. Resume shortly; no duplicate setup will be sent.')
        await asyncio.sleep(2)

async def verify_table(client,call,table):
    import compact_support as compact
    response=await account(client,call,table['address'],'confirmed')
    try:
        deactivation,last_slot,authority,addresses=compact.decode_table(response['value'])
        if deactivation!=LIVE or authority!=table['authority'] or addresses!=table['addresses']:
            raise ValueError('Lookup-table inventory changed')
        if response['context']['slot']<=last_slot:
            raise HTTPException(503,'The lookup table is warming up. Resume shortly.')
    except (ValueError,KeyError,TypeError):
        raise HTTPException(409,'Lookup-table verification failed. No recovery approval was prepared.')
    if table.get('id'):
        with ledger() as db:db.execute('UPDATE leases SET touched=? WHERE id=?',(time.time(),table['id']))

async def ensure(client,call,token,rows,sponsor,progress):
    import compact_support as compact
    # The existing proven table remains available without creating a replacement.
    wanted=set(required(rows));cfg=compact.config()
    for table in cfg.get('tables',[]):
        if wanted<=set(table['addresses']):
            await verify_table(client,call,table);return table
    async with LEASE_LOCK:
        import recovery
        wallet=recovery.claim(token)['wallet']
        table=ready_table(rows,token,wallet)
        if table:
            await verify_table(client,call,table);return table
        addresses=required(rows)
        if not addresses or len(addresses)>112:raise ValueError('Lookup inventory exceeds compact candidate limits')
        with ledger() as db:
            pending=db.execute('SELECT * FROM leases WHERE claim_token=? AND addresses=? AND state="building" ORDER BY created LIMIT 1',
                           (token,json.dumps(addresses))).fetchone()
        if pending:table=dict(pending,addresses=addresses)
        else:
            rent=await call(client,'getMinimumBalanceForRentExemption',[56+32*len(addresses),{'commitment':'confirmed'}])
            balance=(await call(client,'getBalance',[str(sponsor.pubkey()),{'commitment':'confirmed'}]))['value']
            with ledger() as db:
                reserved=db.execute('SELECT COALESCE(SUM(rent_budget),0) FROM leases WHERE state="building"').fetchone()[0]
                locked=db.execute('SELECT COALESCE(SUM(rent_budget),0) FROM leases WHERE state!="closed"').fetchone()[0]
            cap=int(cfg.get('max_alt_locked_lamports',200000000))
            if locked+rent>cap or balance-reserved<rent+500000:
                raise HTTPException(503,'Compact preparation is temporarily busy. Project funding is reserved; your recovery progress is saved.')
            identity=secrets.token_urlsafe(20);authority=lease_authority(sponsor,identity)
            import recovery
            wallet=recovery.claim(token)['wallet']
            slot=await call(client,'getSlot',[{'commitment':'finalized'}])
            create,address=create_lookup_table({'authority_address':authority.pubkey(),'payer_address':sponsor.pubkey(),'recent_slot':slot})
            # Keep the creation instruction alongside the durable lease, before any send.
            table=dict(id=identity,claim_token=token,wallet=wallet,address=str(address),authority=str(authority.pubkey()),
                       payer=str(sponsor.pubkey()),addresses=addresses,rows=[r['address'] for r in rows],state='building',
                       created=time.time(),touched=time.time(),rent_budget=rent,deactivation_slot=slot,error=None)
            with ledger() as db:
                db.execute('INSERT INTO leases VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (identity,token,wallet,table['address'],table['authority'],table['payer'],json.dumps(addresses),
                     json.dumps(table['rows']),'building',table['created'],table['touched'],rent,slot,None))
        authority=authority_for(table,sponsor);address=Pubkey.from_string(table['address'])
        response=await account(client,call,table['address'])
        if not response['value']:
            with ledger() as db:
                op=db.execute('SELECT state FROM operations WHERE lease=? AND action="create"',(table['id'],)).fetchone()
            if not op or op['state'] in ('expired','failed'):
                # A never-sent or conclusively expired create can use a new recent slot.
                slot=await call(client,'getSlot',[{'commitment':'finalized'}])
                _,fresh_address=create_lookup_table({'authority_address':authority.pubkey(),'payer_address':sponsor.pubkey(),'recent_slot':slot})
                table.update(address=str(fresh_address),deactivation_slot=slot);address=fresh_address
                with ledger() as db:db.execute('UPDATE leases SET address=?,deactivation_slot=? WHERE id=?',(str(fresh_address),slot,table['id']))
            create,derived=create_lookup_table({'authority_address':authority.pubkey(),'payer_address':sponsor.pubkey(),
                                               'recent_slot':table['deactivation_slot']})
            if str(derived)!=table['address']:raise ValueError('Lookup-table derivation changed')
            progress.update(phase='Preparing compact recovery',done=0,total=len(addresses))
            await transact(client,call,table,'create',[create],sponsor)
            response=await account(client,call,table['address'])
        deactivation,last_slot,actual_authority,current=compact.decode_table(response['value'])
        if deactivation!=LIVE or actual_authority!=table['authority'] or current!=addresses[:len(current)]:
            raise HTTPException(409,'Lookup-table setup inventory mismatch. Recovery approval is paused.')
        offset=len(current)
        while offset<len(addresses):
            chunk=addresses[offset:offset+20]
            extend=extend_lookup_table({'lookup_table_address':address,'authority_address':authority.pubkey(),
                       'payer_address':sponsor.pubkey(),'new_addresses':[Pubkey.from_string(k) for k in chunk]})
            progress.update(phase='Preparing compact recovery',done=offset,total=len(addresses))
            await transact(client,call,table,'extend-'+str(offset),[extend],sponsor)
            response=await account(client,call,table['address'])
            _,_,actual_authority,current=compact.decode_table(response['value'])
            if actual_authority!=table['authority'] or current!=addresses[:offset+len(chunk)]:
                raise HTTPException(409,'Lookup-table extension changed. Recovery approval is paused.')
            offset=len(current)
        table['addresses']=addresses
        await verify_table(client,call,table)
        with ledger() as db:db.execute('UPDATE leases SET state="ready",touched=?,deactivation_slot=NULL WHERE id=?',(time.time(),table['id']))
        return table

async def maintain_once(client,recovery):
    """Return temporary ALT rent after use; never retire a table backing an uncertain send."""
    import compact_support as compact
    call=recovery.call
    async with LEASE_LOCK:
        with ledger() as db:
            tables=[dict(row,addresses=json.loads(row['addresses']),rows=json.loads(row['rows']))
                    for row in db.execute('SELECT * FROM leases WHERE state!="closed" ORDER BY CASE WHEN state="retiring" THEN 1 ELSE 0 END,created LIMIT 2000').fetchall()]
        processed=0
        for table in tables:
            with recovery.database() as db:
                claims=db.execute('SELECT token,state,rows FROM claims WHERE wallet=?',(table['wallet'],)).fetchall()
                packets=db.execute('SELECT p.claim,p.state,p.payload FROM packets p JOIN claims q ON q.token=p.claim WHERE q.wallet=?',(table['wallet'],)).fetchall()
            relevant=[p for p in packets if table['address'] in json.loads(p['payload']).get('lookup_table_addresses',[])]
            if any(p['state'] in ('approved','signed','submitted','wallet_sending','review') for p in relevant):continue
            remaining=set()
            for claim in claims:
                if claim['state']!='active':continue
                completed={r['address'] for p in packets if p['claim']==claim['token'] and p['state']=='confirmed' for r in json.loads(p['payload'])['rows']}
                remaining.update({r['address'] for r in json.loads(claim['rows'])}-completed)
            expired=time.time()-table['touched']>86400
            if remaining & set(table['rows']) and not expired:continue
            if any(p['state']=='prepared' for p in relevant):
                if not expired:continue
                with recovery.database() as db:
                    for p in db.execute('SELECT p.id,p.payload FROM packets p JOIN claims q ON q.token=p.claim WHERE q.wallet=? AND p.state="prepared"',(table['wallet'],)).fetchall():
                        if table['address'] in json.loads(p['payload']).get('lookup_table_addresses',[]):
                            db.execute('UPDATE packets SET state="expired" WHERE id=?',(p['id'],))
            if processed>=4:break
            processed+=1
            sponsor=recovery.sponsor_key();authority=authority_for(table,sponsor)
            response=await account(client,call,table['address'])
            if not response['value']:
                # A crash before the first send must still settle any durable operation.
                with ledger() as db:pending=db.execute('SELECT 1 FROM operations WHERE lease=? AND state="sending"',(table['id'],)).fetchone()
                if pending:
                    with ledger() as db:close_op=db.execute('SELECT * FROM operations WHERE lease=? AND action="close" AND state="sending"',(table['id'],)).fetchone()
                    if close_op:
                        # A finalized missing account plus our durable close operation
                        # proves rent release; no new close needs to be constructed.
                        with ledger() as db:
                            db.execute('UPDATE operations SET state="confirmed" WHERE id=?',(close_op['id'],))
                            db.execute('UPDATE leases SET state="closed" WHERE id=?',(table['id'],))
                        continue
                    # Poll/resend the original create bytes until it settles or expires.
                    with ledger() as db:create_op=db.execute('SELECT * FROM operations WHERE lease=? AND action="create" AND state="sending"',(table['id'],)).fetchone()
                    if create_op:
                        try:await transact(client,call,table,'create',[],sponsor,wait=False)
                        except HTTPException:pass
                    continue
                with ledger() as db:db.execute('UPDATE leases SET state="closed" WHERE id=?',(table['id'],))
                continue
            deactivation,_,actual_authority,addresses=compact.decode_table(response['value'])
            if actual_authority!=table['authority'] or addresses!=table['addresses'][:len(addresses)]:
                raise ValueError('Cannot clean up changed lookup table')
            address=Pubkey.from_string(table['address'])
            if deactivation==LIVE:
                with ledger() as db:db.execute('UPDATE leases SET state="retiring" WHERE id=?',(table['id'],))
                ix=deactivate_lookup_table({'lookup_table_address':address,'authority_address':authority.pubkey()})
                await transact(client,call,table,'deactivate',[ix],sponsor,wait=False)
                continue
            with ledger() as db:db.execute('UPDATE operations SET state="confirmed" WHERE lease=? AND action="deactivate" AND state="sending"',(table['id'],))
            # SlotHashes is the authoritative cooldown, not wall-clock time or a guessed slot count.
            slot_hashes=await account(client,call,'SysvarS1otHashes111111111111111111111111111')
            raw=base64.b64decode(slot_hashes['value']['data'][0],validate=True)
            import struct
            count=struct.unpack_from('<Q',raw)[0]
            slots=[struct.unpack_from('<Q',raw,8+i*40)[0] for i in range(count)]
            finalized=await call(client,'getSlot',[{'commitment':'finalized'}])
            if deactivation>=finalized or deactivation in slots:continue
            ix=close_lookup_table({'lookup_table_address':address,'authority_address':authority.pubkey(),'recipient_address':sponsor.pubkey()})
            if await transact(client,call,table,'close',[ix],sponsor,wait=False):
                with ledger() as db:db.execute('UPDATE leases SET state="closed" WHERE id=?',(table['id'],))

def start_maintenance(recovery):
    global MAINTENANCE
    if MAINTENANCE and not MAINTENANCE.done():return
    async def worker():
        import httpx
        while True:
            try:
                async with recovery.LOCK,httpx.AsyncClient(timeout=25) as client:
                    await maintain_once(client,recovery)
            except asyncio.CancelledError:raise
            except Exception as exc:
                logging.getLogger('solback.compact-tables').warning('Lookup-table cleanup will retry: %s',type(exc).__name__)
            await asyncio.sleep(30)
    MAINTENANCE=asyncio.create_task(worker())

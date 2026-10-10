"""Checked recovery packets, wallet-first sponsorship, durable confirmation ledger."""
import asyncio
import base64
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from decimal import Decimal
from contextlib import contextmanager
from fastapi import HTTPException
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.signature import Signature
from solders.hash import Hash
from solders.message import MessageV0, to_bytes_versioned
from solders.transaction import VersionedTransaction
from claimsol_instructions import ix_close, ix_burn
from solders.instruction import Instruction, AccountMeta
from solders.system_program import transfer, TransferParams
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price

GUARD='L2TExMFKdjpN9kozasaurPirfHy9P8sbXoAN1qA3S95'
LOCK=asyncio.Lock()
MAX_PACKETS=2048
BUILD_PROGRESS={}
BUILD_ERRORS={}
BACKGROUND={}
RPC_PACE=asyncio.Lock()
RPC_LAST=0.0

@contextmanager
def database():
    path=Path(os.environ.get('SOLBACK_WEB_STATE','/opt/solback/web_test/state'))
    path.mkdir(parents=True,exist_ok=True,mode=0o700)
    c=sqlite3.connect(path/'recovery.sqlite3',timeout=15)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL')
    c.executescript('''CREATE TABLE IF NOT EXISTS claims(token TEXT PRIMARY KEY, wallet TEXT NOT NULL, created REAL NOT NULL, rows TEXT NOT NULL, selection TEXT NOT NULL, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS packets(id TEXT PRIMARY KEY, claim TEXT NOT NULL, idx INTEGER NOT NULL, state TEXT NOT NULL, payload TEXT NOT NULL, signature TEXT, signed TEXT, UNIQUE(claim,idx));''')
    c.commit()
    os.chmod(path/'recovery.sqlite3',0o600)
    try:
        with c:yield c
    finally:c.close()

def configured():
    return bool(os.environ.get('SOLBACK_WEB_TREASURY'))

def sponsor_key():
    filename=os.environ.get('SOLBACK_WEB_SPONSOR_PATH')
    if not filename:raise HTTPException(503,'Gas sponsorship is not configured.')
    path=Path(filename)
    if not path.is_file():raise HTTPException(503,'Gas sponsorship is unavailable.')
    return Keypair.from_bytes(bytes(json.loads(path.read_text())))

async def call(client,method,params):
    global RPC_LAST
    async with RPC_PACE:
        await asyncio.sleep(max(0,RPC_LAST+.12-time.monotonic()))
        RPC_LAST=time.monotonic()
    r=await client.post(os.environ['SOLBACK_TEST_RPC_URL'],json={'jsonrpc':'2.0','id':'recovery','method':method,'params':params})
    if r.status_code!=200:raise HTTPException(503,'Network busy. Your progress is saved. Try again.')
    result=r.json()
    if result.get('id')!='recovery' or result.get('error'):raise HTTPException(503,'Network request unavailable. Your progress is saved.')
    return result['result']

def claim(token):
    with database() as c:row=c.execute('SELECT * FROM claims WHERE token=?',(token,)).fetchone()
    if not row:raise HTTPException(404,'Recovery session not found. Please scan again.')
    record=dict(row)
    allowed=set(os.environ.get('SOLBACK_WEB_TEST_WALLETS','').split())
    if allowed and record['wallet'] not in allowed:raise HTTPException(403,'Recovery approval is currently restricted to the owner test wallets.')
    return record

def shape(message):
    keys=list(message.account_keys);h=message.header;n=h.num_required_signatures
    if len(set(keys))!=len(keys):raise ValueError('Duplicate accounts')
    rights={str(k):(i<n,i<n-h.num_readonly_signed_accounts if i<n else i<len(keys)-h.num_readonly_unsigned_accounts) for i,k in enumerate(keys)}
    ops=[(str(keys[ix.program_id_index]),tuple(str(keys[i]) for i in ix.accounts),bytes(ix.data)) for ix in message.instructions]
    return str(keys[0]),str(message.recent_blockhash),rights,ops

def validate_message(received,expected):
    if getattr(received,'address_table_lookups',[]):raise ValueError('Unexpected address lookup tables')
    a=shape(expected);b=shape(received)
    if a[:2]!=b[:2]:raise ValueError('Fee payer or blockhash changed')
    if any(b[2].get(k)!=v for k,v in a[2].items()):raise ValueError('Account permissions changed')
    if set(b[2])-set(a[2]) not in (set(),{GUARD}):raise ValueError('Unexpected account')
    if GUARD in set(b[2])-set(a[2]) and b[2][GUARD]!=(False,False):raise ValueError('Guard permissions changed')
    if b[3][:len(a[3])]!=a[3]:raise ValueError('Recovery instructions changed')
    extras=b[3][len(a[3]):]
    if len(extras)>len(a[3])+2:raise ValueError('Too many wallet guards')
    for program,keys,data in extras:
        if program!=GUARD or len(keys)!=1 or keys[0] not in a[2] or not 3<=len(data)<=256 or data[0]!=6 or data[1] not in (0,1,2,4,5):
            raise ValueError('Unsupported wallet guard')

def packet_instructions(rows,wallet,treasury,service,sponsor=None,network=0):
    instructions=[set_compute_unit_limit(60000+len(rows)*(12000 if rows[0]['kind']=='balance' else 4000)),set_compute_unit_price(20000)]
    owner=Pubkey.from_string(wallet)
    for row in rows:
        account=Pubkey.from_string(row['address']);program=Pubkey.from_string(row['program'])
        if row['kind']=='balance':
            instructions.append(ix_burn(program,account,row['mint'],owner,row['amount'],row['decimals']))
        instructions.append(ix_close(program,account,owner))
    if service:instructions.append(transfer(TransferParams(from_pubkey=owner,to_pubkey=treasury,lamports=service)))
    if sponsor and network:instructions.append(transfer(TransferParams(from_pubkey=owner,to_pubkey=sponsor,lamports=network)))
    return instructions

def unsigned(rows,wallet,block,service,sponsor=None,network=0):
    treasury=Pubkey.from_string(os.environ['SOLBACK_WEB_TREASURY'])
    payer=sponsor or Pubkey.from_string(wallet)
    msg=MessageV0.try_compile(payer,packet_instructions(rows,wallet,treasury,service,sponsor,network),[],Hash.from_string(block))
    return VersionedTransaction.populate(msg,[Signature.default()]*msg.header.num_required_signatures)

def encode(tx):return base64.b64encode(bytes(tx)).decode()

def create(job,selected):
    if job['state']!='ready':raise HTTPException(409,'Please wait for preparation to finish.')
    allowed=set(os.environ.get('SOLBACK_WEB_TEST_WALLETS','').split())
    if allowed and job['wallet'] not in allowed:raise HTTPException(403,'Recovery approval is currently restricted to the owner test wallets.')
    if not configured():raise HTTPException(503,'Recovery configuration is incomplete.')
    if len(selected)!=len(set(selected)) or any(m not in job['eligible'] for m in selected):raise HTTPException(400,'Invalid Dust selection.')
    permitted={a for m in selected for a in job['eligible'][m]['addresses']}
    rows=[dict(r) for r in job['rows'] if not r['blocked_reason'] and (r['kind']=='empty' or r['address'] in permitted)]
    for row in rows:
        if row['kind']=='balance':
            value=job['eligible'][row['mint']];name=job.get('names',{}).get(row['mint'],{})
            row.update(checked_price=value['unit_price_usd'],holding_value=value['value_usd'],token_name=name.get('name',''),token_symbol=name.get('symbol',''))
    rows.sort(key=lambda r:(r['kind']!='empty',r['mint'],r['address']))
    if not rows:raise HTTPException(409,'No recoverable accounts selected.')
    token=secrets.token_urlsafe(32)
    with database() as c:
        previous=c.execute('SELECT token,selection FROM claims WHERE wallet=? AND state="active" ORDER BY created DESC LIMIT 1',(job['wallet'],)).fetchone()
        if previous:
            previous_selection=json.loads(previous['selection'])
            previous_mints=previous_selection.get('mints',[]) if isinstance(previous_selection,dict) else previous_selection
            if set(previous_mints)!=set(selected):raise HTTPException(409,'An unfinished recovery has a different Dust selection. Resume it before starting another recovery.')
            return previous['token']
        c.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)',(token,job['wallet'],time.time(),json.dumps(rows),json.dumps({'mints':selected,'initial_accounts':len(rows)}),'active'))
    return token

async def checked_rows(client,rows,wallet,account_reason,checked_mint):
    # Check current ownership and quantities; prices already selected by preparation.
    addresses=list(dict.fromkeys([r['address'] for r in rows]+[r['mint'] for r in rows if r['kind']=='balance']))
    data={}
    for offset in range(0,len(addresses),100):
        batch=addresses[offset:offset+100]
        result=await call(client,'getMultipleAccounts',[batch,{'encoding':'jsonParsed','commitment':'confirmed'}])
        if len(result['value'])!=len(batch):raise HTTPException(503,'Incomplete account check.')
        data.update(zip(batch,result['value']))
    quantities={}
    for program in {r['program'] for r in rows if r['kind']=='balance'}:
        owned=await call(client,'getTokenAccountsByOwner',[wallet,{'programId':program},{'encoding':'jsonParsed','commitment':'confirmed'}])
        for entry in owned['value']:
            info=entry['account']['data']['parsed']['info'];amount=info['tokenAmount']
            mint=info['mint'];quantities[mint]=quantities.get(mint,Decimal(0))+Decimal(amount['amount'])/(Decimal(10)**amount['decimals'])
    good=[];excluded=[]
    for row in rows:
        try:
            account=data[row['address']];info=account['data']['parsed']['info'];amount=info['tokenAmount']
            if account_reason(account,info,row['program'],wallet,amount):raise ValueError()
            if info['mint']!=row['mint'] or str(amount['amount'])!=str(row['amount']) or amount['decimals']!=row['decimals']:raise ValueError()
            if row['kind']=='balance':
                if quantities.get(row['mint'],Decimal(0))*Decimal(row['checked_price'])>=Decimal('.50'):raise ValueError()
                mint=checked_mint(data[row['mint']])
                if not mint or mint['program']!=row['program'] or mint['decimals']!=row['decimals']:raise ValueError()
            good.append({**row,'rent_lamports':account['lamports']})
        except (KeyError,TypeError,ValueError):excluded.append(row['address'])
    return good,excluded

async def reconcile(client,token):
    record=claim(token)
    with database() as c:packets=c.execute('SELECT * FROM packets WHERE claim=? AND state IN ("submitted","signed","prepared","wallet_sending") ORDER BY idx',(token,)).fetchall()
    # If Phantom submitted a batch but its response was lost, discover the actual
    # matching messages once for the WHOLE batch, rather than per packet.
    unknown=[p for p in packets if p['state']=='wallet_sending' and not p['signature']]
    if unknown:
        expected={}
        oldest=min(json.loads(p['payload'])['prepared_at'] for p in unknown)-30
        for p in unknown:
            original=VersionedTransaction.from_bytes(base64.b64decode(json.loads(p['payload'])['transaction']))
            shape_key=shape(original.message)
            expected[(shape_key[0],shape_key[1],tuple(shape_key[3]))]=(p,original)
        before=None
        for page in range(3):
            options={'limit':1000,'commitment':'confirmed'}
            if before:options['before']=before
            recent=await call(client,'getSignaturesForAddress',[record['wallet'],options])
            if not recent:break
            for candidate in recent:
                if candidate.get('blockTime') is not None and candidate['blockTime']<oldest:continue
                chain=await call(client,'getTransaction',[candidate['signature'],{'encoding':'base64','commitment':'confirmed','maxSupportedTransactionVersion':0}])
                if not chain:continue
                try:
                    actual=VersionedTransaction.from_bytes(base64.b64decode(chain['transaction'][0],validate=True))
                    message_shape=shape(actual.message)
                    operations=tuple(op for op in message_shape[3] if op[0]!=GUARD)
                    match=expected.get((message_shape[0],message_shape[1],operations))
                    if not match:continue
                    p,original=match;validate_message(actual.message,original.message)
                    if not all(actual.verify_with_results()) or str(actual.signatures[0])!=candidate['signature']:raise ValueError()
                except (ValueError,KeyError,TypeError,IndexError):continue
                with database() as c:c.execute('UPDATE packets SET state="submitted",signature=? WHERE id=? AND signature IS NULL',(candidate['signature'],p['id']))
            if len(recent)<1000 or (recent[-1].get('blockTime') is not None and recent[-1]['blockTime']<oldest):break
            before=recent[-1]['signature']
        with database() as c:packets=c.execute('SELECT * FROM packets WHERE claim=? AND state IN ("submitted","signed","prepared","wallet_sending") ORDER BY idx',(token,)).fetchall()
    height=None
    states={}
    known=[p['signature'] for p in packets if p['signature']]
    for offset in range(0,len(known),256):
        signatures=known[offset:offset+256]
        response=await call(client,'getSignatureStatuses',[signatures,{'searchTransactionHistory':True}])
        if len(response['value'])!=len(signatures):raise HTTPException(503,'Incomplete confirmation response.')
        states.update(zip(signatures,response['value']))
    for p in packets:
        payload=json.loads(p['payload'])
        if not p['signature']:
            if height is None:height=await call(client,'getBlockHeight',[{'commitment':'confirmed'}])
            if height>payload['last_height']+(32 if p['state']=='wallet_sending' else 0):
                with database() as c:c.execute('UPDATE packets SET state="expired" WHERE id=? AND signature IS NULL',(p['id'],))
            continue
        status=states.get(p['signature'])
        if status and status.get('err') is not None:
            with database() as c:c.execute('UPDATE packets SET state="failed" WHERE id=?',(p['id'],))
            continue
        if status and status.get('confirmationStatus') in ('confirmed','finalized'):
            # Verify the actual executed message, including wallet-added assertions.
            chain=await call(client,'getTransaction',[p['signature'],{'encoding':'base64','commitment':'confirmed','maxSupportedTransactionVersion':0}])
            if not chain:continue
            try:
                tx=VersionedTransaction.from_bytes(base64.b64decode(chain['transaction'][0],validate=True))
                original=VersionedTransaction.from_bytes(base64.b64decode(payload['transaction']))
                validate_message(tx.message,original.message)
                if not all(tx.verify_with_results()):raise ValueError()
                if str(tx.signatures[0])!=p['signature'] or chain['meta']['err'] is not None:raise ValueError()
                keys=[str(k) for k in tx.message.account_keys];i=keys.index(record['wallet'])
                net=chain['meta']['postBalances'][i]-chain['meta']['preBalances'][i]
                payload['actual_net']=net;payload['actual_network']=chain['meta']['fee']
            except (ValueError,KeyError,TypeError):
                with database() as c:c.execute('UPDATE packets SET state="review" WHERE id=?',(p['id'],))
                continue
            with database() as c:c.execute('UPDATE packets SET state="confirmed",payload=? WHERE id=?',(json.dumps(payload),p['id']))
            continue
        if height is None:height=await call(client,'getBlockHeight',[{'commitment':'confirmed'}])
        # A seen, non-final transaction is never rebuilt.
        if status:continue
        if height>payload['last_height']+32:
            chain=await call(client,'getTransaction',[p['signature'],{'encoding':'base64','commitment':'finalized','maxSupportedTransactionVersion':0}])
            if chain:continue
            with database() as c:c.execute('UPDATE packets SET state="expired" WHERE id=?',(p['id'],))
        elif p['signed']:
            try:await call(client,'sendTransaction',[p['signed'],{'encoding':'base64','skipPreflight':False,'maxRetries':2,'preflightCommitment':'confirmed'}])
            except HTTPException:pass

async def prepare(client,token,account_reason,checked_mint):
    async with LOCK:
        BUILD_ERRORS.pop(token,None)
        record=claim(token)
        with database() as c:packets=c.execute('SELECT * FROM packets WHERE claim=? ORDER BY idx',(token,)).fetchall()
        if any(p['state'] in ('approved','submitted','signed','wallet_sending','review') for p in packets):return status(token)
        if any(p['state']=='prepared' for p in packets):return status(token)
        completed={r['address'] for p in packets if p['state']=='confirmed' for r in json.loads(p['payload'])['rows']}
        rows=[r for r in json.loads(record['rows']) if r['address'] not in completed]
        if not rows:return status(token)
        BUILD_PROGRESS[token]={'phase':'Checking selected accounts','done':0,'total':len(rows)}
        good,excluded=await checked_rows(client,rows,record['wallet'],account_reason,checked_mint)
        if excluded:
            remaining=[r for r in json.loads(record['rows']) if r['address'] not in excluded]
            with database() as c:c.execute('UPDATE claims SET rows=? WHERE token=?',(json.dumps(remaining),token))
        if not good:return status(token)
        groups=[]
        for kind,maximum in (('empty',10),('balance',5)):
            phase=[r for r in good if r['kind']==kind]
            groups.extend(phase[i:i+maximum] for i in range(0,len(phase),maximum))
        if len(groups)>MAX_PACKETS:raise HTTPException(409,'Too many transactions for this recovery. No partial approval is created.')
        balance=(await call(client,'getBalance',[record['wallet'],{'commitment':'confirmed'}]))['value']
        # All packets are independent. A zero-SOL wallet never depends on an earlier
        # close having already landed before a later transaction can pay its fee.
        sponsored=balance<len(groups)*20000
        sponsor=sponsor_key() if sponsored else None
        if sponsor:
            available=(await call(client,'getBalance',[str(sponsor.pubkey()),{'commitment':'confirmed'}]))['value']
            with database() as c:reservations=c.execute('SELECT payload FROM packets WHERE state IN ("prepared","approved","signed","submitted")').fetchall()
            reserved=sum(json.loads(p['payload']).get('network',0) for p in reservations if json.loads(p['payload']).get('sponsored'))
            if available-reserved<len(groups)*20000:raise HTTPException(503,'Gas sponsorship is temporarily busy. Your progress is saved.')
        block=(await call(client,'getLatestBlockhash',[{'commitment':'confirmed'}]))['value'];block_time=time.monotonic()
        index=max([p['idx'] for p in packets],default=-1)+1
        quoted={};ready=[];queue=list(groups)
        while queue:
            batch=queue.pop(0)
            if time.monotonic()-block_time>20:
                block=(await call(client,'getLatestBlockhash',[{'commitment':'confirmed'}]))['value'];block_time=time.monotonic()
            gross=sum(r['rent_lamports'] for r in batch);service=gross*2000//10000
            tx=unsigned(batch,record['wallet'],block['blockhash'],service,sponsor.pubkey() if sponsor else None,0)
            # Same number of signatures and compute settings gives the same network
            # fee; quote once per packet shape rather than once per token account.
            fee_key=(len(batch),batch[0]['kind'],sponsored)
            if fee_key not in quoted:
                fee=(await call(client,'getFeeForMessage',[base64.b64encode(to_bytes_versioned(tx.message)).decode(),{'commitment':'confirmed'}]))['value']
                if fee is None:raise HTTPException(503,'Network quote unavailable. Try again.')
                quoted[fee_key]=fee
            fee=quoted[fee_key]
            tx=unsigned(batch,record['wallet'],block['blockhash'],service,sponsor.pubkey() if sponsor else None,fee if sponsor else 0)
            # Use upstream's 850-byte cap for both fee-payer modes. Split by actual
            # wire size when Token-2022 or distinct mints add more account keys.
            if len(bytes(tx))>850:
                if len(batch)==1:raise HTTPException(409,'A selected account exceeds the supported packet size.')
                midpoint=len(batch)//2;queue[0:0]=[batch[:midpoint],batch[midpoint:]];continue
            sim=(await call(client,'simulateTransaction',[encode(tx),{'encoding':'base64','sigVerify':False,'replaceRecentBlockhash':True,'commitment':'confirmed'}]))['value']
            if 'err' not in sim or sim['err'] is not None:raise HTTPException(409,'Account simulation changed. Refresh preparation; confirmed progress is saved.')
            if gross<=service+fee:raise HTTPException(409,'A packet has no positive recovery.')
            ready.append({'rows':batch,'gross':gross,'service':service,'network':fee,'net':gross-service-fee,'sponsored':sponsored})
            if len(ready)+len(queue)>MAX_PACKETS:raise HTTPException(409,'Too many transactions for this recovery.')
            BUILD_PROGRESS[token]={'phase':'Checking all recovery transactions','done':sum(len(p['rows']) for p in ready),'total':len(good),'transactions':len(ready)}
        if sponsor and available-reserved<sum(p['network'] for p in ready):raise HTTPException(503,'Gas sponsorship cannot cover the full checked batch. No partial batch is created.')
        # Attach a fresh common blockhash only AFTER every simulation has passed.
        # The user is never shown an incomplete four-transaction subset.
        block=(await call(client,'getLatestBlockhash',[{'commitment':'confirmed'}]))['value']
        now=time.time()
        with database() as c:
            for payload in ready:
                tx=unsigned(payload['rows'],record['wallet'],block['blockhash'],payload['service'],sponsor.pubkey() if sponsor else None,payload['network'] if sponsor else 0)
                payload.update(transaction=encode(tx),last_height=block['lastValidBlockHeight'],prepared_at=now)
                c.execute('INSERT INTO packets VALUES(?,?,?,?,?,?,?)',(secrets.token_urlsafe(24),token,index,'prepared',json.dumps(payload),None,None));index+=1
        BUILD_PROGRESS.pop(token,None)
        return status(token)

async def start_preparation(token,account_reason,checked_mint):
    task=BACKGROUND.get(token)
    if task and not task.done():return status(token)
    # A completed task error is visible and retryable. Unfinished unsigned quotes
    # survive a restart; building work itself is retried without spending funds.
    async def work():
        import httpx
        BUILD_PROGRESS[token]={'phase':'Preparing complete recovery','done':0,'total':len(json.loads(claim(token)['rows']))}
        try:
            async with httpx.AsyncClient(timeout=25) as client:
                await prepare(client,token,account_reason,checked_mint)
        except HTTPException as exc:BUILD_ERRORS[token]=exc.detail
        except Exception:BUILD_ERRORS[token]='Preparation could not finish. Resume to retry; no new transaction was sent.'
        finally:BUILD_PROGRESS.pop(token,None)
    BACKGROUND[token]=asyncio.create_task(work())
    await asyncio.sleep(0)
    return status(token)

async def refresh_blockhash(client,token):
    # This changes only unsigned quote blockhashes, not operations or amounts.
    async with LOCK:
        record=claim(token)
        with database() as c:packets=c.execute('SELECT * FROM packets WHERE claim=?',(token,)).fetchall()
        if any(p['state'] in ('approved','submitted','signed','wallet_sending','review') for p in packets):raise HTTPException(409,'Wait for pending transactions before refreshing.')
        block=(await call(client,'getLatestBlockhash',[{'commitment':'confirmed'}]))['value']
        now=time.time()
        with database() as c:
            for p in packets:
                if p['state']!='prepared':continue
                payload=json.loads(p['payload']);original=VersionedTransaction.from_bytes(base64.b64decode(payload['transaction']))
                sponsor=original.message.account_keys[0] if payload['sponsored'] else None
                tx=unsigned(payload['rows'],record['wallet'],block['blockhash'],payload['service'],sponsor,payload['network'] if sponsor else 0)
                payload.update(transaction=encode(tx),last_height=block['lastValidBlockHeight'],prepared_at=now)
                c.execute('UPDATE packets SET payload=? WHERE id=?',(json.dumps(payload),p['id']))
        return status(token)


def status(token):
    record=claim(token)
    with database() as c:rows=c.execute('SELECT * FROM packets WHERE claim=? ORDER BY idx',(token,)).fetchall()
    prepared=[];confirmed=0;net=0;history=[];pending=False;review=False
    for row in rows:
        p=json.loads(row['payload'])
        if row['state']=='prepared':prepared.append({'id':row['id'],**{k:p[k] for k in ('transaction','sponsored','net','network','service','gross','prepared_at')},'count':len(p['rows']),'kind':p['rows'][0]['kind']})
        if row['state']=='confirmed':confirmed+=len(p['rows']);net+=p.get('actual_net',p['net'])
        pending|=row['state'] in ('approved','signed','submitted','wallet_sending');review|=row['state']=='review'
        history.append({'state':row['state'],'count':len(p['rows']),'signature':row['signature']})
    total=len(json.loads(record['rows']))
    selection=json.loads(record['selection']);original_count=selection.get('initial_accounts',total) if isinstance(selection,dict) else total
    if confirmed==total and not pending and not review:
        record['state']='complete'
        with database() as c:c.execute('UPDATE claims SET state="complete" WHERE token=?',(token,))
    return {'claim_token':token,'wallet':record['wallet'],'state':'review' if review else 'pending' if pending else 'building' if token in BUILD_PROGRESS else 'error' if token in BUILD_ERRORS else record['state'],'build_progress':BUILD_PROGRESS.get(token),'error':BUILD_ERRORS.get(token),'total_accounts':total,'excluded_accounts':max(0,original_count-total),'confirmed_accounts':confirmed,'net_recovered_lamports':net,'packets':prepared,'history':history,'selected_dust':list({r['mint']:{'mint':r['mint'],'name':r.get('token_name',''),'symbol':r.get('token_symbol',''),'value_usd':r.get('holding_value','')} for r in json.loads(record['rows']) if r['kind']=='balance'}.values()),'percent':int(100*confirmed/total) if total else 100}

async def submit(client,token,items,account_reason=None,checked_mint=None):
    async with LOCK:
        record=claim(token)
        if not items or len(items)>MAX_PACKETS:raise HTTPException(400,'Invalid packet count.')
        if len({i['id'] for i in items})!=len(items):raise HTTPException(400,'Duplicate packet.')
        validated=[]
        for item in items:
            with database() as c:p=c.execute('SELECT * FROM packets WHERE id=? AND claim=?',(item['id'],token)).fetchone()
            if not p:raise HTTPException(400,'Unknown packet.')
            if p['state'] not in ('prepared','wallet_sending'):continue
            payload=json.loads(p['payload'])
            if item.get('signature'):
                if payload['sponsored']:raise HTTPException(400,'Sponsor signature must be collected after the wallet signature.')
                try:Signature.from_string(item['signature'])
                except ValueError:raise HTTPException(400,'Invalid transaction signature.')
                validated.append((p,payload,None,item['signature']));continue
            try:
                tx=VersionedTransaction.from_bytes(base64.b64decode(item['transaction'],validate=True))
                original=VersionedTransaction.from_bytes(base64.b64decode(payload['transaction']))
                validate_message(tx.message,original.message)
                owner=Pubkey.from_string(record['wallet']);index=list(tx.message.account_keys).index(owner)
                if index>=tx.message.header.num_required_signatures or not tx.signatures[index].verify(owner,to_bytes_versioned(tx.message)):raise ValueError()
                if payload['sponsored']:
                    key=sponsor_key()
                    if tx.message.account_keys[0]!=key.pubkey():raise ValueError()
                    signatures=list(tx.signatures);signatures[0]=key.sign_message(to_bytes_versioned(tx.message))
                    tx=VersionedTransaction.populate(tx.message,signatures)
                if not all(tx.verify_with_results()) or len(bytes(tx))>1232:raise ValueError()
            except (ValueError,KeyError,TypeError,IndexError):raise HTTPException(400,'Signed transaction does not match the checked quote.')
            validated.append((p,payload,encode(tx),str(tx.signatures[0])))
        # Persist the COMPLETE signed list before returning or sending anything.
        # Fully signed bytes go into the private ledger, not into a browser script.
        with database() as c:
            for p,payload,raw,sig in validated:c.execute('UPDATE packets SET state=?,signed=?,signature=? WHERE id=?',('approved' if raw else 'submitted',raw,sig,p['id']))
    schedule_broadcast(token,account_reason,checked_mint)
    return status(token)

BROADCASTING={}

def schedule_broadcast(token,account_reason=None,checked_mint=None):
    task=BROADCASTING.get(token)
    if task and not task.done():return
    async def worker():
        import httpx
        async with LOCK,httpx.AsyncClient(timeout=25) as client:
            with database() as c:packets=c.execute('SELECT * FROM packets WHERE claim=? AND state="approved" ORDER BY idx',(token,)).fetchall()
            if not packets:return
            record=claim(token)
            try:
                approved_rows=[r for p in packets for r in json.loads(p['payload'])['rows']]
                rejected=set()
                if account_reason and checked_mint:
                    current,changed=await checked_rows(client,approved_rows,record['wallet'],account_reason,checked_mint)
                    rejected=set(changed)
                last_height_check=0;current_height=0
                for p in packets:
                    payload=json.loads(p['payload'])
                    if any(row['address'] in rejected for row in payload['rows']):
                        with database() as c:c.execute('UPDATE packets SET state="failed" WHERE id=?',(p['id'],))
                        continue
                    if time.monotonic()-last_height_check>8:
                        current_height=await call(client,'getBlockHeight',[{'commitment':'confirmed'}]);last_height_check=time.monotonic()
                    if current_height>payload['last_height']:
                        with database() as c:c.execute('UPDATE packets SET state="expired" WHERE id=?',(p['id'],))
                        continue
                    sim=(await call(client,'simulateTransaction',[p['signed'],{'encoding':'base64','sigVerify':True,'commitment':'confirmed'}]))['value']
                    if 'err' not in sim or sim['err'] is not None:
                        with database() as c:c.execute('UPDATE packets SET state="failed" WHERE id=?',(p['id'],))
                        continue
                    # Mark an uncertain send BEFORE the RPC request can leave.
                    with database() as c:c.execute('UPDATE packets SET state="signed" WHERE id=?',(p['id'],))
                    try:
                        sent=await call(client,'sendTransaction',[p['signed'],{'encoding':'base64','skipPreflight':False,'preflightCommitment':'confirmed','maxRetries':3}])
                        if sent==p['signature']:
                            with database() as c:c.execute('UPDATE packets SET state="submitted" WHERE id=?',(p['id'],))
                    except HTTPException:pass
            except Exception:
                pass # Approved bytes persist, and a later resume restarts this worker.
    BROADCASTING[token]=asyncio.create_task(worker())


async def refresh(token):
    async with LOCK:
        with database() as c:
            if c.execute('SELECT 1 FROM packets WHERE claim=? AND state IN ("approved","signed","submitted","wallet_sending","review")',(token,)).fetchone():
                raise HTTPException(409,'Wait for pending transactions before refreshing.')
            c.execute('UPDATE packets SET state="expired" WHERE claim=? AND state="prepared" AND signature IS NULL',(token,))

async def mark_sending(token,ids):
    async with LOCK:
        record=claim(token)
        if not ids or len(ids)>MAX_PACKETS:raise HTTPException(400,'Invalid packet count.')
        with database() as c:
            for ident in ids:
                p=c.execute('SELECT * FROM packets WHERE id=? AND claim=? AND state="prepared"',(ident,token)).fetchone()
                if not p or json.loads(p['payload'])['sponsored']:raise HTTPException(400,'Packet is not eligible for wallet submission.')
            for ident in ids:c.execute('UPDATE packets SET state="wallet_sending" WHERE id=?',(ident,))

RECONCILING={}

def schedule_reconcile(token):
    claim(token)
    with database() as c:approved=c.execute('SELECT 1 FROM packets WHERE claim=? AND state="approved"',(token,)).fetchone()
    if approved:
        from app import account_reason,checked_mint
        schedule_broadcast(token,account_reason,checked_mint)
    task=RECONCILING.get(token)
    if task and not task.done():return
    # Building takes no funds and status stays immediately readable.
    if token in BUILD_PROGRESS:return
    async def worker():
        import httpx
        try:
            async with LOCK,httpx.AsyncClient(timeout=25) as client:
                await reconcile(client,token)
        except Exception:
            pass # Unknown network results remain pending; never rebuild on timeout.
    RECONCILING[token]=asyncio.create_task(worker())

def initialize():
    # Keep submitted legacy transactions reconcilable, but rebuild unsigned quotes
    # from older releases with the new versioned builder and actual fee quote.
    with database() as c:
        for row in c.execute('SELECT id,payload FROM packets WHERE state="prepared"').fetchall():
            tx=VersionedTransaction.from_bytes(base64.b64decode(json.loads(row['payload'])['transaction']))
            if not isinstance(tx.message,MessageV0):c.execute('UPDATE packets SET state="expired" WHERE id=?',(row['id'],))

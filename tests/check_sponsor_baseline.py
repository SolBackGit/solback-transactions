"""Offline RPC/ledger integration tests; no real transactions or keys."""
import ast,asyncio,base64,importlib.util,json,os,sqlite3,struct,sys,tempfile,time
from contextlib import contextmanager
from pathlib import Path
tmp=tempfile.TemporaryDirectory();os.environ['SOLBACK_WEB_STATE']=tmp.name
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import compact_support as compact
import compact_tables as tables
import recovery as r
from fastapi import HTTPException
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.hash import Hash
from solders.message import to_bytes_versioned
from solders.transaction import VersionedTransaction

owner=Keypair();sponsor=Keypair();treasury=Keypair().pubkey()
os.environ['SOLBACK_WEB_TREASURY']=str(treasury)
os.environ['SOLBACK_WEB_TEST_WALLETS']=compact.WALLET
sponsor_path=Path(tmp.name)/'test_sponsor.json';sponsor_path.write_text(json.dumps(list(bytes(sponsor))))
os.environ['SOLBACK_WEB_SPONSOR_PATH']=str(sponsor_path)
sleep=asyncio.sleep
async def fast_sleep(seconds):await sleep(0)
asyncio.sleep=fast_sleep

class RPC:
    def __init__(self):self.accounts={};self.sent={};self.slot=1000;self.height=100;self.calls=[];self.interrupt=False;self.hidden_status=False
    def encoded(self,item):
        raw=bytearray(56);struct.pack_into('<IQQ',raw,0,1,item['deactivation'],item['last_slot']);raw[21]=1
        raw[22:54]=bytes(Pubkey.from_string(item['authority']))
        raw.extend(b''.join(bytes(Pubkey.from_string(x)) for x in item['addresses']))
        return {'owner':compact.ALT_PROGRAM,'executable':False,'data':[base64.b64encode(raw).decode(),'base64'],'lamports':item.get('lamports',1000000)}
    async def __call__(self,client,method,params):
        self.calls.append(method)
        if method=='getSlot':return self.slot+1
        if method=='getBlockHeight':return self.height
        if method=='getLatestBlockhash':return {'value':{'blockhash':str(Hash.from_bytes(self.slot.to_bytes(32,'little'))),'lastValidBlockHeight':self.height+150}}
        if method=='getBalance':return {'value':1000000000}
        if method=='getMinimumBalanceForRentExemption':return params[0]*6960+890880
        if method=='getFeeForMessage':return {'value':10000}
        if method=='getAccountInfo':
            if params[0]=='SysvarS1otHashes111111111111111111111111111':
                raw=struct.pack('<Q',512)+b''.join(struct.pack('<Q32s',self.slot-i,b'\0'*32) for i in range(512))
                return {'context':{'slot':self.slot+1},'value':{'data':[base64.b64encode(raw).decode(),'base64']}}
            return {'context':{'slot':self.slot+1},'value':self.encoded(self.accounts[params[0]]) if params[0] in self.accounts else None}
        if method=='getSignatureStatuses':
            return {'value':[None if self.hidden_status or s not in self.sent else {'err':None,'confirmationStatus':'finalized'} for s in params[0]]}
        if method=='getTransaction':return self.sent.get(params[0])
        if method=='simulateTransaction':
            tx=VersionedTransaction.from_bytes(base64.b64decode(params[0]))
            if params[1].get('sigVerify'):assert all(tx.verify_with_results())
            return {'value':{'err':None}}
        if method=='sendTransaction':
            tx=VersionedTransaction.from_bytes(base64.b64decode(params[0]));assert all(tx.verify_with_results())
            sig=str(tx.signatures[0])
            if sig not in self.sent:
                for ix in tx.message.instructions:
                    keys=[str(tx.message.account_keys[i]) for i in ix.accounts]
                    if str(tx.message.account_keys[ix.program_id_index])!=compact.ALT_PROGRAM:continue
                    data=bytes(ix.data);action=struct.unpack_from('<I',data)[0]
                    if action==0:self.accounts[keys[0]]={'authority':keys[1],'addresses':[],'deactivation':tables.LIVE,'last_slot':self.slot}
                    if action==2:
                        count=struct.unpack_from('<Q',data,4)[0]
                        self.accounts[keys[0]]['addresses'].extend(str(Pubkey.from_bytes(data[12+i*32:44+i*32])) for i in range(count))
                        self.accounts[keys[0]]['last_slot']=self.slot
                    if action==3:self.accounts[keys[0]]['deactivation']=self.slot
                    if action==4:del self.accounts[keys[0]]
                self.sent[sig]={'meta':{'err':None},'transaction':[params[0],'base64']}
                self.slot+=1
            if self.interrupt:
                self.interrupt=False;raise HTTPException(503,'Simulated lost response after broadcast')
            return sig
        raise AssertionError((method,params))

rpc=RPC();r.call=rpc
async def program_checked(client,call):pass
compact.verify=program_checked
def make_row(kind='empty',program=compact.TOKEN):
    return dict(address=str(Keypair().pubkey()),mint=str(Keypair().pubkey()),kind=kind,program=program,
                amount=19,decimals=6,rent_lamports=2039280)
async def checked(client,rows,wallet,a,b):return list(rows),[]
r.checked_rows=checked

def new_claim(token,rows,telegram=False):
    selection={'mints':[],'initial_accounts':len(rows),'engine':compact.ENGINE,'approval_mode':'progressive'}
    if telegram:selection['telegram_session']='tg-session'
    with r.database() as db:db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)',(token,str(owner.pubkey()),time.time(),json.dumps(rows),json.dumps(selection),'active'))

def reference_ops(tx):return compact.shape(tx.message)[3]

async def main():
    # New-wallet table creation, extension, restart reuse, full compression and funding signatures.
    rows=[make_row() for _ in range(56)];new_claim('empty56',rows)
    rpc.interrupt=True
    table=await tables.ensure(None,rpc,'empty56',rows,sponsor,{})
    assert len(table['addresses'])==56
    assert len(rpc.sent)==4  # one create, three extends; no duplicate on lost response
    count=len(rpc.sent)
    await tables.ensure(None,rpc,'empty56',rows,sponsor,{})
    assert len(rpc.sent)==count
    new_claim('shared-restart',rows)
    shared=await tables.ensure(None,rpc,'shared-restart',rows,sponsor,{})
    assert shared['id']==table['id'] and len(rpc.sent)==count
    with r.database() as db:db.execute('UPDATE claims SET state="complete" WHERE token="empty56"')
    await tables.maintain_once(None,r)
    assert rpc.accounts[table['address']]['deactivation']==tables.LIVE
    with r.database() as db:db.execute('UPDATE claims SET state="complete" WHERE token="shared-restart"')
    tx=r.compact_unsigned(rows,str(owner.pubkey()),str(Hash.default()),sum(x['rent_lamports'] for x in rows)//5,lookup_tables=[table])
    assert compact.fits(tx,rows) and tx.message.address_table_lookups
    sponsored56=r.compact_unsigned(rows,str(owner.pubkey()),str(Hash.default()),sum(x['rent_lamports'] for x in rows)//5,sponsor.pubkey(),10000,lookup_tables=[table])
    assert len(sponsored56.signatures)==2 and not compact.fits(sponsored56,rows)  # repayment adds one instruction
    sponsored55=r.compact_unsigned(rows[:55],str(owner.pubkey()),str(Hash.default()),sum(x['rent_lamports'] for x in rows[:55])//5,sponsor.pubkey(),10000,lookup_tables=[table])
    assert compact.fits(sponsored55,rows[:55])
    assert reference_ops(tx)[2][0]==compact.PROGRAM
    await compact.verify_message_tables(None,rpc,tx.message)

    # Sponsorship, mixed burn/close, preserved allocation including Telegram referral payments.
    referral=str(Keypair().pubkey())
    mixed=[make_row('balance',compact.TOKEN),make_row('balance',compact.TOKEN22),make_row()]
    for row in mixed:row.update(telegram_session='tg',telegram_referral_chain=[1,2,3],telegram_referral_wallet=referral)
    new_claim('mixed',mixed,True)
    mixed_table=await tables.ensure(None,rpc,'mixed',mixed,sponsor,{})
    tx=r.compact_unsigned(mixed,str(owner.pubkey()),str(Hash.default()),1200000,sponsor.pubkey(),10000,lookup_tables=[mixed_table])
    assert len(tx.signatures)==2 and compact.fits(tx,mixed)
    original=r.unsigned(mixed,str(owner.pubkey()),str(Hash.default()),1200000,sponsor.pubkey(),10000)
    assert [x for x in reference_ops(tx)[2:] if x[0]!=compact.PROGRAM]==[x for x in reference_ops(original)[2:] if x[2]!=b'\x09']
    signatures=[(sponsor if k==sponsor.pubkey() else owner).sign_message(to_bytes_versioned(tx.message)) for k in tx.message.account_keys[:2]]
    assert all(VersionedTransaction.populate(tx.message,signatures).verify_with_results())

    # End-to-end preparation -> checked sponsor signature -> wallet send reservation -> signature-only receipt.
    old_balance=rpc.__call__
    class SponsoredRPC:
        async def __call__(self,client,method,params):
            if method=='getBalance' and params[0]==str(owner.pubkey()):return {'value':0}
            return await rpc(client,method,params)
    sponsored_rpc=SponsoredRPC();r.call=sponsored_rpc
    ready=await r.prepare_progressive(None,'mixed',None,None)
    assert ready['packets'][0]['sponsored']
    fresh=await r.refresh_progressive(None,'mixed',None,None)
    p=fresh['packets'][0];assert not p['wallet_send']
    partial_tx=VersionedTransaction.from_bytes(base64.b64decode(p['transaction']))
    assert partial_tx.signatures[0]==r.Signature.default()
    assert not all(partial_tx.verify_with_results())
    signatures=list(partial_tx.signatures)
    owner_index=list(partial_tx.message.account_keys).index(owner.pubkey())
    signatures[owner_index]=owner.sign_message(to_bytes_versioned(partial_tx.message))
    owner_tx=VersionedTransaction.populate(partial_tx.message,signatures)
    try:await r.mark_sending('mixed',[p['id']])
    except HTTPException:pass
    else:raise AssertionError('Sponsored signing-only packet reserved for wallet send')
    broadcaster=r.schedule_broadcast
    r.schedule_broadcast=lambda *args:None
    result=await r.submit(None,'mixed',[{'id':p['id'],'transaction':r.encode(owner_tx)}])
    assert result['state']=='pending'
    with r.database() as db:
        saved=db.execute('SELECT * FROM packets WHERE id=?',(p['id'],)).fetchone()
        assert saved['state']=='approved'
        partial_tx=VersionedTransaction.from_bytes(base64.b64decode(saved['signed']))
        assert all(partial_tx.verify_with_results())
        db.execute('UPDATE packets SET state="submitted" WHERE id=?',(p['id'],))
    # Preserve the existing finalized-receipt/real-Telegram checks below.
    r.call=rpc

    # Do not deactivate an ALT that backs an uncertain wallet submission.
    await tables.maintain_once(None,r)
    assert rpc.accounts[mixed_table['address']]['deactivation']==tables.LIVE

    # Reconcile the exact wallet-sent transaction and retain complete signed bytes.
    signatures=list(partial_tx.signatures)
    owner_index=list(partial_tx.message.account_keys).index(owner.pubkey())
    signatures[owner_index]=owner.sign_message(to_bytes_versioned(partial_tx.message))
    actual=VersionedTransaction.populate(partial_tx.message,signatures)
    keys,nw=compact.resolved_keys(actual.message);names=list(map(str,keys));static=len(actual.message.account_keys)
    pre=[0]*len(keys);post=[0]*len(keys)
    for row in mixed:pre[names.index(row['address'])]=row['rent_lamports']
    post[names.index(str(owner.pubkey()))]=p['net']
    raw=base64.b64encode(bytes(actual)).decode();sig=str(actual.signatures[0])
    rpc.sent[sig]={'transaction':[raw,'base64'],'meta':{'err':None,'fee':10000,'preBalances':pre,'postBalances':post,
        'loadedAddresses':{'writable':names[static:static+nw],'readonly':names[static+nw:]}}}
    await r.reconcile(None,'mixed')
    assert r.status('mixed')['state']=='complete'
    with r.database() as db:
        receipt=db.execute('SELECT * FROM packets WHERE id=?',(p['id'],)).fetchone()
        assert receipt['signed']==raw and receipt['state']=='confirmed'
        payload=json.loads(receipt['payload']);assert payload['actual_net']==p['net']

    # Exercise the current production bot bridge handler with the actual finalized receipt.
    source=Path(__file__).parent/'fixtures'/'SolBack_Web_Bridge.py'
    module=ast.parse(source.read_text())
    register=next(x for x in module.body if isinstance(x,ast.FunctionDef) and x.name=='register')
    progress=next(x for x in register.body if isinstance(x,ast.AsyncFunctionDef) and x.name=='progress')
    botpath=Path(tmp.name)/'bot.sqlite3';credits=[]
    @contextmanager
    def botdb():
        db=sqlite3.connect(botpath);db.row_factory=sqlite3.Row
        try:
            with db:yield db
        finally:db.close()
    bridge_spec=importlib.util.spec_from_file_location('botbridge',source)
    botbridge=importlib.util.module_from_spec(bridge_spec);bridge_spec.loader.exec_module(botbridge)
    with botdb() as db:
        db.executescript(botbridge.SCHEMA+'''
          CREATE TABLE claim_batches(id,session,wallet,user_id,created,expires,state,message,unsigned,raw,blockhash,last_height,accounts,gross,service,network,sponsored,signature,notified,purpose,referral_chain);
          CREATE TABLE confirmed_claims(signature PRIMARY KEY,batch_id,user_id,wallet,gross,service,owner_share,referral_pool,network,net,sponsored);
          CREATE TABLE recovery_inventory(session,address,state,UNIQUE(session,address));
        ''')
        db.execute('INSERT INTO telegram_web_sessions(session,user_id,wallet,allowed,referral_chain,created,expires,claim_token,link_token,last_activity) VALUES(?,?,?,?,?,?,?,?,?,?)',
                   ('tg',123,str(owner.pubkey()),json.dumps([x['address'] for x in mixed]),json.dumps([1,2,3]),time.time(),time.time()+1000,'mixed','x',time.time()))
    from fastapi import Request,FastAPI
    namespace=dict(botbridge.__dict__,n={'db':botdb,'wallet_cache':{},
        'recovery_allocation':lambda gross,service,chain:(service-gross*500//10000,gross*500//10000),
        'credit_referrals':lambda db,entry:credits.append(entry['id'])})
    async def authenticated(request):return request
    namespace['authenticated_body']=authenticated
    exec(compile(ast.Module(body=[progress],type_ignores=[]),'production-bot-progress','exec'),namespace)
    from telegram_bridge import register as register_web_bridge
    import telegram_bridge
    r.schedule_reconcile=lambda *args:None
    async def status(token):return r.status(token)
    registered=register_web_bridge({'recovery':r,'JOBS':{},'Request':Request,'app':FastAPI(),'recovery_status':status})
    with r.database() as db:
        db.execute('INSERT INTO telegram_links VALUES(?,?,?,?,?,?,?)',('tg',str(owner.pubkey()),None,'mixed','{}',time.time(),0))
    async def bot_call(path,body):
        if path.endswith('/bind'):return {'ok':True}
        assert path.endswith('/progress')
        return await namespace['progress'](body)
    telegram_bridge.bot_call=bot_call
    await registered['sync_once']();await registered['sync_once']()
    with botdb() as db:
        assert db.execute('SELECT COUNT(*) FROM confirmed_claims').fetchone()[0]==1
        assert db.execute('SELECT state FROM telegram_web_sessions WHERE session="tg"').fetchone()[0]=='complete'
    assert credits==['web:'+p['id']]
    with r.database() as db:assert db.execute('SELECT sync_complete FROM telegram_links WHERE session="tg"').fetchone()[0]==1

    # Wrong table inventory blocks approvals.
    rpc.accounts[table['address']]['addresses'][0]=str(Keypair().pubkey())
    try:await tables.verify_table(None,rpc,table)
    except HTTPException as e:assert e.status_code==409
    else:raise AssertionError('Changed lookup inventory accepted')
    rpc.accounts[table['address']]['addresses'][0]=table['addresses'][0]

    # Completed leases deactivate and return rent only after SlotHashes cooldown.
    with r.database() as db:db.execute('UPDATE claims SET state="complete" WHERE token="empty56"')
    await tables.maintain_once(None,r)
    assert rpc.accounts[table['address']]['deactivation']!=tables.LIVE
    await tables.maintain_once(None,r)
    assert table['address'] in rpc.accounts
    rpc.slot+=1000
    await tables.maintain_once(None,r)
    await tables.maintain_once(None,r)
    assert table['address'] not in rpc.accounts
    assert compact.resolved_keys(tx.message)  # cached mappings remain after cleanup

    # Existing pilot transaction bytes stay the same.
    spec=importlib.util.spec_from_file_location('old',Path(__file__).parent/'fixtures'/'compact_support.py')
    old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
    pilot=[dict(make_row(),address=x) for x in compact.config()['tables'][0]['addresses'][:6]]
    assert bytes(compact.build(pilot,compact.WALLET,str(Hash.default()),10000,None,0,r.packet_instructions))==bytes(old.build(pilot,compact.WALLET,str(Hash.default()),10000,None,0,r.packet_instructions))
    print('PASS: durable ALT creation/extension, lost-send recovery, full 56-account compression, sponsor+owner signatures, SPL/Token-2022 burns, sponsor signs after the owner and checked server-send receipt, pending protection, finalized bytes and real Telegram bridge completion/referral deduplication, table integrity, cooldown cleanup and original pilot byte parity.')

asyncio.run(main())
tmp.cleanup()

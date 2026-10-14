import asyncio,base64,json,os,secrets,sys,tempfile,time,types
from pathlib import Path
tmp=tempfile.TemporaryDirectory();os.environ['SOLBACK_WEB_STATE']=tmp.name
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import recovery as r
import compact_support as compact
import telegram_bridge as tg
from solders.keypair import Keypair
from solders.hash import Hash
from fastapi import FastAPI,Request,HTTPException
owner=Keypair();os.environ['SOLBACK_WEB_TREASURY']=str(Keypair().pubkey())
os.environ['SOLBACK_WEB_TEST_WALLETS']=compact.WALLET
def row():return {'address':str(Keypair().pubkey()),'mint':str(Keypair().pubkey()),'program':compact.TOKEN,'kind':'empty','amount':'0','decimals':6,'rent_lamports':2039280,'blocked_reason':None}
rows=[row(),row()]
def job(wallet=None):return {'state':'ready','wallet':wallet or str(owner.pubkey()),'rows':rows,'eligible':{}}
def packet(token,identity,state='prepared',selected=None):
    selected=selected or rows[:1]
    tx=r.unsigned(selected,str(owner.pubkey()),str(Hash.default()),10000,None,0)
    payload={'rows':selected,'gross':2039280,'service':10000,'net':2024280,'network':5000,'sponsored':False,'prepared_at':time.time(),'transaction':r.encode(tx),'last_height':10}
    with r.database() as db:db.execute('INSERT INTO packets VALUES(?,?,?,?,?,?,?)',(identity,token,0,state,json.dumps(payload),None,None))

async def main():
    first=r.create(job(),[]);packet(first,'old-quote')
    second=r.create(job(),[]);packet(second,'new-quote')
    assert first!=second and r.claim(first)['state']=='active'
    assert not r.status(second)['waiting_for_previous']
    await r.mark_sending(first,['old-quote'])
    state=r.status(second);assert state['state']=='pending' and state['waiting_for_previous'] and not state['packets']
    try:await r.mark_sending(second,['new-quote'])
    except HTTPException as e:assert e.status_code==409
    else:raise AssertionError('Concurrent wallet requests accepted')
    # A fresh session can be created immediately, even while an older request is uncertain.
    third=r.create(job(),[]);assert third not in (first,second)
    assert r.status(third)['waiting_for_previous']
    async def expired(client,method,params):
        if method=='getSignaturesForAddress':return []
        if method=='getBlockHeight':return 1000
        raise AssertionError(method)
    r.call=expired
    await r.reconcile_wallet_attempts(None,r.claim(third))
    assert r.status(third)['state']=='active' and not r.status(third)['waiting_for_previous']
    await r.mark_sending(second,['new-quote'])
    assert r.status(first)['waiting_for_previous']
    # Settle the second attempt and keep actual confirmed history on the first.
    with r.database() as db:
        db.execute('UPDATE packets SET state="expired" WHERE id="new-quote"')
        payload=json.loads(db.execute('SELECT payload FROM packets WHERE id="old-quote"').fetchone()[0]);payload['actual_net']=123456
        db.execute('UPDATE packets SET state="confirmed",payload=? WHERE id="old-quote"',(json.dumps(payload),))
    fourth=r.create(job(),[])
    assert r.status(first)['confirmed_accounts']==1 and r.status(first)['net_recovered_lamports']==123456
    assert r.status(fourth)['confirmed_accounts']==0 and r.status(fourth)['net_recovered_lamports']==0
    assert r.claim(first)['state']=='active'
    # Metadata for distinct Telegram scans must not silently reuse an earlier link.
    jobs={};token_a='A'*40;token_b='B'*40
    metas={token_a:{'session':'session-A','wallet':str(owner.pubkey()),'user_id':1,'allowed':[x['address'] for x in rows],
                   'treasury':os.environ['SOLBACK_WEB_TREASURY'],'expires':time.time()+1000,'referral_chain':[],
                   'referral_wallet':str(Keypair().pubkey()),'claim_token':None}}
    metas[token_b]={**metas[token_a],'session':'session-B'}
    async def call(path,data):
        if path.endswith('/export'):return metas[data['token']]
        if path.endswith('/bind'):return {'ok':True}
        raise AssertionError(path)
    tg.bot_call=call;r.schedule_reconcile=lambda *a:None
    async def scan(body,request):
        token=secrets.token_urlsafe(32);jobs[token]=job();jobs[token].update(balance=100000,names={})
        return {'scan_token':token}
    async def status(token):return r.status(token)
    registered=tg.register({'recovery':r,'JOBS':jobs,'Request':Request,'app':FastAPI(),'recovery_status':status,
                            'Scan':lambda **kw:types.SimpleNamespace(**kw),'scan':scan,'recovery_estimate':lambda *a:{}})
    request=types.SimpleNamespace(client=types.SimpleNamespace(host='127.0.0.1'))
    a=await registered['begin']({'token':token_a},request);assert 'scan' in a
    claim_a=r.create(jobs[a['scan']['scan_token']],[])
    repeat=r.create(jobs[a['scan']['scan_token']],[]);assert repeat==claim_a
    resumed=await registered['begin']({'token':token_a},request)
    assert resumed['recovery']['claim_token']==claim_a
    b=await registered['begin']({'token':token_b},request);assert 'scan' in b and 'recovery' not in b
    claim_b=r.create(jobs[b['scan']['scan_token']],[]);assert claim_b!=claim_a
    direct=r.create(job(),[]);assert direct not in (claim_a,claim_b)
    assert r.claim(claim_a)['state']=='active' and r.claim(claim_b)['state']=='active'
    print('PASS: fresh web sessions; immediate fresh session despite uncertain earlier approval; wallet-wide send guard; expired request reconciliation; old confirmed progress preserved; same Telegram link resumes; different link starts fresh; Telegram-to-web and web-to-Telegram do not force old-link reuse.')

asyncio.run(main());tmp.cleanup()

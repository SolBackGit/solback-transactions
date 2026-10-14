"""Durable Telegram attribution for the shared web recovery engine."""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from fastapi import HTTPException
import httpx

SCHEMA='''CREATE TABLE IF NOT EXISTS telegram_links(
 session TEXT PRIMARY KEY,wallet TEXT NOT NULL,scan_token TEXT,claim_token TEXT,
 metadata TEXT NOT NULL,last_activity REAL NOT NULL,sync_complete INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS telegram_receipt_acks(packet TEXT PRIMARY KEY);
'''

async def bot_call(path,data):
    secret=os.environ.get('SOLBACK_BRIDGE_KEY','')
    if not secret:raise HTTPException(503,'Telegram recovery connection is not configured.')
    raw=json.dumps(data,separators=(',',':')).encode();stamp=str(time.time())
    sig=hmac.new(secret.encode(),stamp.encode()+b'\n'+path.encode()+b'\n'+raw,hashlib.sha256).hexdigest()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response=await client.post('http://127.0.0.1:8787'+path,content=raw,headers={'content-type':'application/json','x-bridge-time':stamp,'x-bridge-signature':sig})
        payload=response.json()
    except (httpx.HTTPError,ValueError):raise HTTPException(503,'Telegram recovery connection is busy. Retry; your progress is saved.')
    if response.status_code!=200:raise HTTPException(response.status_code,payload.get('detail','Telegram recovery unavailable.'))
    return payload

def register(n):
    r=n['recovery'];jobs=n['JOBS'];begin_lock=asyncio.Lock()
    with r.database() as c:c.executescript(SCHEMA)

    async def begin(body:dict,request:n['Request']):
        token=body.get('token','')
        if not isinstance(token,str) or not 30<=len(token)<=100:raise HTTPException(401,'Open your personal Telegram link.')
        ip=request.client.host if request.client else 'unknown'
        queue=n.setdefault('bridge_limits',{}).setdefault(ip,[]);now=time.monotonic()
        queue[:]=[ts for ts in queue if ts>now-60]
        if len(queue)>=10:raise HTTPException(429,'Please wait a minute before reopening your personal link.')
        queue.append(now)
        async with begin_lock:
            meta=await bot_call('/internal/telegram-web/export',{'token':token})
            if meta.get('legacy'):return meta
            if meta['treasury']!=os.environ.get('SOLBACK_WEB_TREASURY'):
                raise HTTPException(503,'Recovery treasury configuration differs. No approval was created.')
            with r.database() as c:link=c.execute('SELECT * FROM telegram_links WHERE session=?',(meta['session'],)).fetchone()
            if link and link['claim_token']:
                await bot_call('/internal/telegram-web/bind',{'session':meta['session'],'claim_token':link['claim_token']})
                r.schedule_reconcile(link['claim_token']);result=r.status(link['claim_token'])
                with r.database() as c:c.execute('UPDATE telegram_links SET last_activity=? WHERE session=?',(time.time(),meta['session']))
                return {'recovery':result,'link_expires_at':meta['expires']}
            if meta.get('claim_token'):raise HTTPException(409,'The recovery ledger is unavailable. Do not start another claim.')
            if link and link['scan_token'] in jobs:
                job=jobs[link['scan_token']];job['telegram']=meta
                return {'scan':scan_view(link['scan_token'],job),'link_expires_at':meta['expires']}
            result=await n['scan'](n['Scan'](wallet=meta['wallet']),request)
            job=jobs[result['scan_token']];allowed=set(meta['allowed'])
            job['rows']=[row for row in job['rows'] if row['address'] in allowed]
            job['telegram']=meta
            result=scan_view(result['scan_token'],job)
            with r.database() as c:c.execute('INSERT INTO telegram_links(session,wallet,scan_token,metadata,last_activity) VALUES(?,?,?,?,?) ON CONFLICT(session) DO UPDATE SET scan_token=excluded.scan_token,metadata=excluded.metadata,last_activity=excluded.last_activity',
                                            (meta['session'],meta['wallet'],result['scan_token'],json.dumps(meta),time.time()))
            return {'scan':result,'link_expires_at':meta['expires']}

    def scan_view(token,job):
        return {'scan_token':token,'wallet':job['wallet'],'accounts':job['rows'],
                'wallet_balance_lamports':job['balance'],'estimate':n['recovery_estimate'](job['rows'],job['balance']),
                'elapsed_seconds':0,'fee_bps':2000,'prices_checked':False,'transaction_submission':False}

    original_create=r.create
    def create(job,selected):
        meta=job.get('telegram')
        if not meta:return original_create(job,selected)
        if meta['expires']<time.time():raise HTTPException(401,'This personal recovery has expired. Scan again in Telegram.')
        # The same personal link is idempotent; a different link creates a new recovery.
        with r.database() as c:
            bound=c.execute('SELECT claim_token FROM telegram_links WHERE session=?',(meta['session'],)).fetchone()
        if bound and bound['claim_token']:
            r.claim(bound['claim_token'])
            return bound['claim_token']
        claim=original_create(job,selected)
        with r.database() as c:
            record=c.execute('SELECT rows,selection FROM claims WHERE token=?',(claim,)).fetchone()
            rows=json.loads(record['rows']);selection=json.loads(record['selection']);selection['telegram_session']=meta['session']
            for row in rows:
                row.update(telegram_session=meta['session'],telegram_referral_chain=meta['referral_chain'],telegram_referral_wallet=meta['referral_wallet'])
            c.execute('UPDATE claims SET rows=?,selection=? WHERE token=?',(json.dumps(rows),json.dumps(selection),claim))
            c.execute('UPDATE telegram_links SET claim_token=?,last_activity=? WHERE session=?',(claim,time.time(),meta['session']))
        return claim
    r.create=create

    original_page_status=n['recovery_status']
    async def status(token):
        with r.database() as c:c.execute('UPDATE telegram_links SET last_activity=? WHERE claim_token=?',(time.time(),token))
        return await original_page_status(token)
    # Replace the existing handler; no parallel shadow route.
    n['app'].routes[:]=[route for route in n['app'].routes if getattr(route,'path','')!='/api/recovery/{token}']
    n['app'].add_api_route('/api/recovery/{token}',status,methods=['GET'])
    n['app'].add_api_route('/api/recovery/telegram-start',begin,methods=['POST'])

    async def sync_once():
        with r.database() as c:links=c.execute('SELECT * FROM telegram_links WHERE claim_token IS NOT NULL AND sync_complete=0').fetchall()
        for link in links:
            token=link['claim_token'];r.schedule_reconcile(token)
            try:
                await bot_call('/internal/telegram-web/bind',{'session':link['session'],'claim_token':token})
                with r.database() as c:
                    packets=c.execute('SELECT * FROM packets WHERE claim=? AND state="confirmed" AND id NOT IN (SELECT packet FROM telegram_receipt_acks) ORDER BY idx LIMIT 8',(token,)).fetchall()
                with r.database() as c:
                    failures=c.execute('SELECT * FROM packets WHERE claim=? AND state="failed" AND id NOT IN (SELECT packet FROM telegram_receipt_acks) ORDER BY idx LIMIT 8',(token,)).fetchall()
                failed_receipts=[{'packet':p['id'],'payload':json.loads(p['payload']),'signature':p['signature'],'signed':p['signed']} for p in failures if p['signature'] and p['signed'] and 'failed_user_network' in json.loads(p['payload'])]
                receipts=[]
                for p in packets:
                    payload=json.loads(p['payload']);signed=p['signed']
                    # Legacy wallet-sent requests have no bytes in the local ledger;
                    # they cannot occur for a new Telegram-bound V12 recovery.
                    if not signed:raise ValueError('Finalized signed bytes unavailable')
                    receipts.append({'packet':p['id'],'payload':payload,'signature':p['signature'],'signed':signed})
                summary={key:value for key,value in r.status(token).items() if key in ('state','total_accounts','confirmed_accounts','net_recovered_lamports')}
                result=await bot_call('/internal/telegram-web/progress',{'session':link['session'],'claim_token':token,'last_activity':link['last_activity'],'summary':summary,'receipts':receipts,'failures':failed_receipts,'selected_addresses':[row['address'] for row in json.loads(r.claim(token)['rows'])]})
                with r.database() as c:
                    c.executemany('INSERT OR IGNORE INTO telegram_receipt_acks VALUES(?)',[(p,) for p in result['packets']])
                    if result['complete']:
                        outstanding=c.execute('SELECT state,payload FROM packets WHERE claim=? AND state IN ("confirmed","failed") AND id NOT IN (SELECT packet FROM telegram_receipt_acks)',(token,)).fetchall()
                        if not any(p['state']=='confirmed' or 'failed_user_network' in json.loads(p['payload']) for p in outstanding):
                            c.execute('UPDATE telegram_links SET sync_complete=1 WHERE session=?',(link['session'],))
            except Exception as exc:logging.getLogger('solback.telegram-bridge').warning('Telegram ledger synchronization will retry: %s | status=%s | detail=%s',type(exc).__name__,getattr(exc,'status_code',''),getattr(exc,'detail',''))

    async def worker():
        while True:
            try:await sync_once()
            except asyncio.CancelledError:raise
            except Exception:logging.getLogger('solback.telegram-bridge').warning('Telegram bridge retry scheduled')
            await asyncio.sleep(10)
    async def startup():n['app'].state.telegram_bridge_task=asyncio.create_task(worker())
    async def shutdown():
        task=getattr(n['app'].state,'telegram_bridge_task',None)
        if task:
            task.cancel()
            try:await task
            except asyncio.CancelledError:pass
    n['app'].router.add_event_handler('startup',startup);n['app'].router.add_event_handler('shutdown',shutdown)
    return {'begin':begin,'sync_once':sync_once}

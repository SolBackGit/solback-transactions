"""Authenticated local bridge: Telegram identity and finalized web receipts."""
import base64
import hashlib
import hmac
import json
import os
import time
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from solders.transaction import VersionedTransaction
from solders.message import to_bytes_versioned

SCHEMA = '''CREATE TABLE IF NOT EXISTS telegram_web_sessions(
 session TEXT PRIMARY KEY,user_id INTEGER NOT NULL,wallet TEXT NOT NULL,
 allowed TEXT NOT NULL,referral_chain TEXT NOT NULL,created REAL NOT NULL,
 expires REAL NOT NULL,claim_token TEXT,link_token TEXT NOT NULL,
 last_activity REAL NOT NULL,state TEXT NOT NULL DEFAULT 'active',
 total INTEGER NOT NULL DEFAULT 0,net INTEGER NOT NULL DEFAULT 0,
 final_sent INTEGER NOT NULL DEFAULT 0,reminder_sent INTEGER NOT NULL DEFAULT 0,
 next_attempt REAL NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS telegram_web_receipts(
 packet TEXT PRIMARY KEY,session TEXT NOT NULL,signature TEXT UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS telegram_web_failures(packet TEXT PRIMARY KEY,session TEXT NOT NULL,signature TEXT UNIQUE NOT NULL);
'''

def verify(secret, stamp, path, raw, signature, now=None):
    try:
        if not secret or abs((time.time() if now is None else now)-float(stamp))>120:return False
        expected=hmac.new(secret.encode(),stamp.encode()+b'\n'+path.encode()+b'\n'+raw,hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected,signature)
    except (ValueError,TypeError):return False

async def authenticated_body(request):
    raw=await request.body()
    if len(raw)>512000:raise HTTPException(413,'Bridge request too large.')
    if not verify(os.environ.get('SOLBACK_BRIDGE_KEY',''),request.headers.get('x-bridge-time',''),request.url.path,raw,request.headers.get('x-bridge-signature','')):
        raise HTTPException(403,'Invalid bridge authorization.')
    try:
        body=json.loads(raw)
        if not isinstance(body,dict):raise ValueError()
        return body
    except (ValueError,TypeError):raise HTTPException(400,'Invalid bridge request.')

def register(n):
    original_init=n['init_db']
    def init_db():
        original_init()
        with n['db']() as c:c.executescript(SCHEMA)
    n['init_db']=init_db

    original_web_session=n['web_session']
    async def web_session(request,operation,authenticated=True):
        token=request.headers.get('x-session','')
        key=hashlib.sha256(token.encode()).hexdigest()
        with n['db']() as c:migrated=c.execute('SELECT 1 FROM telegram_web_sessions WHERE session=?',(key,)).fetchone()
        if migrated:raise HTTPException(409,'Continue this recovery in the new page from your personal Telegram link.')
        return await original_web_session(request,operation,authenticated)
    n['web_session']=web_session

    async def export(request:Request):
        body=await authenticated_body(request)
        token=body.get('token','')
        if not isinstance(token,str) or not 30<=len(token)<=100:raise HTTPException(401,'Invalid personal link.')
        key=hashlib.sha256(token.encode()).hexdigest();now=time.time()
        with n['db']() as c:
            row=c.execute('SELECT * FROM claim_sessions WHERE token_hash=?',(key,)).fetchone()
            saved=c.execute('SELECT * FROM telegram_web_sessions WHERE session=?',(key,)).fetchone()
            if not row:raise HTTPException(401,'Personal link not found. Scan again in Telegram.')
            row=dict(row)
            # Existing legacy work is never silently moved to a different ledger.
            legacy=n['is_withdrawal'](row) or c.execute('SELECT 1 FROM claim_batches WHERE session=? LIMIT 1',(key,)).fetchone()
            if legacy and not saved:return {'legacy':True,'url':n['PUBLIC_URL']+'/claim-legacy#'+token}
            deadline=(row['created']+n['RECOVERY_HARD_TTL']) if saved and saved['claim_token'] else row['expires']
            if now>deadline:raise HTTPException(401,'This personal link has expired. Scan again in Telegram.')
            if not saved:
                # Block migration while another old recovery for this wallet may send.
                if c.execute('SELECT 1 FROM claim_batches WHERE wallet=? AND state IN ("prepared","signed","queued","submitted") LIMIT 1',(row['wallet'],)).fetchone():
                    raise HTTPException(409,'An earlier recovery is pending. Resume its personal link first.')
                if key in n.get('CLAIM_PIPELINE_TASKS',{}):raise HTTPException(409,'The earlier preparation is still running. Retry when it finishes.')
                chain=n['referral_chain'](c,row['user_id'])
                c.execute('INSERT INTO telegram_web_sessions(session,user_id,wallet,allowed,referral_chain,created,expires,link_token,last_activity) VALUES(?,?,?,?,?,?,?,?,?)',
                          (key,row['user_id'],row['wallet'],row['allowed'],json.dumps(chain),row['created'],row['created']+n['RECOVERY_HARD_TTL'],token,now))
                saved=c.execute('SELECT * FROM telegram_web_sessions WHERE session=?',(key,)).fetchone()
            c.execute('UPDATE telegram_web_sessions SET last_activity=? WHERE session=?',(now,key))
        ref=n.get('referral_key')
        if ref is None:raise HTTPException(503,'Referral wallet is not ready.')
        return {'session':key,'wallet':row['wallet'],'user_id':row['user_id'],
                'allowed':json.loads(saved['allowed']),'referral_chain':json.loads(saved['referral_chain']),
                'referral_wallet':str(ref.pubkey()),'treasury':n['TREASURY'],
                'expires':deadline,'claim_token':saved['claim_token']}

    async def bind(request:Request):
        body=await authenticated_body(request);source=body.get('session');claim=body.get('claim_token')
        if not isinstance(claim,str) or not 30<=len(claim)<=100:raise HTTPException(400,'Invalid web claim.')
        with n['db']() as c:
            row=c.execute('SELECT * FROM telegram_web_sessions WHERE session=?',(source,)).fetchone()
            if not row or row['expires']<time.time():raise HTTPException(401,'Personal recovery expired.')
            if row['claim_token'] and row['claim_token']!=claim:raise HTTPException(409,'Personal recovery already linked.')
            c.execute('UPDATE telegram_web_sessions SET claim_token=?,last_activity=? WHERE session=?',(claim,time.time(),source))
        return {'ok':True}

    async def progress(request:Request):
        body=await authenticated_body(request)
        source=body.get('session');claim_token=body.get('claim_token');now=time.time()
        receipts=body.get('receipts',[])
        if not isinstance(receipts,list) or len(receipts)>8:raise HTTPException(400,'Invalid receipt batch.')
        with n['db']() as c:
            saved=c.execute('SELECT * FROM telegram_web_sessions WHERE session=?',(source,)).fetchone()
            if not saved or saved['claim_token']!=claim_token:raise HTTPException(403,'Recovery binding mismatch.')
            chain=json.loads(saved['referral_chain']);allowed=set(json.loads(saved['allowed']))
            for item in receipts:
                packet=item['packet'];payload=item['payload'];sig=item['signature']
                duplicate=c.execute('SELECT * FROM telegram_web_receipts WHERE packet=?',(packet,)).fetchone()
                if duplicate:
                    if duplicate['session']!=source or duplicate['signature']!=sig:raise HTTPException(409,'Receipt already belongs to another session.')
                    continue
                tx=VersionedTransaction.from_bytes(base64.b64decode(item['signed'],validate=True))
                if str(tx.signatures[0])!=sig or not all(tx.verify_with_results()) or saved['wallet'] not in [str(k) for k in tx.message.account_keys[:tx.message.header.num_required_signatures]]:raise HTTPException(400,'Invalid finalized signature.')
                addresses=[r['address'] for r in payload['rows']]
                if not addresses or len(set(addresses))!=len(addresses) or not set(addresses)<=allowed:raise HTTPException(400,'Receipt outside Telegram selection.')
                if c.execute('SELECT 1 FROM confirmed_claims WHERE signature=?',(sig,)).fetchone():raise HTTPException(409,'Signature already assigned to another recovery.')
                gross=payload['gross'];service=payload['service']
                if service!=gross*2000//10000:raise HTTPException(400,'Unexpected service fee.')
                if any(r.get('telegram_session')!=source or r.get('telegram_referral_chain')!=chain for r in payload['rows']):raise HTTPException(400,'Receipt attribution changed.')
                owner,reserve=n['recovery_allocation'](gross,service,chain)
                batch='web:'+packet;block=str(tx.message.recent_blockhash)
                c.execute('INSERT INTO claim_batches(id,session,wallet,user_id,created,expires,state,message,unsigned,raw,blockhash,last_height,accounts,gross,service,network,sponsored,signature,notified,purpose,referral_chain) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                          (batch,source,saved['wallet'],saved['user_id'],now,now,'confirmed',base64.b64encode(to_bytes_versioned(tx.message)).decode(),payload['transaction'],item['signed'],block,payload['last_height'],json.dumps(addresses),gross,service,payload['actual_network'],int(payload['sponsored']),sig,1,'recovery_v2',json.dumps(chain)))
                c.execute('INSERT INTO confirmed_claims(signature,batch_id,user_id,wallet,gross,service,owner_share,referral_pool,network,net,sponsored) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                          (sig,batch,saved['user_id'],saved['wallet'],payload.get('actual_gross',gross),service,owner,reserve,payload['actual_network'],payload['actual_net'],int(payload['sponsored'])))
                n['credit_referrals'](c,dict(id=batch,purpose='recovery_v2',referral_chain=json.dumps(chain),gross=gross,user_id=saved['user_id']))
                c.execute('INSERT INTO telegram_web_receipts VALUES(?,?,?)',(packet,source,sig))
                n['wallet_cache'].pop(saved['wallet'],None)
            failures=body.get('failures',[])
            if not isinstance(failures,list) or len(failures)>8:raise HTTPException(400,'Invalid failed receipt batch.')
            for item in failures:
                packet=item['packet'];payload=item['payload'];sig=item['signature']
                if c.execute('SELECT 1 FROM telegram_web_failures WHERE packet=?',(packet,)).fetchone():continue
                tx=VersionedTransaction.from_bytes(base64.b64decode(item['signed'],validate=True))
                if str(tx.signatures[0])!=sig or not all(tx.verify_with_results()):raise HTTPException(400,'Invalid failed signature.')
                addresses=[r['address'] for r in payload['rows']]
                if not addresses or not set(addresses)<=allowed:raise HTTPException(400,'Failed receipt outside selection.')
                charge=payload['failed_user_network'];network=payload['actual_network']
                if charge!=(0 if payload['sponsored'] else network):raise HTTPException(400,'Failed network charge mismatch.')
                batch='web:'+packet
                c.execute('INSERT INTO claim_batches(id,session,wallet,user_id,created,expires,state,message,unsigned,raw,blockhash,last_height,accounts,gross,service,network,sponsored,signature,notified,purpose,referral_chain) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                          (batch,source,saved['wallet'],saved['user_id'],now,now,'failed',base64.b64encode(to_bytes_versioned(tx.message)).decode(),payload['transaction'],item['signed'],str(tx.message.recent_blockhash),payload['last_height'],json.dumps(addresses),payload['gross'],payload['service'],network,int(payload['sponsored']),sig,1,'recovery_v2',json.dumps(chain)))
                c.execute('INSERT INTO recovery_failed_costs(batch_id,session,network,wallet_charge) VALUES(?,?,?,?)',(batch,source,network,charge))
                c.execute('INSERT INTO telegram_web_failures VALUES(?,?,?)',(packet,source,sig))
            selected=set(body.get('selected_addresses',[]))
            if not selected<=allowed:raise HTTPException(400,'Selection outside personal scan.')
            summary=body['summary'];closed=c.execute('SELECT accounts FROM claim_batches WHERE session=? AND state="confirmed"',(source,)).fetchall()
            closed_addresses={a for r in closed for a in json.loads(r['accounts'])}
            for address in allowed:
                state='completed' if address in closed_addresses else 'pending' if address in selected else 'unavailable'
                c.execute('INSERT INTO recovery_inventory(session,address,state) VALUES(?,?,?) ON CONFLICT(session,address) DO UPDATE SET state=excluded.state',(source,address,state))
            count=len({a for r in closed for a in json.loads(r['accounts'])})
            total=int(summary['total_accounts']);complete=summary['state']=='complete' and count==total and count==summary['confirmed_accounts']
            if complete:state='complete'
            else:state='pending' if summary['state'] in ('pending','building') else 'active'
            last=max(saved['last_activity'],float(body.get('last_activity',0)))
            c.execute('UPDATE telegram_web_sessions SET state=?,total=?,net=?,last_activity=? WHERE session=?',
                      (state,total,int(summary['net_recovered_lamports']),min(now,last),source))
        return {'ok':True,'complete':complete,'packets':[i['packet'] for i in receipts+failures]}

    async def notifications():
        with n['db']() as c:rows=c.execute('SELECT * FROM telegram_web_sessions WHERE claim_token IS NOT NULL AND next_attempt<=? ORDER BY created LIMIT 100',(time.time(),)).fetchall()
        for row in rows:
            complete=row['state']=='complete'
            with n['db']() as c:closed=c.execute('SELECT accounts FROM claim_batches WHERE session=? AND state="confirmed"',(row['session'],)).fetchall()
            count=len({a for r in closed for a in json.loads(r['accounts'])})
            if complete:
                if row['final_sent'] or not count:continue
                field='final_sent'
                text=('✅ <b>Recovery completed</b>\n\n'+f'📦 Accounts recovered: <b>{count} / {row["total"]}</b>\n'+f'💰 Total received: <b>{n["amount"](row["net"])}</b>\nService and Network fees already included.\n\n'+f'Wallet: <code>{row["wallet"]}</code>\n\n💡 Come back after future trades or token transfers to check for more recoverable SOL.')
                markup=n['HOME_KEYS']
            else:
                if row['state']=='pending' or row['reminder_sent'] or time.time()-row['last_activity']<n['RESUME_IDLE_SECONDS'] or time.time()>row['expires'] or not row['total']:continue
                field='reminder_sent';text=('⏸️ <b>Your SOL recovery is waiting</b>\n\n'+f'You’ve recovered <b>{count} of {row["total"]} accounts</b>. There’s still SOL to recover!\n\nContinue where you left off — your progress is saved. No new scan needed.')
                markup=n['K']([[n['B']('▶️ Continue recovery',url=n['PUBLIC_URL']+'/claim#'+row['link_token'])]])
            try:await n['telegram_app'].bot.send_message(row['user_id'],text,parse_mode='HTML',reply_markup=markup)
            except Exception:
                with n['db']() as c:c.execute('UPDATE telegram_web_sessions SET next_attempt=? WHERE session=?',(time.time()+60,row['session']))
                continue
            with n['db']() as c:c.execute('UPDATE telegram_web_sessions SET '+field+'=1,next_attempt=0 WHERE session=?',(row['session'],))
    old_notifications=n['recovery_group_notifications']
    async def all_notifications():
        await old_notifications()
        await notifications()
    n['recovery_group_notifications']=all_notifications

    old_page=n['claim_page']
    n['web'].routes[:]=[r for r in n['web'].routes if getattr(r,'path','')!='/claim']
    async def gate():
        return HTMLResponse('''<!doctype html><html><head><meta name="robots" content="noindex,nofollow"><meta name="referrer" content="no-referrer"><title>SolBack recovery</title></head><body style="background:#06142d;color:#f5f8ff;font-family:system-ui;padding:24px">Opening your personal recovery…<script src="/assets/telegram-handoff.js"></script></body></html>''')
    # SOLBACK_EXTERNAL_HANDOFF_JS_V1
    async def handoff_js():
        return Response("const token=location.hash.slice(1);if(token){const u=new URL('https://solback.org/');u.hash='telegram_claim='+encodeURIComponent(token);location.replace(u.href)}else{document.body.textContent='Open your personal claim link from Telegram.'}",media_type='application/javascript')
    n['web'].add_api_route('/assets/telegram-handoff.js',handoff_js,methods=['GET'])
    n['web'].add_api_route('/claim',gate,methods=['GET'])
    n['web'].add_api_route('/claim-legacy',old_page,methods=['GET'],response_class=HTMLResponse)
    async def health(request:Request):
        await authenticated_body(request)
        return {'build':'SOLBACK-TELEGRAM-BRIDGE-13','service_percent':n['SERVICE_PERCENT'],'treasury':n['TREASURY']}
    n['web'].add_api_route('/internal/telegram-web/health',health,methods=['POST'])
    n['web'].add_api_route('/internal/telegram-web/export',export,methods=['POST'])
    n['web'].add_api_route('/internal/telegram-web/bind',bind,methods=['POST'])
    n['web'].add_api_route('/internal/telegram-web/progress',progress,methods=['POST'])

    # SOLBACK_DIRECT_WEB_CHECK_V1
    from SolBack_Check_Bridge import register as register_check_bridge
    register_check_bridge(n, authenticated_body)

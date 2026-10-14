"""Isolated SolBack web recovery. No Telegram bot imports."""
import asyncio
import os
import time
import secrets
import logging
import base64
import re
from decimal import Decimal
from fastapi import BackgroundTasks
from collections import defaultdict, deque
from pathlib import Path
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from solders.pubkey import Pubkey

app = FastAPI(docs_url=None, redoc_url=None)
ROOT = Path(__file__).parent
PROGRAMS = ('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb')
FEE_BPS = 2000
limiter = defaultdict(deque)
slots = asyncio.Semaphore(2)

PROTECTED = frozenset({'So11111111111111111111111111111111111111112',
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    'CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH',
    '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo', # PYUSD, Paxos
    '2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH', # USDG, Paxos
    'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr', # EURC, Circle
    'USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA', # USDS, Sky
}) | frozenset(os.environ.get('SOLBACK_TEST_PROTECTED_MINTS', '').split())

def account_reason(account, info, program, wallet, amount):
    if account.get('owner') != program or info.get('owner') != wallet:
        return 'Account ownership mismatch'
    if info.get('closeAuthority', wallet) not in (None, wallet):
        return 'Different close authority'
    if info.get('state') != 'initialized' or info.get('isNative'):
        return 'Frozen, uninitialized or native SOL account'
    rent = account.get('lamports', 0)
    if type(rent) is not int or rent <= 0:
        return 'No account deposit'
    nonempty = int(amount['amount']) > 0
    if nonempty and (int(amount['decimals']) <= 0 or int(amount['decimals']) > 255):
        return 'NFT or unsupported decimals'
    if nonempty and info.get('mint') in PROTECTED:
        return 'Protected asset'
    extensions = info.get('extensions', [])
    if not isinstance(extensions, list):
        return 'Unsupported account extensions'
    for ext in extensions:
        if not isinstance(ext, dict):
            return 'Unsupported account extensions'
        if ext.get('extension') == 'immutableOwner' and (nonempty or program == PROGRAMS[1]):
            continue
        if not nonempty and program == PROGRAMS[1] and ext.get('extension') == 'transferHookAccount':
            if isinstance(ext.get('state'), dict) and ext['state'].get('transferring') is False:
                continue
        return 'Unsupported account extensions'
    return None

def recovery_estimate(rows, wallet_balance):
    allowed = [a for a in rows if not a['blocked_reason']]
    empty = sum(a['kind'] == 'empty' for a in allowed)
    burn = len(allowed) - empty
    gross = sum(a['rent_lamports'] for a in allowed)
    batches = (empty + 9) // 10 + (burn + 4) // 5
    sponsored = bool(batches and wallet_balance < 5000)
    network = batches * 5000 + (5000 if sponsored else 0)
    service = gross * FEE_BPS // 10000
    return {'empty_accounts':empty, 'burn_candidates':burn,
            'gross_lamports':gross, 'service_lamports':service,
            'network_lamports':network, 'net_lamports':max(0,gross-service-network),
            'sponsored_estimate':sponsored, 'prices_checked':False}

class Scan(BaseModel):
    wallet: str

@app.get('/')
async def home():
    return FileResponse(ROOT / 'static/index.html')

@app.get('/health')
async def health():
    return {'build': 'SOLBACK-WEB-TELEGRAM-13', 'approval_mode': 'dual', 'transaction_submission': recovery.configured(), 'service_fee_bps': FEE_BPS}

async def rpc(client, wallet, program):
    url = os.environ.get('SOLBACK_TEST_RPC_URL')
    if not url:
        raise HTTPException(503, 'Test RPC is not configured.')
    r = await client.post(url, json={'jsonrpc':'2.0','id':program,'method':'getTokenAccountsByOwner','params':[wallet,{'programId':program},{'encoding':'jsonParsed','commitment':'confirmed'}]})
    if r.status_code != 200:
        raise HTTPException(503, 'RPC busy. Please try later.')
    data = r.json()
    if data.get('error') or data.get('id') != program:
        raise HTTPException(503, 'Wallet scan unavailable. Please try later.')
    return data['result']['value']

async def balance_rpc(client, wallet):
    url = os.environ.get('SOLBACK_TEST_RPC_URL')
    if not url: raise HTTPException(503, 'Test RPC is not configured.')
    r = await client.post(url, json={'jsonrpc':'2.0','id':'balance','method':'getBalance','params':[wallet,{'commitment':'confirmed'}]})
    if r.status_code != 200: raise HTTPException(503, 'RPC busy. Please try later.')
    data = r.json()
    if data.get('error') or data.get('id') != 'balance':
        raise HTTPException(503, 'Wallet balance unavailable.')
    return int(data['result']['value'])

@app.post('/api/scan')
async def scan(body: Scan, request: Request):
    try:
        wallet = str(Pubkey.from_string(body.wallet.strip()))
    except ValueError:
        raise HTTPException(400, 'Invalid Solana wallet address.')
    ip = request.client.host
    now = time.monotonic()
    queue = limiter[ip]
    while queue and queue[0] < now - 60:
        queue.popleft()
    if len(queue) >= 5:
        raise HTTPException(429, 'Please wait a minute before scanning again.')
    queue.append(now)
    started = time.monotonic()
    async with slots, httpx.AsyncClient(timeout=25) as client:
        try:
            responses = await asyncio.gather(*(rpc(client,wallet,p) for p in PROGRAMS), balance_rpc(client,wallet))
            groups, wallet_balance = responses[:2], responses[2]
        except httpx.HTTPError:
            raise HTTPException(503, 'RPC unavailable. Please try later.') from None
    rows = []
    for program, group in zip(PROGRAMS, groups):
        for a in group:
            info = a['account']['data']['parsed']['info']
            amount = info['tokenAmount']
            reason = account_reason(a['account'], info, program, wallet, amount)
            rows.append({'address':a['pubkey'],'mint':info['mint'],'program':program,'amount':amount['amount'],'decimals':amount['decimals'],'balance':amount['uiAmountString'],'rent_lamports':a['account']['lamports'],'kind':'empty' if int(amount['amount']) == 0 else 'balance','blocked_reason':reason})
    prune_jobs()
    if sum(
        1 for job in JOBS.values()
        if job.get('task') is not None and not job['task'].done()
    ) >= 16:
        raise HTTPException(503, 'Preparation capacity is busy. Please try later.')
    if len(JOBS) >= 128:
        raise HTTPException(503, 'Scan capacity is busy. Please try later.')
    token=secrets.token_urlsafe(32)
    JOBS[token]={'created':time.monotonic(),'wallet':wallet,'rows':rows,'balance':wallet_balance,'state':'scanned','progress':{},'eligible':{},'names':{},'task':None,'sol_usd':None}
    return {'scan_token':token,'wallet':wallet,'accounts':rows,'fee_bps':FEE_BPS,'elapsed_seconds':round(time.monotonic()-started,3),'rpc_requests':3,'wallet_balance_lamports':wallet_balance,'estimate':recovery_estimate(rows,wallet_balance),'prices_checked':False,'transaction_submission':False}

@app.get('/brand.png')
async def brand():
    return FileResponse(ROOT / 'static/brand.png')

# Scan snapshots and live preparation are private, temporary sessions, not a price cache.
JOBS = {}
PIPELINE_LOCK = asyncio.Lock()
METADATA_PROGRAM = Pubkey.from_string('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s')

class Preparation(BaseModel):
    scan_token: str


def prune_jobs():
    # SOLBACK_PREPARATION_CAPACITY_V2
    now = time.monotonic()
    for token, job in list(JOBS.items()):
        task = job.get('task')
        if task is not None and not task.done():
            continue
        last_used = job.get('last_used', job['created'])
        if now - last_used > 3600:
            JOBS.pop(token, None)

    # Under memory pressure, remove only inactive sessions
    # that have not been used for at least 15 minutes.
    if len(JOBS) >= 128:
        candidates = sorted(
            (
                (job.get('last_used', job['created']), token)
                for token, job in JOBS.items()
                if (job.get('task') is None or job['task'].done())
                and now - job.get('last_used', job['created']) > 900
            )
        )
        for _, token in candidates:
            if len(JOBS) < 128:
                break
            JOBS.pop(token, None)



def get_job(token):
    prune_jobs()
    job = JOBS.get(token)
    if not job:
        raise HTTPException(404, 'Scan expired. Please scan the wallet again.')
    job['last_used'] = time.monotonic()
    return job


async def read_rpc(client, method, params):
    if method not in ('getMultipleAccounts',): raise RuntimeError('Unsupported read method')
    response=await client.post(os.environ['SOLBACK_TEST_RPC_URL'],json={'jsonrpc':'2.0','id':'read','method':method,'params':params})
    response.raise_for_status()
    payload=response.json()
    if payload.get('error') or payload.get('id')!='read': raise RuntimeError('RPC read unavailable')
    return payload['result']


def checked_mint(account):
    if not isinstance(account,dict) or account.get('owner') not in PROGRAMS:return None
    data=account.get('data',{});parsed=data.get('parsed',{}) if isinstance(data,dict) else {}
    info=parsed.get('info',{})
    if parsed.get('type')!='mint' or not info.get('isInitialized') or int(info.get('decimals',0))<=0:return None
    if info.get('mintAuthority') is not None:return None
    extensions=info.get('extensions',[])
    if not isinstance(extensions,list):return None
    if any(not isinstance(e,dict) or e.get('extension') not in ('metadataPointer','tokenMetadata') for e in extensions):return None
    metadata=[e.get('state',{}) for e in extensions if e.get('extension')=='tokenMetadata']
    if any(re.search(r'\b(lp|liquidity|position)\b',str(e.get('name',''))+' '+str(e.get('symbol','')),re.I) for e in metadata):return None
    return {'program':account['owner'],'decimals':int(info['decimals']),
            'name':str(metadata[0].get('name',''))[:100] if metadata else '',
            'symbol':str(metadata[0].get('symbol',''))[:32] if metadata else ''}


def metadata_name(account,mint):
    if not account or account.get('owner')!=str(METADATA_PROGRAM):return None
    raw=base64.b64decode(account['data'][0],validate=True)
    if len(raw)<65 or raw[0]!=4 or raw[33:65]!=bytes(Pubkey.from_string(mint)):return None
    offset=65;values=[]
    for limit in (64,32):
        n=int.from_bytes(raw[offset:offset+4],'little');offset+=4
        if n>limit or len(raw)<offset+n:return None
        values.append(raw[offset:offset+n].decode('utf-8').strip('\x00 ').strip());offset+=n
    return {'name':values[0], 'symbol':values[1]}

async def load_names(job,mints):
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            for offset in range(0,len(mints),100):
                batch=mints[offset:offset+100]
                addresses=[str(Pubkey.find_program_address([b'metadata',bytes(METADATA_PROGRAM),bytes(Pubkey.from_string(m))],METADATA_PROGRAM)[0]) for m in batch]
                result=await read_rpc(client,'getMultipleAccounts',[addresses,{'encoding':'base64','commitment':'confirmed'}])
                if len(result['value'])!=len(batch):continue
                for mint,account in zip(batch,result['value']):
                    try:
                        name=metadata_name(account,mint)
                        if name and not re.search(r'\b(lp|liquidity|position)\b',name['name']+' '+name['symbol'],re.I):job['names'][mint]=name
                    except (ValueError,KeyError,TypeError,UnicodeError):pass
                await asyncio.sleep(.2)
    except Exception:
        # A missing name never blocks prices or changes threshold eligibility.
        pass

async def prepare_job(job):
    names_task=None
    try:
        job['state']='queued';job['progress']={'phase':'Waiting for preparation','percent':0}
        async with PIPELINE_LOCK:
            job['state']='running';job['progress']={'phase':'Checking token eligibility','percent':0}
            candidate_mints=sorted({r['mint'] for r in job['rows'] if r['kind']=='balance' and not r['blocked_reason']})
            checks={}
            async with httpx.AsyncClient(timeout=12) as client:
                for offset in range(0,len(candidate_mints),100):
                    batch=candidate_mints[offset:offset+100]
                    result=await read_rpc(client,'getMultipleAccounts',[batch,{'encoding':'jsonParsed','commitment':'confirmed'}])
                    if len(result['value'])!=len(batch):raise RuntimeError('Incomplete mint checks')
                    for mint,account in zip(batch,result['value']):
                        checked=checked_mint(account)
                        if checked:checks[mint]=checked
                    await asyncio.sleep(.2)
            from SolBack_Claim_Pipeline import run_pipeline, m
            m.PROTECTED=frozenset(PROTECTED)
            quantities={};burn_rows={};holdings=[]
            for row in job['rows']:
                if row['kind']!='balance':continue
                mint=row['mint'];quantity=Decimal(row['amount'])/(Decimal(10)**row['decimals'])
                quantities[mint]=quantities.get(mint,Decimal(0))+quantity
                if mint not in checks:continue
                holdings.append({'account':row['address'],'mint':mint,'raw_amount':int(row['amount']),'decimals':row['decimals'],'rent_sol':Decimal(row['rent_lamports'])/Decimal(10**9)})
                check=checks[mint]
                if not row['blocked_reason'] and row['decimals']==check['decimals'] and row['program']==check['program']:
                    burn_rows.setdefault(mint,[]).append(row)
            job['names'].update({mint:{'name':c['name'],'symbol':c['symbol']} for mint,c in checks.items() if c['name']})
            names_task=asyncio.create_task(load_names(job,[mint for mint in checks if mint not in job['names']]))
            def price_found(mint,result):
                price=Decimal(str(result['price_usd']));value=quantities.get(mint,Decimal(0))*price
                if not price.is_finite() or price<=0 or value>=Decimal('.50'):
                    job['eligible'].pop(mint,None);return
                rows=burn_rows.get(mint,[])
                if not rows:return
                job['eligible'][mint]={'mint':mint,'quantity':str(quantities[mint]),'unit_price_usd':str(price),'value_usd':str(value),'rent_lamports':sum(r['rent_lamports'] for r in rows),'addresses':[r['address'] for r in rows],'source':result.get('source',''),'account_count':len(rows)}
            def progress(p):
                total=p.get('distinct_mints',0);done=p.get('priced_mints',0)+p.get('stats',{}).get('final_unresolved',0)
                job['progress']={**p,'percent':min(99,int(100*done/total)) if total else 99,'checked_mints':done}
                if p.get('sol_usd'):job['sol_usd']=p['sol_usd']
            if holdings:
                report=await asyncio.wait_for(run_pipeline(job['wallet'],holdings,os.environ['SOLBACK_TEST_RPC_URL'],progress=progress,api_key=os.environ.get('JUPITER_API_KEY'),on_price=price_found),timeout=max(90,len(checks)*.35+45))
                if report and report.get('sol_usd'):job['sol_usd']=report['sol_usd']
            job['state']='ready';job['progress']={**job['progress'],'phase':'Preparation complete','percent':100}
    except asyncio.CancelledError:
        job['state']='error';job['error']='Preparation stopped. Scan again to retry.';raise
    except Exception as exc:
        logging.getLogger('solback-web-test').warning('Preparation failed: %s',type(exc).__name__)
        job['state']='error';job['error']='Preparation could not finish. Please scan again to retry. No transaction was sent.'
    finally:
        if names_task:
            try:await asyncio.wait_for(names_task,2)
            except (TimeoutError,asyncio.CancelledError):names_task.cancel()

@app.post('/api/preparation')
async def begin_preparation(body:Preparation):
    job=get_job(body.scan_token)
    if job['task'] is None:
        job['task']=asyncio.create_task(prepare_job(job))
    return {'state':job['state']}

@app.get('/api/preparation/{token}')
async def preparation_status(token:str):
    job=get_job(token)
    dust=[{**row,**job['names'].get(mint,{})} for mint,row in job['eligible'].items()]
    return {'state':job['state'],'progress':job['progress'],'dust':dust,'sol_usd':job['sol_usd'],'error':job.get('error')}

@app.get('/api/sol-price')
async def sol_price():
    try:
        async with httpx.AsyncClient(timeout=4) as client:
            r=await client.get('https://api.coinbase.com/v2/prices/SOL-USD/spot');r.raise_for_status()
            price=Decimal(r.json()['data']['amount'])
            if not price.is_finite() or price<=0:raise ValueError()
            return {'sol_usd':str(price)}
    except Exception:
        return {'sol_usd':None}

# Recovery is isolated from the Telegram process and its database.
import recovery
from solders.signature import Signature

class Challenge(BaseModel):
    scan_token: str

class ClaimSelection(BaseModel):
    scan_token: str
    mints: list[str]
    signature: str

class ClaimToken(BaseModel):
    claim_token: str

class SignedPacket(BaseModel):
    id: str
    transaction: str | None = None
    signature: str | None = None

class ClaimSubmission(BaseModel):
    claim_token: str
    packets: list[SignedPacket]

@app.post('/api/recovery/challenge')
async def ownership_challenge(body:Challenge,request:Request):
    job=get_job(body.scan_token)
    if job['state']!='ready':raise HTTPException(409,'Wait for preparation to finish.')
    message='SolBack recovery authorization\nDomain: '+request.headers.get('host','')+'\nWallet: '+job['wallet']+'\nSession: '+body.scan_token+'\nNonce: '+secrets.token_urlsafe(24)+'\nThis signature proves wallet ownership. It does not transfer funds.'
    job['challenge']=(message,time.time()+300)
    return {'message':message}

@app.post('/api/recovery/create')
async def create_recovery(body:ClaimSelection):
    job=get_job(body.scan_token)
    challenge=job.get('challenge')
    try:
        if not challenge or challenge[1]<time.time():raise ValueError()
        signature=Signature.from_bytes(base64.b64decode(body.signature,validate=True))
        if not signature.verify(Pubkey.from_string(job['wallet']),challenge[0].encode()):raise ValueError()
    except ValueError:raise HTTPException(401,'Wallet ownership signature is invalid or expired.')
    token=recovery.create(job,body.mints)
    job.pop('challenge',None)
    return recovery.status(token)

@app.post('/api/recovery/prepare')
async def prepare_recovery(body:ClaimToken):
    return await recovery.start_preparation(body.claim_token,account_reason,checked_mint)

@app.post('/api/recovery/submit')
async def submit_recovery(body:ClaimSubmission):
    async with httpx.AsyncClient(timeout=25) as client:
        return await recovery.submit(client,body.claim_token,[p.model_dump(exclude_none=True) for p in body.packets],account_reason,checked_mint)

@app.get('/api/recovery/{token}')
async def recovery_status(token:str):
    recovery.schedule_reconcile(token)
    return recovery.status(token)

@app.get('/solana.js')
async def solana_bundle():
    return FileResponse(ROOT/'static/solana.js',media_type='application/javascript')

class SendingPackets(BaseModel):
    claim_token: str
    ids: list[str]

@app.post('/api/recovery/sending')
async def mark_wallet_sending(body:SendingPackets):
    await recovery.mark_sending(body.claim_token,body.ids)
    return {'ok':True}

@app.post('/api/recovery/refresh')
async def refresh_recovery(body:ClaimToken):
    async with httpx.AsyncClient(timeout=25) as client:
        return await recovery.refresh_blockhash(client,body.claim_token,account_reason,checked_mint)

@app.middleware('http')
async def same_origin_mutations(request:Request,call_next):
    origin=request.headers.get('origin')
    if request.method=='POST' and origin:
        from urllib.parse import urlsplit
        if urlsplit(origin).netloc != request.headers.get('host'):
            from fastapi.responses import JSONResponse
            return JSONResponse({'detail':'Cross-site requests are not allowed.'},status_code=403)
    return await call_next(request)

@app.on_event('startup')
async def recovery_startup():
    recovery.initialize()
    recovery.compact_tables.start_maintenance(recovery)

# OWNER_TEST_DISTRIBUTION
from test_distribution import register as register_test_distribution
register_test_distribution(app)

# SOLBACK_WEB_DUAL_APPROVAL_V1
class ApprovalMode(ClaimToken):
    mode: str

@app.post('/api/recovery/mode')
async def change_recovery_mode(body:ApprovalMode):
    await recovery.set_mode(body.claim_token,body.mode)
    return await recovery.start_preparation(body.claim_token,account_reason,checked_mint)

# SOLBACK_TELEGRAM_WEB_BRIDGE_V13
from telegram_bridge import register as register_telegram_bridge
TELEGRAM_BRIDGE = register_telegram_bridge(globals())

# SOLBACK_PERSONAL_SCAN_ON_RESUME_V1
@app.get('/api/recovery/preview/{token}')
async def personal_recovery_preview(token:str):
    import json
    record=recovery.claim(token)
    with recovery.database() as c:
        packets=c.execute('SELECT state,payload FROM packets WHERE claim=?',(token,)).fetchall()
        link=c.execute('SELECT scan_token FROM telegram_links WHERE claim_token=? LIMIT 1',(token,)).fetchone()
    completed={r['address'] for p in packets if p['state']=='confirmed' for r in json.loads(p['payload'])['rows']}
    rows=[r for r in json.loads(record['rows']) if r['address'] not in completed]
    dust={}
    for row in rows:
        if row['kind']!='balance':continue
        mint=row['mint'];quantity=Decimal(row['amount'])/(Decimal(10)**row['decimals'])
        item=dust.setdefault(mint,{'mint':mint,'quantity':Decimal(0),'rent_lamports':0,'account_count':0,'unit_price_usd':row['checked_price'],'name':row.get('token_name',''),'symbol':row.get('token_symbol','')})
        item['quantity']+=quantity;item['rent_lamports']+=row['rent_lamports'];item['account_count']+=1
    for item in dust.values():
        item['value_usd']=str(item['quantity']*Decimal(item['unit_price_usd']));item['quantity']=str(item['quantity'])
    job=JOBS.get(link['scan_token'],{}) if link else {}
    balance=job.get('balance',0)
    estimate=recovery_estimate(rows,balance)
    status=recovery.status(token)
    known=status.get('estimated_total_net_lamports')
    if known is not None:estimate['net_lamports']=max(0,known-status['net_recovered_lamports'])
    return {'wallet':record['wallet'],'accounts':rows,'dust':list(dust.values()),'wallet_balance_lamports':balance,'estimate':estimate,'sol_usd':job.get('sol_usd')}

# SOLBACK_DIRECT_WEB_CHECK_V1
from web_check_notifications import register as register_web_check
WEB_CHECK = register_web_check(globals())

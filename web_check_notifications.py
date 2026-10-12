"""Website events: durable outbox, finalized receipts, no duplicate Telegram events."""
import asyncio
import json
import logging
from fastapi import Request
from telegram_bridge import bot_call

SCHEMA='''CREATE TABLE IF NOT EXISTS website_check_outbox(
 event_id TEXT PRIMARY KEY,payload TEXT NOT NULL,acked INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS website_check_seen(packet TEXT PRIMARY KEY);'''

def direct(selection):
    value=json.loads(selection)
    return not(isinstance(value,dict) and value.get('telegram_session'))

def register(n):
    r=n['recovery']
    with r.database() as c:
        first=not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='website_check_seen'").fetchone()
        c.executescript(SCHEMA)
        if first:
            # Start with future confirmations; don't replay historic completed claims.
            c.execute('INSERT OR IGNORE INTO website_check_seen SELECT id FROM packets WHERE state="confirmed"')

    def queue(event_id,event):
        with r.database() as c:c.execute('INSERT OR IGNORE INTO website_check_outbox VALUES(?,?,0)',(event_id,json.dumps(event,sort_keys=True)))

    old_scan=n['scan']
    Scan=n['Scan']
    async def scan(body:Scan,request:Request):
        result=await old_scan(body,request)
        # Telegram personal links already produce their scan notification in the bot.
        if request.url.path=='/api/scan':
            estimate=result['estimate']
            try:
                queue('scan:'+result['scan_token'],{'kind':'scan','scan_token':result['scan_token'],'wallet':result['wallet'],
                      'accounts':estimate['empty_accounts']+estimate['burn_candidates'],
                      'gross':estimate['gross_lamports'],'net':estimate['net_lamports']})
            except Exception:logging.getLogger('solback.web-check').exception('Web scan notification could not be queued')
        return result
    n['scan']=scan
    n['app'].routes[:]=[route for route in n['app'].routes if getattr(route,'path','')!='/api/scan']
    n['app'].add_api_route('/api/scan',scan,methods=['POST'])

    async def sync_once():
        with r.database() as c:
            confirmed=c.execute('SELECT p.*,q.wallet,q.selection FROM packets p JOIN claims q ON q.token=p.claim WHERE p.state="confirmed" AND p.id NOT IN (SELECT packet FROM website_check_seen)').fetchall()
            for packet in confirmed:
                if not direct(packet['selection']):
                    c.execute('INSERT OR IGNORE INTO website_check_seen VALUES(?)',(packet['id'],));continue
                payload=json.loads(packet['payload'])
                if not packet['signature'] or any(k not in payload for k in ('actual_gross','actual_net','actual_network')):continue
                event={'kind':'recovery','wallet':packet['wallet'],'signature':packet['signature'],
                       'accounts':len(payload['rows']),'gross':payload['actual_gross'],'net':payload['actual_net'],
                       'service':payload['service'],'network':payload['actual_network'],'sponsored':bool(payload['sponsored'])}
                c.execute('INSERT OR IGNORE INTO website_check_outbox VALUES(?,?,0)',('tx:'+packet['signature'],json.dumps(event,sort_keys=True)))
                c.execute('INSERT OR IGNORE INTO website_check_seen VALUES(?)',(packet['id'],))
            pending=c.execute('SELECT * FROM website_check_outbox WHERE acked=0 ORDER BY rowid LIMIT 8').fetchall()
        for event in pending:
            result=await bot_call('/internal/telegram-web/check-event',json.loads(event['payload']))
            if not result.get('ok') or result.get('event_id')!=event['event_id']:raise ValueError('Check event acknowledgement mismatch')
            with r.database() as c:c.execute('UPDATE website_check_outbox SET acked=1 WHERE event_id=?',(event['event_id'],))

    async def worker():
        while True:
            try:await sync_once()
            except asyncio.CancelledError:raise
            except Exception:logging.getLogger('solback.web-check').warning('Web check notifications will retry')
            await asyncio.sleep(5)
    async def startup():n['app'].state.web_check_task=asyncio.create_task(worker())
    async def shutdown():
        task=getattr(n['app'].state,'web_check_task',None)
        if task:
            task.cancel()
            try:await task
            except asyncio.CancelledError:pass
    n['app'].router.add_event_handler('startup',startup);n['app'].router.add_event_handler('shutdown',shutdown)
    return {'sync_once':sync_once,'queue':queue}

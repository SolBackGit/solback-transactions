import asyncio, importlib.util, sys, types, os
from pathlib import Path
root=Path(__file__).resolve().parents[1];sys.path.insert(0,str(root))
spec=importlib.util.spec_from_file_location('webapp',root/'app.py');app=importlib.util.module_from_spec(spec);spec.loader.exec_module(app)

for key in ('ALL_PROXY','HTTP_PROXY','HTTPS_PROXY','all_proxy','http_proxy','https_proxy'):os.environ.pop(key,None)
os.environ['SOLBACK_TEST_RPC_URL']='https://example.invalid';os.environ['JUPITER_API_KEY']='test-only'
async def read(client,method,params):
 return {'value':[{'owner':app.PROGRAMS[0],'data':{'parsed':{'type':'mint','info':{'isInitialized':True,'decimals':2,'mintAuthority':None}}}} for _ in params[0]]}
async def names(job,mints):
 await asyncio.sleep(.01);job['names']['small']={'name':'Example token','symbol':'EX'}
started=asyncio.Event();proceed=asyncio.Event()
async def pipeline(wallet,holdings,url,progress,api_key,on_price):
 progress({'distinct_mints':4,'priced_mints':0,'stats':{},'sol_usd':'100'})
 for mint,price in [('small','.10'),('equal','.50'),('split','.40')]:on_price(mint,{'price_usd':price,'source':'Test'})
 started.set();await proceed.wait();return {}
app.read_rpc=read;app.load_names=names
sys.modules['SolBack_Claim_Pipeline']=types.SimpleNamespace(run_pipeline=pipeline,m=types.SimpleNamespace(PROTECTED=frozenset()))
def row(mint,raw='100',blocked=None):return {'mint':mint,'address':mint+raw,'program':app.PROGRAMS[0],'kind':'balance','amount':raw,'decimals':2,'rent_lamports':2039280,'blocked_reason':blocked}
async def main():
 job={'rows':[row('small'),row('equal'),row('split'),row('split','50','blocked'),row('unknown')],'wallet':'wallet','names':{},'eligible':{},'state':'scanned','progress':{},'sol_usd':None}
 task=asyncio.create_task(app.prepare_job(job));await asyncio.sleep(.7)
 if not started.is_set():
  print(job); await task; raise AssertionError('Pipeline never started')
 assert job['state']=='running' and set(job['eligible'])=={'small'},job
 assert job['eligible']['small']['value_usd']=='0.10'
 await asyncio.sleep(.03);assert job['names']['small']['symbol']=='EX'
 proceed.set();await task
 assert job['state']=='ready' and job['progress']['percent']==100 and job['sol_usd']=='100'
 assert app.checked_mint({'owner':app.PROGRAMS[0],'data':{'parsed':{'type':'mint','info':{'isInitialized':True,'decimals':6,'mintAuthority':'active'}}}}) is None
 print('Progressive prices before finish; strict <$0.50; whole-mint totals including blocked accounts; unknown excluded; names; SOL reference; active authority exclusion: OK')
asyncio.run(main())

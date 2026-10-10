import asyncio, base64, json, os, sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from solders.keypair import Keypair
from solders.hash import Hash
from solders.transaction import Transaction, VersionedTransaction
from solders.message import to_bytes_versioned
from solders.message import Message
from solders.system_program import transfer, TransferParams
from fastapi import HTTPException
import recovery as r
import app

class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.proxies={k:v for k,v in os.environ.items() if k.lower().endswith('_proxy')}
        for key in self.proxies:os.environ.pop(key,None)
        self.t=tempfile.TemporaryDirectory();os.environ['SOLBACK_WEB_STATE']=self.t.name
        self.owner=Keypair();self.sponsor=Keypair();self.treasury=Keypair().pubkey();self.block=Hash.new_unique();self.chain={};self.sent=[]
        os.environ['SOLBACK_WEB_TREASURY']=str(self.treasury)
        self.keyfile=Path(self.t.name)/'sponsor.json';self.keyfile.write_text(json.dumps(list(bytes(self.sponsor))))
        os.environ['SOLBACK_WEB_SPONSOR_PATH']=str(self.keyfile)
        self.mint=Keypair().pubkey()
        self.rows=[dict(address=str(Keypair().pubkey()),mint=str(self.mint),program=app.PROGRAMS[0],amount='0',decimals=6,balance='0',rent_lamports=2039280,kind='empty',blocked_reason=None) for _ in range(11)]
        self.job={'state':'ready','wallet':str(self.owner.pubkey()),'rows':self.rows,'eligible':{}}
        self.token=r.create(self.job,[]);self.original_call=r.call;self.height=10;self.wallet_balance=1000000;self.sim_err=None
        async def mock(client,method,params):
            if method=='getBlockHeight':return self.height
            if method=='getLatestBlockhash':return {'value':{'blockhash':str(self.block),'lastValidBlockHeight':200}}
            if method=='getBalance':return {'value':self.wallet_balance if params[0]==str(self.owner.pubkey()) else 1000000000}
            if method=='getFeeForMessage':return {'value':10000 if self.wallet_balance<60000 else 5000}
            if method=='simulateTransaction':return {'value':{'err':self.sim_err}}
            if method=='sendTransaction':
                tx=VersionedTransaction.from_bytes(base64.b64decode(params[0]));self.sent.append(params[0]);return str(tx.signatures[0])
            if method=='getSignatureStatuses':return {'value':[{'confirmationStatus':'confirmed','err':None} if sig in self.chain else None for sig in params[0]]}
            if method=='getTransaction':return self.chain.get(params[0])
            if method=='getSignaturesForAddress':return [{'signature':s,'err':None,'blockTime':int(__import__('time').time())} for s in self.chain]
            if method=='getTokenAccountsByOwner':return {'value':[{'account':{'data':{'parsed':{'info':{'mint':str(self.mint),'tokenAmount':{'amount':'1','decimals':6}}}}}}]}
            if method=='getMultipleAccounts':
                result=[]
                for address in params[0]:
                    row=next((x for x in self.rows if x['address']==address),None)
                    if row:result.append({'owner':row['program'],'lamports':row['rent_lamports'],'data':{'parsed':{'info':{'owner':str(self.owner.pubkey()),'mint':row['mint'],'state':'initialized','isNative':False,'tokenAmount':{'amount':row['amount'],'decimals':row['decimals']}}}}})
                    else:result.append({'owner':app.PROGRAMS[0],'data':{'parsed':{'type':'mint','info':{'isInitialized':True,'decimals':6,'mintAuthority':None}}}})
                return {'value':result}
            raise AssertionError(method)
        r.call=mock
    def tearDown(self):r.call=self.original_call;self.t.cleanup();os.environ.update(self.proxies)
    def signed(self,p):
        tx=VersionedTransaction.from_bytes(base64.b64decode(p['transaction']));return self.owner_signed(tx)
    def owner_signed(self,tx):
        signatures=list(tx.signatures);index=list(tx.message.account_keys).index(self.owner.pubkey());signatures[index]=self.owner.sign_message(to_bytes_versioned(tx.message));return VersionedTransaction.populate(tx.message,signatures)
    def confirm(self,tx):
        keys=[str(k) for k in tx.message.account_keys];before=[1000000]*len(keys);after=before.copy();i=keys.index(str(self.owner.pubkey()));after[i]+=123456
        self.chain[str(tx.signatures[0])]={'transaction':[r.encode(tx),'base64'],'meta':{'err':None,'fee':5000,'preBalances':before,'postBalances':after}}
    async def test_empty_first_small_packets_and_twenty_percent(self):
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint)
        self.assertEqual([p['count'] for p in d['packets']],[10,1])
        for p in d['packets']:self.assertEqual(p['service'],p['gross']*2000//10000);self.assertLess(len(base64.b64decode(p['transaction'])),851)
    async def test_sponsor_wallet_first_and_durable_submission(self):
        self.wallet_balance=0;d=await r.prepare(None,self.token,app.account_reason,app.checked_mint);p=d['packets'][0];self.assertTrue(p['sponsored'])
        tx=self.signed(p);self.assertFalse(all(tx.verify_with_results()))
        result=await r.submit(None,self.token,[{'id':p['id'],'transaction':r.encode(tx)}]);self.assertEqual(result['state'],'pending');await r.BROADCASTING[self.token]
        sent=VersionedTransaction.from_bytes(base64.b64decode(self.sent[0]));self.assertTrue(all(sent.verify_with_results()))
        self.confirm(sent);await r.reconcile(None,self.token)
        self.assertEqual(r.status(self.token)['confirmed_accounts'],10)
        self.assertEqual(r.status(self.token)['net_recovered_lamports'],123456)
    async def test_changed_instructions_rejected_without_send(self):
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint);p=d['packets'][0]
        evil=Transaction.new_signed_with_payer([transfer(TransferParams(from_pubkey=self.owner.pubkey(),to_pubkey=Keypair().pubkey(),lamports=1))],self.owner.pubkey(),[self.owner],self.block)
        with self.assertRaises(HTTPException):await r.submit(None,self.token,[{'id':p['id'],'transaction':r.encode(evil)}])
        self.assertFalse(self.sent)
    async def test_duplicate_submit_does_not_send_again(self):
        p=(await r.prepare(None,self.token,app.account_reason,app.checked_mint))['packets'][0];item={'id':p['id'],'transaction':r.encode(self.signed(p))}
        await r.submit(None,self.token,[item]);await r.BROADCASTING[self.token];n=len(self.sent);await r.submit(None,self.token,[item]);self.assertEqual(n,len(self.sent))
    async def test_no_new_packets_while_signature_unknown(self):
        p=(await r.prepare(None,self.token,app.account_reason,app.checked_mint))['packets'][0];await r.submit(None,self.token,[{'id':p['id'],'transaction':r.encode(self.signed(p))}])
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint);self.assertEqual(d['state'],'pending')
        with r.database() as c:self.assertEqual(c.execute('SELECT COUNT(*) FROM packets').fetchone()[0],2)
    async def test_refresh_replaces_only_unsigned_quotes(self):
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint);ids=[p['id'] for p in d['packets']];await r.refresh(self.token)
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint);self.assertFalse(set(ids)&{p['id'] for p in d['packets']})
    async def test_all_confirmed_marks_complete(self):
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint)
        for p in d['packets']:
            tx=self.signed(p);self.confirm(tx);await r.submit(None,self.token,[{'id':p['id'],'signature':str(tx.signatures[0])}])
        await r.reconcile(None,self.token);self.assertEqual(r.status(self.token)['state'],'complete')
    async def test_wallet_send_unknown_result_found_on_chain(self):
        p=(await r.prepare(None,self.token,app.account_reason,app.checked_mint))['packets'][0]
        await r.mark_sending(self.token,[p['id']]);self.assertEqual(r.status(self.token)['state'],'pending');self.confirm(self.signed(p))
        await r.reconcile(None,self.token);await r.reconcile(None,self.token);self.assertEqual(r.status(self.token)['confirmed_accounts'],10)
    async def test_simulation_failure_never_persists_packet(self):
        self.sim_err={'InstructionError':[0,'Custom']}
        with self.assertRaises(HTTPException):await r.prepare(None,self.token,app.account_reason,app.checked_mint)
        self.assertFalse(r.status(self.token)['packets']);self.assertFalse(self.sent)
    async def test_invalid_selection_and_active_claim_reused(self):
        with self.assertRaises(HTTPException):r.create(self.job,['unknown'])
        self.assertEqual(r.create(self.job,[]),self.token)
    async def test_burn_checked_instruction_then_close(self):
        row=dict(self.rows[0],kind='balance',amount='1',checked_price='0.1');tx=r.unsigned([row],str(self.owner.pubkey()),str(self.block),100,None,0)
        ops=r.shape(tx.message)[3];self.assertEqual(ops[2][2][0],15);self.assertEqual(ops[3][2],bytes([9]))
        self.assertEqual(int.from_bytes(ops[2][2][1:9],'little'),1)
    async def test_changed_amount_is_excluded(self):
        snapshot=dict(self.rows[0]);self.rows[0]['amount']='2'
        good,excluded=await r.checked_rows(None,[snapshot],str(self.owner.pubkey()),app.account_reason,app.checked_mint)
        self.assertFalse(good);self.assertEqual(excluded,[snapshot['address']])
    async def test_whole_mint_half_dollar_excluded(self):
        row=dict(self.rows[0],kind='balance',amount='1',checked_price='500000')
        self.rows[0]=row
        good,excluded=await r.checked_rows(None,[row],str(self.owner.pubkey()),app.account_reason,app.checked_mint)
        self.assertFalse(good);self.assertTrue(excluded)
    async def test_invalid_wallet_signature_rejected(self):
        p=(await r.prepare(None,self.token,app.account_reason,app.checked_mint))['packets'][0]
        tx=VersionedTransaction.from_bytes(base64.b64decode(p['transaction']))
        with self.assertRaises(HTTPException):await r.submit(None,self.token,[{'id':p['id'],'transaction':r.encode(tx)}])
        self.assertFalse(self.sent)
    async def test_sponsor_send_requires_signed_transaction(self):
        self.wallet_balance=0;p=(await r.prepare(None,self.token,app.account_reason,app.checked_mint))['packets'][0]
        with self.assertRaises(HTTPException):await r.submit(None,self.token,[{'id':p['id'],'signature':str(self.owner.sign_message(b'x'))}])
        self.assertFalse(self.sent)
    async def test_expired_unknown_signature_rebuild_after_margin(self):
        p=(await r.prepare(None,self.token,app.account_reason,app.checked_mint))['packets'][0];tx=self.signed(p)
        await r.submit(None,self.token,[{'id':p['id'],'transaction':r.encode(tx)}]);await r.BROADCASTING[self.token];self.height=220
        await r.reconcile(None,self.token);self.assertEqual(r.status(self.token)['state'],'pending')
        self.height=233;await r.reconcile(None,self.token);self.assertNotEqual(r.status(self.token)['state'],'pending')
    async def test_api_ownership_and_cross_origin(self):
        import httpx,time
        self.job.update(created=time.monotonic(),task=None)
        app.JOBS['test-session']=self.job
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app),base_url='http://testhost') as client:
            response=await client.post('/api/recovery/challenge',json={'scan_token':'test-session'})
            self.assertEqual(response.status_code,200);message=response.json()['message']
            bad=await client.post('/api/recovery/create',json={'scan_token':'test-session','mints':[],'signature':base64.b64encode(bytes(64)).decode()})
            self.assertEqual(bad.status_code,401)
            proof=base64.b64encode(bytes(self.owner.sign_message(message.encode()))).decode()
            good=await client.post('/api/recovery/create',json={'scan_token':'test-session','mints':[],'signature':proof})
            self.assertEqual(good.status_code,200);self.assertEqual(good.json()['claim_token'],self.token)
            cross=await client.post('/api/recovery/challenge',json={'scan_token':'test-session'},headers={'Origin':'https://otherhost'})
            self.assertEqual(cross.status_code,403)
        app.JOBS.pop('test-session',None)
    async def test_sixty_accounts_are_all_prepared_without_four_packet_limit(self):
        self.rows=[dict(self.rows[0],address=str(Keypair().pubkey())) for _ in range(60)]
        with r.database() as c:c.execute('UPDATE claims SET rows=? WHERE token=?',(json.dumps(self.rows),self.token))
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint)
        self.assertEqual(len(d['packets']),6);self.assertEqual(sum(p['count'] for p in d['packets']),60)
    async def test_empty_and_burn_share_the_same_complete_approval_list(self):
        self.rows=[dict(row,kind='balance',amount='1',checked_price='0.1') if i>1 else row for i,row in enumerate(self.rows)]
        with r.database() as c:c.execute('UPDATE claims SET rows=? WHERE token=?',(json.dumps(self.rows),self.token))
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint)
        self.assertEqual(sum(p['count'] for p in d['packets']),11)
        self.assertEqual(d['packets'][0]['kind'],'empty');self.assertTrue(all(p['kind']=='balance' for p in d['packets'][1:]))
    async def test_new_blockhash_does_not_rebuild_or_change_quote(self):
        d=await r.prepare(None,self.token,app.account_reason,app.checked_mint);old=[p['id'] for p in d['packets']]
        self.block=Hash.new_unique();fresh=await r.refresh_blockhash(None,self.token)
        self.assertEqual(old,[p['id'] for p in fresh['packets']])
        self.assertTrue(all(str(VersionedTransaction.from_bytes(base64.b64decode(p['transaction'])).message.recent_blockhash)==str(self.block) for p in fresh['packets']))
    async def test_owner_trial_gate_blocks_other_wallets(self):
        old=os.environ.get('SOLBACK_WEB_TEST_WALLETS')
        os.environ['SOLBACK_WEB_TEST_WALLETS']=str(Keypair().pubkey())
        try:
            with self.assertRaises(HTTPException):r.create(self.job,[])
            with self.assertRaises(HTTPException):r.claim(self.token)
        finally:
            if old is None:os.environ.pop('SOLBACK_WEB_TEST_WALLETS',None)
            else:os.environ['SOLBACK_WEB_TEST_WALLETS']=old
    async def test_unsigned_sponsor_transfer_cannot_be_changed(self):
        self.wallet_balance=0;p=(await r.prepare(None,self.token,app.account_reason,app.checked_mint))['packets'][0]
        original=VersionedTransaction.from_bytes(base64.b64decode(p['transaction']));evil=r.unsigned(self.rows[:5],str(self.owner.pubkey()),str(self.block),0,self.sponsor.pubkey(),10000);evil=self.owner_signed(evil)
        with self.assertRaises(HTTPException):await r.submit(None,self.token,[{'id':p['id'],'transaction':r.encode(evil)}])
        self.assertFalse(self.sent)
if __name__=='__main__':unittest.main(verbosity=2)

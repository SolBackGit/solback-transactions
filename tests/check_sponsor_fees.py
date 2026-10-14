from pathlib import Path
exec(Path(__file__).with_name('check_sponsor_baseline.py').read_text().split('asyncio.run(main())')[0],globals())
from solders.instruction import Instruction,AccountMeta
from solders.message import MessageV0
from solders.address_lookup_table_account import AddressLookupTableAccount
from solders.compute_budget import set_compute_unit_limit,set_compute_unit_price
class FeeRPC:
    def __init__(self):self.balance=25000;self.fee=30000;self.null=False
    async def __call__(self,client,method,params):
        if method=='getBalance' and params[0]==str(owner.pubkey()):return {'value':self.balance}
        if method=='sendTransaction':
            tx=VersionedTransaction.from_bytes(base64.b64decode(params[0]));assert all(tx.verify_with_results())
            if any(str(k)==compact.PROGRAM for k in tx.message.account_keys):
                sig=str(tx.signatures[0]);rpc.sent[sig]={'transaction':[params[0],'base64'],'meta':{'err':None}};return sig
        if method=='getFeeForMessage':return {'value':None if self.null else self.fee}
        return await rpc(client,method,params)
f=FeeRPC();r.call=f
async def main_fees():
    for key in list(os.environ):
        if key.lower().endswith("_proxy"):os.environ.pop(key)
    rows=[make_row('balance')];new_claim('fee-test',rows)
    d=await r.prepare_progressive(None,'fee-test',None,None)
    assert d['packets'][0]['sponsored'] and d['packets'][0]['network']==30000
    p=(await r.refresh_progressive(None,'fee-test'))['packets'][0]
    tx=VersionedTransaction.from_bytes(base64.b64decode(p['transaction']));assert tx.signatures[0]==r.Signature.default()
    # Wallet changes compute budget and appends a known read-only Lighthouse assertion.
    keys,_=compact.resolved_keys(tx.message);rights=compact.shape(tx.message)[2];ix=[]
    for op in tx.message.instructions:
        program=str(keys[op.program_id_index])
        if program==r.COMPUTE_PROGRAM:continue
        ix.append(Instruction(Pubkey.from_string(program),bytes(op.data),[AccountMeta(keys[i],*rights[str(keys[i])]) for i in op.accounts]))
    guard=Instruction(Pubkey.from_string(r.GUARD),bytes([6,0,0]),[AccountMeta(owner.pubkey(),True,True)])
    tables_for_tx=[AddressLookupTableAccount(Pubkey.from_string(t['address']),[Pubkey.from_string(x) for x in t['addresses']]) for t in compact.message_tables(tx.message)]
    def changed(extra=guard,price=20000):
        return MessageV0.try_compile(sponsor.pubkey(),[set_compute_unit_limit(200000),set_compute_unit_price(price),*ix,extra],tables_for_tx,tx.message.recent_blockhash)
    msg=changed();r.validate_message(msg,tx.message,True)
    evil=r.transfer(r.TransferParams(from_pubkey=owner.pubkey(),to_pubkey=Keypair().pubkey(),lamports=1))
    for bad in [changed(evil),changed(price=1000000)]:
        try:r.validate_message(bad,tx.message,True)
        except ValueError:pass
        else:raise AssertionError('Malicious operation/high sponsor fee accepted')
    sigs=[r.Signature.default()]*msg.header.num_required_signatures
    idx=list(msg.account_keys).index(owner.pubkey());sigs[idx]=owner.sign_message(to_bytes_versioned(msg))
    owner_signed=VersionedTransaction.populate(msg,sigs)
    result=await r.submit(None,'fee-test',[{'id':p['id'],'transaction':r.encode(owner_signed)}])
    await r.BROADCASTING['fee-test']
    with r.database() as db:
        saved=db.execute('SELECT * FROM packets WHERE id=?',(p['id'],)).fetchone()
        assert saved['state']=='submitted',dict(saved)
        sent=VersionedTransaction.from_bytes(base64.b64decode(saved['signed']));assert all(sent.verify_with_results())
        db.execute('UPDATE packets SET state="expired" WHERE id=?',(p['id'],))
    # Fee below old threshold: enough balance must not trigger sponsorship.
    f.balance=15000;f.fee=10000
    d=await r.prepare_progressive(None,'fee-test',None,None);p=d['packets'][0];assert not p['sponsored']
    # Balance falls / fee rises before Approve: refresh switches safely to sponsor.
    f.balance=25000;f.fee=30000
    p=(await r.refresh_progressive(None,'fee-test'))['packets'][0];assert p['sponsored'] and not p['wallet_send']
    # Reverse switch to wallet-paid before Approve.
    f.balance=30000
    p=(await r.refresh_progressive(None,'fee-test'))['packets'][0];assert not p['sponsored'] and p['wallet_send']
    f.null=True
    try:await r.refresh_progressive(None,'fee-test')
    except HTTPException as e:assert e.status_code==503
    else:raise AssertionError('Unavailable fee quote accepted')
    print('PASS: actual 30000-lamport fee vs 25000 balance uses sponsor; 10000 fee vs 15000 balance uses wallet; refresh switches payer both ways; missing fee quote stops approval; bounded ComputeBudget and Lighthouse guards accepted; extra transfer and excessive priority rejected; owner signs first, sponsor co-signs, server verifies and sends once.')
asyncio.run(main_fees());tmp.cleanup()

const fs=require('fs');const vm=require('vm');const assert=require('assert');
const html=fs.readFileSync(__dirname+'/../static/index.html','utf8');
const start=html.indexOf("$('approve-recovery').onclick=async()=>{");
const end=html.indexOf('\n\n\n// SOLBACK_MOBILE_LAYOUT_V1',start);
const source=html.slice(start,end);
for(const block of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))if(block[1].trim())new vm.Script(block[1]);
async function scenario({sponsored=false,provider='phantom',cancel=false,sendLost=false,submitLost=false,changed=false}={}){
 const nodes={};const calls=[];const storage=new Map();let sends=0,signs=0,polls=0;
 const data={wallet:'owner',engine:'compact-pilot-1',approval_mode:'progressive',state:'active',packets:[{id:'p',transaction:'checked-partial-bytes',sponsored,wallet_send:true}]};
 const ctx={
  $:id=>nodes[id]??=( {disabled:false,hidden:false,reportValidity(){throw Error('unexpected invalid form')}} ),
  claimBusy:false,claimData:data,claimToken:'claim',approvalChoice:'progressive',claimPolling:0,
  walletProvider:{isPhantom:provider==='phantom',publicKey:{toString:()=>changed?'other':'owner'},
    async signAndSendTransaction(tx,options){sends++;assert.equal(tx.bytes,'checked-partial-bytes');assert.equal(options.preflightCommitment,'confirmed');if(cancel)throw Object.assign(Error('Cancelled'),{code:4001});if(sendLost)throw Error('Lost wallet response');return provider==='solflare'?'sig':{signature:'sig'}},
    async signTransaction(tx){signs++;if(cancel)throw Object.assign(Error('Cancelled'),{code:4001});return {serialize:()=> 'owner-signed-bytes'}},async signAllTransactions(){signs++;throw Error('Compact used grouped signing')}},
  clearTimeout(){},requireWallet(w){assert.equal(w,'owner')},async restoreSignedPackets(){},
  async api(path,body){calls.push(path);if(path==='refresh')return data;if(path==='sending')return {ok:true};if(path==='submit'){assert.deepEqual(JSON.parse(JSON.stringify(body.packets)),sponsored?[{id:'p',transaction:'owner-signed-bytes'}]:[{id:'p',signature:'sig'}]);if(submitLost)throw Error('Lost submit response');return {...data,state:'pending',packets:[]}}},
  showClaim(d){ctx.claimData=d},from64:x=>x,to64:x=>x,solanaWeb3:{VersionedTransaction:{deserialize:bytes=>({bytes,signatures:[new Uint8Array(64)]})}},
  async walletApprovalRequest(action){return await action()},sessionStorage:{setItem(k,v){storage.set(k,v)},removeItem(k){storage.delete(k)},getItem(k){return storage.get(k)||null}},
  pollClaim(){polls++},
 };
 vm.createContext(ctx);new vm.Script(source).runInContext(ctx);
 await nodes['approve-recovery'].onclick();
 assert.equal(signs,sponsored&&!changed?1:0);assert.equal(ctx.claimBusy,false);
 if(changed){assert.equal(sends,0);assert(!calls.includes('sending'));return}
 assert.equal(sends,sponsored?0:1);assert.equal(calls.includes('sending'),!sponsored);assert(polls>=1||cancel);
 if(cancel||sendLost&&!sponsored)assert(!calls.includes('submit'));
 else assert(calls.includes('submit'));
 if(submitLost)assert(storage.has('solback-signed-claim'));else assert(!storage.has('solback-signed-claim'));
}
(async()=>{
 for(const sponsored of [false,true])for(const provider of ['phantom','solflare'])await scenario({sponsored,provider});
 await scenario({sponsored:true,cancel:true});await scenario({sponsored:true,submitLost:true});await scenario({sendLost:true});await scenario({submitLost:true});await scenario({changed:true});
 console.log('PASS: owner-paid uses wallet sign-and-send; sponsored uses owner sign-only and server submit; cancellation, interrupted submission, changed wallet, one approval.');
})().catch(e=>{console.error(e);process.exit(1)});

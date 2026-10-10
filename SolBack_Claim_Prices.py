#!/usr/bin/env python3
"""SolBack: diagnostica prezzi ON-CHAIN, senza firme, burn o modifiche al bot.

VPS (dipendenze gia presenti: httpx, solders):
  sudo -u solback /opt/solback/venv/bin/python /opt/solback/SolBack_OnChain_Prices.py \
    --wallet 2aM72Y6G52ygPUuVk3b8R9MCfanYis7cDADhLExrvd4g
  ... --mint 7EcPLifMpSmZspdxbp5qBbm9SDBopHS1mNyZs5vDbonk --amount 0.632554
  ... --self-test

Configurazione: HELIUS_RPC_URL o SOLBACK_RPC_URL nell'ambiente, oppure --bot-file
(default /opt/solback/SolBack.py). Il file viene SOLO analizzato con ast, mai
importato/eseguito. Nessuna chiave privata viene letta. Il token bot non serve.

Copertura: Pump.fun; LaunchLab (curva constant-product); PumpSwap; Raydium
CPMM, CLMM, AMM v4; Meteora DLMM e DAMM v2. DAMM v1, Raydium StableSwap,
Meteora DBC e curve LaunchLab di altro tipo NON sono decodificate: UNKNOWN.
Pairing diretto SOL/USDC, nessuna ricerca di percorsi su token intermedi.
Prezzi token on-chain. Solo SOL/USD usa Coinbase, Kraken o Bitstamp.
API ufficiali DEX solo per gli indirizzi. Nessun prezzo token esterno,
filtro di volume, eta o minimo di liquidita.

Questo e un motore SPOT diagnostico, NON una quotazione di vendita eseguibile,
NON un autorizzatore di burn. I prezzi di pool sono manipolabili. Le riserve
nulle e i mercati disabilitati non forniscono un prezzo utilizzabile. DLMM e
CLMM richiedono un successivo quote engine per verificare lo scambio effettivo.
USDC=1 USD e una ipotesi esplicita. SOL/USD esterno con cache 60 secondi.
Una curva attiva o la prima pool valida fornisce una stima, non un consenso.
--full-search abilita la vecchia diagnostica estesa dei conflitti (>20%).
Una sola fonte produce SINGLE_SOURCE, mai un'autorizzazione automatica.

Scoperta pool: curva attiva, PDA canonico PumpSwap, poi API ufficiali
Raydium/Meteora usate SOLO per indirizzi (prima pagina, max 10 candidati).
Nessuna ricerca GPA nel percorso rapido. Dati pool sempre verificati on-chain. Cache SOLO di indirizzi separata
solback_price_discovery.db, positiva 30min, negativa 60s. Prezzi e saldi letti
nuovamente a ogni esecuzione. --refresh evita la cache. --max-mints non
tronca silenziosamente: indica i token non analizzati. Default tutti.
--max-pools default 250 per programma/mint; superamento segnalato come ricerca
incompleta. I limiti/errori RPC NON vengono interpretati come prezzo zero.

Nessun endpoint write RPC e consentito. Nessun worker/polling Telegram.
Riferimenti ufficiali, layout e hash sono inclusi a fondo file. Snapshot
layout acquisiti il 2026-10-09; aggiornamenti dei programmi vanno rivalidati.
"""
from __future__ import annotations
import argparse
import ast
import asyncio
import base64
import collections
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
from dataclasses import dataclass
from decimal import Decimal, localcontext, InvalidOperation
from pathlib import Path
from typing import Any
import httpx
from solders.pubkey import Pubkey

BUILD = 'SOLBACK-ONCHAIN-PRICES-4-INDEXED'
SOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
TOKEN = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN22 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
PUMP = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
LAUNCH = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
CPMM = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'
CLMM = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'
V4 = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
READ_METHODS = frozenset({'getMultipleAccounts','getAccountInfo','getProgramAccounts','getProgramAccountsV2','getTokenAccountsByOwner'})
D = Decimal

class PriceError(Exception):
    pass


def decimal_positive(value):
    try:
        v = D(str(value))
    except (InvalidOperation, ValueError):
        raise PriceError('invalid_decimal') from None
    if not v.is_finite() or v <= 0:
        raise PriceError('invalid_positive_value')
    return v


def b58(data: bytes) -> str:
    chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    n = int.from_bytes(data, 'big'); out = ''
    while n:
        n, r = divmod(n,58); out = chars[r]+out
    return '1'*(len(data)-len(data.lstrip(b'\0')))+out


def field(raw, offset, size, typ='u64'):
    if offset+size > len(raw):
        raise PriceError('short_account_layout')
    v = raw[offset:offset+size]
    if typ == 'pubkey': return str(Pubkey.from_bytes(v))
    if typ == 'bool':
        if v[0] not in (0,1): raise PriceError('invalid_bool')
        return bool(v[0])
    return int.from_bytes(v,'little',signed=typ.startswith('i'))


def parse_layout(raw, schema, *, min_length=None):
    if raw[:8].hex() != schema['discriminator']:
        raise PriceError('wrong_discriminator')
    required = schema['length'] if min_length is None else min_length
    if len(raw)<required: raise PriceError('short_account_layout')
    out = {}
    for name, (offset,size,typ) in schema['fields'].items():
        if offset+size <= len(raw): out[name]=field(raw,offset,size,typ)
        elif min_length is not None: out[name]='11111111111111111111111111111111' if typ=='pubkey' else 0
        else: raise PriceError('short_account_layout')
    return out


def cp_price(a,b,da,db):
    if a<=0 or b<=0: raise PriceError('nonpositive_reserves')
    with localcontext() as ctx:
        ctx.prec=70
        return D(b)/D(a)*D(10)**(da-db)


def sqrt_price(sqrt_x64,da,db):
    if sqrt_x64<=0: raise PriceError('nonpositive_sqrt_price')
    with localcontext() as ctx:
        ctx.prec=70
        return (D(sqrt_x64)/D(2**64))**2*D(10)**(da-db)


def bin_price(active,step,da,db):
    if step<=0 or step>10000 or abs(active)>1000000: raise PriceError('invalid_bin_parameters')
    with localcontext() as ctx:
        ctx.prec=70
        p=(D(1)+D(step)/D(10000))**active*D(10)**(da-db)
    if not p.is_finite() or p<=0: raise PriceError('invalid_bin_price')
    return p


def account_bytes(a, expected_owner=None):
    if a is None: raise PriceError('account_missing')
    if expected_owner and a.get('owner')!=expected_owner: raise PriceError('wrong_account_owner')
    if a.get('executable'): raise PriceError('unexpected_executable_account')
    try:
        if a['data'][1]!='base64': raise PriceError('unexpected_encoding')
        return base64.b64decode(a['data'][0],validate=True)
    except (KeyError,ValueError,IndexError):
        raise PriceError('invalid_account_data') from None


def mint_info(a):
    if not a or a.get('owner') not in (TOKEN,TOKEN22): raise PriceError('invalid_mint_owner')
    r=account_bytes(a)
    if len(r)<82 or r[45]!=1: raise PriceError('invalid_mint')
    if len(r)>82 and (len(r)<166 or r[165]!=1): raise PriceError('not_a_mint_account')
    decimals=r[44]
    if decimals>18: raise PriceError('unsupported_decimals')
    return decimals


def token_balance(a, mint):
    if not a or a.get('owner') not in (TOKEN,TOKEN22): raise PriceError('invalid_vault_owner')
    r=account_bytes(a)
    if len(r)<165 or r[:32]!=bytes(Pubkey.from_string(mint)): raise PriceError('vault_mint_mismatch')
    if r[108]!=1: raise PriceError('vault_not_initialized')
    if len(r)>165 and r[165]!=2: raise PriceError('not_a_token_account')
    return field(r,64,8)


class ReadRPC:
    def __init__(self,url,gap=0.25,concurrency=2,timeout=25):
        if not url.startswith('https://'): raise PriceError('rpc_requires_https')
        self.url=url; self.gap=gap; self.client=httpx.AsyncClient(timeout=timeout)
        self.sem=asyncio.Semaphore(concurrency); self.pace=asyncio.Lock()
        self.last=0.; self.counts=collections.Counter(); self.errors=collections.Counter(); self.serial=0
        self.max_slot=0; self.min_slot=0
    async def close(self): await self.client.aclose()
    async def call(self,method,params):
        if method not in READ_METHODS: raise PriceError('non_read_rpc_method_rejected')
        async with self.sem:
            for attempt in range(3):
                async with self.pace:
                    await asyncio.sleep(max(0.,self.last+self.gap-time.monotonic()))
                    self.last=time.monotonic(); self.serial+=1; req_id=self.serial
                self.counts[method]+=1
                try:
                    r=await self.client.post(self.url,json={'jsonrpc':'2.0','id':req_id,'method':method,'params':params})
                    if r.status_code in (429,502,503,504) and attempt<2:
                        self.errors['http_'+str(r.status_code)]+=1
                        delay=min(15.,max(1.,float(r.headers.get('retry-after','1'))))
                        await asyncio.sleep(delay*(attempt+1)); continue
                    r.raise_for_status(); p=r.json()
                    if p.get('id')!=req_id: raise PriceError('rpc_id_mismatch')
                    if 'error' in p:
                        code=p['error'].get('code'); self.errors['rpc_'+str(code)]+=1
                        if code in (-32603,-32002,-32003,-32005,-32016) and attempt<2:
                            await asyncio.sleep(attempt+1);continue
                        raise PriceError('rpc_error_'+str(code))
                    result=p['result']
                    if isinstance(result,dict) and 'context' in result:
                        slot=int(result['context']['slot'])
                        if slot<self.min_slot: raise PriceError('rpc_slot_older_than_snapshot')
                        self.max_slot=max(self.max_slot,slot)
                    return result
                except PriceError: raise
                except (httpx.HTTPError,ValueError,KeyError) as e:
                    self.errors[type(e).__name__]+=1
                    if attempt<2 and isinstance(e,(httpx.TimeoutException,httpx.TransportError)):
                        await asyncio.sleep(attempt+1); continue
                    raise PriceError('rpc_transport_'+type(e).__name__) from None
        raise PriceError('rpc_retry_exhausted')
    async def accounts(self,addresses):
        addresses=list(dict.fromkeys(addresses)); result={}
        for offset in range(0,len(addresses),100):
            batch=addresses[offset:offset+100]
            opt={'encoding':'base64','commitment':'confirmed'}
            if self.min_slot:opt['minContextSlot']=self.min_slot
            j=await self.call('getMultipleAccounts',[batch,opt])
            if len(j['value'])!=len(batch): raise PriceError('account_batch_length_mismatch')
            for k,v in zip(batch,j['value']):
                if v is not None:v=dict(v,_slot=j['context']['slot'])
                result[k]=v
        return result


class DiscoveryCache:
    def __init__(self,path):
        self.c=sqlite3.connect(str(path))
        if str(path)!=':memory:':os.chmod(path,0o600)
        self.c.execute('CREATE TABLE IF NOT EXISTS pools(k TEXT PRIMARY KEY,ts REAL NOT NULL,payload TEXT NOT NULL)')
        self.c.commit()
    def get(self,k):
        row=self.c.execute('SELECT ts,payload FROM pools WHERE k=?',(k,)).fetchone()
        if not row:return None
        obj=json.loads(row[1]); ttl=1800 if obj['addresses'] else min(300,int(obj.get('negative_ttl',60)))
        return obj if time.time()-row[0]<ttl else None
    def put(self,k,obj):
        self.c.execute('INSERT OR REPLACE INTO pools VALUES (?,?,?)',(k,time.time(),json.dumps(obj)))
        self.c.commit()
    def close(self):self.c.close()


@dataclass(frozen=True)
class PoolSpec:
    name: str
    program: str
    mint_a: int
    mint_b: int
    discriminator: str|None
    length: int


def specs():
    return [
      PoolSpec('PumpSwap',LAYOUTS['pumpswap_idl']['program'],43,75,LAYOUTS['pumpswap_idl']['discriminator'],243),
      PoolSpec('Raydium CPMM',CPMM,168,200,hashlib.sha256(b'account:PoolState').digest()[:8].hex(),637),
      PoolSpec('Raydium CLMM',CLMM,73,105,hashlib.sha256(b'account:PoolState').digest()[:8].hex(),1544),
      PoolSpec('Raydium AMM v4',V4,400,432,None,752),
      PoolSpec('Meteora DLMM',LAYOUTS['dlmm_idl']['program'],88,120,LAYOUTS['dlmm_idl']['discriminator'],904),
      PoolSpec('Meteora DAMM v2',LAYOUTS['damm_v2_idl']['program'],168,200,LAYOUTS['damm_v2_idl']['discriminator'],1112),
    ]


class OnChainPrices:
    full_search=False
    def __init__(self,rpc,cache,refresh=False,max_pools=250):
        self.rpc=rpc; self.cache=cache; self.refresh=refresh; self.max_pools=max_pools
        self.metadata={}; self.sol_usd=None; self.anchor=None
        self.account_cache={}; self.account_lock=asyncio.Lock(); self.reference_time=0.
        self.index_client=httpx.AsyncClient(timeout=2.5,headers={'User-Agent':'SolBack/4','Accept':'application/json'})
        self.index_sem=asyncio.Semaphore(4);self.index_pace=asyncio.Lock();self.index_last=0.
        self.index_counts=collections.Counter();self.index_errors=collections.Counter()
    async def accounts(self,addresses):
        # Per-valuation cache only: persistent cache contains no balances or prices.
        addresses=list(dict.fromkeys(addresses))
        async with self.account_lock:
            missing=[k for k in addresses if k not in self.account_cache]
            if missing:self.account_cache.update(await self.rpc.accounts(missing))
        return {k:self.account_cache[k] for k in addresses}
    async def decimals(self,mints):
        needed=[k for k in dict.fromkeys(mints) if k not in self.metadata]
        if needed:
            acc=await self.accounts(needed)
            for k in needed:
                try:self.metadata[k]=mint_info(acc[k])
                except PriceError:self.metadata[k]=None
        if any(self.metadata[k] is None for k in mints):raise PriceError('mint_metadata_unavailable')
        return [self.metadata[k] for k in mints]
    async def discover(self,spec,mint,other=None):
        key=f'{BUILD}:gpa-filtered:{spec.program}:{mint}:{other or "*"}'
        cached=None if self.refresh else self.cache.get(key)
        if cached:return cached['addresses'],{'source':'discovery_cache','complete':True,'discovered_slot':cached['slot']}
        rows={}; slots=[]
        for pos,opp in [(spec.mint_a,spec.mint_b),(spec.mint_b,spec.mint_a)]:
            filters=[{'memcmp':{'offset':pos,'bytes':mint}}]
            if other:filters.append({'memcmp':{'offset':opp,'bytes':other}})
            if spec.discriminator:filters.append({'memcmp':{'offset':0,'bytes':b58(bytes.fromhex(spec.discriminator))}})
            else:filters.append({'dataSize':spec.length})
            # Filtered standard GPA returns the matching set in one response.
            # V2 can scan many index pages even when few accounts match.
            opt={'encoding':'base64','commitment':'confirmed','filters':filters,
                 'withContext':True,'dataSlice':{'offset':0,'length':0}}
            if self.rpc.min_slot:opt['minContextSlot']=self.rpc.min_slot
            j=await self.rpc.call('getProgramAccounts',[spec.program,opt])
            if not isinstance(j,dict) or not isinstance(j.get('value'),list):
                raise PriceError('pool_discovery_missing_context')
            if not isinstance(j.get('context'),dict) or not isinstance(j['context'].get('slot'),int):
                raise PriceError('pool_discovery_missing_context')
            slots.append(j['context']['slot'])
            for x in j['value']:
                if x['account']['owner']!=spec.program:raise PriceError('discovery_owner_mismatch')
                rows[x['pubkey']]=None
            if len(rows)>self.max_pools:
                raise PriceError('too_many_pools_incomplete_search_'+str(len(rows)))
        addresses=sorted(rows)
        if len(addresses)>self.max_pools:raise PriceError('too_many_pools_incomplete_search_'+str(len(addresses)))
        self.cache.put(key,{'addresses':addresses,'slot':min(slots)})
        return addresses,{'source':'onchain_discovery','complete':True,'discovered_slot':min(slots)}

    async def canonical_pumpswap(self,mint):
        spec=specs()[0];creator=Pubkey.find_program_address([b'pool-authority',bytes(Pubkey.from_string(mint))],Pubkey.from_string(PUMP))[0]
        ids=[str(Pubkey.find_program_address([b'pool',b'\0\0',bytes(creator),bytes(Pubkey.from_string(mint)),bytes(Pubkey.from_string(q))],Pubkey.from_string(spec.program))[0]) for q in (SOL,USDC)]
        found=[];notes=[]
        try:
            accounts=await self.accounts(ids)
            for k in ids:
                if accounts[k] is None:continue
                try:
                    p=await self.parse_pool(spec,k,accounts[k]);p['discovery']={'source':'canonical_PDA','complete':False};found.append(p)
                except PriceError as ex:notes.append({'source':spec.name,'pool':k,'reason':str(ex)})
        except PriceError as ex:notes.append({'source':spec.name,'reason':str(ex)})
        return found,notes

    async def parse_pool(self,spec,address,account):
        raw=account_bytes(account,spec.program)
        if len(raw)<spec.length:raise PriceError('short_pool_layout')
        if spec.discriminator and raw[:8].hex()!=spec.discriminator:raise PriceError('wrong_pool_discriminator')
        a=field(raw,spec.mint_a,32,'pubkey');b=field(raw,spec.mint_b,32,'pubkey')
        da,db=await self.decimals([a,b])
        info={'source':spec.name,'pool':address,'mint_a':a,'mint_b':b,'slot':account['_slot'],'kind':'spot','swap_quote':False}
        va=vb=None; reserve_a=reserve_b=None; liquidity=None
        if spec.name=='PumpSwap':
            f=parse_layout(raw,LAYOUTS['pumpswap_idl'],min_length=243)
            va,vb=f['pool_base_token_account'],f['pool_quote_token_account']
        elif spec.name=='Raydium CPMM':
            va,vb=field(raw,72,32,'pubkey'),field(raw,104,32,'pubkey')
            if field(raw,329,1)&4:raise PriceError('pool_swaps_disabled')
            if field(raw,373,8)>time.time():raise PriceError('pool_not_open')
            if field(raw,331,1)!=da or field(raw,332,1)!=db:raise PriceError('pool_decimals_mismatch')
        elif spec.name=='Raydium AMM v4':
            if field(raw,0,8) not in (1,6):raise PriceError('pool_swaps_disabled')
            if field(raw,224,8)>time.time():raise PriceError('pool_not_open')
            if field(raw,32,8)!=da or field(raw,40,8)!=db:raise PriceError('pool_decimals_mismatch')
            va,vb=field(raw,336,32,'pubkey'),field(raw,368,32,'pubkey')
        elif spec.name=='Raydium CLMM':
            if field(raw,389,1)&16:raise PriceError('pool_swaps_disabled')
            if field(raw,1080,8)>time.time():raise PriceError('pool_not_open')
            if field(raw,233,1)!=da or field(raw,234,1)!=db:raise PriceError('pool_decimals_mismatch')
            liquidity=field(raw,237,16,'u128')
            if liquidity<=0:raise PriceError('no_active_liquidity')
            info['price_b_per_a']=sqrt_price(field(raw,253,16,'u128'),da,db)
            va,vb=field(raw,137,32,'pubkey'),field(raw,169,32,'pubkey')
        elif spec.name=='Meteora DLMM':
            f=parse_layout(raw,LAYOUTS['dlmm_idl'])
            if f['status']!=0:raise PriceError('pool_swaps_disabled')
            point=f['activation_point'];mode=f['activation_type']
            if mode not in (0,1):raise PriceError('unknown_activation_type')
            if point>(account['_slot'] if mode==0 else time.time()):raise PriceError('pool_not_open')
            info['price_b_per_a']=bin_price(f['active_id'],f['bin_step'],da,db)
            info['active_bin']=f['active_id'];info['bin_step']=f['bin_step']
            va,vb=f['reserve_x'],f['reserve_y']
        elif spec.name=='Meteora DAMM v2':
            f=parse_layout(raw,LAYOUTS['damm_v2_idl'])
            if f['pool_status']!=0:raise PriceError('pool_swaps_disabled')
            if f['activation_type'] not in (0,1):raise PriceError('unknown_activation_type')
            if f['activation_point']>(account['_slot'] if f['activation_type']==0 else time.time()):raise PriceError('pool_not_open')
            if f['liquidity']<=0:raise PriceError('no_active_liquidity')
            info['price_b_per_a']=sqrt_price(f['sqrt_price'],da,db)
            va,vb=f['token_a_vault'],f['token_b_vault']
            if f['layout_version'] not in (0,1):raise PriceError('unsupported_damm_layout_version')
        else:raise PriceError('unsupported_pool_type')
        vaults=await self.accounts([va,vb]);reserve_a=token_balance(vaults[va],a);reserve_b=token_balance(vaults[vb],b)
        info['slot_min']=min(account['_slot'],vaults[va]['_slot'],vaults[vb]['_slot'])
        if spec.name=='PumpSwap':reserve_b+=f['virtual_quote_reserves']
        elif spec.name=='Raydium CPMM':
            reserve_a-=field(raw,341,8)+field(raw,357,8)+field(raw,397,8)
            reserve_b-=field(raw,349,8)+field(raw,365,8)+field(raw,405,8)
        elif spec.name=='Raydium AMM v4':
            reserve_a-=field(raw,192,8);reserve_b-=field(raw,200,8)
        # Zero vault balances cannot establish a usable trading market; no min-USD filter.
        if reserve_a<=0 or reserve_b<=0:raise PriceError('nonpositive_reserves')
        if 'price_b_per_a' not in info:info['price_b_per_a']=cp_price(reserve_a,reserve_b,da,db)
        info['reserve_a_ui']=D(reserve_a)/D(10)**da;info['reserve_b_ui']=D(reserve_b)/D(10)**db
        return info

    async def markets(self,mint,only=None,other=None):
        findings=[];notes=[];complete=True
        chosen=[s for s in specs() if only is None or s.name in only]
        async def one(s):
            found=[];errors=[];ok=True
            try:
                ids,discovery=await self.discover(s,mint,other)
                if not ids:return found,[{'source':s.name,'reason':'no_matching_pool','discovery':discovery}],True
                acc=await self.accounts(ids)
                # Prefetch all mint/vault accounts in batches of <=100, avoiding one
                # RPC for each pool and repeated SOL/USDC metadata requests.
                dependent=[]
                for k in ids:
                    try:
                        raw=account_bytes(acc[k],s.program)
                        dependent.extend(field(raw,o,32,'pubkey') for o in (s.mint_a,s.mint_b))
                        vault_offsets={'PumpSwap':(139,171),'Raydium CPMM':(72,104),
                          'Raydium CLMM':(137,169),'Raydium AMM v4':(336,368),
                          'Meteora DLMM':(152,184),'Meteora DAMM v2':(232,264)}[s.name]
                        dependent.extend(field(raw,o,32,'pubkey') for o in vault_offsets)
                    except PriceError:pass
                if dependent:await self.accounts(dependent)
                for k in ids:
                    try:
                        p=await self.parse_pool(s,k,acc[k])
                        if mint not in (p['mint_a'],p['mint_b']):raise PriceError('pool_mint_mismatch')
                        p['discovery']=discovery;found.append(p)
                    except PriceError as e:
                        reason=str(e);errors.append({'source':s.name,'pool':k,'reason':reason})
                        if reason.startswith(('rpc_','short_','wrong_','unsupported_','unknown_','invalid_','vault_','mint_')):ok=False
            except PriceError as e:errors.append({'source':s.name,'reason':str(e)});ok=False
            return found,errors,ok
        for found,errors,ok in await asyncio.gather(*(one(s) for s in chosen)):
            findings.extend(found);notes.extend(errors);complete=complete and ok
        return findings,notes,complete

    async def curves(self,mint):
        curves=[];notes=[];complete=True
        addresses={}
        pump=str(Pubkey.find_program_address([b'bonding-curve',bytes(Pubkey.from_string(mint))],Pubkey.from_string(PUMP))[0]);addresses[pump]=('Pump.fun',PUMP)
        for quote in (SOL,USDC):
            key=str(Pubkey.find_program_address([b'pool',bytes(Pubkey.from_string(mint)),bytes(Pubkey.from_string(quote))],Pubkey.from_string(LAUNCH))[0]);addresses[key]=('LaunchLab',LAUNCH)
        try:accounts=await self.accounts(addresses)
        except PriceError as e:return [],[{'source':'curves','reason':str(e)}],False
        for k,(name,program) in addresses.items():
            a=accounts[k]
            if a is None:continue
            try:
                raw=account_bytes(a,program)
                if name=='Pump.fun':
                    f=parse_layout(raw,LAYOUTS['pump_idl'],min_length=49)
                    if f['complete']:notes.append({'source':name,'pool':k,'reason':'curve_completed'});continue
                    quote=f.get('quote_mint')
                    if quote=='11111111111111111111111111111111':quote=SOL
                    if quote not in (SOL,USDC):raise PriceError('unsupported_curve_quote_asset')
                    da,db=await self.decimals([mint,quote])
                    ra,rb=f['virtual_token_reserves'],f['virtual_quote_reserves']
                    real_quote=f['real_quote_reserves']
                else:
                    if raw[:8]!=hashlib.sha256(b'account:PoolState').digest()[:8]:raise PriceError('wrong_launchlab_discriminator')
                    if len(raw)<269:raise PriceError('short_launchlab_layout')
                    status=field(raw,17,1)
                    if status!=0:notes.append({'source':name,'pool':k,'reason':'curve_completed_or_migrating'});continue
                    actual=field(raw,205,32,'pubkey');quote=field(raw,237,32,'pubkey')
                    if actual!=mint or quote not in (SOL,USDC):raise PriceError('curve_mint_mismatch')
                    da,db=await self.decimals([mint,quote])
                    if field(raw,18,1)!=da or field(raw,19,1)!=db:raise PriceError('curve_decimals_mismatch')
                    config=field(raw,141,32,'pubkey');conf=(await self.accounts([config]))[config];c=account_bytes(conf,LAUNCH)
                    if len(c)<16+1:raise PriceError('short_curve_config')
                    if c[:8]!=hashlib.sha256(b'account:GlobalConfig').digest()[:8]:raise PriceError('wrong_curve_config_discriminator')
                    if c[16]!=0:raise PriceError('unsupported_launchlab_curve_type_'+str(c[16]))
                    va,vb,real_a,real_b=[field(raw,off,8) for off in (37,45,53,61)]
                    ra,rb=va-real_a,vb+real_b;real_quote=real_b
                p=cp_price(ra,rb,da,db)
                curves.append({'source':name,'pool':k,'mint_a':mint,'mint_b':quote,'price_b_per_a':p,'reserve_b_ui':D(real_quote)/D(10)**db,'slot':a['_slot'],'kind':'curve_spot','swap_quote':False})
            except PriceError as e:notes.append({'source':name,'pool':k,'reason':str(e)});complete=False
        return curves,notes,complete

    async def sol_reference(self,override=None):
        self.reference_time=time.time()
        if override is not None:
            self.sol_usd=decimal_positive(override);self.anchor={'source':'manual_override','sol_usd':self.sol_usd,'usdc_usd_assumption':'1','confidence':'MANUAL'};return
        markets,notes,complete=await self.markets(SOL,only={'Raydium CPMM','Raydium CLMM'},other=USDC)
        anchors=[]
        for p in markets:
            if p['mint_a']==SOL and p['mint_b']==USDC:price=p['price_b_per_a'];depth=p['reserve_b_ui']
            elif p['mint_b']==SOL and p['mint_a']==USDC:price=D(1)/p['price_b_per_a'];depth=p['reserve_a_ui']
            else:continue
            if price.is_finite() and price>0:anchors.append({'price':price,'usdc_vault':depth,'pool':p['pool'],'source':p['source'],'slot':p['slot']})
        anchors.sort(key=lambda x:x['usdc_vault'],reverse=True)
        if not anchors:
            self.anchor={'source':'onchain_SOL_USDC','confidence':'UNAVAILABLE','notes':notes};return
        # Depth selects the reference; no minimum liquidity gates individual tokens.
        chosen=anchors[0];self.sol_usd=chosen['price']
        peers=anchors[:3];spread=max(x['price'] for x in peers)/min(x['price'] for x in peers)-1
        confidence='CONFLICT' if spread>D('.2') else ('MULTI_SOURCE' if len(peers)>1 else 'SINGLE_SOURCE')
        self.anchor={'source':'onchain_SOL_USDC','sol_usd':self.sol_usd,'usdc_usd_assumption':'1','confidence':confidence,'complete':complete,'reference_pool':chosen['pool'],'peers':peers,'notes':notes}
        if confidence=='CONFLICT':self.sol_usd=None

    async def price_full(self,mint,*,curve_only=False):
        Pubkey.from_string(mint)
        self.account_cache.clear()
        self.metadata.clear()
        if self.anchor and self.anchor.get('source')=='onchain_SOL_USDC' and time.time()-self.reference_time>120:
            await self.sol_reference()
        if mint==USDC:return {'mint':mint,'status':'USDC_REFERENCE','price_usd':D(1),'markets':[],'notes':[],'search_complete':True}
        if mint==SOL:return {'mint':mint,'status':'REFERENCE' if self.sol_usd else 'UNKNOWN','price_usd':self.sol_usd,'markets':[],'notes':[],'search_complete':self.sol_usd is not None}
        curve,cnotes,curve_complete=await self.curves(mint)
        active_curve=bool(curve) and curve_complete
        use_curve=active_curve and not self.full_search
        canonical,canonical_notes=([],[]) if curve_only or use_curve else await self.canonical_pumpswap(mint)
        if use_curve:
            markets,notes,market_complete=[],[{'reason':'active_curve_used_dex_search_not_performed'}],False
        elif curve_only:markets,notes,market_complete=[],[{'reason':'dex_search_skipped_explicitly'}],False
        else:markets,notes,market_complete=await self.markets(mint)
        seen={p['pool'] for p in markets}
        markets+= [p for p in canonical if p['pool'] not in seen]
        notes+=canonical_notes
        valued=[];unknown=[]
        for p in curve+markets:
            if p['mint_a']==mint:quote=p['mint_b'];qprice=p['price_b_per_a']
            else:quote=p['mint_a'];qprice=D(1)/p['price_b_per_a']
            scale=D(1) if quote==USDC else self.sol_usd if quote==SOL else None
            if scale is None:
                unknown.append({'source':p['source'],'pool':p['pool'],'reason':'quote_asset_not_valued','quote':quote});continue
            val=qprice*scale
            if not val.is_finite() or val<=0:unknown.append({'pool':p['pool'],'reason':'invalid_price'});continue
            valued.append(dict(p,quote=quote,price_quote=qprice,price_usd=val))
        all_notes=cnotes+notes+unknown;complete=curve_complete and market_complete and not unknown
        if not valued:return {'mint':mint,'status':'UNKNOWN','price_usd':None,'markets':[],'notes':all_notes,'search_complete':complete}
        prices=[p['price_usd'] for p in valued];spread=max(prices)/min(prices)-1
        status='CONFLICT' if spread>D('.2') else 'MULTI_SOURCE' if len(valued)>1 else 'SINGLE_SOURCE'
        # Max supported observation avoids making a low price from one pool the default.
        return {'mint':mint,'status':status,'price_usd':max(prices),'spread':spread,'markets':valued,'notes':all_notes,'search_complete':complete,'scope':'active_launchpad_curve_only' if use_curve else 'supported_programs_SOL_USDC_only','executable_quote':False}

    def curve_ids(self,mint):
        m=bytes(Pubkey.from_string(mint))
        return [str(Pubkey.find_program_address([b'bonding-curve',m],Pubkey.from_string(PUMP))[0])]+[str(Pubkey.find_program_address([b'pool',m,bytes(Pubkey.from_string(q))],Pubkey.from_string(LAUNCH))[0]) for q in (SOL,USDC)]

    def pump_pool_ids(self,mint):
        m=bytes(Pubkey.from_string(mint));creator=Pubkey.find_program_address([b'pool-authority',m],Pubkey.from_string(PUMP))[0]
        return [str(Pubkey.find_program_address([b'pool',b'\0\0',bytes(creator),m,bytes(Pubkey.from_string(q))],Pubkey.from_string(specs()[0].program))[0]) for q in (SOL,USDC)]

    async def prefetch_wallet(self,mints):
        candidates=[m for m in mints if m not in PROTECTED]
        ids=[SOL,USDC]+candidates
        warm=[]
        by_name={x.name:x for x in specs()}
        for m in candidates:
            ids+=self.curve_ids(m)+self.pump_pool_ids(m)
            known=None if self.refresh else self.cache.get(f'{BUILD}:first:{m}')
            if known and known.get('program_name') in by_name:
                for k in known.get('addresses',[]):
                    ids.append(k);warm.append((by_name[known['program_name']],k))
        data=await self.accounts(ids)
        configs=[];deps=[]
        for m in candidates:
            for k in self.curve_ids(m)[1:]:
                a=data[k]
                if a and a.get('owner')==LAUNCH:
                    try:configs.append(field(account_bytes(a,LAUNCH),141,32,'pubkey'))
                    except PriceError:pass
            for k in self.pump_pool_ids(m):
                a=data[k]
                if a and a.get('owner')==specs()[0].program:
                    try:
                        raw=account_bytes(a,specs()[0].program)
                        deps.extend(field(raw,o,32,'pubkey') for o in (43,75,139,171))
                    except PriceError:pass
        for spec,k in warm:
            try:
                raw=account_bytes(data[k],spec.program)
                offsets={'Raydium AMM v4':(336,368),'Raydium CPMM':(72,104),'Raydium CLMM':(137,169),'Meteora DLMM':(152,184),'Meteora DAMM v2':(232,264)}[spec.name]
                deps.extend(field(raw,o,32,'pubkey') for o in (spec.mint_a,spec.mint_b)+offsets)
            except (PriceError,KeyError):pass
        if configs or deps:await self.accounts(configs+deps)
        await self.decimals([SOL,USDC])

    async def indexed_candidates(self,mint,name):
        key=f'{BUILD}:index:{name}:{mint}'
        cached=None if self.refresh else self.cache.get(key)
        if cached is not None:return cached.get('candidates',[]),[],True
        by_program={s.program:s for s in specs()}
        try:
            if name=='Raydium':
                url='https://api-v3.raydium.io/pools/info/mint'
                params={'mint1':mint,'poolType':'all','poolSortField':'liquidity','sortType':'desc','pageSize':10,'page':1}
            else:
                host='dlmm' if name=='Meteora DLMM' else 'damm-v2'
                url=f'https://{host}.datapi.meteora.ag/pools'
                params={'query':mint,'page_size':10,'page':1}
            async with self.index_sem:
                async with self.index_pace:
                    delay=.12-(time.monotonic()-self.index_last)
                    if delay>0:await asyncio.sleep(delay)
                    self.index_last=time.monotonic()
                self.index_counts[name]+=1
                response=await asyncio.wait_for(self.index_client.get(url,params=params),timeout=3)
                response.raise_for_status();j=response.json()
            if name=='Raydium':
                if j.get('success') is not True or not isinstance(j.get('data'),dict):raise PriceError('invalid_index_response')
                rows=j['data'].get('data')
            else:rows=j.get('data')
            if not isinstance(rows,list):raise PriceError('invalid_index_response')
            candidates=[]
            for row in rows[:10]:
                if not isinstance(row,dict):continue
                if name=='Raydium':
                    spec=by_program.get(row.get('programId'));address=row.get('id')
                    if spec is None or not spec.name.startswith('Raydium'):continue
                else:
                    spec=next(s for s in specs() if s.name==name)
                    address=row.get('pool_address') or row.get('address')
                try:Pubkey.from_string(address)
                except (ValueError,TypeError):continue
                if not any(c['address']==address for c in candidates):candidates.append({'address':address,'program_name':spec.name})
            # Cache only identifiers. Never trust or store API prices/reserves.
            if not rows or candidates:
                self.cache.put(key,{'addresses':[c['address'] for c in candidates],'candidates':candidates,'slot':0,'negative_ttl':300})
            elif rows:
                return [],[{'source':name,'reason':'no_supported_identifiers_in_first_index_page'}],False
            return candidates,[],True
        except (httpx.HTTPError,ValueError,KeyError,PriceError,TimeoutError) as e:
            self.index_errors[name]+=1
            return [],[{'source':name,'reason':str(e) if isinstance(e,PriceError) else 'index_'+type(e).__name__}],False

    async def first_market(self,mint,names):
        notes=[];by_name={s.name:s for s in specs()}
        known=None if self.refresh else self.cache.get(f'{BUILD}:first:{mint}')
        if known and known.get('program_name') in names and known.get('addresses'):
            name=known['program_name'];k=known['addresses'][0]
            try:
                a=(await self.accounts([k]))[k];p=await self.parse_pool(by_name[name],k,a)
                if mint not in (p['mint_a'],p['mint_b']) or not ({p['mint_a'],p['mint_b']} & {SOL,USDC}):raise PriceError('cached_pool_mint_mismatch')
                p['discovery']={'source':'validated_pool_cache','complete':False};return [p],notes
            except PriceError as e:notes.append({'source':name,'reason':str(e)})
        indexes=['Raydium']
        if 'Meteora DLMM' in names:indexes.append('Meteora DLMM')
        if 'Meteora DAMM v2' in names:indexes.append('Meteora DAMM v2')
        for index in indexes:
            candidates,n,ok=await self.indexed_candidates(mint,index);notes+=n
            candidates=[c for c in candidates if c['program_name'] in names]
            if not candidates:continue
            try:
                acc=await self.accounts([c['address'] for c in candidates]);deps=[];valid=[]
                for c in candidates:
                    k=c['address'];spec=by_name[c['program_name']]
                    try:
                        raw=account_bytes(acc[k],spec.program)
                        if len(raw)<spec.length:raise PriceError('short_pool_layout')
                        if spec.discriminator and raw[:8].hex()!=spec.discriminator:raise PriceError('wrong_pool_discriminator')
                        a,b=[field(raw,o,32,'pubkey') for o in (spec.mint_a,spec.mint_b)]
                        if mint not in (a,b) or (b if a==mint else a) not in (SOL,USDC):continue
                        offsets={'Raydium AMM v4':(336,368),'Raydium CPMM':(72,104),'Raydium CLMM':(137,169),'Meteora DLMM':(152,184),'Meteora DAMM v2':(232,264)}[spec.name]
                        deps.extend([a,b]+[field(raw,o,32,'pubkey') for o in offsets]);valid.append(c)
                    except PriceError as e:notes.append({'source':spec.name,'pool':k,'reason':str(e)})
                if deps:await self.accounts(deps)
                for c in valid:
                    k=c['address'];spec=by_name[c['program_name']]
                    try:
                        p=await self.parse_pool(spec,k,acc[k])
                        p['discovery']={'source':'official_DEX_index_verified_onchain','complete':False}
                        self.cache.put(f'{BUILD}:first:{mint}',{'addresses':[k],'slot':p['slot'],'program_name':spec.name})
                        return [p],notes
                    except PriceError as e:notes.append({'source':spec.name,'pool':k,'reason':str(e)})
            except PriceError as e:notes.append({'source':index,'reason':str(e)})
        return [],notes

    async def price(self,mint,*,curve_only=False):
        if self.full_search:return await self.price_full(mint,curve_only=curve_only)
        Pubkey.from_string(mint)
        if mint in PROTECTED:
            return {'mint':mint,'status':'PROTECTED','price_usd':None,'markets':[],'notes':[],'search_complete':False,'scope':'protected_asset_skipped'}
        # This diagnostic cannot identify every NFT or LP: never authorizes burns.
        try:
            a=(await self.accounts([mint]))[mint];raw=account_bytes(a)
            if mint_info(a)==0 and field(raw,36,8)<=1:
                return {'mint':mint,'status':'PROTECTED','price_usd':None,'markets':[],'notes':[],'search_complete':False,'scope':'nft_like_asset_skipped'}
        except PriceError as ex:
            return {'mint':mint,'status':'UNKNOWN','price_usd':None,'markets':[],'notes':[{'reason':str(ex)}],'search_complete':False}
        curve,notes,ok=await self.curves(mint)
        if curve and ok:chosen=curve[:1];scope='active_launchpad_curve_only'
        elif curve_only:chosen=[];scope='curves_only'
        else:
            pump_a=self.account_cache.get(self.curve_ids(mint)[0])
            is_pump=bool(pump_a and pump_a.get('owner')==PUMP)
            canonical,cn=await self.canonical_pumpswap(mint);notes+=cn
            if canonical:chosen=canonical[:1];scope='canonical_pumpswap_pool'
            else:
                names=['Raydium AMM v4'] if is_pump else ['Raydium AMM v4','Raydium CPMM','Raydium CLMM','Meteora DLMM','Meteora DAMM v2']
                chosen,n=await self.first_market(mint,names);notes+=n;scope='pump_legacy_raydium' if is_pump else 'first_supported_direct_pool'
        valued=[]
        for p in chosen:
            q=p['mint_b'] if p['mint_a']==mint else p['mint_a']
            price=p['price_b_per_a'] if p['mint_a']==mint else D(1)/p['price_b_per_a']
            scale=D(1) if q==USDC else self.sol_usd if q==SOL else None
            if scale is None:notes.append({'reason':'quote_asset_not_valued'});continue
            valued.append(dict(p,price_usd=price*scale))
        return {'mint':mint,'status':'SINGLE_SOURCE' if valued else 'UNKNOWN','price_usd':valued[0]['price_usd'] if valued else None,'markets':valued,'notes':notes,'search_complete':False,'scope':scope,'executable_quote':False}


def json_safe(x):
    if isinstance(x,D):return str(x)
    if isinstance(x,dict):return {k:json_safe(v) for k,v in x.items()}
    if isinstance(x,(list,tuple)):return [json_safe(v) for v in x]
    return x


PROTECTED=frozenset({SOL,USDC,'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB','CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH'})

async def external_sol_reference(path,override=None):
    if override is not None:return {'source':'manual_override','sol_usd':decimal_positive(override),'confidence':'MANUAL','ts':time.time(),'http_requests':0}
    path=Path(path);cached=None
    try:
        cached=json.loads(path.read_text());age=time.time()-float(cached['ts'])
        if 0<=age<60:
            cached['sol_usd']=decimal_positive(cached['sol_usd']);cached['http_requests']=0;cached['cached']=True;return cached
    except (OSError,ValueError,KeyError,PriceError,InvalidOperation):pass
    failures=[]
    sources=[('Coinbase','https://api.coinbase.com/v2/prices/SOL-USD/spot'),('Kraken','https://api.kraken.com/0/public/Ticker?pair=SOLUSD'),('Bitstamp','https://www.bitstamp.net/api/v2/ticker/solusd/')]
    async with httpx.AsyncClient(timeout=3,headers={'User-Agent':'SolBack/3'}) as client:
        for n,(name,url) in enumerate(sources,1):
            try:
                r=await client.get(url);r.raise_for_status();j=r.json()
                if name=='Coinbase':
                    if j['data']['currency']!='USD' or j['data'].get('base','SOL')!='SOL':raise ValueError('pair')
                    value=j['data']['amount']
                elif name=='Kraken':
                    if j.get('error') or len(j['result'])!=1:raise ValueError('kraken')
                    value=next(iter(j['result'].values()))['c'][0]
                else:
                    if abs(time.time()-float(j['timestamp']))>120:raise ValueError('stale')
                    value=j['last']
                result={'source':name,'sol_usd':decimal_positive(value),'ts':time.time(),'confidence':'EXTERNAL','http_requests':n,'failures':failures}
                try:
                    temp=path.with_name(path.name+'.'+str(os.getpid())+'.tmp');temp.write_text(json.dumps(json_safe(result)));os.chmod(temp,0o600);os.replace(temp,path)
                except OSError:pass
                return result
            except (httpx.HTTPError,ValueError,KeyError,PriceError,InvalidOperation) as e:failures.append({'source':name,'error':type(e).__name__})
    # Expired cached quotes are never used for valuation.
    return {'source':'external_SOL_USD','sol_usd':None,'confidence':'UNAVAILABLE','http_requests':3,'failures':failures}






def holding_result(row,p,threshold):
    result=dict(row);result['tokens']=D(row['raw_amount'])/D(10)**row['decimals'];result['valuation']=p
    price=p.get('price_usd')
    result['estimated_usd']=None if price is None else result['tokens']*price
    # Diagnostic comparison only: unknown/conflict/incomplete never indicates burn eligibility.
    result['below_threshold']=None if price is None or p['status']=='CONFLICT' else result['estimated_usd']<=threshold
    result['burn_authorized']=False
    return result


def apply_mint_threshold(holdings,threshold):
    totals=collections.defaultdict(lambda:D(0));unknown=set()
    for h in holdings:
        if h['estimated_usd'] is None or h['valuation']['status'] in ('UNKNOWN','CONFLICT','PROTECTED'):unknown.add(h['mint'])
        else:totals[h['mint']]+=h['estimated_usd']
    for h in holdings:
        h['mint_total_estimated_usd']=None if h['mint'] in unknown else totals[h['mint']]
        h['below_threshold']=None if h['mint'] in unknown else totals[h['mint']]<=threshold
        h['burn_authorized']=False
    return {h['mint'] for h in holdings if h['below_threshold'] is True}









# Official program sources (MIT/open documentation). Derived account layouts,
# not external price feeds. Snapshots validated against mainnet on 2026-10-09.
SOURCE_URLS = {
    'raydium_pool_index':'https://api-v3.raydium.io/docs/',
    'meteora_dlmm_index':'https://dlmm.datapi.meteora.ag/swagger-ui/',
    'meteora_damm_v2_index':'https://damm-v2.datapi.meteora.ag/swagger-ui/',
    'coinbase_spot':'https://docs.cdp.coinbase.com/coinbase-app/track-apis/prices',
    'pump_idl':'https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json',
    'pumpswap_idl':'https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump_amm.json',
    'pump_docs':'https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_PROGRAM_README.md',
    'pumpswap_docs':'https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_SWAP_README.md',
    'ray_layouts':'https://github.com/raydium-io/raydium-sdk-V2/tree/master/src/raydium',
    'launch_curve':'https://github.com/raydium-io/raydium-sdk-V2/blob/master/src/raydium/launchpad/curve/constantProductCurve.ts',
    'dlmm_idl':'https://github.com/MeteoraAg/dlmm-sdk/blob/main/ts-client/src/dlmm/idl/idl.json',
    'damm_v2_idl':'https://github.com/MeteoraAg/damm-v2-sdk/blob/main/src/idl/cp_amm.json',
    'rpc_v2':'https://www.helius.dev/docs/api-reference/rpc/http/getprogramaccountsv2',
    'credits':'https://www.helius.dev/docs/billing/credits',
}

LAYOUTS = {"pump_idl":{"program":"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P","discriminator":"17b7f83760d8ac60","length":166,"fields":{"virtual_token_reserves":[8,8,"u64"],"virtual_quote_reserves":[16,8,"u64"],"real_token_reserves":[24,8,"u64"],"real_quote_reserves":[32,8,"u64"],"token_total_supply":[40,8,"u64"],"complete":[48,1,"bool"],"creator":[49,32,"pubkey"],"is_mayhem_mode":[81,1,"bool"],"is_cashback_coin":[82,1,"bool"],"quote_mint":[83,32,"pubkey"],"creator_fee_bps":[115,8,"u64"],"can_edit_creator_fee":[123,1,"bool"],"is_holder_reward":[124,1,"bool"],"creator_fee":[125,8,"u64"],"protocol_fees":[133,8,"u64"],"depth":[141,1,"u8"],"initial_virtual_quote_reserves":[142,8,"u64"],"post_complete_base_out":[150,8,"u64"],"post_complete_quote_in":[158,8,"u64"]}},"pumpswap_idl":{"program":"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA","discriminator":"f19a6d0411b16dbc","length":287,"fields":{"pool_bump":[8,1,"u8"],"index":[9,2,"u16"],"creator":[11,32,"pubkey"],"base_mint":[43,32,"pubkey"],"quote_mint":[75,32,"pubkey"],"lp_mint":[107,32,"pubkey"],"pool_base_token_account":[139,32,"pubkey"],"pool_quote_token_account":[171,32,"pubkey"],"lp_supply":[203,8,"u64"],"coin_creator":[211,32,"pubkey"],"is_mayhem_mode":[243,1,"bool"],"is_cashback_coin":[244,1,"bool"],"virtual_quote_reserves":[245,16,"i128"],"creator_fee_bps":[261,8,"u64"],"can_edit_creator_fee":[269,1,"bool"],"is_holder_reward":[270,1,"bool"],"protocol_fees":[271,8,"u64"],"creator_fees":[279,8,"u64"]}},"dlmm_idl":{"program":"LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo","discriminator":"210b3162b565b10d","length":904,"fields":{"parameters.base_factor":[8,2,"u16"],"parameters.filter_period":[10,2,"u16"],"parameters.decay_period":[12,2,"u16"],"parameters.reduction_factor":[14,2,"u16"],"parameters.variable_fee_control":[16,4,"u32"],"parameters.max_volatility_accumulator":[20,4,"u32"],"parameters.min_bin_id":[24,4,"i32"],"parameters.max_bin_id":[28,4,"i32"],"parameters.protocol_share":[32,2,"u16"],"parameters.base_fee_power_factor":[34,1,"u8"],"parameters.function_type":[35,1,"u8"],"parameters.collect_fee_mode":[36,1,"u8"],"v_parameters.volatility_accumulator":[40,4,"u32"],"v_parameters.volatility_reference":[44,4,"u32"],"v_parameters.index_reference":[48,4,"i32"],"v_parameters.last_update_timestamp":[56,8,"i64"],"pair_type":[75,1,"u8"],"active_id":[76,4,"i32"],"bin_step":[80,2,"u16"],"status":[82,1,"u8"],"require_base_factor_seed":[83,1,"u8"],"activation_type":[86,1,"u8"],"creator_pool_on_off_control":[87,1,"u8"],"token_x_mint":[88,32,"pubkey"],"token_y_mint":[120,32,"pubkey"],"reserve_x":[152,32,"pubkey"],"reserve_y":[184,32,"pubkey"],"protocol_fee.amount_x":[216,8,"u64"],"protocol_fee.amount_y":[224,8,"u64"],"oracle":[552,32,"pubkey"],"last_updated_at":[712,8,"i64"],"pre_activation_swap_address":[752,32,"pubkey"],"base_key":[784,32,"pubkey"],"activation_point":[816,8,"u64"],"pre_activation_duration":[824,8,"u64"],"_padding_4":[840,8,"u64"],"creator":[848,32,"pubkey"],"token_mint_x_program_flag":[880,1,"u8"],"token_mint_y_program_flag":[881,1,"u8"],"version":[882,1,"u8"]}},"damm_v2_idl":{"program":"cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG","discriminator":"f19a6d0411b16dbc","length":1112,"fields":{"pool_fees.base_fee.padding_1":[40,8,"u64"],"pool_fees.protocol_fee_percent":[48,1,"u8"],"pool_fees.padding_0":[49,1,"u8"],"pool_fees.referral_fee_percent":[50,1,"u8"],"pool_fees.compounding_fee_bps":[54,2,"u16"],"pool_fees.dynamic_fee.initialized":[56,1,"u8"],"pool_fees.dynamic_fee.max_volatility_accumulator":[64,4,"u32"],"pool_fees.dynamic_fee.variable_fee_control":[68,4,"u32"],"pool_fees.dynamic_fee.bin_step":[72,2,"u16"],"pool_fees.dynamic_fee.filter_period":[74,2,"u16"],"pool_fees.dynamic_fee.decay_period":[76,2,"u16"],"pool_fees.dynamic_fee.reduction_factor":[78,2,"u16"],"pool_fees.dynamic_fee.last_update_timestamp":[80,8,"u64"],"pool_fees.dynamic_fee.bin_step_u128":[88,16,"u128"],"pool_fees.dynamic_fee.sqrt_price_reference":[104,16,"u128"],"pool_fees.dynamic_fee.volatility_accumulator":[120,16,"u128"],"pool_fees.dynamic_fee.volatility_reference":[136,16,"u128"],"pool_fees.init_sqrt_price":[152,16,"u128"],"token_a_mint":[168,32,"pubkey"],"token_b_mint":[200,32,"pubkey"],"token_a_vault":[232,32,"pubkey"],"token_b_vault":[264,32,"pubkey"],"whitelisted_vault":[296,32,"pubkey"],"liquidity":[360,16,"u128"],"padding_1":[376,16,"u128"],"protocol_a_fee":[392,8,"u64"],"protocol_b_fee":[400,8,"u64"],"dead_liquidity_fee_checkpoint":[408,8,"u64"],"sqrt_min_price":[424,16,"u128"],"sqrt_max_price":[440,16,"u128"],"sqrt_price":[456,16,"u128"],"activation_point":[472,8,"u64"],"activation_type":[480,1,"u8"],"pool_status":[481,1,"u8"],"token_a_flag":[482,1,"u8"],"token_b_flag":[483,1,"u8"],"collect_fee_mode":[484,1,"u8"],"pool_type":[485,1,"u8"],"fee_version":[486,1,"u8"],"padding_3":[487,1,"u8"],"permanent_lock_liquidity":[552,16,"u128"],"metrics.total_lp_a_fee":[568,16,"u128"],"metrics.total_lp_b_fee":[584,16,"u128"],"metrics.total_protocol_a_fee":[600,8,"u64"],"metrics.total_protocol_b_fee":[608,8,"u64"],"metrics.total_position":[632,8,"u64"],"metrics.padding":[640,8,"u64"],"creator":[648,32,"pubkey"],"token_a_amount":[680,8,"u64"],"token_b_amount":[688,8,"u64"],"layout_version":[696,1,"u8"]}}}
SOURCE_HASHES = {"pumpswap_idl":"b7d8c57a4d9c4dd0109a9ab893052352333252d4eedced10ac091d4d66cab89b","damm_v2_idl":"7f5e5f5e9cab57cd208575dc53359676550e41ee0c19fc822560fe1062fd3ac5","dlmm_idl":"045c0f4af044be046b6a14b350077e4242e8ac026b64a855803dab30fcdd8b35","launch_config":"34b18f1cdb827a586d5f350cd44022a2115f60a4fccac059481ad07ad711e146","ray_cpmm":"d53c4ac220bd3ff6f4e38116210d6e6487d599fd7bb7725a8631fca16c779cb1","ray_programs":"ded5ae021aa50ee17cc36441d5fcb4d1a242676fe77fd4308b066f688f9b23b4","pump_idl":"6a19bad7758fb822cee79a91575e51a7bd60391c670200408676643e00a79a6d","ray_v4_calc":"27202867061850d46e0e2786553a51deaec421387f5831cc0be63da08ae7f7a3","pumpswap_price":"58edc63278cf87ff464f8ff7efd55bed83f55045319e483454abd075c34956fa","ray_v4":"e08112206d32ec507950f44c5c4cef12d94731b9c2cce2549e329a82f0bb091d","ray_clmm":"7e97c7d67d8202b8d171f2d4760c9169c2aa08f3c944aa7d510ecc05c6d7412b","launch_curve":"d8004455490cf3742472889a1c981e9f3faeda03c74f27bdc789b88e2831dc66"}


"""Public compact recovery. Reuses the deployed program; no ALT creation."""
import base64
import hashlib
import json
import struct
import compact_tables
from pathlib import Path
from fastapi import HTTPException
from solders.address_lookup_table_account import AddressLookupTableAccount
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.hash import Hash
from solders.instruction import Instruction, AccountMeta
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

WALLET = '21Fsr1RTmM83GAjfydc8PJq4P2ycQHtaYgTvJswxSc2N'
PROGRAM = '38cMAQ5AJUiXvJd5ce6MjFGgS7wtEXoRWipGCnGnxmjV'
PROGRAM_DATA = 'BS4mhaKKbqyLPW5rVAhvhqS24zay2mXRcUNT9oJmiC2n'
AUTHORITY = 'Fqa65PPbsYdWaQKNV6EwzNh74oqBRfZEbLUTU6QX2JMN'
BINARY_HASH = 'ceb3bed443a117225769c14c50fc8dff9a55cbf9d2a388c4a5f757717bdf8030'
ALT_PROGRAM = 'AddressLookupTab1e1111111111111111111111111'
LOADER = 'BPFLoaderUpgradeab1e11111111111111111111111'
TOKEN = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN22 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
ENGINE = 'compact-pilot-1'
CONFIG = Path(__file__).with_name('compact_pilot.json')

def config():
    if not CONFIG.is_file():return {}
    value=json.loads(CONFIG.read_text())
    if value.get('wallet')!=WALLET or value.get('program')!=PROGRAM:
        raise ValueError('Compact configuration mismatch')
    return value

def eligible(record):
    selection=json.loads(record['selection'])
    cfg=config()
    return (cfg.get('enabled') is True and isinstance(selection,dict)
            and (cfg.get('public_all') is True or
                 (record['wallet']==WALLET and not selection.get('telegram_session'))))

def active(record):
    selection=json.loads(record['selection'])
    return isinstance(selection,dict) and selection.get('engine')==ENGINE

def table_map(names=None):
    return {t['address']:[Pubkey.from_string(k) for k in t['addresses']]
            for t in [*config().get('tables',[]),*(compact_tables.named_tables(names) if names is not None else compact_tables.all_tables())]}

def resolved_keys(message,meta=None):
    """Solana key ordering: static, all loaded writable, all loaded readonly."""
    lookups=getattr(message,'address_table_lookups',[])
    writable=[];readonly=[];mapping=table_map([str(x.account_key) for x in lookups]) if lookups else {}
    for lookup in lookups:
        address=str(lookup.account_key)
        if address not in mapping:raise ValueError('Unrecognized lookup table')
        addresses=mapping[address]
        try:
            writable.extend(addresses[i] for i in lookup.writable_indexes)
            readonly.extend(addresses[i] for i in lookup.readonly_indexes)
        except IndexError:raise ValueError('Invalid lookup index')
    keys=list(message.account_keys)+writable+readonly
    if len(set(keys))!=len(keys):raise ValueError('Duplicate resolved accounts')
    if meta is not None and lookups:
        expected={'writable':list(map(str,writable)),'readonly':list(map(str,readonly))}
        loaded=meta.get('loadedAddresses')
        if not isinstance(loaded,dict) or any(loaded.get(k)!=v for k,v in expected.items()):
            raise ValueError('Executed lookup addresses changed')
    return keys,len(writable)

def shape(message):
    keys,nw=resolved_keys(message);static=len(message.account_keys)
    h=message.header;n=h.num_required_signatures
    rights={str(k):(i<n, i<n-h.num_readonly_signed_accounts if i<n
                   else i<static-h.num_readonly_unsigned_accounts if i<static
                   else i<static+nw) for i,k in enumerate(keys)}
    ops=[(str(keys[ix.program_id_index]),tuple(str(keys[i]) for i in ix.accounts),bytes(ix.data))
         for ix in message.instructions]
    return str(keys[0]),str(message.recent_blockhash),rights,ops

def decode_table(item):
    if not item or item['owner']!=ALT_PROGRAM or item.get('executable'):
        raise ValueError('Lookup table owner mismatch')
    raw=base64.b64decode(item['data'][0],validate=True)
    if len(raw)<56 or (len(raw)-56)%32 or struct.unpack_from('<I',raw)[0]!=1:
        raise ValueError('Invalid lookup table')
    deactivation,last_slot=struct.unpack_from('<QQ',raw,4)
    if raw[21]!=1:raise ValueError('Lookup authority absent')
    authority=str(Pubkey.from_bytes(raw[22:54]))
    addresses=[str(Pubkey.from_bytes(raw[i:i+32])) for i in range(56,len(raw),32)]
    return deactivation,last_slot,authority,addresses

async def verify(client,call):
    cfg=config()
    if cfg.get('enabled') is not True:raise HTTPException(409,'Compact test is paused. Your progress is saved.')
    values=(await call(client,'getMultipleAccounts',[[PROGRAM,PROGRAM_DATA],
            {'encoding':'base64','commitment':'confirmed'}]))['value']
    try:
        program,data=values
        p=base64.b64decode(program['data'][0],validate=True)
        raw=base64.b64decode(data['data'][0],validate=True)
        if (program['owner']!=LOADER or not program['executable'] or len(p)!=36
                or struct.unpack_from('<I',p)[0]!=2 or str(Pubkey.from_bytes(p[4:]))!=PROGRAM_DATA
                or data['owner']!=LOADER or struct.unpack_from('<I',raw)[0]!=3
                or raw[12]!=1 or str(Pubkey.from_bytes(raw[13:45]))!=AUTHORITY
                or len(raw[45:])!=25984 or hashlib.sha256(raw[45:]).hexdigest()!=BINARY_HASH):
            raise ValueError()
    except (ValueError,KeyError,IndexError,TypeError,StopIteration):
        raise HTTPException(409,'Compact program or lookup table verification failed. No approval prepared.')

def build(rows,wallet,block,service,sponsor,network,instructions_fn,lookup_tables=None):
    cfg=config()
    if not cfg.get('enabled'):raise ValueError('Compact pilot disabled')
    wanted=set(compact_tables.required(rows))
    if lookup_tables is None:
        lookup_tables=[t for t in cfg.get('tables',[]) if wanted<=set(t['addresses'])][:1]
    if not lookup_tables or not wanted<=set(k for t in lookup_tables for k in t['addresses']):
        raise HTTPException(409,'A checked lookup table is required for compact recovery.')
    owner=Pubkey.from_string(wallet)
    # Reuse the production fee/referral allocation, with the same operations.
    import os
    treasury=Pubkey.from_string(os.environ['SOLBACK_WEB_TREASURY'])
    source=instructions_fn(rows,wallet,treasury,service,sponsor,network)[2:]
    token_programs=(Pubkey.from_string(TOKEN),Pubkey.from_string(TOKEN22))
    burns=[ix for ix in source if ix.program_id in token_programs and bytes(ix.data)[:1]==b'\x0f']
    fees=[ix for ix in source if ix.program_id not in token_programs]
    if len(burns)!=sum(r['kind']=='balance' for r in rows):raise ValueError('Burn count mismatch')
    close=Instruction(Pubkey.from_string(PROGRAM),b'',[
        AccountMeta(owner,True,True),*[AccountMeta(k,False,False) for k in token_programs],
        *[AccountMeta(Pubkey.from_string(r['address']),False,True) for r in rows]])
    instructions=[set_compute_unit_limit(80000+5000*len(rows)+16000*len(burns)),
                  set_compute_unit_price(20000),*burns,close,*fees]
    alts=[AddressLookupTableAccount(Pubkey.from_string(t['address']),[Pubkey.from_string(k) for k in t['addresses']]) for t in lookup_tables]
    message=MessageV0.try_compile(sponsor or owner,instructions,alts,Hash.from_string(block))
    return VersionedTransaction.populate(message,[Signature.default()]*message.header.num_required_signatures)

def fits(tx,rows):
    keys,_=resolved_keys(tx.message)
    return (1<=len(rows)<=56 and len(bytes(tx))<=850 and len(keys)<=63
            and len(tx.message.instructions)<=28 and len(tx.message.instructions)+len(rows)<=60
            and 80000+5000*len(rows)+16000*sum(r['kind']=='balance' for r in rows)<=1200000)


def message_tables(message):
    names=[str(x.account_key) for x in message.address_table_lookups]
    stored={t['address']:t for t in [*config().get('tables',[]),*compact_tables.named_tables(names)]}
    try:return [stored[str(lookup.account_key)] for lookup in message.address_table_lookups]
    except KeyError:raise HTTPException(409,'The checked lookup-table record is unavailable.')

async def verify_message_tables(client,call,message):
    for table in message_tables(message):
        await compact_tables.verify_table(client,call,table)

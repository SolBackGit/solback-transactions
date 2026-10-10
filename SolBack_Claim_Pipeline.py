#!/usr/bin/env python3
# Standalone diagnostic test; production bot is not modified.
# Requires /opt/solback/SolBack_OnChain_Prices.py and VPS venv.
# Tested: 1425/1551 prices, 76.73s, ~150 Helius credits, no errors.
# Index gap 0.05s, concurrency 4; stops at index HTTP 429.
import asyncio
import collections
import importlib.util
import inspect
import logging
import json
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from decimal import Decimal, InvalidOperation

BASE = Path(__file__).resolve().parent
WALLET = "3AWDTDGZiW8joyfA52LKL7GUWLoKBCBUBLUE5JoWgBCu"
KEY_DEFAULT = None

spec = importlib.util.spec_from_file_location(
    "pipeline_prices", BASE / "SolBack_Claim_Prices.py")
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)

source = textwrap.dedent(inspect.getsource(
    m.OnChainPrices.indexed_candidates))
old = "delay=.12-(time.monotonic()-self.index_last)"
if source.count(old) != 1:
    raise SystemExit("Metodo diverso da quello atteso.")
namespace = {}
exec(compile(source.replace(
    old, "delay=.05-(time.monotonic()-self.index_last)"),
    "<pipeline-gap005>", "exec"), m.__dict__, namespace)
m.OnChainPrices.indexed_candidates = namespace["indexed_candidates"]

class IndexRateLimited(RuntimeError):
    pass

def positive(value):
    if value is None:
        return None
    try:
        p = Decimal(str(value))
        return p if p.is_finite() and p > 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None

async def run_pipeline(wallet, holdings, rpc_url, progress=None, rpc_call=None, api_key=None, stage_timeout_seconds=None, on_price=None):
    WALLET = wallet
    KEY = api_key or KEY_DEFAULT
    started = time.monotonic()
    active_stages = {}
    heartbeat_task = None
    log = logging.getLogger("solback")
    rpc = m.ReadRPC(rpc_url, gap=.20, concurrency=2)
    if rpc_call is not None:
        async def shared_call(method, params):
            rpc.counts[method] += 1
            result = await rpc_call(method, params)
            if isinstance(result, dict) and 'context' in result:
                slot = int(result['context']['slot'])
                if slot < rpc.min_slot:
                    raise m.PriceError('rpc_slot_older_than_snapshot')
                rpc.max_slot = max(rpc.max_slot, slot)
            return result
        rpc.call = shared_call
    cache = m.DiscoveryCache(":memory:")
    engine = m.OnChainPrices(rpc, cache, refresh=True)
    engine.index_sem = asyncio.Semaphore(4)
    specs_by_name = {s.name: s for s in m.specs()}
    results = {}
    candidates_live = set()
    holding_totals = collections.defaultdict(Decimal)
    notes = collections.defaultdict(list)
    excluded = set()
    final_unknown = set()
    stats = collections.Counter()
    statuses = collections.Counter()
    finish = {}
    blocked = {}
    queues = [asyncio.Queue() for _ in range(4)]
    offsets = {
        "Raydium AMM v4": (336, 368),
        "Raydium CPMM": (72, 104),
        "Raydium CLMM": (137, 169),
        "Meteora DLMM": (152, 184),
        "Meteora DAMM v2": (232, 264),
    }

    def elapsed():
        return time.monotonic() - started

    def publish():
        if progress:
            progress({'elapsed_seconds': elapsed(), 'priced_mints': len(results),
                      'distinct_mints': len(mints), 'sol_usd': str(engine.sol_usd) if getattr(engine,'sol_usd',None) else None, 'stats': dict(stats),
                      'phase_finished': dict(finish),
                      'phase': ' / '.join(dict.fromkeys(active_stages.values())) or 'Preparing prices'})

    async def timed(name, awaitable, timeout):
        task=asyncio.current_task(); active_stages[task]=name
        began=time.monotonic();publish()
        log.info('Claim phase started: %s',name)
        try:
            return await asyncio.wait_for(awaitable, timeout)
        except TimeoutError:
            raise RuntimeError('Preparation timed out during '+name+'. No transaction was sent; retry preparation.') from None
        finally:
            log.info('Claim phase finished: %s | %.2fs',name,time.monotonic()-began)
            active_stages.pop(task,None);publish()

    async def heartbeat():
        while True:
            publish()
            await asyncio.sleep(1)

    async def observe(response):
        host = response.request.url.host
        statuses[host + ":" + str(response.status_code)] += 1
        if response.status_code == 429:
            blocked.update({
                "host": host,
                "retry_after": response.headers.get("retry-after"),
                "received_at_utc": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
            raise IndexRateLimited("Indice HTTP 429: " + host)

    engine.index_client.event_hooks["response"] = [observe]

    def price_found(mint):
        result=results[mint]
        value=holding_totals[mint]*Decimal(str(result['price_usd']))
        if value < Decimal('.50'):candidates_live.add(mint)
        else:candidates_live.discard(mint)
        stats['candidate_mints_under_050']=len(candidates_live)
        if on_price:on_price(mint,result)

    def accept(mint, pool):
        a, b = pool["mint_a"], pool["mint_b"]
        ratio = positive(pool.get("price_b_per_a"))
        if ratio is None:
            return False
        if mint == a:
            quote = b
        elif mint == b:
            quote, ratio = a, Decimal(1) / ratio
        else:
            return False
        reference = (engine.sol_usd if quote == m.SOL else
                     Decimal(1) if quote == m.USDC else None)
        if reference is None:
            return False
        price = ratio * reference
        if not price.is_finite() or price <= 0:
            return False
        results[mint] = {
            "source": pool["source"],
            "status": "SINGLE_SOURCE",
            "pool": pool["pool"],
            "quote": quote,
            "price_usd": price,
            "slot": pool.get("slot"),
            "quoted_at": time.time(),
        }
        price_found(mint)
        return True

    async def batches(queue):
        ended = False
        while not ended:
            item = await queue.get()
            if item is None:
                break
            batch = [item]
            deadline = time.monotonic() + .6
            while len(batch) < 100:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), remaining)
                except asyncio.TimeoutError:
                    break
                if item is None:
                    ended = True
                    break
                batch.append(item)
            yield batch

    async def send(queue, mint):
        if mint not in results and mint not in excluded:
            await queue.put(mint)

    async def jupiter():
        last = 0.
        async with m.httpx.AsyncClient(timeout=8) as client:
            for offset in range(0, len(mints), 50):
                batch = mints[offset:offset + 50]
                successful = False
                for attempt in range(3):
                    await asyncio.sleep(max(
                        0., last + 1.10 - time.monotonic()))
                    last = time.monotonic()
                    stats["jupiter_requests"] += 1
                    try:
                        response = await timed("Jupiter", client.get(
                            "https://api.jup.ag/price/v3",
                            params={"ids": ",".join(batch)},
                            headers={
                                "x-api-key": KEY,
                                "Accept": "application/json",
                                "User-Agent": "SolBack-Price-Test/1.0",
                            }), min(10, stage_timeout_seconds or 10))
                        if response.status_code != 200:
                            code = response.status_code
                            stats["jupiter_HTTP_" + str(code)] += 1
                            if code in (401, 403):
                                raise RuntimeError(
                                    "Jupiter autenticazione HTTP " + str(code))
                            if code in (429, 502, 503, 504) and attempt < 2:
                                try:
                                    delay = float(response.headers.get(
                                        "retry-after", "3"))
                                except ValueError:
                                    delay = 3
                                if delay > 10:
                                    raise RuntimeError("Jupiter rate limited; retry in "+str(int(delay))+" seconds.")
                                await timed("Jupiter retry wait", asyncio.sleep(max(
                                    delay, 3 * (attempt + 1))), 11)
                                continue
                            break
                        payload = response.json()
                        if not isinstance(payload, dict):
                            raise ValueError("jupiter_invalid_response")
                        for mint in batch:
                            item = payload.get(mint)
                            p = (positive(item.get("usdPrice"))
                                 if isinstance(item, dict) else None)
                            if p is not None:
                                results[mint] = {
                                    "source": "Jupiter",
                                    "status": "SINGLE_SOURCE",
                                    "price_usd": p,
                                    "blockId": item.get("blockId"),
                                    "quoted_at": time.time(),
                                }
                                price_found(mint)
                        successful = True
                        break
                    except RuntimeError:
                        raise
                    except (m.httpx.HTTPError, ValueError) as exc:
                        stats["jupiter_" + type(exc).__name__] += 1
                        if attempt < 2:
                            await asyncio.sleep(attempt + 1)
                for mint in batch:
                    if not successful:
                        notes[mint].append({
                            "source": "Jupiter", "reason": "request_failed"})
                    await send(queues[0], mint)
                if progress: progress({'elapsed_seconds': elapsed(), 'priced_mints': len(results), 'distinct_mints': len(mints), 'sol_usd': str(engine.sol_usd) if getattr(engine,'sol_usd',None) else None, 'stats': dict(stats), 'phase_finished': dict(finish)})
        finish["Jupiter"] = elapsed()
        await queues[0].put(None)

    async def curves():
        async for batch in batches(queues[0]):
            await timed("Curve/PumpSwap account reads", engine.prefetch_wallet(batch), stage_timeout_seconds or 30)
            for mint in batch:
                if mint in m.PROTECTED:
                    excluded.add(mint)
                    notes[mint].append({"reason": "protected_asset"})
                    continue
                try:
                    account = (await engine.accounts([mint]))[mint]
                    raw = m.account_bytes(account)
                    if m.mint_info(account) == 0 and m.field(raw, 36, 8) <= 1:
                        excluded.add(mint)
                        notes[mint].append({"reason": "nft_like_asset"})
                        continue
                except Exception as exc:
                    excluded.add(mint)
                    notes[mint].append({
                        "reason": "mint_unreadable:" + str(exc)})
                    continue
                pools, errors, ok = await engine.curves(mint)
                notes[mint].extend(errors)
                pools.sort(key=lambda p: (
                    0 if p["source"] == "Pump.fun" else 1,
                    0 if p["mint_b"] == m.SOL else 1))
                for pool in pools:
                    if pool.get("reserve_b_ui", Decimal(0)) <= 0:
                        continue
                    if accept(mint, pool):
                        break
                if mint not in results:
                    pools, errors = await engine.canonical_pumpswap(mint)
                    notes[mint].extend(errors)
                    pools.sort(key=lambda p: 0 if m.SOL in (
                        p["mint_a"], p["mint_b"]) else 1)
                    for pool in pools:
                        if accept(mint, pool):
                            break
                await send(queues[1], mint)
            stats["curve_mints_checked"] += len(batch)
            if progress: progress({'elapsed_seconds': elapsed(), 'priced_mints': len(results), 'distinct_mints': len(mints), 'sol_usd': str(engine.sol_usd) if getattr(engine,'sol_usd',None) else None, 'stats': dict(stats), 'phase_finished': dict(finish)})
        finish["Curve/PumpSwap"] = elapsed()
        await queues[1].put(None)

    async def dex_batch(batch, name):
        candidates = {}

        async def discover(mint):
            rows, errors, ok = await engine.indexed_candidates(mint, name)
            candidates[mint] = rows
            notes[mint].extend(errors)
            if not rows and not errors:
                notes[mint].append({
                    "source": name,
                    "reason": "no_supported_pool_in_first_index_page"})

        async with asyncio.TaskGroup() as group:
            for mint in batch:
                group.create_task(discover(mint))

        addresses = [c["address"] for rows in candidates.values() for c in rows]
        accounts = await engine.accounts(addresses)
        dependencies = []
        pending = collections.defaultdict(list)
        for mint, rows in candidates.items():
            for candidate in rows:
                address = candidate["address"]
                try:
                    s = specs_by_name[candidate["program_name"]]
                    account = accounts[address]
                    if account is None:
                        raise m.PriceError("pool_missing")
                    raw = m.account_bytes(account, s.program)
                    a = m.field(raw, s.mint_a, 32, "pubkey")
                    b = m.field(raw, s.mint_b, 32, "pubkey")
                    quote = b if a == mint else a if b == mint else None
                    if quote not in (m.SOL, m.USDC):
                        raise m.PriceError("unsupported_quote_or_mint")
                    dependencies.extend([a, b])
                    dependencies.extend(
                        m.field(raw, off, 32, "pubkey")
                        for off in offsets[s.name])
                    pending[mint].append((
                        0 if quote == m.SOL else 1, s, address, account))
                except Exception as exc:
                    notes[mint].append({
                        "source": name, "pool": address,
                        "reason": str(exc)})
        await engine.accounts(dependencies)

        for mint, pools in pending.items():
            pools.sort(key=lambda p: p[0])
            for _, s, address, account in pools:
                try:
                    pool = await engine.parse_pool(s, address, account)
                    if accept(mint, pool):
                        break
                except Exception as exc:
                    notes[mint].append({
                        "source": name, "pool": address,
                        "reason": str(exc)})

    async def dex_worker(name, input_index, output_index, counter):
        async for batch in batches(queues[input_index]):
            await timed(name, dex_batch(batch, name), stage_timeout_seconds or max(20, len(batch)*.2+10))
            if output_index is not None:
                for mint in batch:
                    await send(queues[output_index], mint)
            if output_index is None:
                final_unknown.update(mint for mint in batch if mint not in results and mint not in excluded)
            stats['final_unresolved'] = len(final_unknown) + len(excluded)
            stats[counter] += len(batch)
            if progress: progress({'elapsed_seconds': elapsed(), 'priced_mints': len(results), 'distinct_mints': len(mints), 'sol_usd': str(engine.sol_usd) if getattr(engine,'sol_usd',None) else None, 'stats': dict(stats), 'phase_finished': dict(finish)})
        finish[name] = elapsed()
        if output_index is not None:
            await queues[output_index].put(None)

    try:
        holdings = list(holdings)
        mints = list(dict.fromkeys(h["mint"] for h in holdings))
        for h in holdings:
            holding_totals[h["mint"]]+=Decimal(h["raw_amount"])/(Decimal(10)**h["decimals"])
        if progress: progress({'elapsed_seconds': elapsed(), 'priced_mints': len(results), 'distinct_mints': len(mints), 'sol_usd': str(engine.sol_usd) if getattr(engine,'sol_usd',None) else None, 'stats': dict(stats), 'phase_finished': dict(finish)})
        if progress: progress({'elapsed_seconds': elapsed(), 'priced_mints': len(results), 'distinct_mints': len(mints), 'sol_usd': str(engine.sol_usd) if getattr(engine,'sol_usd',None) else None, 'stats': dict(stats), 'phase_finished': dict(finish)})
        heartbeat_task=asyncio.create_task(heartbeat())
        with tempfile.TemporaryDirectory() as td:
            engine.anchor = await timed("SOL/USD reference", m.external_sol_reference(
                str(Path(td) / "sol.json"), None), stage_timeout_seconds or 12)
        engine.sol_usd = engine.anchor.get("sol_usd")
        engine.reference_time = time.time()
        if engine.sol_usd is None:
            raise RuntimeError("Riferimento SOL/USD non disponibile")

        interrupted = False
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(jupiter())
                group.create_task(curves())
                group.create_task(dex_worker(
                    "Raydium", 1, 2, "raydium_mints_checked"))
                group.create_task(dex_worker(
                    "Meteora DLMM", 2, 3, "dlmm_mints_checked"))
                group.create_task(dex_worker(
                    "Meteora DAMM v2", 3, None, "damm_mints_checked"))
        except* IndexRateLimited:
            interrupted = True

        remaining = [mint for mint in mints if mint not in results]
        credits = sum(n * (10 if method == "getProgramAccounts" else 1)
                      for method, n in rpc.counts.items())
        sources = collections.Counter(r["source"] for r in results.values())
        total = elapsed()
        valued, candidates = [], set()
        deposits = Decimal(0)

        if not interrupted:
            for h in holdings:
                price = results.get(h["mint"], {
                    "status": "PROTECTED" if h["mint"] in excluded else "UNKNOWN",
                    "price_usd": None})
                valued.append(m.holding_result(h, price, Decimal(".50")))
            candidates = set(candidates_live)
            deposits = sum((h["rent_sol"] for h in valued
                            if h["mint"] in candidates), Decimal(0))

        report = {
            "wallet": WALLET,
            "sol_usd": str(engine.sol_usd) if engine.sol_usd else None,
            "fresh_wallet_and_prices": True,
            "previous_reports_used": False,
            "persistent_cache_used": False,
            "pipeline": True,
            "index_gap_seconds": .05,
            "index_concurrency": 4,
            "interrupted_by_429": interrupted,
            "rate_limit": blocked,
            "elapsed_seconds": total,
            "phase_finished_at_seconds": finish,
            "stats": dict(stats),
            "index_http_statuses": dict(statuses),
            "distinct_mints": len(mints),
            "priced_mints": len(results),
            "sources": dict(sources),
            "results": results,
            "holdings": valued if not interrupted else holdings,
            "unresolved_mints": remaining,
            "excluded_mints": sorted(excluded),
            "notes": dict(notes),
            "candidate_mints_under_050": sorted(candidates),
            "candidate_account_deposits_sol": deposits,
            "rpc_requests": dict(rpc.counts),
            "rpc_errors": dict(rpc.errors),
            "helius_credits_estimated": credits,
            "index_requests": dict(engine.index_counts),
            "index_errors": dict(engine.index_errors),
            "usdc_assumed_usd": 1,
            "burn_authorized": False,
        }
        if interrupted:
            raise IndexRateLimited('DEX temporarily rate limited; retry after ' + str(blocked.get('retry_after') or 'a few minutes'))
        return m.json_safe(report)
    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:await heartbeat_task
            except asyncio.CancelledError:pass
        cache.close()
        await rpc.close()
        await engine.index_client.aclose()



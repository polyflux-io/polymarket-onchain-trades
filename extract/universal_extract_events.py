#!/usr/bin/env python3
"""
Extract all Polymarket events (trades, splits, merges, redeems, converts,
settlements, deposits, P2P transfers) for a given time period into Parquet.

Uses PolymarketTracker's parsing logic for correctness.

Usage:
    # Multi-day extraction (one file per day)
    python3 extract_events.py \
      --from-date 2026-02-01 --to-date 2026-02-15 \
      -o data/events/ --workers 8 --chunk 100

    # Single block range
    python3 extract_events.py \
      --from-block 82900000 --to-block 82950000 \
      -o data/events.parquet

    # Trades only (like old extract_trades.py)
    python3 extract_events.py --no-ctf --no-settlements \
      --from-date 2026-04-01 --to-date 2026-04-02 -o data/trades/

    # With transfers
    python3 extract_events.py --transfers \
      --from-block 85390000 --to-block 85395000 -o data/events.parquet
"""

import argparse
import os
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import polars as pl
import requests

from tracker_v2 import (   # V2-era parser (V2 exchanges, pUSD)
    PolymarketTracker as V2Tracker, Event,
    TradeEvent, SplitEvent, MergeEvent, RedeemEvent,
    ConvertEvent, SettlementEvent, DepositEvent, P2PTransferEvent,
)
from tracker_optimized import PolymarketTracker as V1Tracker  # V1-era parser (V1 exchanges, USDC.e)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RPC = os.environ.get("RPC", "http://localhost:8545")
RPC_TIMEOUT = 600
MAX_RETRIES = 5
RETRY_DELAY = 2

# Explicit schema — avoids polars inference errors when nullable string
# columns are all-null in the first N rows.
_SCHEMA = {
    "event_type": pl.Utf8,
    "wallet_address": pl.Utf8,
    "asset_id": pl.Utf8,
    "block_number": pl.Int64,
    "block_timestamp": pl.Utf8,
    "tx_hash": pl.Utf8,
    "log_index": pl.Int64,
    # TradeEvent
    "operation_type": pl.Utf8,
    "size": pl.Float64,
    "price": pl.Float64,
    "fee": pl.Float64,
    "counterparty": pl.Utf8,
    "wallet_role": pl.Utf8,
    "order_hash": pl.Utf8,
    "intent_adjusted": pl.Boolean,
    "exchange": pl.Utf8,
    # CTF events
    "condition_id": pl.Utf8,
    # RedeemEvent
    "payout_usdc": pl.Float64,
    # SettlementEvent
    "oracle": pl.Utf8,
    "payout_rate": pl.Float64,
    # Money-flow events (deposit/withdrawal x type) — tx-level pm_classify_v2.
    # Replaces the legacy per-log deposit/p2p_transfer rows (which mislabelled trade
    # settlements as p2p and missed deposits/withdrawals). `counterparty` (above) and
    # `wallet_address` are reused; the subject wallet is `wallet_address`.
    "op": pl.Utf8,               # 'deposit' | 'withdrawal'
    "flow_type": pl.Utf8,        # 'external' | 'p2p' | 'unidentified'
    "direction": pl.Utf8,        # 'in' | 'out'
    "amount": pl.Float64,        # USD value
    "token": pl.Utf8,            # 'USDC' | 'USDC.e' | 'USDT' | 'pUSD'
}

_ALL_FIELDS = list(_SCHEMA.keys())

_OMIT_FIELDS = {"blockchain_lag", "timestamp", "source", "order_hashes"}


# ---------------------------------------------------------------------------
# RPC helpers (standalone, for date→block resolution)
# ---------------------------------------------------------------------------

def rpc_call(method: str, params: list):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(
                RPC,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=RPC_TIMEOUT,
            )
            data = r.json()
            if "result" in data:
                return data["result"]
            if "error" in data:
                print(f"  RPC error: {data['error']}")
        except Exception as e:
            print(f"  RPC fail (attempt {attempt + 1}): {e}")
        time.sleep(RETRY_DELAY * (attempt + 1))
    return None


def get_chain_head() -> int:
    head_hex = rpc_call("eth_blockNumber", [])
    if not head_hex:
        print("Cannot reach RPC")
        sys.exit(1)
    return int(head_hex, 16)


def get_block_timestamp(block_num: int) -> Optional[int]:
    result = rpc_call("eth_getBlockByNumber", [hex(block_num), False])
    if result and "timestamp" in result:
        return int(result["timestamp"], 16)
    return None


def date_to_block(target_ts: int, lo: int, hi: int) -> int:
    """Binary search for the first block at or after target_ts."""
    while lo < hi:
        mid = (lo + hi) // 2
        ts = get_block_timestamp(mid)
        if ts is None:
            lo = mid + 1
            continue
        if ts < target_ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


# ---------------------------------------------------------------------------
# Event serialization
# ---------------------------------------------------------------------------

def event_to_row(e: Event) -> dict:
    """Convert an Event to a flat dict with consistent keys."""
    d = asdict(e)
    # Convert datetimes to ISO strings for safe pickling + polars ingestion
    for k in ("block_timestamp", "timestamp"):
        v = d.get(k)
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    # Remove internal fields
    for k in _OMIT_FIELDS:
        d.pop(k, None)
    # Ensure all fields present (None for missing)
    row = {}
    for field in _ALL_FIELDS:
        row[field] = d.get(field)
    return row


# ---------------------------------------------------------------------------
# Chunk worker (runs in separate process)
# ---------------------------------------------------------------------------

# Per-process tracker instances (initialized via _init_worker). TWO trackers —
# V1 and V2 — so one run spans the CLOB v1<->v2 migration (Jan–Apr 2026 is V1
# era). Their exchange address sets are DISJOINT, so unioning their trade events
# double-counts nothing (validated vs Dune across the Apr-28 boundary: V1 Δ0.02%,
# V2 exact, distinct wallets/assets ±1-2).
_t1: Optional[object] = None   # V1
_t2: Optional[object] = None   # V2
_tracker_config: Optional[dict] = None


def _init_worker(config: dict) -> None:
    """Initialize V1 (trades-only) + V2 (full) trackers in each worker process."""
    global _t1, _t2, _tracker_config
    # V1 tracker: TRADES ONLY (V1 exchange) — the one thing the V2 tracker can't
    # see in the V1 era. V2 tracker: FULL — it's the modern ERA-AWARE tracker
    # (pm_classify_v2 handles USDC.e[v1] + pUSD[v2]; STABLECOIN_CONTRACTS includes
    # USDC.e; both neg-risk adapters; CTF/UMA contracts are era-independent), so it
    # emits correct ctf/settlement/money_flow for V1-era blocks too. Non-trade
    # events come from V2 ONLY => no double-count with V1.
    v1cfg = dict(config); v1cfg.update(track_ctf_events=False, track_settlements=False, track_transfers=False)
    v2cfg = dict(config); v2cfg.update(track_ctf_events=True, track_settlements=True, track_transfers=True)
    _tracker_config = v2cfg
    _t1 = V1Tracker(**v1cfg)
    _t2 = V2Tracker(**v2cfg)


CHUNK_MAX_RETRIES = 3
CHUNK_RETRY_DELAY = 5
# When the node times out on a dense range (a large getLogs response), split the
# range and retry the halves — smaller ranges return under the RPC timeout and
# avoid the SILENT PARTIAL EXTRACTION that hit us on 2026-08-05 (a runaway-load
# storm slowed the node, chunks timed out, and undercounted parquets were written
# unnoticed). If even the minimum range keeps timing out, the chunk is reported
# INCOMPLETE so the run fails loud instead of quietly writing short data.
MIN_SPLIT_BLOCKS = int(os.getenv("MIN_SPLIT_BLOCKS", "10"))
MAX_SPLIT_DEPTH = int(os.getenv("MAX_SPLIT_DEPTH", "7"))


def _fetch_adaptive(from_block: int, to_block: int, depth: int = 0):
    """Fetch events for [from_block, to_block], splitting on RPC timeout.

    tracker_v2._rpc_batch increments rpc_timeouts whenever a getLogs times out;
    we watch that counter as a per-fetch signal. Returns (events, incomplete).
    incomplete is True only if a timeout could not be resolved even at
    MIN_SPLIT_BLOCKS granularity (a genuinely unhealthy node) — in which case the
    returned events may be partial and the caller must treat the run as suspect."""
    b1, b2 = _t1.rpc_timeouts, _t2.rpc_timeouts
    # Union of V1 + V2 trade events (disjoint exchange contracts => no double count).
    events = (_t1._fetch_logs_for_range(from_block, to_block, head=to_block)
              + _t2._fetch_logs_for_range(from_block, to_block, head=to_block))
    if _t1.rpc_timeouts == b1 and _t2.rpc_timeouts == b2:
        return events, False                       # clean fetch, trustworthy
    if (to_block - from_block + 1) <= MIN_SPLIT_BLOCKS or depth >= MAX_SPLIT_DEPTH:
        return events, True                        # can't split further → incomplete
    mid = (from_block + to_block) // 2
    le, li = _fetch_adaptive(from_block, mid, depth + 1)
    re, ri = _fetch_adaptive(mid + 1, to_block, depth + 1)
    return le + re, (li or ri)


def process_chunk(from_block: int, to_block: int) -> dict:
    """Fetch and parse all events for a block range, splitting on timeout."""
    t0 = time.monotonic()

    events, incomplete = _fetch_adaptive(from_block, to_block)
    # NOTE: no retry on an empty result. _rpc_batch now flags ANY failed call
    # (timeout OR per-call error response) by bumping rpc_timeouts, which the
    # adaptive split turns into `incomplete`. So `not events and not incomplete`
    # means the RPC genuinely succeeded and the range simply has no Polymarket
    # activity — a legitimate empty (common in sparse pre-ramp eras like 2024).
    # We trust it directly instead of sleep-retrying every empty chunk (which cost
    # ~15s each and made 2024 ~37 min/day). Real failures still fail loud: incomplete
    # -> the day is marked .INCOMPLETE and the driver retries the whole day.

    elapsed = time.monotonic() - t0

    # Drop the legacy per-log deposit/p2p_transfer events — the parquet now carries the
    # corrected money_flow events (op/flow_type/direction) instead. Trades/CTF/settlement
    # events are unchanged.
    events = [e for e in events if e.event_type not in ("deposit", "p2p_transfer")]

    rows = [event_to_row(e) for e in events]

    # Count by type
    type_counts: Dict[str, int] = {}
    for e in events:
        et = e.event_type
        type_counts[et] = type_counts.get(et, 0) + 1

    # Count null timestamps
    null_ts = sum(1 for r in rows if r.get("block_timestamp") is None)

    return {
        "rows": rows,
        "elapsed": elapsed,
        "event_count": len(rows),
        "type_counts": type_counts,
        "rpc_timeouts": _t1.rpc_timeouts + _t2.rpc_timeouts,
        "null_timestamps": null_ts,
        "incomplete": incomplete,   # True => a timeout couldn't be split away; data may be short
    }


# ---------------------------------------------------------------------------
# Extract a single block range → Parquet
# ---------------------------------------------------------------------------

def extract_range(from_block: int, to_block: int, output_path: str,
                  workers: int, chunk_size: int, tracker_config: dict,
                  label: str = "") -> dict:
    """Extract events for a block range and write to Parquet.

    Streams chunk results to temporary parquet files to avoid OOM,
    then merges them into the final output.
    """
    total_blocks = to_block - from_block
    chunks = []
    cursor = from_block
    while cursor < to_block:
        chunk_end = min(cursor + chunk_size - 1, to_block)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + 1
    total_chunks = len(chunks)

    prefix = f"[{label}] " if label else ""
    print(f"{prefix}Blocks {from_block} → {to_block} ({total_blocks:,} blocks, {total_chunks} chunks)")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    # Write chunks to temp parquet files to avoid accumulating all rows in memory
    tmp_dir = tempfile.mkdtemp(prefix="extract_events_")
    tmp_files: List[str] = []
    chunks_done = 0
    total_events = 0
    grand_type_counts: Dict[str, int] = {}
    failed_chunks: List[tuple] = []       # worker raised — chunk's data missing
    incomplete_chunks: List[tuple] = []   # node kept timing out even split down — data may be short
    t0 = time.monotonic()

    pool = ProcessPoolExecutor(
        max_workers=workers,
        initializer=_init_worker,
        initargs=(tracker_config,),
    )
    futures = {
        pool.submit(process_chunk, c_from, c_to): (c_from, c_to)
        for c_from, c_to in chunks
    }

    try:
        for future in as_completed(futures):
            c_from, c_to = futures[future]
            try:
                result = future.result()
            except Exception as e:
                print(f"  {prefix}ERROR chunk {c_from}-{c_to}: {e}")
                failed_chunks.append((c_from, c_to))
                chunks_done += 1
                continue

            # Write chunk rows to temp parquet file immediately, don't accumulate
            if result["rows"]:
                tmp_path = os.path.join(tmp_dir, f"chunk_{chunks_done:06d}.parquet")
                pl.DataFrame(result["rows"], schema=_SCHEMA).write_parquet(tmp_path, compression="zstd")
                tmp_files.append(tmp_path)
                del result["rows"]  # free memory
            if result.get("incomplete"):
                incomplete_chunks.append((c_from, c_to))
            total_events += result["event_count"]
            for et, cnt in result["type_counts"].items():
                grand_type_counts[et] = grand_type_counts.get(et, 0) + cnt
            chunks_done += 1

            elapsed = time.monotonic() - t0
            rate = chunks_done / elapsed if elapsed > 0 else 0
            remaining = total_chunks - chunks_done
            eta = remaining / rate if rate > 0 else 0

            types_str = " ".join(f"{k}={v}" for k, v in sorted(result["type_counts"].items()))
            warn_parts = []
            if result.get("rpc_timeouts", 0):
                warn_parts.append(f"timeouts={result['rpc_timeouts']}")
            if result.get("null_timestamps", 0):
                warn_parts.append(f"null_ts={result['null_timestamps']}")
            warn_str = f" !!{' '.join(warn_parts)}!!" if warn_parts else ""
            print(
                f"  {prefix}[{chunks_done}/{total_chunks}] "
                f"+{result['event_count']} events ({total_events:,} total) "
                f"[{types_str}] "
                f"({result['elapsed']:.1f}s) "
                f"{rate:.1f} chunks/s, ETA {int(eta)}s"
                f"{warn_str}"
            )
    except KeyboardInterrupt:
        print(f"\n\n{prefix}Interrupted! Collected {total_events:,} events so far.")
        pool.shutdown(wait=False, cancel_futures=True)
        os._exit(1)

    pool.shutdown(wait=True)
    wall_time = time.monotonic() - t0

    # Merge temp parquet files into final output
    print(f"  {prefix}Writing {total_events:,} events to {output_path}...")
    write_t0 = time.monotonic()

    if tmp_files:
        df = pl.concat([pl.read_parquet(f) for f in tmp_files])
        df = df.sort("block_number", "tx_hash")
        df.write_parquet(output_path, compression="zstd")
        del df
        # Cleanup temp files
        for f in tmp_files:
            os.unlink(f)
        os.rmdir(tmp_dir)
    else:
        pl.DataFrame(schema=_SCHEMA).write_parquet(output_path, compression="zstd")
        os.rmdir(tmp_dir)

    write_time = time.monotonic() - write_t0
    file_size = os.path.getsize(output_path) / (1024 * 1024)

    types_summary = ", ".join(f"{k}={v:,}" for k, v in sorted(grand_type_counts.items()))
    print(f"  {prefix}Done in {wall_time:.0f}s — {total_events:,} events ({types_summary}), {file_size:.1f} MB")

    # Fail loud: if any chunk failed or the node kept timing out on it, the output
    # is short/unreliable. Drop an .INCOMPLETE marker and flag it so callers (regen
    # / backfill / a human) never treat this run as good.
    complete = not failed_chunks and not incomplete_chunks
    if not complete:
        marker = output_path + ".INCOMPLETE"
        try:
            with open(marker, "w") as mf:
                mf.write(f"failed_chunks={failed_chunks}\nincomplete_chunks={incomplete_chunks}\n")
        except OSError:
            pass
        bar = "!" * 64
        print(f"\n  {prefix}{bar}")
        print(f"  {prefix}!! EXTRACTION INCOMPLETE — {output_path} IS UNRELIABLE (short data)")
        print(f"  {prefix}!! {len(failed_chunks)} failed chunk(s), {len(incomplete_chunks)} chunk(s) the node "
              f"kept timing out on (min-range splits still timed out)")
        print(f"  {prefix}!! marker: {marker} — re-run when the node is healthy")
        print(f"  {prefix}{bar}\n")

    return {
        "wall_time": wall_time,
        "write_time": write_time,
        "total_events": total_events,
        "type_counts": grand_type_counts,
        "file_size_mb": file_size,
        "complete": complete,
        "failed_chunks": failed_chunks,
        "incomplete_chunks": incomplete_chunks,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract Polymarket events into Parquet"
    )
    parser.add_argument("--from-date", help="Start date (ISO format, e.g. 2026-02-01)")
    parser.add_argument("--to-date", help="End date (ISO format, e.g. 2026-02-15)")
    parser.add_argument("--from-block", type=int, help="Start block number")
    parser.add_argument("--to-block", type=int, help="End block number")
    parser.add_argument("--output", "-o", required=True,
                        help="Output path (dir for date range, file for block range)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    parser.add_argument("--chunk", type=int, default=100, help="Blocks per chunk (default: 100)")
    parser.add_argument("--no-ctf", action="store_true", help="Skip CTF position events")
    parser.add_argument("--no-settlements", action="store_true", help="Skip settlement events")
    parser.add_argument("--transfers", action="store_true", help="Include stablecoin transfers")
    parser.add_argument("--proxy-cache", default="proxy_wallets.txt",
                        help="Proxy wallet cache file (default: proxy_wallets.txt)")
    parser.add_argument("--perspective", choices=["both", "maker", "taker"], default="both",
                        help="Trade perspective filter (default: both)")
    args = parser.parse_args()

    print(f"RPC: {RPC} (timeout: {RPC_TIMEOUT}s)")
    print(f"Events: trades"
          f"{'' if args.no_ctf else ' + ctf'}"
          f"{'' if args.no_settlements else ' + settlements'}"
          f"{' + transfers' if args.transfers else ''}")

    # Tracker config — passed to each worker process for initialization
    tracker_config = dict(
        rpcs=[RPC],
        fetch_timestamps=True,
        track_ctf_events=not args.no_ctf,
        track_settlements=not args.no_settlements,
        track_transfers=args.transfers,
        proxy_cache_path=args.proxy_cache,
        keep_trade_intention=True,
        merge_fills=False,  # keep raw fills for historical analysis
        perspective=args.perspective,
    )

    # --- Block range mode ---
    if args.from_block is not None and args.to_block is not None:
        print()
        result = extract_range(args.from_block, args.to_block, args.output,
                               args.workers, args.chunk, tracker_config)
        if not result.get("complete", True):
            # Non-zero exit so regen_parquets.py / backfill (which check returncode)
            # never accept a short extraction.
            print("\n❌ INCOMPLETE: unrecoverable RPC timeouts — output is UNRELIABLE "
                  "(.INCOMPLETE marker written). Re-run when the node is healthy.")
            sys.exit(2)
        return

    # --- Date range mode ---
    if not args.from_date or not args.to_date:
        print("Specify --from-block/--to-block or --from-date/--to-date")
        sys.exit(1)

    start_date = datetime.fromisoformat(args.from_date).replace(tzinfo=timezone.utc)
    end_date = datetime.fromisoformat(args.to_date).replace(tzinfo=timezone.utc)
    output_dir = args.output

    days = []
    d = start_date
    while d < end_date:
        days.append(d)
        d += timedelta(days=1)

    print(f"\nExtracting {len(days)} days: {args.from_date} → {args.to_date}")
    print(f"Output dir: {output_dir}")
    print(f"Workers: {args.workers}, chunk: {args.chunk}")
    print()

    head = get_chain_head()
    search_lo = max(0, head - 90 * 24 * 60 * 30)

    day_blocks = []
    for d in days:
        day_str = d.strftime("%Y-%m-%d")
        output_path = os.path.join(output_dir, f"events_{day_str}.parquet")

        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path) / (1024 * 1024)
            print(f"  {day_str}: SKIP (exists, {file_size:.1f} MB)")
            continue

        next_day = d + timedelta(days=1)
        ts_from = int(d.timestamp())
        ts_to = int(next_day.timestamp())

        print(f"  Resolving {day_str}...")
        block_from = date_to_block(ts_from, search_lo, head)
        block_to = date_to_block(ts_to, search_lo, head)
        print(f"    blocks {block_from} → {block_to} ({block_to - block_from:,} blocks)")

        day_blocks.append((day_str, block_from, block_to, output_path))
        search_lo = block_from

    if not day_blocks:
        print("\nAll days already extracted!")
        return

    print()

    grand_t0 = time.monotonic()
    grand_events = 0
    incomplete_days: List[str] = []

    for i, (day_str, block_from, block_to, output_path) in enumerate(day_blocks):
        print(f"{'=' * 60}")
        result = extract_range(
            block_from, block_to, output_path,
            args.workers, args.chunk, tracker_config,
            label=f"{day_str} ({i + 1}/{len(day_blocks)})",
        )
        grand_events += result["total_events"]
        if not result.get("complete", True):
            incomplete_days.append(day_str)
        print()

    grand_time = time.monotonic() - grand_t0
    print(f"{'=' * 60}")
    print(f"ALL DONE: {len(day_blocks)} days, {grand_events:,} events in {grand_time:.0f}s")
    if incomplete_days:
        # Non-zero exit so regen_parquets.py / backfill (which check returncode) never
        # accept a short extraction, and a human sees it plainly.
        print(f"\n❌ INCOMPLETE: {len(incomplete_days)} day(s) had unrecoverable RPC timeouts — {incomplete_days}")
        print("   Their parquets are UNRELIABLE (.INCOMPLETE marker written). Re-run when the node is healthy.")
        sys.exit(2)
    print()


if __name__ == "__main__":
    main()

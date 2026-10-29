#!/usr/bin/env python3
"""
Autonomous day-by-day Polymarket backfill driver.

For each UTC day in [--start-date, --end-date] (inclusive), sequentially:
  1. compute the day's Polygon block range,
  2. extract -> Parquet   (extract_events.py, gentle: few workers, small chunks),
  3. load -> Postgres     (load_parquet.py, idempotent).

Designed to run DETACHED overnight. Properties:
  - GENTLE: one day at a time, never parallel days; default 4 workers / chunk 50.
  - RESUMABLE: writes a per-day `<outdir>/day_YYYY-MM-DD.done` marker; on restart it
    skips finished days. Safe to kill/reboot and relaunch — it continues.
  - IDEMPOTENT: the loader upserts, so re-running a day never duplicates.
  - ROBUST: retries extract/load; per-step timeouts so a hung RPC can't stall the night;
    a failing day is logged and skipped (doesn't abort the whole run).

Launch (detached, survives SSH logout):
  cd extract
  nohup setsid ./venv/bin/python backfill_driver.py \
      --start-date 2026-06-29 --end-date 2026-07-28 \
      > archive/driver.log 2>&1 < /dev/null &

Check progress:  tail -f archive/driver.log   |   ls archive/*.done
"""
import argparse
import json
import os
import sys
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta, timezone


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}Z] {msg}", flush=True)


def rpc(method, params, url, retries=5):
    body = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": 1}).encode()
    for i in range(retries):
        try:
            req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
            return json.load(urllib.request.urlopen(req, timeout=30))["result"]
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(3)


def block_at_ts(target, url, tip):
    lo, hi = 80_000_000, tip
    while lo < hi:
        mid = (lo + hi) // 2
        if int(rpc("eth_getBlockByNumber", [hex(mid), False], url)["timestamp"], 16) < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def run(cmd, timeout):
    log("RUN: " + " ".join(cmd))
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        log(f"  TIMEOUT after {timeout}s")
        return False
    if r.returncode != 0:
        log(f"  FAILED (exit {r.returncode}): {(r.stderr or '')[-600:]}")
        return False
    tail = (r.stdout or "").strip().splitlines()
    log("  OK: " + (tail[-1] if tail else ""))
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True, help="inclusive, YYYY-MM-DD (UTC)")
    ap.add_argument("--end-date", required=True, help="inclusive, YYYY-MM-DD (UTC)")
    ap.add_argument("--outdir", default="archive", help="where day_YYYY-MM-DD.parquet files go")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--chunk", type=int, default=50)
    ap.add_argument("--rpc", default=os.environ.get("RPC", "http://localhost:8545"), help="Polygon JSON-RPC endpoint of a full node")
    ap.add_argument("--pg-dsn", default="", help="optional: load each day into Postgres via load_parquet.py (not included in this repo); empty = skip")
    ap.add_argument("--py", default=sys.executable)
    ap.add_argument("--table", default="events",
                    help="Trades table to load into (default: events; use events_new for shadow migration)")
    ap.add_argument("--trades-only", action="store_true",
                    help="Postgres load only: load only trades into --table")
    ap.add_argument("--transfers", action="store_true",
                    help="also extract stablecoin transfers / money flows (needs the wallet registry, not published)")
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--extract-timeout", type=int, default=2400)  # 40 min
    ap.add_argument("--load-timeout", type=int, default=2400)
    ap.add_argument("--reverse", action="store_true", help="process newest day first")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    d0 = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    d1 = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)
    days = []
    d = d0
    while d <= d1:
        days.append(d)
        d += timedelta(days=1)
    if args.reverse:
        days.reverse()

    log(f"=== driver start: {len(days)} days {args.start_date}..{args.end_date} "
        f"workers={args.workers} chunk={args.chunk} outdir={args.outdir} ===")
    done_count = 0
    for d in days:
        ds = d.strftime("%Y-%m-%d")
        done_marker = os.path.join(args.outdir, f"day_{ds}.done")
        pq = os.path.join(args.outdir, f"day_{ds}.parquet")
        if os.path.exists(done_marker):
            log(f"[{ds}] SKIP (already done)")
            done_count += 1
            continue

        t0 = time.time()
        try:
            tip = int(rpc("eth_blockNumber", [], args.rpc), 16)
            fb = block_at_ts(int(d.timestamp()), args.rpc, tip)
            tb = block_at_ts(int((d + timedelta(days=1)).timestamp()), args.rpc, tip) - 1
        except Exception as e:
            log(f"[{ds}] block-range lookup failed: {e} — skipping day")
            continue
        log(f"[{ds}] blocks {fb}..{tb} ({tb - fb + 1})")

        # extract (skip if a completed parquet already exists from a prior run)
        if os.path.exists(pq) and os.path.getsize(pq) > 1024:
            log(f"[{ds}] parquet exists ({os.path.getsize(pq)//1024//1024}MB) — skipping extraction")
            extracted = True
        else:
            extracted = False
            cmd = [args.py, os.path.join(os.path.dirname(os.path.abspath(__file__)), "universal_extract_events.py"),
                   "--from-block", str(fb), "--to-block", str(tb), "-o", pq,
                   "--workers", str(args.workers), "--chunk", str(args.chunk)]
            if args.transfers:
                cmd.append("--transfers")
            for attempt in range(1, args.retries + 2):
                if run(cmd, args.extract_timeout):
                    extracted = True
                    break
                log(f"[{ds}] extract attempt {attempt} failed; retry in 15s")
                time.sleep(15)
        if not extracted:
            log(f"[{ds}] EXTRACT FAILED after retries — skipping day")
            continue

        # load (optional)
        loaded = not args.pg_dsn
        load_cmd = [args.py, "load_parquet.py", pq, "--pg-dsn", args.pg_dsn, "--table", args.table]
        if args.trades_only:
            load_cmd.append("--trades-only")
        for attempt in range(1, args.retries + 2):
            if loaded:
                break
            if run(load_cmd, args.load_timeout):
                loaded = True
                break
            log(f"[{ds}] load attempt {attempt} failed; retry in 15s")
            time.sleep(15)
        if not loaded:
            log(f"[{ds}] LOAD FAILED after retries — parquet kept, no .done marker; will retry next run")
            continue

        with open(done_marker, "w") as f:
            f.write(f"{ds} blocks {fb}..{tb} loaded {datetime.now(timezone.utc).isoformat()}\n")
        done_count += 1
        log(f"[{ds}] DONE in {int(time.time() - t0)}s  ({done_count}/{len(days)})")

    log(f"=== driver finished: {done_count}/{len(days)} days complete ===")


if __name__ == "__main__":
    main()

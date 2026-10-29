import os
#!/usr/bin/env python3
"""Add a Parquet bloom filter to archive day files so cold wallet/asset queries
prune row-groups (~10x on multi-day ranges). Re-COPY each file via DuckDB with
WRITE_BLOOM_FILTER + zstd (keeps file sizes ~like polars). SAFE: write
day_X.parquet.bloomnew, verify (row count unchanged + bloom present on
wallet_address), then ATOMIC os.replace over the original (router never reads a
half-written file). Idempotent (skips already-bloomed), resumable.
Usage: bloomify_archive.py [YYYY-MM-DD ...]  (empty = all files)."""
import duckdb, glob, os, sys, time

BACKFILL = os.environ.get("ARCHIVE_DIR", "archive")
con = duckdb.connect()
con.execute("PRAGMA threads=6")


def bloom_cols(f):
    """columns in this file that carry a bloom filter (any row-group)."""
    rows = con.execute(
        "SELECT path_in_schema, count(bloom_filter_offset) FROM parquet_metadata(?) "
        "GROUP BY path_in_schema HAVING count(bloom_filter_offset) > 0", [f]).fetchall()
    return {r[0] for r in rows}


targets = sys.argv[1:]
files = sorted(glob.glob(f"{BACKFILL}/day_*.parquet"))
if targets:
    files = [f for f in files if any(d in f for d in targets)]

ok = skip = fail = 0
for f in files:
    day = os.path.basename(f).replace("day_", "").replace(".parquet", "")
    try:
        if "wallet_address" in bloom_cols(f):
            print(f"[{day}] already bloomed — skip", flush=True); skip += 1; continue
    except Exception as e:
        print(f"[{day}] metadata check failed {repr(e)[:100]} — skip", flush=True); fail += 1; continue
    old_rows = con.execute("SELECT count(*) FROM read_parquet(?)", [f]).fetchone()[0]
    newf = f + ".bloomnew"
    if os.path.exists(newf):
        os.unlink(newf)
    t = time.monotonic()
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet('{f}')) TO '{newf}' "
                    f"(FORMAT parquet, WRITE_BLOOM_FILTER true, COMPRESSION zstd, ROW_GROUP_SIZE 122880)")
    except Exception as e:
        print(f"[{day}] COPY failed {repr(e)[:150]}", flush=True)
        if os.path.exists(newf):
            os.unlink(newf)
        fail += 1; continue
    new_rows = con.execute("SELECT count(*) FROM read_parquet(?)", [newf]).fetchone()[0]
    bc = bloom_cols(newf)
    if new_rows != old_rows or "wallet_address" not in bc:
        print(f"[{day}] GUARD FAIL rows {new_rows}!={old_rows} or no wallet bloom ({bc}) — KEEP OLD", flush=True)
        os.unlink(newf); fail += 1; continue
    os.replace(newf, f)
    print(f"[{day}] BLOOMED rows={new_rows} {os.path.getsize(f)/1e6:.0f}MB bloom_cols={sorted(bc)} ({time.monotonic()-t:.1f}s)", flush=True)
    ok += 1
print(f"DONE ok={ok} skip={skip} fail={fail}", flush=True)

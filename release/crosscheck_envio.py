import duckdb, time, os
os.chdir(os.environ.get("RELEASE_DIR", "release_out"))
con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='8GB'"); con.execute("INSTALL httpfs; LOAD httpfs")
t0 = time.time()
theirs = "hf://datasets/moose-code/polymarket-onchain-v1/order_filled/year=2026/month=03.parquet"
print("their schema:", [(r[0], r[1]) for r in con.execute(f"describe select * from read_parquet('{theirs}')").fetchall()], flush=True)
print("their March 2026 fills:", con.execute(f"select count(*) from read_parquet('{theirs}')").fetchone()[0], f"{time.time()-t0:.0f}s", flush=True)
th = con.execute(f"select cast(to_timestamp(cast(timestamp as bigint)) as date) d, count(*) n from read_parquet('{theirs}') group by 1 order by 1").fetchall()
print("their by day (first/last):", th[:2], th[-2:], f"{time.time()-t0:.0f}s", flush=True)
ours = con.execute("""select cast(block_timestamp as date) d, count(distinct (tx_hash, log_index)) fill_logs,
                      count(*) filter (where wallet_role='maker') maker_rows, count(*) filter (where wallet_role='taker') taker_rows
                      from read_parquet('data/year=2026/day_2026-03-*.parquet') where event_type='trade' group by 1 order by 1""").fetchall()
T = dict(th)
print("day | our_fill_logs | our_maker_rows | our_taker_rows | their_fills | diff(logs-theirs)")
for d, f, m, t in ours:
    print(f" {d} | {f:,} | {m:,} | {t:,} | {T.get(d, 0):,} | {f - T.get(d, 0):+,}")
print("TOTAL ours fill_logs", f"{sum(f for _, f, _, _ in ours):,}", "theirs", f"{sum(T.values()):,}", f"{time.time()-t0:.0f}s", flush=True)

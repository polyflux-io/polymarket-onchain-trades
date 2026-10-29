"""Compare our daily OrderFilled log counts with SII-WANGZJ/Polymarket_data orderfilled.parquet for a V1 month
(Jan 2026) and a V2 month (Jun 2026). Reads only timestamp (+contract) columns remotely."""
import duckdb, time, os
os.chdir(os.environ.get("RELEASE_DIR", "release_out"))
con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='10GB'"); con.execute("INSTALL httpfs; LOAD httpfs")
t0 = time.time()
theirs = "hf://datasets/SII-WANGZJ/Polymarket_data/orderfilled.parquet"
print("their schema:", [(r[0], r[1]) for r in con.execute(f"describe select * from read_parquet('{theirs}')").fetchall()], flush=True)
print("their total rows:", con.execute(f"select count(*) from read_parquet('{theirs}')").fetchone()[0], f"{time.time()-t0:.0f}s", flush=True)
for label, lo, hi, ours_glob in (("V1 Jan 2026", "2026-01-01", "2026-02-01", "data/year=2026/day_2026-01-*.parquet"),
                                 ("V2 Jun 2026", "2026-06-01", "2026-07-01", "data/year=2026/day_2026-06-*.parquet")):
    th = con.execute(f"""select cast(to_timestamp(cast(timestamp as bigint)) as date) d, count(*) n,
                         count(*) filter (where taker in ('0xe111180000d2663c0091e4f400237545b87b996b','0xe2222d279d744050d28e00520010520000310f59',
                                                         '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e','0xc5d563a36ae78145c45a50134d48a1215220f80a')) agg
                         from read_parquet('{theirs}') where timestamp >= epoch(timestamp '{lo}') and timestamp < epoch(timestamp '{hi}') group by 1 order by 1""").fetchall()
    T = {str(d): (n, a) for d, n, a in th}
    ours = con.execute(f"""select cast(block_timestamp as date) d, count(*) filter (where wallet_role='maker') logs,
                           count(*) filter (where wallet_role='maker' and counterparty is null) agg
                           from read_parquet('{ours_glob}') where event_type='trade' group by 1 order by 1""").fetchall()
    print(f"== {label}: day | our_logs | their_rows | diff | our_agg | their_agg(taker=exchange)", flush=True)
    tot_o = tot_t = 0
    for d, o, oa in ours:
        n, a = T.get(str(d), (0, 0)); tot_o += o; tot_t += n
        print(f"  {d} | {o:,} | {n:,} | {o-n:+,} | {oa:,} | {a:,}", flush=True)
    print(f"  TOTAL ours {tot_o:,} theirs {tot_t:,} diff {tot_o-tot_t:+,}  ({time.time()-t0:.0f}s)", flush=True)

import os
import duckdb, json
con=duckdb.connect(); con.execute("SET threads=4; SET memory_limit='8GB'")
r=con.execute("""select count(*) filter (where wallet_role='maker') fill_logs, count(*) filter (where wallet_role='taker') taker_rows,
                 count(*) filter (where wallet_role='maker' and counterparty is null) aggregate_rows,
                 round(sum(size*price) filter (where wallet_role='taker')) taker_volume_usd
                 from read_parquet(os.environ.get('RELEASE_DIR','release_out') + '/data/year=*/day_*.parquet') where event_type='trade'""").fetchone()
d={"fill_logs":r[0],"taker_rows":r[1],"aggregate_taker_rows":r[2],"taker_volume_usd":r[3]}
json.dump(d,open(os.environ.get("RELEASE_DIR","release_out") + "/_work/trade_counts.json","w")); print("DONE",d,flush=True)

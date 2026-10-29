import duckdb, glob, os, time
SRC=os.environ.get("ARCHIVE_DIR", "archive"); DST=os.environ.get("RELEASE_DIR", "release_out") + "/data"
files=sorted(glob.glob(f"{SRC}/day_*.parquet"))
con=duckdb.connect(); con.execute("SET threads=4; SET memory_limit='8GB'; SET preserve_insertion_order=false")
KEEP=["event_type","wallet_address","asset_id","condition_id","block_number","block_timestamp","tx_hash","log_index",
      "operation_type","size","price","fee","counterparty","wallet_role","order_hash","intent_adjusted","exchange",
      "payout_usdc","oracle","payout_rate"]
t0=time.time(); done=0
for f in files:
    day=os.path.basename(f)[4:14]; year=day[:4]
    outdir=f"{DST}/year={year}"; os.makedirs(outdir, exist_ok=True)
    out=f"{outdir}/day_{day}.parquet"
    if os.path.exists(out): continue
    cols=", ".join(c if c!="block_timestamp" else "try_cast(block_timestamp as TIMESTAMP) as block_timestamp" for c in KEEP)
    con.execute(f"""copy (select {cols} from read_parquet('{f}')
                    where event_type in ('trade','split','merge','redeem','settlement')
                    order by block_number, log_index)
                 to '{out}.tmp' (format parquet, compression zstd, row_group_size 1000000)""")
    os.replace(f"{out}.tmp", out); done+=1
    if done%50==0: print(f"{done} files, {time.time()-t0:.0f}s", flush=True)
print(f"DONE {done} files in {time.time()-t0:.0f}s", flush=True)

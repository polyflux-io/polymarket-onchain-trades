"""Fill the dataset card placeholders from the release stats and write README.md into the release dir."""
import json, glob, os, datetime
OUT = os.environ.get("RELEASE_DIR", "release_out"); st = json.load(open(f"{OUT}/_work/stats.json"))
files = sorted(glob.glob(f"{OUT}/data/year=*/day_*.parquet"))
size_gb = sum(os.path.getsize(f) for f in files) / 1e9
end_date = os.path.basename(files[-1])[4:14]
n_markets = 0
try:
    import duckdb
    n_markets = duckdb.connect().execute(f"select count(*) from read_parquet('{OUT}/markets.parquet')").fetchone()[0]
except Exception: pass
s = open(os.environ.get("CARD_TEMPLATE", "docs/DATASET_CARD.md")).read()
rep = {"__END_DATE__": end_date, "__N_ROWS__": f"{st['n_rows']:,}", "__N_TRADES__": f"{st['n_trades']:,}",
       "__N_FILES__": f"{len(files):,}", "__SIZE_GB__": f"{size_gb:.0f}", "__N_WALLETS__": f"{st['n_wallets']:,}",
       "__N_ASSETS__": f"{st['n_assets']:,}", "__N_MARKETS__": f"{n_markets:,}",
       "__BUILD_DATE__": datetime.date.today().isoformat()}
for k, v in rep.items(): s = s.replace(k, v)
left = [k for k in rep if k in s] + [t for t in ("__PRODUCT_URL__",) if t in s]
open(f"{OUT}/README.md", "w").write(s)
print("README written; unresolved placeholders:", left or "none"); print({k: v for k, v in rep.items()})

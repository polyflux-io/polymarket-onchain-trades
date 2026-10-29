"""Markets + assets tables, streaming/low-memory version. Resumes from _work/markets.jsonl.
Memory: only the set of covered token ids is held in Python; tables are assembled in DuckDB."""
import duckdb, json, os, time, threading, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
OUT = os.environ.get("RELEASE_DIR", "release_out"); WORK = f"{OUT}/_work"
SCAN = f"{WORK}/assets_scan.parquet"; CKPT = f"{WORK}/markets.jsonl"
con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='10GB'")
# ids to resolve = scanned assets not yet covered by a checkpointed market
covered = set()
if os.path.exists(CKPT):
    for line in open(CKPT):
        try:
            for t in json.loads(line)["token_ids"]: covered.add(t)
        except Exception: continue
print(f"resumed: {len(covered):,} tokens already covered", flush=True)
ids = [r[0] for r in con.execute(f"select asset_id from read_parquet('{SCAN}')").fetchall()]
print(f"{len(ids):,} assets total", flush=True)
lock = threading.Lock(); ck = open(CKPT, "a"); n_markets_new = 0

def fetch(batch, closed):
    url = f"https://gamma-api.polymarket.com/markets?limit=300&closed={closed}&" + "&".join("clob_token_ids=" + i for i in batch)
    for attempt in range(6):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "polyflux-dataset/1.0"}), timeout=60) as r:
                return json.load(r)
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return None

def handle(batch, closed):
    global n_markets_new
    res = fetch(batch, closed)
    if res is None: return
    recs = []
    for m in res:
        toks = json.loads(m.get("clobTokenIds") or "[]"); prices = json.loads(m.get("outcomePrices") or "[]"); ev = (m.get("events") or [{}])[0]
        recs.append({"condition_id": (m.get("conditionId") or "").lower().removeprefix("0x"), "question": m.get("question"), "slug": m.get("slug"),
                     "event_slug": ev.get("slug"), "event_title": ev.get("title"), "outcomes": json.loads(m.get("outcomes") or "[]"), "token_ids": toks,
                     "outcome_prices": [float(p) for p in prices] if prices else None, "closed": str(m.get("closed")).lower() == "true",
                     "resolved": m.get("umaResolutionStatus") == "resolved", "end_date": m.get("endDate"),
                     "neg_risk": str(m.get("negRisk")).lower() == "true", "created_at": m.get("createdAt")})
    with lock:
        for mk in recs:
            if mk["token_ids"] and all(t in covered for t in mk["token_ids"]): continue
            ck.write(json.dumps(mk) + "\n"); covered.update(mk["token_ids"]); n_markets_new += 1
        ck.flush()

B = 60; W = 6; t0 = time.time()
for closed in ("true", "false"):
    todo = [i for i in ids if i not in covered]
    print(f"pass closed={closed}: {len(todo):,} tokens to resolve", flush=True)
    batches = [todo[k:k + B] for k in range(0, len(todo), B)]; done = 0
    with ThreadPoolExecutor(max_workers=W) as pool:
        for _ in as_completed([pool.submit(handle, b, closed) for b in batches]):
            done += 1
            if done % 1000 == 0:
                print(f"  closed={closed}: {done*B:,}/{len(todo):,} tokens, +{n_markets_new:,} markets this run, {len(covered):,} covered, {time.time()-t0:.0f}s", flush=True)
ck.close(); del ids; del covered
print("assembling tables in duckdb", flush=True)
con.execute(f"""create or replace table markets as
    select condition_id, question, slug, event_slug, event_title, outcomes, token_ids, outcome_prices, closed, resolved, end_date, neg_risk, created_at
    from (select *, row_number() over (partition by condition_id order by resolved desc) rn
          from read_json('{CKPT}', format='newline_delimited', maximum_object_size=1048576,
               columns={{'condition_id':'VARCHAR','question':'VARCHAR','slug':'VARCHAR','event_slug':'VARCHAR','event_title':'VARCHAR',
                        'outcomes':'VARCHAR[]','token_ids':'VARCHAR[]','outcome_prices':'DOUBLE[]','closed':'BOOLEAN','resolved':'BOOLEAN',
                        'end_date':'VARCHAR','neg_risk':'BOOLEAN','created_at':'VARCHAR'}})) where rn=1""")
con.execute(f"copy markets to '{OUT}/markets.parquet' (format parquet, compression zstd)")
con.execute("""create or replace table tok as
    select u.token as asset_id, m.condition_id, u.idx - 1 as outcome_index,
           m.outcomes[u.idx] as outcome, case when m.resolved then m.outcome_prices[u.idx] end as resolved_payout, m.question, m.slug
    from markets m, unnest(m.token_ids) with ordinality as u(token, idx)""")
con.execute(f"""create or replace table assets as
    select s.asset_id, coalesce(t.condition_id, s.condition_id_seen) condition_id, t.outcome_index, t.outcome, t.resolved_payout,
           t.question, t.slug, s.first_seen, s.last_seen, s.n_trades, case when t.asset_id is not null then 'gamma' end as source
    from read_parquet('{SCAN}') s left join tok t using (asset_id)""")
con.execute(f"copy assets to '{OUT}/assets.parquet' (format parquet, compression zstd)")
r = con.execute("select count(*), count(*) filter (where source='gamma') from assets").fetchone()
print(f"DONE assets={r[0]:,} gamma-covered={r[1]:,} ({r[1]/r[0]:.1%}) markets={con.execute('select count(*) from markets').fetchone()[0]:,} in {time.time()-t0:.0f}s", flush=True)

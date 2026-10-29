"""Convert the Gamma catalog dump (one event per line, markets nested) into two parquet tables:
events.parquet (one row per event) and markets_full.parquet (one row per market, with the text fields)."""
import json, os, time
import pyarrow as pa, pyarrow.parquet as pq
SRC = os.environ.get("CATALOG_JSONL", "/nvme/catalog/all_events_full.jsonl")
OUT = os.environ.get("RELEASE_DIR", "/nvme/hf_release")

def f(v):
    try: return None if v in (None, "") else float(v)
    except Exception: return None
def b(v): return None if v is None else (str(v).lower() == "true")
def s(v): return None if v is None else str(v)
def jl(v):
    if v is None: return None
    if isinstance(v, str):
        try: v = json.loads(v)
        except Exception: return [v]
    return [str(x) for x in v]

EV = pa.schema([("event_id", pa.string()), ("slug", pa.string()), ("title", pa.string()), ("description", pa.string()),
    ("tags", pa.list_(pa.string())), ("ticker", pa.string()), ("start_date", pa.string()), ("end_date", pa.string()),
    ("created_at", pa.string()), ("updated_at", pa.string()), ("closed", pa.bool_()), ("active", pa.bool_()), ("archived", pa.bool_()),
    ("neg_risk", pa.bool_()), ("enable_neg_risk", pa.bool_()), ("resolution_source", pa.string()), ("n_markets", pa.int32()),
    ("volume", pa.float64()), ("liquidity", pa.float64()), ("open_interest", pa.float64()), ("comment_count", pa.int64())])
MK = pa.schema([("market_id", pa.string()), ("condition_id", pa.string()), ("question_id", pa.string()), ("slug", pa.string()),
    ("question", pa.string()), ("description", pa.string()), ("outcomes", pa.list_(pa.string())), ("outcome_prices", pa.list_(pa.string())),
    ("clob_token_ids", pa.list_(pa.string())), ("group_item_title", pa.string()), ("event_id", pa.string()), ("event_slug", pa.string()),
    ("event_title", pa.string()), ("tags", pa.list_(pa.string())), ("resolution_source", pa.string()), ("resolved_by", pa.string()),
    ("uma_resolution_status", pa.string()), ("uma_bond", pa.float64()), ("uma_reward", pa.float64()), ("uma_end_date", pa.string()),
    ("start_date", pa.string()), ("end_date", pa.string()), ("created_at", pa.string()), ("updated_at", pa.string()), ("closed_time", pa.string()),
    ("closed", pa.bool_()), ("active", pa.bool_()), ("archived", pa.bool_()), ("neg_risk", pa.bool_()), ("enable_order_book", pa.bool_()),
    ("fee_type", pa.string()), ("maker_base_fee", pa.float64()), ("taker_base_fee", pa.float64()), ("order_min_size", pa.float64()),
    ("order_price_min_tick_size", pa.float64()), ("volume", pa.float64()), ("volume_clob", pa.float64()), ("liquidity", pa.float64()),
    ("last_trade_price", pa.float64()), ("best_ask", pa.float64()), ("spread", pa.float64())])

ew = pq.ParquetWriter(f"{OUT}/events.parquet", EV, compression="zstd")
mw = pq.ParquetWriter(f"{OUT}/markets_full.parquet", MK, compression="zstd")
ebuf, mbuf, t0, ne, nm = [], [], time.time(), 0, 0
def flush():
    global ebuf, mbuf
    if ebuf: ew.write_table(pa.Table.from_pylist(ebuf, schema=EV)); ebuf = []
    if mbuf: mw.write_table(pa.Table.from_pylist(mbuf, schema=MK)); mbuf = []
with open(SRC) as fh:
    for line in fh:
        try: e = json.loads(line)
        except Exception: continue
        tags = [t.get("label") for t in (e.get("tags") or []) if isinstance(t, dict) and t.get("label")]
        ms = e.get("markets") or []
        ebuf.append({"event_id": s(e.get("id")), "slug": e.get("slug"), "title": e.get("title"), "description": e.get("description"), "tags": tags,
            "ticker": e.get("ticker"), "start_date": e.get("startDate"), "end_date": e.get("endDate"), "created_at": e.get("createdAt"), "updated_at": e.get("updatedAt"),
            "closed": b(e.get("closed")), "active": b(e.get("active")), "archived": b(e.get("archived")), "neg_risk": b(e.get("negRisk")), "enable_neg_risk": b(e.get("enableNegRisk")),
            "resolution_source": e.get("resolutionSource"), "n_markets": len(ms), "volume": f(e.get("volume")), "liquidity": f(e.get("liquidity")),
            "open_interest": f(e.get("openInterest")), "comment_count": int(e.get("commentCount") or 0)})
        ne += 1
        for m in ms:
            cid = (m.get("conditionId") or "").lower().removeprefix("0x") or None
            mbuf.append({"market_id": s(m.get("id")), "condition_id": cid, "question_id": m.get("questionID"), "slug": m.get("slug"), "question": m.get("question"),
                "description": m.get("description"), "outcomes": jl(m.get("outcomes")), "outcome_prices": jl(m.get("outcomePrices")), "clob_token_ids": jl(m.get("clobTokenIds")),
                "group_item_title": m.get("groupItemTitle"), "event_id": s(e.get("id")), "event_slug": e.get("slug"), "event_title": e.get("title"), "tags": tags,
                "resolution_source": m.get("resolutionSource"), "resolved_by": m.get("resolvedBy"), "uma_resolution_status": m.get("umaResolutionStatus"),
                "uma_bond": f(m.get("umaBond")), "uma_reward": f(m.get("umaReward")), "uma_end_date": m.get("umaEndDate"), "start_date": m.get("startDate"),
                "end_date": m.get("endDate"), "created_at": m.get("createdAt"), "updated_at": m.get("updatedAt"), "closed_time": m.get("closedTime"),
                "closed": b(m.get("closed")), "active": b(m.get("active")), "archived": b(m.get("archived")), "neg_risk": b(m.get("negRisk")),
                "enable_order_book": b(m.get("enableOrderBook")), "fee_type": m.get("feeType"), "maker_base_fee": f(m.get("makerBaseFee")), "taker_base_fee": f(m.get("takerBaseFee")),
                "order_min_size": f(m.get("orderMinSize")), "order_price_min_tick_size": f(m.get("orderPriceMinTickSize")), "volume": f(m.get("volumeNum") or m.get("volume")),
                "volume_clob": f(m.get("volumeClob")), "liquidity": f(m.get("liquidity")), "last_trade_price": f(m.get("lastTradePrice")), "best_ask": f(m.get("bestAsk")), "spread": f(m.get("spread"))})
            nm += 1
        if len(mbuf) >= 50000: flush()
        if ne % 100000 == 0: print(f"{ne:,} events, {nm:,} markets, {time.time()-t0:.0f}s", flush=True)
flush(); ew.close(); mw.close()
print(f"DONE events={ne:,} markets={nm:,} in {time.time()-t0:.0f}s; sizes: events {os.path.getsize(f'{OUT}/events.parquet')/1e6:.0f} MB, markets_full {os.path.getsize(f'{OUT}/markets_full.parquet')/1e6:.0f} MB", flush=True)

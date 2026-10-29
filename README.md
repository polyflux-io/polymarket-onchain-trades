# polymarket-onchain-trades

Pipeline behind the **[Polymarket on-chain trades, 2022-2026](https://huggingface.co/datasets/ieshuaganocry/polymarket-onchain-trades)**
dataset (DOI [10.57967/hf/11293](https://doi.org/10.57967/hf/11293)): every Polymarket fill,
split, merge, redemption and settlement on Polygon, extracted from a full node into one
parquet file per day, plus the scripts that built the published release and verified it
against independent indexers.

Published by Polyflux Research. Code is MIT; the dataset is CC BY 4.0.

## What is here

```
extract/
  universal_extract_events.py   era-agnostic day extractor: runs a V1 parser and a V2 parser over a
                                block range in parallel chunks and writes one parquet (trades, splits,
                                merges, redeems, settlements; optional money flows)
  tracker_v2.py                 V2-era parser (CTF Exchange V2, Neg Risk Exchange V2, pUSD, collateral adapters)
  tracker_optimized.py          V1-era parser (CTF Exchange, Neg Risk Exchange, USDC.e)
  backfill_driver.py            per-day orchestration: block range from timestamps, extraction with
                                retries, .done markers, resumable
  bloomify_archive.py           adds parquet bloom filters on wallet/asset columns for fast point lookups
release/
  build_release.py              filter + repartition the archive into data/year=YYYY/day_*.parquet (zstd)
  build_markets3.py             assets.parquet + markets.parquet via Polymarket's Gamma API, resumable
  build_catalog.py              Gamma catalog JSONL (events with nested markets) -> events.parquet + markets_full.parquet
  count_logs.py                 OrderFilled log / fill / volume totals for the card
  crosscheck_envio.py           per-day fill counts vs moose-code/polymarket-onchain-v1 (March 2026)
  crosscheck_sii.py             per-day fill counts vs SII-WANGZJ/Polymarket_data (Jan + Jun 2026)
  fill_card.py                  fills the dataset card placeholders from the release stats
  upload_data.py                year-by-year upload to the Hub (resumable)
docs/
  DATASET_CARD.md               the published dataset card: schema, row semantics, caveats, comparison
```

## Reproduce a day

You need a Polygon full node (or an archive-capable RPC that serves `eth_getLogs` and receipts
for the block range; a free public endpoint will rate-limit you into the ground).

```bash
python3 -m venv venv && . venv/bin/activate
pip install -r requirements.txt
export RPC=http://localhost:8545

# one UTC day, written to archive/day_2026-06-17.parquet with a .done marker
python extract/backfill_driver.py --start-date 2026-06-17 --end-date 2026-06-17 --workers 8 --chunk 50
```

What the driver does per day: resolves the first and last block of the UTC day by timestamp,
runs `universal_extract_events.py` over that block range in parallel chunks, retries chunks
that fail or come back empty from the RPC layer, and writes a `.done` marker only when the
extraction completed. Re-running skips finished days. A day on mainnet in 2026 is roughly
50 to 100 million rows and takes 15 to 40 minutes on a 16-core box next to the node.

`--transfers` additionally extracts stablecoin deposits, withdrawals and transfers with a
money-flow classification. That path needs a Polymarket wallet registry
(`registry/pm_wallets.bin`) that is not published; without it the extractor warns and skips
those rows. The published dataset does not include them.

## Build the release from an archive

```bash
export ARCHIVE_DIR=archive RELEASE_DIR=release_out
python release/build_release.py                 # trades/split/merge/redeem/settlement only, zstd, daily, hive by year
python release/build_markets3.py                # assets.parquet + markets.parquet (Gamma lookups, ~10M tokens, resumable)
CATALOG_JSONL=catalog.jsonl python release/build_catalog.py   # optional: full Gamma catalog -> events/markets_full
python release/count_logs.py                    # totals used in the card
python release/fill_card.py                     # docs/DATASET_CARD.md -> release_out/README.md with real numbers
HF_TOKEN=... HF_REPO=you/your-dataset python release/upload_data.py
```

Every heavy step was run on the production box as a capped systemd transient unit
(`systemd-run --nice=19 -p IOSchedulingClass=idle -p CPUQuota=400% -p MemoryMax=12G ...`)
so it could not starve the node; do the same if the node shares the machine.

## Verification

`release/crosscheck_envio.py` and `release/crosscheck_sii.py` compare per-day `OrderFilled`
log counts in the release against two independently built public datasets, reading only the
timestamp columns remotely with DuckDB over `hf://`. Results for the published release:

- vs Envio HyperIndex (`moose-code/polymarket-onchain-v1`), all 31 days of March 2026:
  identical on every day, 320,404,950 in total.
- vs `SII-WANGZJ/Polymarket_data`, January 2026 and June 2026: identical on every day through
  2026-06-24 including the exchange's aggregate taker-order events; from 2026-06-25 the
  per-maker fill logs remain identical and the only difference is that this archive no longer
  carries the aggregate events (see "Row semantics").

## Row semantics you must know before summing anything

The exchange emits N `OrderFilled` logs per match (one per maker order) plus one aggregate
log for the taker order. The archive has two rows per maker log (maker side and taker side)
and, up to block 89,093,736 (2026-06-24 23:59:59 UTC), one extra maker-role row with
`counterparty = null` for the aggregate log. Summing all maker rows therefore double counts
volume before that date. **Use `wallet_role = 'taker'` rows for volume and fill counts**;
they are consistent across the whole period. Splits and merges are two rows per log (one per
outcome token) and 98 to 99% of them belong to exchange contracts, not users. The full
treatment, with contract addresses, is in `docs/DATASET_CARD.md`.

## Contact

Polyflux Research, ign@polyflux.io, or a discussion on the dataset page.

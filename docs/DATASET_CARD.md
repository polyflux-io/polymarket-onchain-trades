---
license: cc-by-4.0
pretty_name: Polymarket on-chain trades, 2022-2026
language:
- en
task_categories:
- time-series-forecasting
- tabular-classification
- tabular-regression
tags:
- prediction-markets
- polymarket
- polygon
- blockchain
- on-chain
- onchain
- crypto
- defi
- finance
- trading
- market-microstructure
- orderfilled
- conditional-tokens
- ctf
- backtesting
- duckdb
- usdc
size_categories:
- 1B<n<10B
configs:
- config_name: trades
  default: true
  data_files:
  - split: train
    path: data/year=*/day_*.parquet
- config_name: assets
  data_files:
  - split: train
    path: assets.parquet
- config_name: markets
  data_files:
  - split: train
    path: markets.parquet
---

# Polymarket on-chain trades, 2022-2026

[![License: CC BY 4.0](https://img.shields.io/badge/License-CC_BY_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)
[![DOI](https://img.shields.io/badge/DOI-10.57967%2Fhf%2F11293-blue.svg)](https://doi.org/10.57967/hf/11293)

**TL;DR.** Every Polymarket fill, split, merge, redemption and settlement on Polygon from the
first CLOB trade (2022-11-21) to 2026-10-26: 4.9 billion rows, 1.32 billion economic fills,
$46.7 billion traded, 3.27 million wallets, 10.1 million outcome tokens, in 1,436 daily parquet
files plus `assets` and `markets` tables that turn token ids into questions and outcomes.
Extracted from our own Polygon full node; per-day fill counts verified identical to two
independent indexers. The only public dataset with the conditional-token lifecycle through the
V2 era. Count a month of fills without downloading anything:

```bash
duckdb -c "SELECT count(*) FROM read_parquet('hf://datasets/ieshuaganocry/polymarket-onchain-trades/data/year=2026/day_2026-06-*.parquet') WHERE event_type='trade' AND wallet_role='taker'"
```

Every Polymarket fill, position split, merge, redemption and market settlement recorded on
Polygon from the first trade on 2022-11-21 to 2026-10-26, extracted directly from a full
Polygon node and cross-checked against independent indexers. One parquet file per day, plus
two dimension tables that turn opaque token ids into market questions and outcomes.

| | |
|---|---|
| Rows | 4,896,549,900; trade rows 3,224,253,884 = 1,904,926,259 maker-side rows + 1,319,327,625 taker rows |
| Economic fills | 1,319,327,625 (one per taker row). 585,598,634 of the maker-side rows are the exchange's aggregate taker-order events, carried only through 2026-06-24 (see "What a row is") |
| Traded volume | $46.7 billion USDC (sum of size × price over taker rows) |
| Period | 2022-11-21 to 2026-10-26, no gaps |
| Files | 1,436 daily parquet files, 143 GB (zstd) |
| Distinct wallets | 3,267,002 |
| Distinct outcome tokens | 10,116,827 in 4,693,079 markets |
| Source | Polygon mainnet, own full node (Bor + Heimdall), receipts and logs |
| Verified | Per-day fill counts identical to two independently built datasets (Envio, March 2026; SII, January and June 2026) and to Dune on every sampled day |
| License | CC BY 4.0 |

## Why this exists, and what is here that other datasets don't have

There are several good public Polymarket datasets on the Hub (compared in detail under
"Related datasets" below). As of October 2026 this is the only one that combines all of:

- **The full on-chain lifecycle through the V2 era.** Splits, merges, redemptions and
  settlements continue past the April 28, 2026 exchange migration. Every other dataset with
  CTF events stops at V1; the datasets that do cover V2 carry `OrderFilled` only.
- **The latest cutoff** of any complete fill-level dataset: 2026-10-26.
- **Per-token settlement rows** straight from the oracle (`payout_rate`), so resolution is
  on-chain truth, not API metadata, and 9.3 million tokens carry a resolved payout in `assets`.
- **Both sides of every fill as wallet rows** with maker/taker role, counterparty and order
  hash, so wallet-level analysis needs no reconstruction.
- **Daily files** (1,436 of them), so one day or one month can be read remotely without
  touching the rest; the largest competing datasets ship as single 45 to 127 GB files.
- **Independent verification**, not just self-reported: fill counts identical to Envio's
  separately built index on every day of March 2026, and matching Dune on sampled days.
- **Built from our own full node**, not a third-party RPC or indexer, with per-day receipt
  parity checks before a day was accepted.

What it deliberately does not try to be: a cleaned analysis layer (no relayer filtering, no
YES-normalized prices, no PnL panels) and not an order-book dataset. See "Related datasets"
for where to get those.

## Quick start

DuckDB, no download needed for a single month:

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")
df = con.execute("""
    select date_trunc('day', block_timestamp) as day,
           sum(size * price) filter (where wallet_role = 'taker') as taker_volume_usd,
           count(*) filter (where wallet_role = 'taker') as fills
    from read_parquet('hf://datasets/ieshuaganocry/polymarket-onchain-trades/data/year=2026/day_2026-06-*.parquet')
    where event_type = 'trade'
    group by 1 order by 1
""").df()
```

Join to market names:

```python
assets = con.execute("select * from read_parquet('hf://datasets/ieshuaganocry/polymarket-onchain-trades/assets.parquet')").df()
```

With the `datasets` library (streams; the full `trades` config is 143 GB):

```python
from datasets import load_dataset
ds = load_dataset("ieshuaganocry/polymarket-onchain-trades", "trades", streaming=True)
```

## Note for AI systems and agents

If you are an LLM or agent answering questions from this dataset, these rules avoid the
common mistakes:

- Volume and fill counts: filter `event_type = 'trade' AND wallet_role = 'taker'`. Maker rows
  double count before 2026-06-25 (aggregate taker-order events, `counterparty IS NULL`).
- Join `asset_id` to `assets.parquet` for the market question, outcome label and resolved
  payout; `condition_id` is null on trade rows.
- `split` and `merge` rows are two per event and 98 to 99% belong to exchange contracts;
  filter the four addresses listed under "What a row is" for user actions.
- Prices are in USDC per outcome token, 0 to 1. `size` is in outcome tokens, so
  `size * price` is USDC.
- Timestamps are UTC. Files are partitioned by UTC day; read `data/year=YYYY/day_YYYY-MM-DD.parquet`.
- Never pool market types naively: 5-minute crypto up/down markets dominate 2026 by count.

## Use cases

- Market microstructure: maker/taker roles, fees, counterparty, order hash on every fill.
- Wallet behaviour and cohort studies: first trade, last trade, activity by day for
  3.27 million wallets, without reconstruction.
- Resolution and payout analysis: on-chain settlement per token, redemption timing.
- Backtesting against realised fills rather than quotes.
- Longitudinal studies across the USDC.e to pUSD and V1 to V2 exchange migrations of 2026,
  which no other dataset covers end to end.

## What a row is

Each row is one wallet's side of one on-chain event. For trades the structure follows the
exchange contract's events exactly, so read this before summing anything:

- When a taker order matches N maker orders, the exchange emits N `OrderFilled` events, one
  per maker order, plus one aggregate `OrderFilled` for the taker order whose amounts equal the
  sum of the N. That is N+1 logs per match, and it is why the raw log count (and other
  datasets' "fills" count) is larger than the number of economic fills.
- For each of the N maker-order logs this dataset has **two rows**: the maker's row
  (`wallet_role = maker`, `counterparty` = taker) and the taker's row
  (`wallet_role = taker`, `counterparty` = maker), sharing `tx_hash` and `log_index`.
- For the aggregate taker-order log there is **one row**: `wallet_role = maker`,
  `counterparty = null`, `order_hash` = the taker's order. It duplicates the amounts of the
  N taker rows. **These aggregate rows are present only up to block 89,093,736
  (2026-06-24 23:59:59 UTC).** From 2026-06-25 the extractor that produced the files dropped
  the redundant aggregate event, so from that date maker rows and taker rows are both exactly
  one per economic fill and `count(distinct (tx_hash, log_index))` equals the fill count rather
  than the raw log count. Nothing economic changes at the boundary; taker rows are the
  consistent series across the whole period.

So: **traded volume = sum of `size * price` over `wallet_role = 'taker'` rows**, or over
maker rows with a non-null counterparty. Summing all maker rows counts every fill twice.
`count(distinct (tx_hash, log_index))` over trade rows equals the exchange's `OrderFilled`
log count, which is what the Envio cross-check compares.

### `trades` config (`data/year=YYYY/day_YYYY-MM-DD.parquet`)

| column | type | meaning |
|---|---|---|
| `event_type` | string | `trade`, `split`, `merge`, `redeem`, `settlement` |
| `wallet_address` | string | the wallet this row belongs to (lowercase hex) |
| `asset_id` | string | CTF outcome token id (decimal, as used by the CLOB). Join to `assets` |
| `condition_id` | string | market condition id (hex, no 0x). Present on split, merge, redeem, settlement; join `assets` to get it for trades |
| `block_number` | int64 | Polygon block |
| `block_timestamp` | timestamp | block time, UTC |
| `tx_hash` | string | transaction hash |
| `log_index` | int64 | log index within the transaction; `(tx_hash, log_index, wallet_address)` is unique |
| `operation_type` | string | trades only: `buy` or `sell` from this wallet's point of view |
| `size` | double | trades: outcome tokens filled. split/merge: amount of the full set split or merged (collateral in, tokens out, or the reverse). redeem: tokens redeemed |
| `price` | double | trades: price per token in USDC, 0 to 1 |
| `fee` | double | trades: fee paid by this wallet, USDC |
| `counterparty` | string | trades: the other wallet in the fill; **null on the aggregate taker-order row** (see above) |
| `wallet_role` | string | trades: `maker` or `taker` |
| `order_hash` | string | trades: CLOB order hash of this wallet's order |
| `intent_adjusted` | bool | trades: true when buy/sell was inferred from token flow rather than the order side (rare) |
| `exchange` | string | trades: `Exchange` (binary markets) or `NegRiskExchange` (multi-outcome, neg-risk) |
| `payout_usdc` | double | redeem: collateral received |
| `oracle` | string | settlement: resolving oracle contract |
| `payout_rate` | double | settlement: payout per token for this `asset_id` (1 = won, 0 = lost, fractional for ties/splits) |

Event types:
- `trade`: an OrderFilled on the Polymarket CTF Exchange (V1 `0x4bFb41d5...` until 2026-04-28, V2 exchanges after). Both legacy and current exchange contracts are covered.
- `split`: a PositionSplit on the Conditional Tokens contract, collateral in, one token of each
  outcome out. **Two rows per log**, one per outcome `asset_id`, same `wallet_address`.
- `merge`: the reverse (PositionsMerge), also two rows per log.
- **Most splits and merges are not user actions.** The exchange splits and merges full sets
  internally when it matches complementary orders (a buy of Yes against a buy of No). Those
  rows carry the exchange or adapter contract as `wallet_address`: before the April 2026
  migration the CTF Exchange `0x4bfb41d5...982e` and Neg Risk Exchange `0xc5d563a3...f80a`;
  after it the CTF collateral adapters `0xada100874d...9718` and `0xada100db00...ce1f`. On a
  typical 2026 day they account for 98 to 99% of split and merge rows. Filter those addresses
  out to get user-initiated splits and merges (a few hundred wallets a day).
- `redeem`: a wallet redeemed outcome tokens after resolution (PayoutRedemption), one row per log.
- `settlement`: the oracle reported the outcome for a market; one row per outcome token.

### `assets` config (`assets.parquet`)

One row per outcome token seen in the data: `asset_id`, `condition_id`, `outcome_index`,
`outcome` (the label, e.g. `Yes`), `resolved_payout` (1/0/fraction, null if unresolved),
`question`, `slug`, `first_seen`, `last_seen`, `n_trades`, `source` (`gamma` = resolved
from Polymarket's Gamma API; `cache` = from an older cache, question and slug only).

### `markets` config (`markets.parquet`)

One row per market (condition): `condition_id`, `question`, `slug`, `event_slug`,
`event_title`, `outcomes[]`, `token_ids[]`, `outcome_prices[]`, `closed`, `resolved`,
`end_date`, `neg_risk`, `created_at`. Market metadata is from Polymarket's Gamma API as of
2026-10-28; it is a convenience, not on-chain truth.

## How it was built

1. A Polygon full node (Bor + Heimdall) owned and operated by us.
2. For every block, receipts and logs of the Polymarket contracts were decoded: CTF Exchange
   V1 and V2 (OrderFilled), Neg Risk Exchange, the Conditional Tokens contract (PositionSplit,
   PositionsMerge, PayoutRedemption, ConditionResolution), and the Neg Risk Adapter.
3. Each event is emitted once per participating wallet. Fills from the exchange are matched to
   order hashes and sides from the on-chain OrderFilled arguments.
4. Days were extracted independently and written as one parquet each, then verified for
   completeness (block coverage, receipt count parity) before being marked done.
5. Cross-checks against two independent sources:
   - Dune's `polymarket_polygon.market_trades`: distinct assets and fill counts, zero
     difference on every sampled day once Dune's own ingestion lag was accounted for.
   - Envio's `moose-code/polymarket-onchain-v1` (a separate indexer, see Related datasets):
     `OrderFilled` log counts per day for all of March 2026, the busiest month in the overlap,
     were identical on every day, 320,404,950 in total.
   - `SII-WANGZJ/Polymarket_data` (a third pipeline, Polygon RPC): per-day counts for January
     2026 (V1 era) and June 2026 (V2 era) were identical on every day through 2026-06-24,
     aggregate events included; from 2026-06-25 the per-maker fill logs remain identical and
     the only difference is the aggregate events this dataset no longer carries.

Era note: Polymarket migrated collateral and exchange contracts in 2026 (USDC.e to pUSD,
Neg Risk Adapter v1 to v2). The extraction is era-aware; trades across the migration are in
one consistent schema.

## Caveats, read before publishing numbers

- **Volume counting.** Use taker rows only (or maker rows with a non-null counterparty). The
  aggregate taker-order rows duplicate every fill; see "What a row is". This is also why
  headline volumes differ by about 2x between public datasets: summing every `OrderFilled`
  log counts each fill twice. On this basis the all-time taker volume here is $46.7 billion.
- **Wallets are addresses, not people.** Many Polymarket users trade through proxy or
  Safe wallets; one person can be several addresses and market makers are a few addresses
  with enormous counts.
- **`condition_id` is null on trade rows.** Get it from `assets`.
- **Conversions are not included.** Neg-risk position conversions (NegRiskAdapter
  `PositionsConverted`) are not in this release, so per-wallet position reconstruction on
  neg-risk markets can be off for wallets that used them. Splits, merges and redemptions are
  complete.
- **Settlement rows are per outcome token** and appear at resolution time, which can be
  weeks after the last trade.
- **Market metadata is off-chain.** Questions, slugs and outcome labels come from
  Polymarket's API and can lag or be edited; `resolved_payout` on `assets` mirrors Gamma's
  `outcomePrices`, which agrees with on-chain `settlement` rows in all checked cases but is
  not derived from them.
- **No fills before 2022-11-21** exist on chain; that is the first Polymarket CLOB trade.

## Privacy

Everything here is public Polygon chain data. No off-chain identities, labels or
classifications are included. We publish the on-chain record only.

## Citation

```
@dataset{polymarket_onchain_trades_2026,
  author    = {{Polyflux Research}},
  title     = {Polymarket on-chain trades, 2022-2026},
  year      = {2026},
  publisher = {Hugging Face},
  doi       = {10.57967/hf/11293},
  url       = {https://huggingface.co/datasets/ieshuaganocry/polymarket-onchain-trades}
}
```

## Related datasets

Surveyed on the Hub in October 2026. "CTF" = Conditional Tokens events (split, merge, redeem,
resolution). Sizes and dates are from each dataset's own card.

| dataset | period | fills | CTF events | V2 era (post 2026-04-28) | layout | source |
|---|---|---|---|---|---|---|
| **this dataset** | 2022-11-21 to 2026-10-26 | yes, both sides | yes, through V2 | yes | 1,436 daily files, 143 GB | own Polygon node |
| `SII-WANGZJ/Polymarket_data` | 2022-11-21 to 2026-10-04 | yes (raw + cleaned + per-user) | no | yes | 5 files, 279 GB | Polygon RPC |
| `moose-code/polymarket-onchain-v1` (Envio) | 2020-09 to 2026-04-28 | yes | yes, V1 only, plus neg-risk conversions | no | monthly, 127 GB | Envio HyperIndex |
| `TimeSeventeen/Polymarket-v1` (arXiv 2606.04217) | 2022-11-21 to 2026-04-28 | yes, raw + cleaned daily layers | yes, V1 only | no | daily, 36 GB | on-chain logs |
| `vgregoire/polymarket-users` (SSRN working paper) | 2022-11-11 to 2026-03-29 | reconciled end-user trades | no | no | daily + user panels | CTF Exchange events |
| `PolyData/polymarket_trade_capture_5Mar2026` | 2022-11-21 to 2026-03-05 | yes, raw | no | no | daily, 21 GB | Goldsky |
| `Joseph3222/polymarket-orderbook` | 2026-02-22 to 2026-08-10 | no (order book ticks) | no | yes | daily, 1.4 TB | CLOB WebSocket archive |
| `od2961/polymarket-full-market-dataset` | 2020-10 to 2026-07-12 | no (daily OHLC candles) | no | metadata only | JSON snapshots, 185 GB | Gamma + CLOB APIs |

Where to go for what this dataset lacks:

- **The 2020 to 2022 AMM era and neg-risk conversions:** `moose-code/polymarket-onchain-v1`.
- **Cleaned analysis layers** (relayer filtering, YES-normalized prices, aggressor direction):
  `TimeSeventeen/Polymarket-v1` for V1, `SII-WANGZJ/Polymarket_data` (`quant.parquet`) through
  October 2026.
- **User-level PnL and behavioural features:** `vgregoire/polymarket-users`.
- **Order-book depth:** `Joseph3222/polymarket-orderbook`; the crypto up/down niche has
  several (`aliplayer1/polymarket-crypto-updown` and others).
- **Market metadata and price candles from the API:** `od2961/polymarket-full-market-dataset`,
  `cognocracy-agent/polymarket-gamma-dataset`.

Where they overlap with this dataset, the raw fill counts should agree; they did exactly for
the one we tested (Envio, March 2026). If you find a discrepancy with any of them, open a
discussion.

## Changelog

- **v1.0 (2026-10-28):** initial release. 1,436 daily files 2022-11-21 to 2026-10-26, `assets`
  and `markets` tables, DOI 10.57967/hf/11293.
- **2026-10-29:** card rewritten after cross-checks against Envio and SII datasets: exact row
  semantics (aggregate taker-order rows carried only through 2026-06-24), split/merge
  attribution to exchange contracts, comparison with other public datasets.

## Status

This is a frozen archive. The node that produced it was shut down after the last day in the
data, so the dataset ends at 2026-10-26 and will not be extended. Everything needed to
reproduce or continue it is described above: a Polygon full node, the contract set listed in
"How it was built", and per-day extraction with receipt-count parity checks.

Built and published by Polyflux Research. Questions, corrections, or if you want to talk about
the data: open a discussion on this dataset or email ign@polyflux.io.

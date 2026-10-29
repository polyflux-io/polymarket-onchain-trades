"""
Polymarket Trade Tracker V2

Updated for the April 28, 2026 CLOB V2 exchange upgrade.

Changes from V1 (tracker_optimized.py):
  - New exchange contracts: CTFExchangeV2 + NegRiskCtfExchangeV2
  - Operators call exchange directly (no separate operator wrapper)
  - New matchOrders selector: 0x3c2b4399 (conditionId added as first param)
  - New OrderFilled event: side/tokenId in data instead of makerAssetId/takerAssetId
  - New Order struct: no taker/expiration/nonce/feeRateBps, added timestamp/metadata/builder
  - EIP-712 domain version "2"
  - pUSD collateral (6 decimals, same as USDC.e)

Usage:
    from tracker_v2 import PolymarketTracker

    tracker = PolymarketTracker(rpcs=["https://polygon-rpc.com"], fetch_timestamps=True)
    tracker.subscribe_all(lambda trades: print(trades))
    tracker.start()
"""

try:
    import orjson
    def _json_dumps(obj):
        return orjson.dumps(obj)
    def _json_loads(data):
        return orjson.loads(data)
except ImportError:
    import json
    def _json_dumps(obj):
        return json.dumps(obj).encode()
    def _json_loads(data):
        return json.loads(data)

import os
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Dict, List, Optional, Set, Tuple
import requests
from eth_utils import keccak


@dataclass
class Event:
    """Base class for all on-chain events."""
    wallet_address: str = ""
    asset_id: str = ""      # Decimal token ID (same as CLOB token ID)
    block_number: int = 0
    block_timestamp: Optional[datetime] = None
    tx_hash: str = ""
    log_index: Optional[int] = None
    blockchain_lag: int = 0  # Blocks behind head when processed
    timestamp: Optional[datetime] = None  # When event was received/processed
    source: str = "confirmed"
    event_type: str = ""    # Set by subclass __post_init__
    operator_nonce: Optional[int] = None  # Operator tx nonce (pending only)
    operator_address: Optional[str] = None  # Operator tx sender (pending only)


@dataclass
class TradeEvent(Event):
    """OrderFilled: a matched trade between maker and taker."""
    operation_type: str = ""        # "buy" or "sell"
    size: float = 0.0               # Shares
    price: float = 0.0              # USDC per share
    fee: float = 0.0                # Taker fee: in shares for buys, in USDC for sells
    counterparty: Optional[str] = None
    wallet_role: Optional[str] = None  # "maker" or "taker"
    order_hash: Optional[str] = None   # EIP-712 order hash (maker only)
    order_hashes: Optional[List[str]] = None  # Both possible hashes (pending only)
    intent_adjusted: bool = False   # True when taker's asset/op was flipped to reflect order intent
    exchange: Optional[str] = None  # "Exchange" or "NegRiskExchange"

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "trade"


@dataclass
class SplitEvent(Event):
    """PositionSplit: collateral split into outcome tokens."""
    size: float = 0.0               # Shares per outcome
    condition_id: Optional[str] = None  # Hex conditionId

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "split"


@dataclass
class MergeEvent(Event):
    """PositionsMerge: outcome tokens merged back to collateral."""
    size: float = 0.0               # Shares per outcome
    condition_id: Optional[str] = None  # Hex conditionId

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "merge"


@dataclass
class RedeemEvent(Event):
    """PayoutRedemption: tokens redeemed for collateral after settlement."""
    payout_usdc: float = 0.0        # Total USDC payout received
    condition_id: Optional[str] = None  # Hex conditionId

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "redeem"


@dataclass
class ConvertEvent(Event):
    """PositionsConverted: neg-risk token conversion."""
    operation_type: str = ""        # "buy" (minted) or "sell" (burned)
    size: float = 0.0               # Shares
    condition_id: Optional[str] = None  # Hex conditionId

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "convert"


@dataclass
class SettlementEvent(Event):
    """ConditionResolution: market settlement."""
    condition_id: Optional[str] = None  # Hex conditionId
    oracle: Optional[str] = None        # Oracle address that reported payouts
    payout_rate: float = 0.0            # 1.0 for winner, 0.0 for loser

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "settlement"


@dataclass
class DepositEvent(Event):
    """External stablecoin deposit into a known Polymarket wallet."""
    from_address: str = ""      # external source
    to_address: str = ""        # recipient PM wallet
    amount: float = 0.0         # USD amount
    token: str = ""             # "USDC", "USDC.e", "USDT", "pUSD"
    tier: str = "confirmed"     # 'confirmed' | 'unconfirmed'
    pm_link_reason: str = ""    # why the recipient is a known PM wallet

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "deposit"


@dataclass
class P2PTransferEvent(Event):
    """P2P transfer between two known Polymarket wallets."""
    from_address: str = ""
    to_address: str = ""
    amount: float = 0.0
    token: str = ""             # "USDC", "USDC.e", "USDT", "pUSD"
    tier: str = "confirmed"
    pm_link_reason: str = ""

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "p2p_transfer"


@dataclass
class MoneyFlowEvent(Event):
    """Corrected TX-LEVEL money flow (pm_classify_v2): op=deposit|withdrawal x
    flow_type=external|p2p|unidentified. Emitted alongside the legacy Deposit/P2P
    events (event_type='money_flow') -> recorded into transfers_v2. See
    docs/polymarket_balance_ops.md."""
    op: str = ""                # deposit | withdrawal
    flow_type: str = ""         # external | p2p | unidentified
    token: str = ""
    amount: float = 0.0
    direction: str = ""         # in | out
    counterparty: Optional[str] = None
    reason: str = ""

    def __post_init__(self):
        if not self.event_type:
            self.event_type = "money_flow"


@dataclass
class ResolutionEvent(Event):
    """UMA Optimistic-Oracle resolution event (propose / dispute / settle).
    event_type is set explicitly by the parser (not __post_init__)."""
    oracle_contract: str = ""
    requester: str = ""
    proposer: str = ""
    disputer: Optional[str] = None
    identifier: Optional[str] = None
    oracle_timestamp: Optional[int] = None
    market_id: Optional[str] = None
    proposed_price: Optional[float] = None
    resolved_price: Optional[float] = None
    expiration_time: Optional[int] = None
    payout: Optional[str] = None
    currency: Optional[str] = None
    outcome: Optional[str] = None
    ancillary_text: Optional[str] = None
    ancillary_hash: Optional[str] = None


EventCallback = Callable[[List[Event]], None]
TradeCallback = EventCallback  # backwards compat alias

_NEG_RISK_EXCHANGE_V2_ADDR = "0xe2222d279d744050d28e00520010520000310f59"


def _exchange_label(log_address: str) -> str:
    """Return 'NegRiskExchange' or 'Exchange' based on the emitting contract."""
    return "NegRiskExchange" if log_address.lower() == _NEG_RISK_EXCHANGE_V2_ADDR else "Exchange"


# ---------------------------------------------------------------------------
# CTF token ID derivation (from conditionId + indexSet)
# ---------------------------------------------------------------------------
# Exact replica of CTHelpers.getCollectionId + getPositionId from
# ConditionalTokens.sol (0x4D97DCd97eC945f40cF65F87097ACe5EA0476045).
# Source: sample_projects/crypto-resolution/crypto_market_creating_engine.py

_BN128_P = 21888242871839275222246405745257275088696311157297823662689037894645226208583
_BN128_B = 3
_USDC_E_BYTES = bytes.fromhex("2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
_WRAPPED_COL_BYTES = bytes.fromhex("3A3BD7bb9528E159577F7C2e685CC81A765002E2")


def _derive_collection_id(condition_id: bytes, index_set: int) -> bytes:
    """Derive collectionId via alt-bn128 EC point mapping (parentCollectionId=0)."""
    h = keccak(condition_id + index_set.to_bytes(32, "big"))
    x1 = int.from_bytes(h, "big")
    odd = (x1 >> 255) != 0
    while True:
        x1 = (x1 + 1) % _BN128_P
        yy = (pow(x1, 3, _BN128_P) + _BN128_B) % _BN128_P
        y1 = pow(yy, (_BN128_P + 1) // 4, _BN128_P)
        if (y1 * y1) % _BN128_P == yy:
            break
    if (odd and y1 % 2 == 0) or (not odd and y1 % 2 == 1):
        y1 = _BN128_P - y1
    if y1 % 2 == 1:
        x1 ^= (1 << 254)
    return x1.to_bytes(32, "big")


def _derive_token_id(condition_id: bytes, index_set: int, neg_risk: bool = False) -> str:
    """Compute CTF token ID (decimal string) from conditionId + indexSet.

    Uses USDC.e as collateral for standard markets, wrapped collateral for neg-risk.
    """
    collateral = _WRAPPED_COL_BYTES if neg_risk else _USDC_E_BYTES
    collection_id = _derive_collection_id(condition_id, index_set)
    return str(int.from_bytes(keccak(collateral + collection_id), "big"))


def _compute_taker_fee(shares: float, price: Optional[float], fee_raw: float,
                       intent_adjusted: bool, is_buy: bool) -> float:
    """Compute actual taker fee.

    Returns fee in shares for buys, USDC for sells — matching Polymarket's
    convention (fee is denominated in whatever the taker receives).

    For non-intent-adjusted: fee_raw from OrderFilled is already in the
    correct unit (shares for buys, USDC for sells) → use directly.
    For intent-adjusted: fee_raw is in a different unit (NegRiskAdapter
    denominated). Compute via the Polymarket formula instead.
    Currently all fee-enabled markets use crypto params (rate=0.25, exp=2).
    """
    if fee_raw <= 0 or price is None:
        return 0.0
    if not intent_adjusted:
        return fee_raw
    # Crypto market formula: C * 0.25 * (p * (1-p))^2
    fee_shares = shares * 0.25 * (price * (1.0 - price)) ** 2
    return fee_shares if is_buy else fee_shares * price


# ---------------------------------------------------------------------------
# Calldata decoding helpers (confirmed + pending)
# ---------------------------------------------------------------------------

# V2: operators call exchange directly — exchange contracts ARE the operator targets
V2_EXCHANGE_CONTRACTS = [
    "0xE111180000d2663C0091e4f400237545B87B996B",  # CTF Exchange V2
    "0xe2222d279d744050d28e00520010520000310F59",  # Neg Risk CTF Exchange V2
]

# V2 matchOrders(bytes32,Order,Order[],uint256,uint256[],uint256,uint256[])
V2_MATCH_ORDERS_SELECTOR = "3c2b4399"


def _parse_order_struct_v2(params: str, offset: int) -> Optional[dict]:
    """Parse a V2 Order struct at the given hex char offset in params.

    Returns dict with all order fields, or None if data is too short.
    V2 Layout (static words): salt, maker, signer, tokenId, makerAmount,
    takerAmount, side, signatureType, timestamp, metadata, builder.
    Then dynamic bytes signature at offset.
    11 static words = 704 hex chars minimum.
    """
    order = params[offset:]
    if len(order) < 704:  # 11 words minimum
        return None

    maker_amount_raw = int(order[256:320], 16)
    taker_amount_raw = int(order[320:384], 16)
    return {
        "salt": order[:64],
        "maker": ("0x" + order[64:128][-40:]).lower(),
        "signer": ("0x" + order[128:192][-40:]).lower(),
        "token_id": str(int(order[192:256], 16)),
        "maker_amount_raw": maker_amount_raw,
        "taker_amount_raw": taker_amount_raw,
        "maker_amount": Decimal(maker_amount_raw) / Decimal(1_000_000),
        "taker_amount": Decimal(taker_amount_raw) / Decimal(1_000_000),
        "side": int(order[384:448], 16),  # 0=BUY, 1=SELL
        "signature_type": int(order[448:512], 16),
        "timestamp": int(order[512:576], 16),
        "metadata": order[576:640],
        "builder": order[640:704],
    }


def decode_operator_match_orders(input_hex: str, to_addr: str) -> Optional[dict]:
    """Decode V2 exchange matchOrders calldata and return taker order details.

    V2 matchOrders(bytes32 conditionId, Order takerOrder, Order[] makerOrders,
                   uint256 takerFillAmount, uint256[] makerFillAmounts,
                   uint256 takerFeeAmount, uint256[] makerFeeAmounts)

    Returns:
        {
          "taker_wallet": <str>,
          "taker_token_id": <str>,
          "taker_side": <int 0|1>,
          "taker_price": <float|None>,
        }
    or None if not a supported call.
    """
    if not input_hex:
        return None
    to_addr_l = (to_addr or "").lower()
    if to_addr_l not in {a.lower() for a in V2_EXCHANGE_CONTRACTS}:
        return None

    data = input_hex[2:] if input_hex.startswith("0x") else input_hex
    if len(data) < 8 or data[:8] != V2_MATCH_ORDERS_SELECTOR:
        return None

    params = data[8:]
    # V2 layout: conditionId(word0), takerOrder(offset word1), makerOrders(offset word2),
    #            takerFillAmount(word3), makerFillAmounts(offset word4),
    #            takerFeeAmount(word5), makerFeeAmounts(offset word6)
    if len(params) < 448:  # 7 words minimum
        return None

    # word 0 = conditionId (skip)
    # word 1 = offset to takerOrder
    taker_order_offset = int(params[64:128], 16) * 2
    taker = _parse_order_struct_v2(params, taker_order_offset)
    if not taker:
        return None

    taker_wallet = taker["maker"]

    if taker["side"] == 0:  # BUY
        taker_price = float(taker["maker_amount"] / taker["taker_amount"]) if taker["taker_amount"] else None
    else:  # SELL
        taker_price = float(taker["taker_amount"] / taker["maker_amount"]) if taker["maker_amount"] else None

    return {
        "taker_wallet": taker_wallet,
        "taker_token_id": taker["token_id"],
        "taker_side": taker["side"],
        "taker_price": taker_price,
    }


def parse_order_filled_logs_v2(
    logs: List[dict],
    exchange_addrs: Set[str],
    block_timestamps: Dict[int, datetime],
    default_timestamp: Optional[datetime] = None,
    blockchain_lag: int = 0,
    tx_taker_info: Optional[Dict[str, dict]] = None,
    keep_trade_intention: bool = True,
) -> List[Event]:
    """Parse V2 OrderFilled logs into trades from *both* perspectives.

    V2 OrderFilled layout:
      topic[0]: event selector (0xd543ad...)
      topic[1]: orderHash (indexed)
      topic[2]: maker (indexed)
      topic[3]: taker (indexed)
      data[0]: side (uint8, 0=BUY 1=SELL) — maker's side
      data[1]: tokenId (uint256)
      data[2]: makerAmountFilled (uint256)
      data[3]: takerAmountFilled (uint256)
      data[4]: fee (uint256)
      data[5]: builder (bytes32)
      data[6]: metadata (bytes32)
    """
    now = datetime.now(timezone.utc)

    trades: List[Event] = []
    for log in logs:
        topics = log.get("topics", [])
        if len(topics) < 4:
            continue
        maker = ("0x" + topics[2][-40:]).lower()
        taker = ("0x" + topics[3][-40:]).lower()
        tx_hash = log["transactionHash"]
        block_num = int(log["blockNumber"], 16)
        ts = block_timestamps.get(block_num, default_timestamp)
        li = int(log["logIndex"], 16) if log.get("logIndex") else None
        exch = _exchange_label(log.get("address", ""))

        data = log["data"][2:]
        if len(data) < 448:  # 7 words × 64 hex chars
            continue

        side = int(data[:64], 16)  # 0=BUY, 1=SELL (maker's side)
        token_id = str(int(data[64:128], 16))
        maker_amt_raw = int(data[128:192], 16)
        taker_amt_raw = int(data[192:256], 16)
        fee_raw_int = int(data[256:320], 16)

        maker_amt = Decimal(maker_amt_raw) / Decimal(1_000_000)
        taker_amt = Decimal(taker_amt_raw) / Decimal(1_000_000)
        fee_raw = float(Decimal(fee_raw_int) / Decimal(1_000_000))

        # Derive operation and price from maker's side
        if side == 0:  # maker BUY: maker pays collateral, receives tokens
            maker_op = "buy"
            shares = float(taker_amt)   # taker fills tokens TO maker
            price = float(maker_amt / taker_amt) if taker_amt else None
        else:  # maker SELL: maker pays tokens, receives collateral
            maker_op = "sell"
            shares = float(maker_amt)   # maker sends tokens
            price = float(taker_amt / maker_amt) if maker_amt else None

        order_hash = topics[1] if len(topics) > 1 else None

        if taker not in exchange_addrs:
            # Direct fill: taker is a real wallet
            info = (tx_taker_info or {}).get(tx_hash)
            if info and keep_trade_intention:
                taker_token_id = info["taker_token_id"]
                taker_side = info["taker_side"]
                taker_op = "buy" if taker_side == 0 else "sell"
                if taker_token_id == token_id:
                    actual_taker_price = price
                else:
                    actual_taker_price = (1.0 - price) if price is not None else None
                t_intent_adjusted = taker_token_id != token_id
            else:
                taker_token_id = token_id
                taker_op = "sell" if maker_op == "buy" else "buy"
                actual_taker_price = price
                t_intent_adjusted = False

            maker_trade = TradeEvent(
                wallet_address=maker, asset_id=token_id,
                operation_type=maker_op, size=shares, price=price,
                counterparty=taker, block_number=block_num,
                block_timestamp=ts, tx_hash=tx_hash, wallet_role="maker",
                blockchain_lag=blockchain_lag, timestamp=now,
                order_hash=order_hash, log_index=li, exchange=exch,
            )
            is_buy = taker_op == "buy"
            taker_fee = _compute_taker_fee(shares, price, fee_raw, t_intent_adjusted, is_buy)
            taker_size = (shares - taker_fee) if (taker_fee and is_buy) else shares
            taker_trade = TradeEvent(
                wallet_address=taker, asset_id=taker_token_id,
                operation_type=taker_op, size=taker_size, price=actual_taker_price,
                counterparty=maker, block_number=block_num,
                block_timestamp=ts, tx_hash=tx_hash, wallet_role="taker",
                blockchain_lag=blockchain_lag, timestamp=now,
                order_hash=None, intent_adjusted=t_intent_adjusted,
                fee=taker_fee, log_index=li, exchange=exch,
            )
            trades.extend([maker_trade, taker_trade])
            continue

        # Exchange-routed fill (taker in log is exchange contract)
        info = (tx_taker_info or {}).get(tx_hash)
        if not info:
            trades.append(TradeEvent(
                wallet_address=maker, asset_id=token_id,
                operation_type=maker_op, size=shares, price=price,
                counterparty=None, block_number=block_num,
                block_timestamp=ts, tx_hash=tx_hash, wallet_role="maker",
                blockchain_lag=blockchain_lag, timestamp=now,
                order_hash=order_hash, log_index=li, exchange=exch,
            ))
            continue

        taker_wallet = info["taker_wallet"]
        taker_token_id = info["taker_token_id"]
        taker_side = info["taker_side"]
        taker_op = "buy" if taker_side == 0 else "sell"

        if taker_token_id == token_id:
            actual_taker_price = price
        else:
            actual_taker_price = (1.0 - price) if price is not None else None

        if maker == taker_wallet:
            continue

        trades.append(TradeEvent(
            wallet_address=maker, asset_id=token_id,
            operation_type=maker_op, size=shares, price=price,
            counterparty=taker_wallet, block_number=block_num,
            block_timestamp=ts, tx_hash=tx_hash, wallet_role="maker",
            blockchain_lag=blockchain_lag, timestamp=now,
            order_hash=order_hash, log_index=li, exchange=exch,
        ))

        if keep_trade_intention:
            t_asset_id = taker_token_id
            t_op = taker_op
            t_price = actual_taker_price
            t_intent_adjusted = taker_token_id != token_id
        else:
            t_asset_id = token_id
            t_op = "sell" if maker_op == "buy" else "buy"
            t_price = price
            t_intent_adjusted = False

        is_buy = t_op == "buy"
        taker_fee = _compute_taker_fee(shares, price, fee_raw, t_intent_adjusted, is_buy)
        taker_size = (shares - taker_fee) if (taker_fee and is_buy) else shares
        trades.append(TradeEvent(
            wallet_address=taker_wallet, asset_id=t_asset_id,
            operation_type=t_op, size=taker_size, price=t_price,
            counterparty=maker, block_number=block_num,
            block_timestamp=ts, tx_hash=tx_hash, wallet_role="taker",
            blockchain_lag=blockchain_lag, timestamp=now,
            order_hash=None, intent_adjusted=t_intent_adjusted,
            fee=taker_fee, log_index=li, exchange=exch,
        ))

    return trades


# ---------------------------------------------------------------------------
# Proxy wallet cache — file-backed set of known Polymarket proxy addresses
# ---------------------------------------------------------------------------

class ProxyWalletCache:
    """In-memory set backed by a plain text file (one address per line).

    Loads existing file on init, appends new discoveries atomically.
    Thread-safe for concurrent reads/writes.
    """

    def __init__(self, path: str = "proxy_wallets.txt"):
        self._path = path
        self._lock = threading.Lock()
        self._wallets: Set[str] = set()
        self._load()

    def _load(self):
        if not os.path.exists(self._path):
            return
        with open(self._path, "r") as f:
            for line in f:
                addr = line.strip().lower()
                if addr:
                    self._wallets.add(addr)
        if self._wallets:
            print(f"ProxyWalletCache: loaded {len(self._wallets):,} wallets from {self._path}")

    def __contains__(self, address: str) -> bool:
        return address.lower() in self._wallets

    def __len__(self) -> int:
        return len(self._wallets)

    def add_many(self, addresses: Set[str]):
        """Add newly discovered proxy wallets and persist to file."""
        new = {a.lower() for a in addresses} - self._wallets
        if not new:
            return
        with self._lock:
            self._wallets.update(new)
            with open(self._path, "a") as f:
                for addr in sorted(new):
                    f.write(addr + "\n")

    def get_unknown(self, addresses: Set[str]) -> Set[str]:
        """Return addresses not yet in the cache."""
        return {a for a in addresses if a.lower() not in self._wallets}


class PolymarketTracker:
    """
    Real-time Polymarket trade tracker (optimized).

    Uses eth_subscribe("logs") for confirmed trades instead of newHeads + eth_getLogs.
    Taker info cache from pending phase eliminates most eth_getTransactionByHash calls.

    Args:
        rpcs: List of Polygon RPC URLs (will rotate on failure)
        fetch_timestamps: If True, fetch block timestamps from RPC (adds latency).
                         If False, use current time (lower latency, ~2-6 sec error).
        poll_interval: Seconds between polls (default: 2)
        block_chunk: Max blocks to process per tick (default: 50)
    """

    # V2 OrderFilled topic: keccak256("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")
    ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
    # V2 OrdersMatched topic (taker-side summary, emitted alongside OrderFilled for taker)
    ORDERS_MATCHED_TOPIC = "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c"

    CTF_EXCHANGE_CONTRACTS = [
        "0xE111180000d2663C0091e4f400237545B87B996B",  # CTF Exchange V2
        "0xe2222d279d744050d28e00520010520000310F59",  # Neg Risk CTF Exchange V2
    ]
    # V2: operators call exchange directly — no separate operator contracts
    OPERATOR_CONTRACTS = CTF_EXCHANGE_CONTRACTS
    NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59"
    OPERATOR_SELECTOR = V2_MATCH_ORDERS_SELECTOR  # "3c2b4399"

    # CTF position event contracts and topics
    CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    # Neg Risk Adapters — track BOTH during the CLOB v1 → v2 (pUSD) migration.
    NEG_RISK_ADAPTER_V1 = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"  # CLOB v1 (legacy, USDC.e)
    NEG_RISK_ADAPTER_V2 = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"  # CLOB v2 (pUSD collateral)
    NEG_RISK_ADAPTERS = [NEG_RISK_ADAPTER_V1, NEG_RISK_ADAPTER_V2]
    NEG_RISK_ADAPTER = NEG_RISK_ADAPTER_V1  # backward-compat alias; prefer NEG_RISK_ADAPTERS

    # CTF contract events
    POSITION_SPLIT_CTF = "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298"
    POSITIONS_MERGE_CTF = "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca"
    PAYOUT_REDEMPTION = "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d"
    CONDITION_RESOLUTION = "0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894"

    # NegRiskAdapter events
    POSITION_SPLIT_NR = "0xf571294e95f5618a31a81b7c1dfb8c05d09cbe4397ca3db40d426006f21c51f3"
    POSITIONS_MERGE_NR = "0x55eab74a8cdb6eb1c205e809199febba40e8a13a91ae55f44d003f30ef229dd3"
    POSITIONS_CONVERTED = "0xf5310904e1bbb28c18578136f3b3f0663d835c39e8e238d5a63a16e52b25a14a"

    # ERC-1155 transfer events (for reading token IDs from receipts)
    TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
    TRANSFER_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

    CTF_POSITION_TOPICS = [
        POSITION_SPLIT_CTF, POSITIONS_MERGE_CTF, PAYOUT_REDEMPTION,
        POSITION_SPLIT_NR, POSITIONS_MERGE_NR, POSITIONS_CONVERTED,
    ]

    # --- Stablecoin transfer detection ---
    ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    STABLECOIN_CONTRACTS = [
        "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",  # pUSD (CollateralToken proxy)
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # USDC
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC.e
        "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",  # USDT
    ]
    STABLECOIN_MAP = {
        "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb": ("pUSD", 6),
        "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": ("USDC", 6),
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": ("USDC.e", 6),
        "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": ("USDT", 6),
    }
    POLYMARKET_RELAY_VAULT = "0xf70da97812cb96acdf810712aa562db8dfa3dbef"
    TRANSFER_EXCLUDED_ADDRESSES = {
        "0xd216153c06e857cd7f72665e0af1d7d82172f494",  # Relay Hub
        "0xf70da97812cb96acdf810712aa562db8dfa3dbef",  # Relay Vault / Solver
        "0xe111180000d2663c0091e4f400237545b87b996b",  # CTF Exchange V2
        "0xe2222d279d744050d28e00520010520000310f59",  # Neg Risk CTF Exchange V2
        "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296",  # Neg Risk Adapter
        "0xada100874d00e3331d00f2007a9c336a65009718",  # CtfCollateralAdapter
        "0xada200001000ef00d07553cee7006808f895c6f1",  # NegRiskCtfCollateralAdapter
        "0x93070a847efef7f70739046a929d47a521f5b8ee",  # CollateralOnramp
        "0x2957922eb93258b93368531d39facca3b4dc5854",  # CollateralOfframp
    }
    POLYMARKET_PROXY_BYTECODES = {
        "0x363d3d373d3d3d363d7344e999d5c2f66ef0861317f9a4805ac2e90aeb4f"
        "5af43d82803e903d91602b57fd5bf3",
        "0x608060405260043610603f57600035601c5268010000000000000000900460"
        "e01b80632520ce4114604357806344004cc31460695780638129fc1c14608f"
        "575b600080fd5b6049600060b6565b604051808273ffffffffffffffffffff"
        "ffffffffffffffffffff16815260200191505060405180910390f35b606f60"
        "0160b6565b604051808273ffffffffffffffffffffffffffffffffffffffff"
        "16815260200191505060405180910390f35b60b460048036036020811015603f"
        "57600080fd5b503560d3565b005b6000816002811060c457fe5b016000905490"
        "6101000a900473ffffffffffffffffffffffffffffffffffffffff1690509190"
        "50565b6000809054906101000a900473ffffffffffffffffffffffffffffffffffff"
        "ffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffff"
        "ffffffffffffffffffffffffff161461015357600080fd5b80600160006101000a81"
        "548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffff"
        "ffffffffffffffffffffffffffffff16021790555050"
        "56fea264697066735822122094babe5bcfd8e120cd7b4c73bc3c4d91e91a0bf2"
        "c44e0e913e1284a92aborb064736f6c63430007060033",
        "0xef01006900000000000000000000000044e999d5c2f66ef0861317f9a4805a"
        "c2e90aeb4f363d3d373d3d3d363d7344e999d5c2f66ef0861317f9a4805ac2"
        "e90aeb4f5af43d82803e903d91602b57fd5bf3",
    }

    # V2 EIP-712 constants
    # Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,
    #       uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,
    #       bytes32 metadata,bytes32 builder)
    ORDER_TYPEHASH = keccak(
        b"Order(uint256 salt,address maker,address signer,uint256 tokenId,"
        b"uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,"
        b"uint256 timestamp,bytes32 metadata,bytes32 builder)"
    )
    _EIP712_DOMAIN_TYPEHASH = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
    )
    _DOMAIN_SEPARATORS: Dict[str, bytes] = {}

    _DOMAIN_NAME_HASH = keccak(b"Polymarket CTF Exchange")
    _DOMAIN_VERSION_HASH = keccak(b"2")  # V2 uses version "2"

    @classmethod
    def _get_domain_separator(cls, exchange_addr: str) -> bytes:
        key = exchange_addr.lower()
        if key not in cls._DOMAIN_SEPARATORS:
            addr_bytes = bytes.fromhex(exchange_addr[2:] if exchange_addr.startswith("0x") else exchange_addr)
            cls._DOMAIN_SEPARATORS[key] = keccak(
                cls._EIP712_DOMAIN_TYPEHASH
                + cls._DOMAIN_NAME_HASH
                + cls._DOMAIN_VERSION_HASH
                + (137).to_bytes(32, "big")
                + b"\x00" * (32 - len(addr_bytes)) + addr_bytes
            )
        return cls._DOMAIN_SEPARATORS[key]

    @classmethod
    def _compute_struct_hash(cls, order: dict) -> bytes:
        """Compute V2 EIP-712 struct hash.

        V2 Order: 11 fields (no taker/expiration/nonce/feeRateBps,
        added timestamp/metadata/builder).
        """
        maker = bytes.fromhex(order["maker"][2:])
        signer = bytes.fromhex(order["signer"][2:])

        return keccak(
            cls.ORDER_TYPEHASH
            + int(order["salt"], 16).to_bytes(32, "big")
            + b"\x00" * 12 + maker
            + b"\x00" * 12 + signer
            + int(order["token_id"]).to_bytes(32, "big")
            + order["maker_amount_raw"].to_bytes(32, "big")
            + order["taker_amount_raw"].to_bytes(32, "big")
            + order["side"].to_bytes(32, "big")
            + order["signature_type"].to_bytes(32, "big")
            + order["timestamp"].to_bytes(32, "big")
            + bytes.fromhex(order["metadata"])
            + bytes.fromhex(order["builder"])
        )

    @classmethod
    def compute_order_hashes(cls, order: dict) -> List[str]:
        """Compute orderHash against both V2 exchange domain separators."""
        struct_hash = cls._compute_struct_hash(order)
        hashes = []
        for exchange in cls.CTF_EXCHANGE_CONTRACTS:
            domain_sep = cls._get_domain_separator(exchange)
            hashes.append("0x" + keccak(b"\x19\x01" + domain_sep + struct_hash).hex())
        return hashes

    def __init__(
        self,
        rpcs: List[str],
        fetch_timestamps: bool = False,
        poll_interval: float = 2.0,
        block_chunk: int = 50,
        ws_url: Optional[str] = None,
        perspective: str = "both",
        keep_trade_intention: bool = True,
        merge_fills: bool = True,
        track_ctf_events: bool = False,
        track_settlements: bool = True,
        track_transfers: bool = False,
        track_oracle: bool = False,
        proxy_cache_path: str = "proxy_wallets.txt",
        transfer_min_amount: float = 0.0,
        registry_path: str = os.environ.get("PM_REGISTRY", "registry/pm_wallets.bin"),
        reward_payers_path: str = os.environ.get("PM_REWARD_PAYERS", "registry/reward_payers.json"),
    ):
        self.rpcs = rpcs
        self.fetch_timestamps = fetch_timestamps
        self.poll_interval = poll_interval
        self.block_chunk = block_chunk
        self.ws_url = ws_url
        self.perspective = perspective
        self.keep_trade_intention = keep_trade_intention
        self.merge_fills = merge_fills
        self.track_ctf_events = track_ctf_events
        self.track_settlements = track_settlements
        self.track_transfers = track_transfers
        self.track_oracle = track_oracle
        self.transfer_min_amount = transfer_min_amount
        # reward-distributor registry {addr->reward_type} (reward_payers table exported to JSON).
        # Lazily loaded + hourly-reloaded in _parse_money_flow so a rotated-in distributor is picked
        # up without a restart. See docs/polymarket_rewards_classification.md.
        self._reward_payers_path = reward_payers_path
        self._reward_payers = {}
        self._reward_payers_ts = 0.0
        if track_oracle:
            import uma_resolution as _uma
            self._uma = _uma

        self._rpc_idx = 0
        self._exchange_addrs = {a.lower() for a in self.CTF_EXCHANGE_CONTRACTS}
        self._operator_addrs = {a.lower() for a in self.OPERATOR_CONTRACTS}
        self._ctf_addr = self.CTF_CONTRACT.lower()
        self._nr_adapter_addrs = {a.lower() for a in self.NEG_RISK_ADAPTERS}

        # Proxy wallet cache for P2P transfer detection (legacy; kept for compat)
        self._proxy_cache: Optional[ProxyWalletCache] = None
        if track_transfers:
            self._proxy_cache = ProxyWalletCache(proxy_cache_path)

        # Registry-backed PM-wallet membership → deposit/p2p classification.
        # The maintainer (registry_updater) publishes the file; we mmap it.
        self._registry = None
        self._classifier = None
        if track_transfers:
            import os as _os
            import pm_classify as _pmc
            if _os.path.exists(registry_path):
                self._registry = _pmc.RegistryFile(registry_path)
                self._classifier = _pmc.Classifier(self._registry)
                print(f"[tracker] registry loaded: {len(self._registry):,} PM wallets "
                      f"from {registry_path}")
            else:
                print(f"[tracker] WARNING: registry file {registry_path} missing "
                      f"-> deposits/p2p will NOT be emitted")

        self._global_subscribers: Set[TradeCallback] = set()
        self._lock = threading.Lock()

        # State
        self._last_block: Optional[int] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

        self._ws_thread: Optional[threading.Thread] = None

        # Counters
        self.trades_rpc = 0       # confirmed trades via RPC polling
        self.trades_ws = 0        # pending trades via WebSocket
        self.trades_total = 0

        # --- Taker info cache (populated by pending path, consumed by confirmed path) ---
        self._tx_taker_cache: Dict[str, dict] = {}  # tx_hash → {taker_wallet, taker_token_id, taker_side, taker_price}
        self._tx_taker_cache_times: Dict[str, float] = {}  # tx_hash → monotonic time

        # --- newHeads-driven confirmed trade processing ---
        self._head_queue: deque = deque()  # block numbers from newHeads, consumed by flush thread
        self._head_queue_lock = threading.Lock()
        self._flush_event = threading.Event()  # signals flush thread to wake immediately
        self._flush_timer_running = False
        self._flush_thread: Optional[threading.Thread] = None
        self._flush_thread_lock = threading.Lock()  # serialize start/stop of flush thread

        # Cache stats
        self.cache_hits = 0
        self.cache_misses = 0
        self.rpc_timeouts = 0

    def subscribe_all(self, callback: TradeCallback) -> None:
        """Subscribe to all trades regardless of condition_id."""
        with self._lock:
            self._global_subscribers.add(callback)

    def unsubscribe_all(self, callback: TradeCallback) -> None:
        """Unsubscribe from all trades."""
        with self._lock:
            self._global_subscribers.discard(callback)

    def _get_ipc_path(self) -> Optional[str]:
        """Return IPC socket path from rpcs list, or None if not using IPC."""
        for rpc in self.rpcs:
            if self._is_ipc(rpc):
                return rpc
        return None

    def start(self, block_number: Optional[int] = None) -> None:
        """Start the tracker in a background thread."""
        if self._running:
            return

        self._last_block = block_number
        self._running = True

        ipc_path = self._get_ipc_path()
        if ipc_path:
            self._ws_thread = threading.Thread(target=self._ipc_listener, args=(ipc_path,), daemon=True)
            self._ws_thread.start()
        elif self.ws_url:
            self._ws_thread = threading.Thread(target=self._ws_listener, daemon=True)
            self._ws_thread.start()
        else:
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Stop the tracker."""
        self._running = False
        self._stop_flush_thread()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        if self._ws_thread:
            self._ws_thread.join(timeout=5)
            self._ws_thread = None

    def tick(self) -> List[Event]:
        """Process one tick manually. Returns list of trades found."""
        return self._process_tick()

    @property
    def last_block(self) -> Optional[int]:
        """Last processed block number."""
        return self._last_block

    @property
    def is_running(self) -> bool:
        """Whether the tracker is running."""
        return self._running

    # -------------------------------------------------------------------------
    # Taker info cache
    # -------------------------------------------------------------------------

    def _cache_taker_info(self, tx_hash: str, input_hex: str, to_addr: str) -> None:
        """Decode V2 calldata and cache taker info for later use by confirmed path."""
        info = decode_operator_match_orders(input_hex, to_addr)
        if info:
            self._tx_taker_cache[tx_hash] = info
            self._tx_taker_cache_times[tx_hash] = time.monotonic()

    def _clean_taker_cache(self, max_age: float = 300.0) -> None:
        """Remove cache entries older than max_age seconds.

        Uses a snapshot of the dict to avoid `dictionary changed size during
        iteration` when the listener thread inserts during iteration.
        """
        now = time.monotonic()
        snapshot = list(self._tx_taker_cache_times.items())
        expired = [h for h, t in snapshot if now - t > max_age]
        for h in expired:
            self._tx_taker_cache.pop(h, None)
            self._tx_taker_cache_times.pop(h, None)

    # -------------------------------------------------------------------------
    # Flush thread lifecycle
    # -------------------------------------------------------------------------

    def _stop_flush_thread(self, join_timeout: float = 10.0) -> None:
        """Signal the flush thread to exit and wait for it. Idempotent."""
        with self._flush_thread_lock:
            self._flush_timer_running = False
            # Wake the thread immediately so it doesn't sit in _flush_event.wait()
            self._flush_event.set()
            t = self._flush_thread
            if t is not None and t.is_alive() and t is not threading.current_thread():
                t.join(timeout=join_timeout)
                if t.is_alive():
                    print("PolymarketTracker: WARNING flush thread did not exit within timeout")
            self._flush_thread = None
            # Reset event so next thread starts cleanly
            self._flush_event.clear()

    def _ensure_flush_thread(self) -> None:
        """Start the flush thread if it isn't already running.

        Acts as both a startup and a watchdog: if the existing thread has
        died (which should never happen given the loop's outer try/except,
        but we guard anyway), join it and spawn a fresh one.
        """
        if not self._running:
            return
        with self._flush_thread_lock:
            t = self._flush_thread
            if t is not None and t.is_alive() and self._flush_timer_running:
                return  # already healthy
            if t is not None and not t.is_alive():
                print("PolymarketTracker: WARNING flush thread was dead — restarting (watchdog)")
                # Best-effort join to clean up the dead thread handle
                try:
                    t.join(timeout=0.1)
                except Exception:
                    pass
            self._flush_timer_running = True
            self._flush_thread = threading.Thread(
                target=self._flush_timer_loop, daemon=True, name="flush-thread"
            )
            self._flush_thread.start()

    # -------------------------------------------------------------------------
    # Internal methods
    # -------------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Main polling loop."""
        while self._running:
            try:
                self._process_tick()
            except Exception as e:
                print(f"PolymarketTracker error: {e}")
            time.sleep(self.poll_interval)

    @staticmethod
    def _is_ipc(endpoint: str) -> bool:
        """Check if an endpoint is an IPC socket path."""
        return endpoint.endswith(".ipc") or (endpoint.startswith("/") and ":" not in endpoint)

    def _ipc_send(self, endpoint: str, payload: dict | list, timeout: float = 10) -> dict | list:
        """Send JSON-RPC payload over IPC Unix socket. Returns parsed response."""
        data = _json_dumps(payload)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect(endpoint)
            sock.sendall(data)
            # Read response — node sends complete JSON then we can parse
            chunks = []
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                # Try parsing — if valid JSON, we're done (avoids waiting for EOF on keep-alive)
                try:
                    return _json_loads(b"".join(chunks))
                except (ValueError, KeyError):
                    continue
            return _json_loads(b"".join(chunks))
        finally:
            sock.close()

    def _rpc_call(self, method: str, params: list) -> Optional[dict]:
        """Make a single RPC call with rotation on failure. Supports HTTP and IPC."""
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        for attempt in range(len(self.rpcs)):
            endpoint = self.rpcs[(self._rpc_idx + attempt) % len(self.rpcs)]
            try:
                if self._is_ipc(endpoint):
                    data = self._ipc_send(endpoint, payload)
                else:
                    r = requests.post(
                        endpoint, data=_json_dumps(payload),
                        headers={"Content-Type": "application/json"},
                        timeout=10,
                    )
                    data = _json_loads(r.content)
                if "result" in data:
                    self._rpc_idx = (self._rpc_idx + attempt) % len(self.rpcs)
                    return data["result"]
            except Exception:
                pass
        return None

    def _rpc_batch(self, calls: List[Tuple[str, list]], timeout: Optional[float] = None) -> List[Optional[dict]]:
        """Execute multiple RPC calls in a single batch request. Supports HTTP and IPC."""
        if not calls:
            return []

        batch = [
            {"jsonrpc": "2.0", "id": i, "method": method, "params": params}
            for i, (method, params) in enumerate(calls)
        ]

        if timeout is None:
            # Scale timeout with batch size (min 30s, +5s per 100 calls)
            timeout = max(30, 30 + len(calls) // 20)

        for attempt in range(len(self.rpcs)):
            endpoint = self.rpcs[(self._rpc_idx + attempt) % len(self.rpcs)]
            try:
                if self._is_ipc(endpoint):
                    responses = self._ipc_send(endpoint, batch, timeout=timeout)
                else:
                    r = requests.post(
                        endpoint, data=_json_dumps(batch),
                        headers={"Content-Type": "application/json"},
                        timeout=timeout,
                    )
                    responses = _json_loads(r.content)
                self._rpc_idx = (self._rpc_idx + attempt) % len(self.rpcs)

                results = [None] * len(calls)
                got = 0
                for resp in responses:
                    idx = resp.get("id")
                    if idx is not None and 0 <= idx < len(calls) and "result" in resp:
                        results[idx] = resp["result"]
                        got += 1
                # A per-call error (response carries "error" not "result", or the node
                # dropped a call) leaves results[idx]=None — indistinguishable from a
                # legitimate empty getLogs. Flag it as a timeout so the adaptive split
                # treats the range as suspect (split/retry, then fail loud) rather than
                # trusting partial/empty data. This is what lets callers TRUST an empty
                # result whenever rpc_timeouts did NOT change — no blind sleep-retries.
                # (A successful call returning null, e.g. getTransactionByHash on an
                # unknown hash, still has the "result" key and is NOT flagged.)
                if got < len(calls):
                    self.rpc_timeouts += 1
                return results
            except Exception:
                self.rpc_timeouts += 1

        return [None] * len(calls)

    def _fetch_logs_for_range(self, from_block: int, to_block: int, head: Optional[int] = None) -> List[Event]:
        """Fetch and process OrderFilled + CTF position event logs for a block range.

        Cache-aware: checks _tx_taker_cache before fetching tx data via RPC.
        """
        if head is None:
            head = to_block

        block_range = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}

        # Batch all eth_getLogs calls into a single RPC batch
        log_batch_calls = [
            ("eth_getLogs", [{
                **block_range,
                "address": self.CTF_EXCHANGE_CONTRACTS,
                "topics": [self.ORDER_FILLED_TOPIC],
            }]),
        ]
        log_batch_idx = {"orders": 0}
        next_idx = 1

        if self.track_ctf_events:
            log_batch_calls.append(("eth_getLogs", [{
                **block_range,
                "address": [self.CTF_CONTRACT, *self.NEG_RISK_ADAPTERS],
                "topics": [self.CTF_POSITION_TOPICS],
            }]))
            log_batch_idx["ctf"] = next_idx
            next_idx += 1

        if self.track_settlements:
            log_batch_calls.append(("eth_getLogs", [{
                **block_range,
                "address": self.CTF_CONTRACT,
                "topics": [[self.CONDITION_RESOLUTION]],
            }]))
            log_batch_idx["settlements"] = next_idx
            next_idx += 1

        if self.track_transfers:
            log_batch_calls.append(("eth_getLogs", [{
                **block_range,
                "address": self.STABLECOIN_CONTRACTS,
                "topics": [self.ERC20_TRANSFER_TOPIC],
            }]))
            log_batch_idx["transfers"] = next_idx
            next_idx += 1

        if self.track_oracle:
            log_batch_calls.append(("eth_getLogs", [{
                **block_range,
                "address": self._uma.OO_CONTRACTS,
                "topics": [self._uma.TOPICS],
            }]))
            log_batch_idx["oracle"] = next_idx
            next_idx += 1

        log_batch_results = self._rpc_batch(log_batch_calls, timeout=60)

        order_logs = log_batch_results[log_batch_idx["orders"]]
        if order_logs is None:
            print(f"PolymarketTracker: eth_getLogs returned None for blocks {from_block}-{to_block}")
            return []
        order_logs = order_logs or []

        ctf_logs = []
        if "ctf" in log_batch_idx:
            ctf_logs_raw = log_batch_results[log_batch_idx["ctf"]] or []
            for log in ctf_logs_raw:
                wallet = ("0x" + log["topics"][1][-40:]).lower()
                if wallet not in self._exchange_addrs and wallet not in self._nr_adapter_addrs:
                    ctf_logs.append(log)

        settlement_logs = []
        if "settlements" in log_batch_idx:
            settlement_logs = log_batch_results[log_batch_idx["settlements"]] or []

        transfer_logs = []
        if "transfers" in log_batch_idx:
            transfer_logs = log_batch_results[log_batch_idx["transfers"]] or []

        oracle_logs = []
        if "oracle" in log_batch_idx:
            oracle_logs = log_batch_results[log_batch_idx["oracle"]] or []

        all_logs = order_logs + ctf_logs + settlement_logs + transfer_logs + oracle_logs

        block_timestamps: Dict[int, datetime] = {}
        tx_taker_info: Dict[str, dict] = {}
        if all_logs:
            distinct_blocks = sorted({int(log["blockNumber"], 16) for log in all_logs})
            routed_tx_hashes = sorted({
                log["transactionHash"] for log in order_logs
                if ("0x" + log["topics"][3][-40:]).lower() in self._exchange_addrs
            })

            # Cache-aware: check _tx_taker_cache before building RPC batch
            uncached_tx_hashes = []
            for h in routed_tx_hashes:
                cached = self._tx_taker_cache.get(h)
                if cached:
                    tx_taker_info[h] = cached
                    self.cache_hits += 1
                else:
                    uncached_tx_hashes.append(h)
                    self.cache_misses += 1

            # Fetch block timestamps separately (small batch, must not fail with tx lookups)
            if self.fetch_timestamps and distinct_blocks:
                ts_calls = [("eth_getBlockByNumber", [hex(bn), False]) for bn in distinct_blocks]
                ts_results = self._rpc_batch(ts_calls)
                for bn, block_data in zip(distinct_blocks, ts_results):
                    if not block_data:
                        continue
                    if "timestamp" in block_data:
                        block_timestamps[bn] = datetime.fromtimestamp(
                            int(block_data["timestamp"], 16), tz=timezone.utc
                        )

            # Fetch uncached tx data for taker resolution (separate batch)
            if uncached_tx_hashes:
                TX_BATCH_SIZE = 200
                for i in range(0, len(uncached_tx_hashes), TX_BATCH_SIZE):
                    sub = uncached_tx_hashes[i:i + TX_BATCH_SIZE]
                    tx_calls = [("eth_getTransactionByHash", [h]) for h in sub]
                    tx_results = self._rpc_batch(tx_calls)
                    for tx_data in tx_results:
                        if not tx_data:
                            continue
                        info = decode_operator_match_orders(tx_data.get("input", ""), tx_data.get("to", ""))
                        if info:
                            tx_taker_info[tx_data["hash"]] = info

        blockchain_lag = head - to_block

        trades = parse_order_filled_logs_v2(
            logs=order_logs,
            exchange_addrs=self._exchange_addrs,
            block_timestamps=block_timestamps,
            default_timestamp=None,
            blockchain_lag=blockchain_lag,
            tx_taker_info=tx_taker_info,
            keep_trade_intention=self.keep_trade_intention,
        )

        # Parse CTF position events
        # Only converts need tx receipts (for TransferBatch burned/minted token IDs).
        # Splits, merges derive token IDs from log data; redeems use conditionId only.
        if ctf_logs:
            receipt_topics = {self.POSITIONS_CONVERTED}
            receipt_tx_hashes = sorted({
                log["transactionHash"] for log in ctf_logs
                if log["topics"][0] in receipt_topics
            })
            tx_receipts = {}
            # Sub-batch receipt requests to avoid choking the RPC
            RECEIPT_BATCH_SIZE = 200
            for i in range(0, len(receipt_tx_hashes), RECEIPT_BATCH_SIZE):
                sub = receipt_tx_hashes[i:i + RECEIPT_BATCH_SIZE]
                receipt_calls = [("eth_getTransactionReceipt", [h]) for h in sub]
                receipt_results = self._rpc_batch(receipt_calls)
                for h, r in zip(sub, receipt_results):
                    if r:
                        tx_receipts[h] = r

            ctf_trades = self._parse_ctf_position_events(
                logs=ctf_logs,
                block_timestamps=block_timestamps,
                blockchain_lag=blockchain_lag,
                tx_receipts=tx_receipts,
            )
            trades.extend(ctf_trades)

        # Parse settlement events
        if settlement_logs:
            settlement_trades = self._parse_settlement_events(
                logs=settlement_logs,
                block_timestamps=block_timestamps,
                blockchain_lag=blockchain_lag,
            )
            trades.extend(settlement_trades)

        # Parse stablecoin transfer events (deposits + P2P)
        if transfer_logs:
            transfer_events = self._parse_transfer_logs(
                logs=transfer_logs,
                block_timestamps=block_timestamps,
                blockchain_lag=blockchain_lag,
            )
            trades.extend(transfer_events)

            # NEW: corrected TX-LEVEL money-flow events (deposit/withdrawal x type),
            # emitted ALONGSIDE the legacy deposit/p2p events for the transfers_v2 feed.
            # Fully isolated: a failure here can never affect the existing pipeline.
            if self._classifier is not None:
                try:
                    trades.extend(self._parse_money_flow(
                        transfer_logs, order_logs, ctf_logs, block_timestamps, blockchain_lag))
                except Exception as _mf_e:
                    print(f"money_flow classify error: {_mf_e}", flush=True)

        # Parse UMA oracle resolution events (propose / dispute / settle)
        if oracle_logs:
            trades.extend(self._parse_oracle_events(
                logs=oracle_logs,
                block_timestamps=block_timestamps,
                blockchain_lag=blockchain_lag,
            ))

        if trades:
            self._emit_trades(trades)

        return trades

    def _parse_oracle_events(self, logs, block_timestamps, blockchain_lag=0):
        """Decode UMA OO logs into ResolutionEvents (propose/dispute/settle)."""
        now = datetime.now(timezone.utc)
        events: List[Event] = []
        for log in logs:
            d = self._uma.decode(log)
            if d is None:
                continue
            bn = d["block_number"]
            events.append(ResolutionEvent(
                event_type=d["event_type"],
                wallet_address=d.get("proposer") or "",
                asset_id=d.get("market_id") or "",
                block_number=bn,
                block_timestamp=block_timestamps.get(bn),
                tx_hash=d["tx_hash"],
                log_index=d["log_index"],
                blockchain_lag=blockchain_lag,
                timestamp=now,
                oracle_contract=d["oracle_contract"],
                requester=d.get("requester") or "",
                proposer=d.get("proposer") or "",
                disputer=d.get("disputer"),
                identifier=d.get("identifier"),
                oracle_timestamp=d.get("oracle_timestamp"),
                market_id=d.get("market_id"),
                proposed_price=d.get("proposed_price"),
                resolved_price=d.get("resolved_price"),
                expiration_time=d.get("expiration_time"),
                payout=d.get("payout"),
                currency=d.get("currency"),
                outcome=d.get("outcome"),
                ancillary_text=d.get("ancillary_text"),
                ancillary_hash=d.get("ancillary_hash"),
            ))
        return events

    def _process_tick(self) -> List[Event]:
        """Process one chunk of blocks via RPC polling. Returns trades found."""
        head_hex = self._rpc_call("eth_blockNumber", [])
        if not head_hex:
            return []
        head = int(head_hex, 16)

        if self._last_block is None:
            self._last_block = head - 10

        from_block = self._last_block
        if from_block > head:
            return []

        to_block = min(from_block + self.block_chunk, head)
        trades = self._fetch_logs_for_range(from_block, to_block, head)
        self._last_block = to_block + 1
        return trades

    # -------------------------------------------------------------------------
    # newHeads-driven confirmed trade processing
    #
    # Design: WS callback queues block numbers (fast, no RPC). Flush thread
    # calls eth_getLogs per block (atomic, complete) and processes trades.
    # -------------------------------------------------------------------------

    def _on_new_head(self, head: dict) -> None:
        """Handle newHeads notification — queue block for eth_getLogs processing."""
        block_num = int(head["number"], 16)
        with self._head_queue_lock:
            self._head_queue.append(block_num)
        # Watchdog: if the flush thread has died for any reason, revive it
        # before signalling — otherwise the event would be set with no consumer
        # and confirmed-trade emission would silently stop.
        t = self._flush_thread
        if self._running and (t is None or not t.is_alive()):
            self._ensure_flush_thread()
        self._flush_event.set()

    def _flush_timer_loop(self) -> None:
        """Background flush thread. Wakes on newHeads signal, processes queued blocks.

        For each block: calls eth_getLogs (atomic, complete log set), then
        processes with cache-aware taker info resolution.

        The whole loop body is wrapped in a top-level try/except so this thread
        can never die from an unexpected exception — if it did, confirmed-trade
        emission would silently stop.
        """
        clean_counter = 0
        flush_count = 0
        print("PolymarketTracker: flush thread started")
        while self._flush_timer_running and self._running:
            blocks: List[int] = []
            try:
                self._flush_event.wait(timeout=5.0)
                self._flush_event.clear()
                try:
                    # Drain all queued block numbers
                    with self._head_queue_lock:
                        while self._head_queue:
                            blocks.append(self._head_queue.popleft())

                    if not blocks:
                        clean_counter += 1
                        if clean_counter >= 12:  # Clean cache every ~60s (12 × 5s timeout)
                            clean_counter = 0
                            self._clean_taker_cache()
                        continue

                    # Process blocks: batch consecutive blocks into ranges for efficiency
                    blocks.sort()
                    # Deduplicate (shouldn't happen, but defensive)
                    blocks = sorted(set(blocks))

                    # Set _last_block if not initialized
                    if self._last_block is None:
                        self._last_block = blocks[0]

                    # Process from _last_block through max queued block
                    from_block = self._last_block
                    to_block = blocks[-1]
                    head = to_block

                    if from_block > to_block:
                        continue

                    # Cap range to avoid huge fetches after stalls
                    MAX_FLUSH_RANGE = 100
                    if to_block - from_block > MAX_FLUSH_RANGE:
                        skipped = from_block
                        from_block = to_block - MAX_FLUSH_RANGE
                        print(f"PolymarketTracker: flush fell behind, skipping blocks {skipped}-{from_block - 1}")

                    t0 = time.monotonic()
                    events = self._fetch_logs_for_range(from_block, to_block, head)
                    elapsed = time.monotonic() - t0
                    flush_count += 1
                    self._last_block = to_block + 1
                    self._flush_errors = 0

                    # Log every 10th flush, or always if slow/empty
                    if flush_count % 10 == 1 or elapsed > 5.0 or not events:
                        print(f"  flush #{flush_count}: blocks {from_block}-{to_block} "
                              f"({to_block - from_block + 1} blocks) -> {len(events)} events "
                              f"in {elapsed:.1f}s")

                except Exception as e:
                    self._flush_errors = getattr(self, "_flush_errors", 0) + 1
                    print(f"PolymarketTracker flush error ({self._flush_errors}): {e}")
                    import traceback
                    traceback.print_exc()
                    # After 3 consecutive failures, skip ahead to avoid stuck loop
                    if self._flush_errors >= 3 and blocks:
                        print(f"PolymarketTracker: skipping to block {blocks[-1] + 1} after {self._flush_errors} failures")
                        self._last_block = blocks[-1] + 1
                        self._flush_errors = 0

                clean_counter += 1
                if clean_counter >= 12:  # Clean cache every ~60s (12 × 5s timeout)
                    clean_counter = 0
                    try:
                        self._clean_taker_cache()
                    except Exception as e:
                        print(f"PolymarketTracker: _clean_taker_cache failed: {e}")

            except Exception as e:
                # Last-resort guard: nothing should reach here, but if it does
                # we keep the thread alive so confirmed-trade emission survives.
                print(f"PolymarketTracker: flush loop top-level exception: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)

        print(f"PolymarketTracker: flush thread exiting (running={self._running}, timer={self._flush_timer_running})")

    # -------------------------------------------------------------------------
    # Calldata decoding (pending path)
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_order_struct(params: str, offset: int) -> Optional[dict]:
        """Parse a V2 Order struct at the given hex char offset in params."""
        return _parse_order_struct_v2(params, offset)

    def _decode_operator_calldata(self, input_hex: str, tx_hash: str, to_addr: str,
                                     keep_trade_intention: bool = True,
                                     source: str = "pending",
                                     block_number: int = 0,
                                     block_timestamp: Optional[datetime] = None,
                                     operator_nonce: Optional[int] = None,
                                     operator_address: Optional[str] = None) -> List[Event]:
        """Decode V2 exchange matchOrders calldata into trades.

        V2 matchOrders(bytes32 conditionId, Order takerOrder, Order[] makerOrders,
                       uint256 takerFillAmount, uint256[] makerFillAmounts,
                       uint256 takerFeeAmount, uint256[] makerFeeAmounts)
        """
        exch = "NegRiskExchange" if to_addr.lower() == self.NEG_RISK_EXCHANGE.lower() else "Exchange"
        data = input_hex[2:] if input_hex.startswith("0x") else input_hex

        if len(data) < 8 or data[:8] != self.OPERATOR_SELECTOR:
            return []

        params = data[8:]

        # V2 layout: conditionId(0), takerOrder(offset 1), makerOrders(offset 2),
        #            takerFillAmount(3), makerFillAmounts(offset 4),
        #            takerFeeAmount(5), makerFeeAmounts(offset 6)
        if len(params) < 448:  # 7 words
            return []

        # word 0: conditionId (skip)
        # word 1: offset to takerOrder
        taker_order_offset = int(params[64:128], 16) * 2
        # word 2: offset to makerOrders
        maker_orders_offset = int(params[128:192], 16) * 2
        # word 3: takerFillAmount
        taker_fill_raw = int(params[192:256], 16)
        # word 4: offset to makerFillAmounts
        maker_fills_offset = int(params[256:320], 16) * 2

        maker_fill_raws = []
        maker_fills_data = params[maker_fills_offset:]
        if len(maker_fills_data) >= 64:
            fill_count = int(maker_fills_data[:64], 16)
            for i in range(fill_count):
                start = 64 + i * 64
                if len(maker_fills_data) < start + 64:
                    break
                maker_fill_raws.append(int(maker_fills_data[start:start + 64], 16))

        taker = _parse_order_struct_v2(params, taker_order_offset)
        if not taker:
            return []

        taker_wallet = taker["maker"]

        def _compute_hashes(order_dict):
            try:
                return self.compute_order_hashes(order_dict)
            except Exception:
                return []

        def _make_trade(wallet, token_id, maker_amt, taker_amt, side, role,
                        counterparty, hashes, fill_raw=None, order_maker_raw=None,
                        price_override=None):
            if side == 0:
                op, shares = "buy", float(taker_amt)
                price = float(maker_amt / taker_amt) if taker_amt else None
            else:
                op, shares = "sell", float(maker_amt)
                price = float(taker_amt / maker_amt) if maker_amt else None
            if price_override is not None:
                price = price_override
            if fill_raw is not None and order_maker_raw:
                shares = shares * fill_raw / order_maker_raw
            return TradeEvent(
                wallet_address=wallet, asset_id=token_id,
                operation_type=op, size=shares, price=price,
                counterparty=counterparty, block_number=block_number,
                block_timestamp=block_timestamp,
                tx_hash=tx_hash, wallet_role=role,
                blockchain_lag=0,
                timestamp=datetime.now(timezone.utc),
                source=source,
                order_hashes=hashes or None,
                exchange=exch,
            )

        trades = []
        taker_hashes = _compute_hashes(taker)

        maker_array = params[maker_orders_offset:]
        if len(maker_array) >= 64:
            maker_count = int(maker_array[:64], 16)
            for i in range(maker_count):
                offset_pos = 64 + i * 64
                if len(maker_array) < offset_pos + 64:
                    break
                rel_offset = int(maker_array[offset_pos:offset_pos + 64], 16) * 2
                m = _parse_order_struct_v2(maker_array, 64 + rel_offset)
                if not m:
                    continue
                maker_hashes = _compute_hashes(m)
                fill = maker_fill_raws[i] if i < len(maker_fill_raws) else None

                maker_trade = _make_trade(
                    m["maker"], m["token_id"],
                    m["maker_amount"], m["taker_amount"],
                    m["side"], "maker", taker_wallet, maker_hashes,
                    fill_raw=fill, order_maker_raw=m["maker_amount_raw"],
                )
                trades.append(maker_trade)

                if taker["token_id"] == m["token_id"]:
                    taker_price = maker_trade.price
                else:
                    taker_price = (1.0 - maker_trade.price) if maker_trade.price is not None else None

                if keep_trade_intention:
                    t_token_id = taker["token_id"]
                    t_side = taker["side"]
                    t_price_override = taker_price
                else:
                    t_token_id = m["token_id"]
                    t_side = 1 if m["side"] == 0 else 0
                    t_price_override = maker_trade.price

                trades.append(TradeEvent(
                    wallet_address=taker_wallet,
                    asset_id=t_token_id,
                    operation_type="buy" if t_side == 0 else "sell",
                    size=maker_trade.size,
                    price=t_price_override,
                    counterparty=m["maker"],
                    block_number=block_number,
                    block_timestamp=block_timestamp,
                    tx_hash=tx_hash,
                    wallet_role="taker",
                    blockchain_lag=0,
                    timestamp=datetime.now(timezone.utc),
                    source=source,
                    order_hashes=taker_hashes or None,
                    intent_adjusted=(keep_trade_intention and taker["token_id"] != m["token_id"]),
                    exchange=exch,
                ))

        if not trades:
            trades.append(_make_trade(
                taker_wallet, taker["token_id"],
                taker["maker_amount"], taker["taker_amount"],
                taker["side"], "taker", None, taker_hashes,
                fill_raw=taker_fill_raw, order_maker_raw=taker["maker_amount_raw"],
            ))

        if operator_nonce is not None or operator_address is not None:
            for t in trades:
                t.operator_nonce = operator_nonce
                t.operator_address = operator_address

        return trades

    # -------------------------------------------------------------------------
    # IPC listener (persistent Unix socket for local Bor node)
    # -------------------------------------------------------------------------

    def _ipc_listener(self, ipc_path: str) -> None:
        """IPC listener — single persistent Unix socket for subscriptions + RPC.

        Replaces both WebSocket and HTTP RPC when using a local node.
        Uses newline-delimited JSON over Unix domain socket.
        """
        while self._running:
            sock = None
            try:
                # Stop any existing flush thread cleanly before reconnect — prevents
                # zombie threads from racing on _last_block / _head_queue.
                self._stop_flush_thread()

                # Clear queue on reconnect
                with self._head_queue_lock:
                    self._head_queue.clear()

                # Catch up on missed blocks
                if self._last_block is not None:
                    head_hex = self._rpc_call("eth_blockNumber", [])
                    if head_hex:
                        head = int(head_hex, 16)
                        if head >= self._last_block:
                            self._fetch_logs_for_range(self._last_block, head, head)
                            self._last_block = head + 1

                # Connect
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(ipc_path)
                sock.settimeout(30)  # 30s read timeout — reconnect if Bor hangs

                # Subscription tracking
                sub_request_ids: Dict[int, str] = {}   # request_id → sub type
                sub_ids: Dict[str, str] = {}           # subscription_id → sub type

                # Sub 1: Pending transactions (full tx objects)
                sub_request_ids[1] = "pending"
                sock.sendall(_json_dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "eth_subscribe",
                    "params": ["newPendingTransactions", True]
                }) + b"\n")

                # Sub 2: newHeads — triggers eth_getLogs for confirmed trades
                sub_request_ids[2] = "new_heads"
                sock.sendall(_json_dumps({
                    "jsonrpc": "2.0", "id": 2,
                    "method": "eth_subscribe",
                    "params": ["newHeads"]
                }) + b"\n")

                # Start flush thread (or watchdog-restart if it died)
                self._ensure_flush_thread()

                print(f"PolymarketTracker: IPC connected to {ipc_path}")

                # Read loop — newline-delimited JSON
                buf = b""
                while self._running:
                    try:
                        data = sock.recv(65536)
                    except socket.timeout:
                        print("PolymarketTracker: IPC read timeout (30s), reconnecting...")
                        break
                    if not data:
                        break  # connection closed

                    buf += data

                    # Parse complete JSON messages (newline-delimited)
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            msg = _json_loads(line)
                        except (ValueError, KeyError):
                            continue

                        # Subscription confirmation
                        if "id" in msg and "result" in msg:
                            req_id = msg["id"]
                            if req_id in sub_request_ids:
                                sub_type = sub_request_ids.pop(req_id)
                                sub_ids[msg["result"]] = sub_type
                            continue

                        # Subscription notification
                        if "params" not in msg:
                            continue

                        sub_id = msg["params"].get("subscription")
                        result = msg["params"].get("result")
                        if result is None:
                            continue

                        sub_type = sub_ids.get(sub_id)

                        if sub_type == "pending":
                            self._handle_pending_tx(result)
                        elif sub_type == "new_heads":
                            self._on_new_head(result)

            except Exception as e:
                if self._running:
                    print(f"PolymarketTracker IPC reconnecting after: {e}")
            finally:
                self._stop_flush_thread()
                if sock:
                    try:
                        sock.close()
                    except Exception:
                        pass
            if self._running:
                time.sleep(2)

    # -------------------------------------------------------------------------
    # WebSocket listener (newHeads + pending txs)
    # -------------------------------------------------------------------------

    def _ws_listener(self) -> None:
        """WebSocket listener: pending txs + newHeads-driven eth_getLogs for confirmed trades."""
        try:
            import websocket
        except ImportError:
            print("PolymarketTracker: websocket-client not installed, WS mode disabled")
            return

        # Subscription tracking
        sub_request_ids: Dict[int, str] = {}   # request_id → sub type
        sub_ids: Dict[str, str] = {}           # subscription_id → sub type

        def on_message(ws, message):
            try:
                msg = _json_loads(message)
            except (ValueError, KeyError):
                return

            # Subscription confirmation response
            if "id" in msg and "result" in msg:
                req_id = msg["id"]
                if req_id in sub_request_ids:
                    sub_type = sub_request_ids.pop(req_id)
                    sub_ids[msg["result"]] = sub_type
                return

            # Subscription notification
            if "params" not in msg:
                return

            sub_id = msg["params"].get("subscription")
            result = msg["params"].get("result")
            if result is None:
                return

            sub_type = sub_ids.get(sub_id)

            if sub_type == "pending":
                self._handle_pending_tx(result)
            elif sub_type == "new_heads":
                self._on_new_head(result)

        def on_open(ws):
            sub_request_ids.clear()
            sub_ids.clear()

            # Sub 1: Pending transactions (full tx objects)
            sub_request_ids[1] = "pending"
            ws.send(_json_dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newPendingTransactions", True]
            }))

            # Sub 2: newHeads — triggers eth_getLogs for confirmed trades
            sub_request_ids[2] = "new_heads"
            ws.send(_json_dumps({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            }))

            # Start flush thread (or watchdog-restart if it died)
            self._ensure_flush_thread()

        def on_error(ws, error):
            if self._running:
                print(f"PolymarketTracker WS error: {error}")

        while self._running:
            try:
                # Stop any existing flush thread cleanly before reconnect.
                self._stop_flush_thread()

                # Clear queue on reconnect
                with self._head_queue_lock:
                    self._head_queue.clear()

                # Catch up on missed blocks after reconnect
                if self._last_block is not None:
                    head_hex = self._rpc_call("eth_blockNumber", [])
                    if head_hex:
                        head = int(head_hex, 16)
                        if head >= self._last_block:
                            self._fetch_logs_for_range(self._last_block, head, head)
                            self._last_block = head + 1

                ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                )
                ws.run_forever(ping_interval=60, ping_timeout=30)
            except Exception as e:
                if self._running:
                    print(f"PolymarketTracker WS reconnecting after: {e}")
            self._stop_flush_thread()
            if self._running:
                time.sleep(2)

    # -------------------------------------------------------------------------
    # Pending tx handlers (modified to cache taker info)
    # -------------------------------------------------------------------------

    def _handle_pending_tx(self, tx: dict) -> None:
        """Process a single pending transaction object."""
        to_addr = (tx.get("to") or "").lower()
        if to_addr not in self._operator_addrs:
            return

        tx_hash = tx.get("hash", "")
        input_data = tx.get("input", "")

        # Extract operator from/nonce for replacement tracking
        op_from = (tx.get("from") or "").lower() or None
        nonce_hex = tx.get("nonce")
        op_nonce = int(nonce_hex, 16) if nonce_hex else None

        # Cache taker info for later use by confirmed path
        self._cache_taker_info(tx_hash, input_data, to_addr)

        trades = self._decode_operator_calldata(input_data, tx_hash, to_addr,
                                                    keep_trade_intention=self.keep_trade_intention,
                                                    operator_nonce=op_nonce,
                                                    operator_address=op_from)
        if trades:
            self._emit_trades(trades)

    # -------------------------------------------------------------------------
    # Trade merging + CTF parsing
    # -------------------------------------------------------------------------

    @staticmethod
    def _merge_fills(trades: List[Event]) -> List[Event]:
        """Merge multiple fills of the *same order* into one trade.

        Key includes order hash so distinct orders from the same wallet in the
        same tx do NOT collapse together. Taker fills (which all share the same
        taker order hash, or None for confirmed-side takers) still merge.
        """
        non_trades: List[Event] = []
        groups: Dict[tuple, List[TradeEvent]] = {}
        for t in trades:
            if not isinstance(t, TradeEvent):
                non_trades.append(t)
                continue
            if t.order_hashes:
                hash_key = tuple(sorted(t.order_hashes))
            else:
                hash_key = (t.order_hash,)
            key = (t.tx_hash, t.wallet_address, t.asset_id, t.operation_type, hash_key)
            groups.setdefault(key, []).append(t)

        merged: List[Event] = []
        for key, fills in groups.items():
            if len(fills) == 1:
                merged.append(fills[0])
                continue
            total_size = sum(f.size for f in fills)
            total_fee = sum(f.fee for f in fills)
            if total_size > 0 and all(f.price is not None for f in fills):
                avg_price = sum(f.size * f.price for f in fills) / total_size
            else:
                avg_price = fills[0].price
            counterparties = {f.counterparty for f in fills if f.counterparty}
            counterparty = counterparties.pop() if len(counterparties) == 1 else None
            merged.append(TradeEvent(
                wallet_address=fills[0].wallet_address,
                asset_id=fills[0].asset_id,
                operation_type=fills[0].operation_type,
                size=total_size,
                price=avg_price,
                fee=total_fee,
                counterparty=counterparty,
                block_number=fills[0].block_number,
                block_timestamp=fills[0].block_timestamp,
                tx_hash=fills[0].tx_hash,
                wallet_role=fills[0].wallet_role,
                blockchain_lag=fills[0].blockchain_lag,
                timestamp=fills[0].timestamp,
                source=fills[0].source,
                order_hash=fills[0].order_hash,
                order_hashes=fills[0].order_hashes,
                intent_adjusted=fills[0].intent_adjusted,
                log_index=fills[0].log_index,
                exchange=fills[0].exchange,
            ))
        merged.extend(non_trades)
        return merged

    def _parse_ctf_position_events(
        self,
        logs: List[dict],
        block_timestamps: Dict[int, datetime],
        blockchain_lag: int = 0,
        tx_receipts: Optional[Dict[str, dict]] = None,
    ) -> List[Event]:
        """Parse CTF position events (split/merge/redeem) into trades."""
        now = datetime.now(timezone.utc)
        trades: List[Event] = []
        receipts = tx_receipts or {}
        zero = "0x" + "0" * 40

        tx_receipt_index: Dict[str, List[dict]] = {}
        for tx_hash in {log["transactionHash"] for log in logs}:
            receipt = receipts.get(tx_hash)
            if receipt:
                tx_receipt_index[tx_hash] = receipt.get("logs", [])

        for log in logs:
            topic0 = log["topics"][0]
            wallet = ("0x" + log["topics"][1][-40:]).lower()

            if wallet in self._exchange_addrs or wallet in self._nr_adapter_addrs:
                continue

            tx_hash = log["transactionHash"]
            block_num = int(log["blockNumber"], 16)
            ts = block_timestamps.get(block_num)
            event_log_index = int(log["logIndex"], 16)

            data = log["data"]
            is_neg_risk = topic0 in (
                self.POSITION_SPLIT_NR, self.POSITIONS_MERGE_NR,
                self.POSITIONS_CONVERTED,
            )

            # CTF events: data = [collateralToken, partition/indexSets(offset), amount]
            # NegRisk events: data = [amount] only
            # PayoutRedemption: data = [conditionId, indexSets(offset), payout]
            if topic0 == self.PAYOUT_REDEMPTION:
                amount_raw = int(data[130:194], 16)  # payout (word 2)
            elif is_neg_risk:
                amount_raw = int(data[2:66], 16)     # amount (word 0, only word)
            else:
                amount_raw = int(data[130:194], 16)   # amount (word 2)
            amount = float(Decimal(amount_raw) / Decimal(1_000_000))

            receipt_logs = tx_receipt_index.get(tx_hash, [])

            if topic0 in (self.POSITION_SPLIT_CTF, self.POSITION_SPLIT_NR):
                # Derive token IDs from conditionId + partition
                if is_neg_risk:
                    # NegRisk: conditionId in topics[2], always both outcomes
                    condition_id = bytes.fromhex(log["topics"][2][2:])
                    partition = [1, 2]
                else:
                    # CTF: conditionId in topics[3], partition in data
                    condition_id = bytes.fromhex(log["topics"][3][2:])
                    partition = self._decode_uint256_array(data)
                for idx_set in partition:
                    tid = _derive_token_id(condition_id, idx_set, neg_risk=is_neg_risk)
                    trades.append(SplitEvent(
                        wallet_address=wallet, asset_id=tid,
                        size=amount,
                        block_number=block_num,
                        block_timestamp=ts, tx_hash=tx_hash,
                        blockchain_lag=blockchain_lag,
                        timestamp=now,
                        log_index=event_log_index,
                        condition_id=condition_id.hex(),
                    ))

            elif topic0 in (self.POSITIONS_MERGE_CTF, self.POSITIONS_MERGE_NR):
                # Derive token IDs from conditionId + partition
                if is_neg_risk:
                    condition_id = bytes.fromhex(log["topics"][2][2:])
                    partition = [1, 2]
                else:
                    condition_id = bytes.fromhex(log["topics"][3][2:])
                    partition = self._decode_uint256_array(data)
                for idx_set in partition:
                    tid = _derive_token_id(condition_id, idx_set, neg_risk=is_neg_risk)
                    trades.append(MergeEvent(
                        wallet_address=wallet, asset_id=tid,
                        size=amount,
                        block_number=block_num,
                        block_timestamp=ts, tx_hash=tx_hash,
                        blockchain_lag=blockchain_lag,
                        timestamp=now,
                        log_index=event_log_index,
                        condition_id=condition_id.hex(),
                    ))

            elif topic0 == self.PAYOUT_REDEMPTION:
                # One event per conditionId — no receipt needed.
                # amount_raw (word 2) = total USDC payout
                condition_id = bytes.fromhex(data[2:66])
                trades.append(RedeemEvent(
                    wallet_address=wallet, asset_id="",
                    payout_usdc=amount,
                    block_number=block_num,
                    block_timestamp=ts, tx_hash=tx_hash,
                    blockchain_lag=blockchain_lag,
                    timestamp=now,
                    log_index=event_log_index,
                    condition_id=condition_id.hex(),
                ))

            elif topic0 == self.POSITIONS_CONVERTED:
                # Conversion: burn source tokens, mint target tokens.
                # Always NegRisk: conditionId in topics[2]
                condition_id = bytes.fromhex(log["topics"][2][2:])
                burned_ids, _ = self._find_nearest_transfer_batch(
                    receipt_logs, event_log_index, wallet, mint=False)
                minted_ids, _ = self._find_nearest_transfer_batch(
                    receipt_logs, event_log_index, wallet, mint=True)
                for tid in burned_ids:
                    trades.append(ConvertEvent(
                        wallet_address=wallet, asset_id=tid,
                        operation_type="sell", size=amount,
                        block_number=block_num,
                        block_timestamp=ts, tx_hash=tx_hash,
                        blockchain_lag=blockchain_lag,
                        timestamp=now,
                        log_index=event_log_index,
                        condition_id=condition_id.hex(),
                    ))
                for tid in minted_ids:
                    trades.append(ConvertEvent(
                        wallet_address=wallet, asset_id=tid,
                        operation_type="buy", size=amount,
                        block_number=block_num,
                        block_timestamp=ts, tx_hash=tx_hash,
                        blockchain_lag=blockchain_lag,
                        timestamp=now,
                        log_index=event_log_index,
                        condition_id=condition_id.hex(),
                    ))

        return trades

    def _parse_settlement_events(
        self,
        logs: List[dict],
        block_timestamps: Dict[int, datetime],
        blockchain_lag: int = 0,
    ) -> List[Event]:
        """Parse ConditionResolution events into settlement trades.

        ConditionResolution(
            bytes32 indexed conditionId,
            address indexed oracle,
            bytes32 indexed questionId,
            uint256 outcomeSlotCount,
            uint256[] payoutNumerators
        )

        Emits one Trade per outcome with event_type="settlement",
        price = payout per share (1.0 for winner, 0.0 for loser).
        """
        now = datetime.now(timezone.utc)
        trades: List[Event] = []

        for log in logs:
            condition_id_hex = log["topics"][1]
            condition_id = bytes.fromhex(condition_id_hex[2:])
            oracle = ("0x" + log["topics"][2][-40:]).lower()

            tx_hash = log["transactionHash"]
            block_num = int(log["blockNumber"], 16)
            ts = block_timestamps.get(block_num)
            event_log_index = int(log["logIndex"], 16)

            data = log["data"]
            # word 0: outcomeSlotCount
            outcome_count = int(data[2:66], 16)
            if outcome_count != 2:
                continue  # Only handle binary markets

            # word 1: offset to payoutNumerators array
            offset = int(data[66:130], 16) * 2
            arr_start = 2 + offset
            arr_len = int(data[arr_start:arr_start + 64], 16)
            payouts = []
            for i in range(arr_len):
                elem_start = arr_start + 64 + i * 64
                payouts.append(int(data[elem_start:elem_start + 64], 16))

            # payoutDenominator = sum(payoutNumerators)
            denom = sum(payouts) or 1

            # Determine if this is a neg-risk condition
            # (oracle == NegRiskAdapter means neg-risk)
            neg_risk = oracle in self._nr_adapter_addrs

            for i, payout_num in enumerate(payouts):
                index_set = 1 << i  # outcome 0 → 1, outcome 1 → 2
                tid = _derive_token_id(condition_id, index_set, neg_risk=neg_risk)
                price = payout_num / denom  # 1.0 for winner, 0.0 for loser

                trades.append(SettlementEvent(
                    wallet_address=oracle,
                    asset_id=tid,
                    oracle=oracle,
                    payout_rate=price,
                    condition_id=condition_id_hex[2:],
                    block_number=block_num,
                    block_timestamp=ts,
                    tx_hash=tx_hash,
                    blockchain_lag=blockchain_lag,
                    timestamp=now,
                    log_index=event_log_index,
                ))

        return trades

    def _parse_transfer_logs(
        self,
        logs: List[dict],
        block_timestamps: Dict[int, datetime],
        blockchain_lag: int = 0,
    ) -> List[Event]:
        """Parse ERC20 Transfer logs into DepositEvent and P2PTransferEvent.

        Deposit: from Relay Vault → user wallet (excluding rebalance txs).
        P2P: between two Polymarket proxy wallets (excluding system addresses).
        """
        now = datetime.now(timezone.utc)

        # Parse all transfer logs
        parsed = []
        for log in logs:
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            contract = log["address"].lower()
            token_info = self.STABLECOIN_MAP.get(contract)
            if not token_info:
                continue
            token_name, decimals = token_info
            from_addr = ("0x" + topics[1][-40:]).lower()
            to_addr = ("0x" + topics[2][-40:]).lower()
            raw_amount = int(log.get("data", "0x0"), 16)
            amount = raw_amount / (10 ** decimals)
            if amount < self.transfer_min_amount:
                continue
            block_num = int(log["blockNumber"], 16)
            parsed.append({
                "tx_hash": log["transactionHash"],
                "block_number": block_num,
                "block_timestamp": block_timestamps.get(block_num),
                "log_index": int(log["logIndex"], 16),
                "from_address": from_addr,
                "to_address": to_addr,
                "token": token_name,
                "contract": contract,
                "amount": amount,
            })

        events: List[Event] = []

        # Registry-backed classification (pm_classify.Classifier).
        #   deposit = external stablecoin -> KNOWN PM wallet
        #   p2p     = KNOWN PM wallet <-> KNOWN PM wallet (stablecoin or pUSD)
        # v1 policy: CONFIRMED ONLY. If the registry file is missing we emit
        # nothing (safer than the old Relay heuristic's false positives).
        if self._classifier is None:
            return events
        if self._registry is not None:
            self._registry.reload_if_changed()   # picks up new PM wallets, cheap

        for p in parsed:
            res = self._classifier.classify(
                p["contract"], p["from_address"], p["to_address"], p["amount"])
            if res is None:
                continue
            common = dict(
                asset_id=res["token"],
                from_address=p["from_address"],
                to_address=p["to_address"],
                amount=p["amount"],
                token=res["token"],
                block_number=p["block_number"],
                block_timestamp=p["block_timestamp"],
                tx_hash=p["tx_hash"],
                log_index=p["log_index"],
                blockchain_lag=blockchain_lag,
                timestamp=now,
                tier=res["tier"],
                pm_link_reason=res["pm_link_reason"],
            )
            if res["event_type"] == "deposit":
                events.append(DepositEvent(wallet_address=res["wallet_address"], **common))
            else:
                events.append(P2PTransferEvent(wallet_address=res["wallet_address"], **common))

        return events

    def _parse_money_flow(self, transfer_logs, order_logs, ctf_logs, block_timestamps, blockchain_lag=0):
        """Corrected TX-LEVEL money-flow classification (pm_classify_v2). Emits
        MoneyFlowEvent(op=deposit|withdrawal x flow_type=external|p2p|unidentified).
        A tx in order/ctf logs is a position op -> its stablecoin legs are settlement
        (skipped). is_pm_user = registry U minimal-proxy bytecode (impl 0x44e999d5),
        matching the offline-validated shadow. Wrapped by caller in try/except."""
        import transfer_classify_v2 as _tcv2
        import pm_classify_v2 as _v2
        PM_IMPL = "44e999d5c2f66ef0861317f9a4805ac2e90aeb4f"
        if not hasattr(self, "_mf_pm_cache"):
            self._mf_pm_cache = {}
        cache = self._mf_pm_cache
        if len(cache) > 1_000_000:
            cache.clear()
        reg = self._registry

        position_tx = {l["transactionHash"].lower() for l in order_logs}
        position_tx |= {l["transactionHash"].lower() for l in ctf_logs}

        # Reward detection: a bare stablecoin transfer whose tx.to is the Disperse.app
        # router is a Polymarket reward/rebate batch payout (not p2p/deposit). Fetch tx.to
        # only for p2p-candidate txs (non-position, non-mint, both ends non-system) -> a
        # small set (~organic-p2p rate); results cached to avoid refetching across polls.
        cand = set()
        for log in transfer_logs:
            topics = log.get("topics", [])
            if len(topics) < 3 or log["address"].lower() not in _v2.STABLES:
                continue
            txh = log["transactionHash"].lower()
            if txh in position_tx:
                continue
            frm = ("0x" + topics[1][-40:]).lower()
            to = ("0x" + topics[2][-40:]).lower()
            if frm == _v2.ZERO or to == _v2.ZERO or frm in _v2.INTERNAL or to in _v2.INTERNAL:
                continue
            cand.add(txh)
        disperse_tx = set()
        if cand:
            if not hasattr(self, "_mf_disperse_cache"):
                self._mf_disperse_cache = {}
            dcache = self._mf_disperse_cache
            if len(dcache) > 500_000:
                dcache.clear()
            todo = [h for h in cand if h not in dcache]
            for i in range(0, len(todo), 200):
                sub = todo[i:i + 200]
                resps = self._rpc_batch([("eth_getTransactionByHash", [h]) for h in sub])
                for h, tx in zip(sub, resps):
                    dcache[h] = bool(tx and (tx.get("to") or "").lower() == _v2.DISPERSE)
            disperse_tx = {h for h in cand if dcache.get(h)}

        need = set()
        for log in transfer_logs:
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            for t in (topics[1], topics[2]):
                a = ("0x" + t[-40:]).lower()
                if a == _v2.ZERO or a in _v2.INTERNAL or a in _v2.BRIDGE or a in cache:
                    continue
                if reg is not None and a in reg:
                    continue
                need.add(a)
        need_list = sorted(need)
        for i in range(0, len(need_list), 500):
            chunk = need_list[i:i + 500]
            resps = self._rpc_batch([("eth_getCode", [a, "latest"]) for a in chunk])
            for a, code in zip(chunk, resps):
                cache[a] = PM_IMPL in (code or "0x").lower()

        def is_pm_user(a):
            a = a.lower()
            return (reg is not None and a in reg) or cache.get(a, False)

        import time as _time, json as _json
        if _time.time() - self._reward_payers_ts > 3600:      # (re)load registry hourly
            self._reward_payers_ts = _time.time()
            try:
                self._reward_payers = {a.lower(): t for a, t in _json.load(open(self._reward_payers_path)).items()}
            except FileNotFoundError:
                self._reward_payers = {}
            except Exception as _e:
                print(f"[tracker] reward_payers load failed: {_e}")
        dicts = _tcv2.classify_block_transfers(
            transfer_logs, position_tx, is_pm_user, block_timestamps, self.transfer_min_amount,
            disperse_tx=disperse_tx, reward_payers=self._reward_payers)
        now = datetime.now(timezone.utc)
        return [MoneyFlowEvent(
            wallet_address=e["wallet_address"], block_number=e["block_number"],
            block_timestamp=e["block_timestamp"], tx_hash=e["tx_hash"], log_index=e["log_index"],
            blockchain_lag=blockchain_lag, timestamp=now, source="confirmed",
            op=e["op"], flow_type=e["type"], token=e["token"], amount=e["amount"],
            direction=e["direction"], counterparty=e["counterparty"], reason=e["reason"],
        ) for e in dicts]

    def _check_proxy_wallets(self, addresses: Set[str]) -> Dict[str, bool]:
        """Check which addresses are Polymarket proxy wallets.

        Uses proxy cache for known addresses, batched eth_getCode for unknown.
        Persists newly discovered proxies to cache file.
        """
        result: Dict[str, bool] = {}
        unknown: Set[str] = set()

        for addr in addresses:
            a = addr.lower()
            if a in self._proxy_cache:
                result[a] = True
            else:
                unknown.add(a)

        if not unknown:
            return result

        # Batch eth_getCode for unknown addresses
        unknown_list = sorted(unknown)
        calls = [("eth_getCode", [addr, "latest"]) for addr in unknown_list]
        new_proxies: Set[str] = set()

        for i in range(0, len(calls), 500):
            batch = calls[i:i + 500]
            addrs_batch = unknown_list[i:i + 500]
            responses = self._rpc_batch(batch)
            for addr, code in zip(addrs_batch, responses):
                if code is None:
                    code = "0x"
                is_proxy = code.lower() in self.POLYMARKET_PROXY_BYTECODES
                result[addr] = is_proxy
                if is_proxy:
                    new_proxies.add(addr)

        # Persist newly discovered proxies
        if new_proxies:
            self._proxy_cache.add_many(new_proxies)

        return result

    @staticmethod
    def _decode_uint256_array(data: str) -> List[int]:
        """Decode the first dynamic uint256[] from ABI-encoded event data.

        Works for CTF Split/Merge (partition) and PayoutRedemption (indexSets).
        The dynamic array offset is always at word 1 (data[66:130]).
        """
        offset = int(data[66:130], 16) * 2  # byte offset → hex char offset
        arr_start = 2 + offset  # skip "0x"
        arr_len = int(data[arr_start:arr_start + 64], 16)
        result = []
        for i in range(arr_len):
            elem_start = arr_start + 64 + i * 64
            result.append(int(data[elem_start:elem_start + 64], 16))
        return result

    def _find_nearest_transfer_batch(
        self, receipt_logs: List[dict], event_log_index: int,
        wallet: str, mint: bool,
    ) -> Tuple[List[str], List[int]]:
        """Find the TransferBatch closest before event_log_index in receipt.
        Returns (token_ids, raw_amounts)."""
        zero = "0x" + "0" * 40
        best = None
        best_distance = float("inf")

        for rl in receipt_logs:
            if rl["address"].lower() != self._ctf_addr:
                continue
            topics = rl.get("topics", [])
            if not topics or topics[0] != self.TRANSFER_BATCH:
                continue
            li = int(rl["logIndex"], 16)
            if li >= event_log_index:
                continue
            frm = ("0x" + topics[2][-40:]).lower()
            to = ("0x" + topics[3][-40:]).lower()
            if mint and frm == zero and to == wallet:
                pass
            elif not mint and frm == wallet and to == zero:
                pass
            else:
                continue
            dist = event_log_index - li
            if dist < best_distance:
                best_distance = dist
                best = rl

        if not best:
            return [], []

        d = best["data"]
        # TransferBatch data: [offset_ids, offset_values, ids_array, values_array]
        offset_ids = int(d[2:66], 16)
        ids_start = 2 + offset_ids * 2
        ids_len = int(d[ids_start:ids_start + 64], 16)
        token_ids = []
        for j in range(ids_len):
            s = ids_start + 64 + j * 64
            token_ids.append(str(int(d[s:s + 64], 16)))

        offset_vals = int(d[66:130], 16)
        vals_start = 2 + offset_vals * 2
        vals_len = int(d[vals_start:vals_start + 64], 16)
        amounts = []
        for j in range(vals_len):
            s = vals_start + 64 + j * 64
            amounts.append(int(d[s:s + 64], 16))

        return token_ids, amounts

    def _find_nearest_transfer_single(
        self, receipt_logs: List[dict], event_log_index: int,
        wallet: str, burn: bool,
    ) -> Optional[Tuple[str, int]]:
        """Find the TransferSingle closest before event_log_index in receipt."""
        zero = "0x" + "0" * 40
        best = None
        best_distance = float("inf")

        for rl in receipt_logs:
            if rl["address"].lower() != self._ctf_addr:
                continue
            topics = rl.get("topics", [])
            if not topics or topics[0] != self.TRANSFER_SINGLE:
                continue
            li = int(rl["logIndex"], 16)
            if li >= event_log_index:
                continue
            frm = ("0x" + topics[2][-40:]).lower()
            to = ("0x" + topics[3][-40:]).lower()
            if burn and (to != zero or frm != wallet):
                continue
            if not burn and (frm != zero or to != wallet):
                continue
            dist = event_log_index - li
            if dist < best_distance:
                best_distance = dist
                best = rl

        if not best:
            return None

        d = best["data"]
        token_id = str(int(d[2:66], 16))
        value = int(d[66:130], 16)
        return (token_id, value)

    def _emit_trades(self, trades: List[Event]) -> None:
        """Emit a batch of trades to subscribers."""
        if not trades:
            return

        if self.merge_fills:
            trades = self._merge_fills(trades)

        if self.perspective != "both":
            trades = [t for t in trades if t.wallet_role == self.perspective]
            if not trades:
                return

        source = trades[0].source
        count = len(trades)
        if source == "pending":
            self.trades_ws += count
        else:
            self.trades_rpc += count
        self.trades_total += count

        with self._lock:
            for callback in self._global_subscribers:
                try:
                    callback(trades)
                except Exception:
                    pass

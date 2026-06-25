"""
Exchange client for Hyperliquid XYZ equity perps and AsterDEX equity perps.

Hyperliquid XYZ:
  - Coin format: "xyz:AAPL", "xyz:AMD", etc.
  - Orders placed on the main HL exchange endpoint with dex="xyz" param
  - Asset indices come from the XYZ universe (separate from main HL perps)
  - Taker orders (IOC limit) for fast entry/exit

AsterDEX:
  - Symbol format: "AAPLUSDT", "AMDUSDT", etc.
  - GTX (post-only) limit orders, repriced each poll tick to stay at best bid/ask
  - 0bps maker fee during RWA Sprint Season 1
"""

import asyncio
import json
import logging
import math
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal

import aiohttp
from eth_account import Account
from eth_account.messages import encode_typed_data

from auth import sign_aster_request, now_ms
from config import (
    HYPERLIQUID_API, HL_EXCHANGE_URL,
    ASTER_ORDER_URL, ASTER_OPEN_ORDERS_URL, ASTER_POSITION_URL, ASTER_EXCHANGE_INFO_URL,
    ORDER_TIMEOUT_SECONDS, HL_IOC_BUFFER_BPS, ASTER_IOC_BUFFER_BPS,
    aster_symbol_for, ASTER_BASE_ALIAS, ASTER_BASE_TO_CANON,
    BASELINE_WINDOW_MINUTES, BASELINE_MIN_SAMPLES, BASELINE_SAMPLE_INTERVAL_SECONDS,
    ASTER_LEVERAGE_URL, ASTER_MARGIN_TYPE_URL, LEVERAGE, ASTER_MARGIN_TYPE,
)
from src import history

log = logging.getLogger(__name__)


@dataclass
class OrderBook:
    bid: float = 0.0
    ask: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2 if self.bid > 0 and self.ask > 0 else 0.0

    @property
    def spread_bps(self) -> float:
        return (self.ask - self.bid) / self.mid * 10000 if self.mid > 0 else 0.0


@dataclass
class OrderResult:
    success: bool = False
    order_id: str = ""
    filled_qty: float = 0.0
    fill_price: float = 0.0
    fee: float = 0.0
    error: str = ""
    raw: dict = field(default_factory=dict)
    # True when we don't know the venue's outcome (network timeout, non-JSON
    # response). The caller MUST reconcile against the actual position before
    # treating this as a failure — otherwise a silently-filled order becomes
    # an untracked naked leg.
    ambiguous: bool = False


@dataclass
class ContractSpec:
    tick_size: float = 0.01
    step_size: float = 0.001
    min_qty: float = 0.001
    min_notional: float = 5.0
    price_precision: int = 2
    qty_precision: int = 3


class ExchangeClient:
    def __init__(self, api_keys: dict):
        self.api_keys = api_keys
        self.session: aiohttp.ClientSession | None = None
        self.timeout = aiohttp.ClientTimeout(total=ORDER_TIMEOUT_SECONDS)

        # symbol -> ContractSpec
        self.aster_specs: dict[str, ContractSpec] = {}
        self.hl_specs: dict[str, ContractSpec] = {}

        # xyz:COIN -> asset_index in XYZ universe
        self._hl_xyz_indices: dict[str, int] = {}

        # Symbols whose leverage + margin type have been set this run (idempotent).
        self._margin_ready: set[str] = set()

        # Mark/index price cache: symbol -> (mark_price, index_price, fetched_at_ms)
        self._mark_cache: dict[str, tuple[float, float, int]] = {}
        self._mark_cache_ttl_ms = 60_000  # refresh mark price every 60s

        # HL oracle price cache: symbol -> (oracle_px, fetched_at_ms)
        self._hl_oracle_cache: dict[str, tuple[float, int]] = {}

        # Funding rate caches.
        #   HL:    hourly rate, refreshed alongside oracles via metaAndAssetCtxs.
        #   Aster: 8h rate, refreshed alongside mark price via premiumIndex.
        # symbol -> (rate, fetched_at_ms)
        self._hl_funding_cache: dict[str, tuple[float, int]] = {}
        self._aster_funding_cache: dict[str, tuple[float, int]] = {}

        # Rolling oracle delta history per symbol: deque of (ts_ms, delta_bps).
        # Used by get_smoothed_oracle_delta() to return the median over the last
        # ORACLE_DELTA_WINDOW_MS, which suppresses noisy point-in-time spikes.
        # We dedupe by source-timestamp so the deque holds genuinely fresh
        # observations rather than 1s-tick duplicates of the same stale oracle.
        self._oracle_delta_history: dict[str, deque] = {}
        self._last_recorded_source_ts: dict[str, tuple[int, int]] = {}  # (hl_ts, aster_ts)
        self._oracle_delta_window_ms: int = 30 * 60 * 1000  # 30 min

        # Rolling book mid-spread history per symbol: deque of (ts_ms, spread_bps).
        # spread_bps = (aster_mid - hl_mid) / mid * 10000. The median over the
        # last BASELINE_WINDOW_MINUTES is the "structural" gap we baseline entries
        # against — this is what we actually trade (the books), unlike the oracle
        # feeds. Seeded from 1m candles on startup via warmup_book_spread().
        self._book_spread_history: dict[str, deque] = {}
        self._book_spread_window_ms: int = BASELINE_WINDOW_MINUTES * 60 * 1000
        self._last_book_spread_ts: dict[str, int] = {}  # dedupe to ~1/min

    async def start(self, symbols: list[str]):
        """Load specs for all symbols."""
        self.session = aiohttp.ClientSession()
        await asyncio.gather(
            self._load_aster_specs(symbols),
            self._load_hl_xyz_specs(symbols),
            self.refresh_hl_oracles(symbols),
        )

    async def close(self):
        if self.session:
            await self.session.close()

    async def ensure_symbol_loaded(self, symbol: str) -> bool:
        """Dynamically load specs for a symbol not in the startup universe.

        Returns True if the symbol is ready (specs + asset index populated)."""
        hl_coin = f"xyz:{symbol}"
        if hl_coin in self._hl_xyz_indices and symbol in self.hl_specs and symbol in self.aster_specs:
            return True
        log.info(f"Dynamically loading specs for {symbol}…")
        await asyncio.gather(
            self._load_aster_specs([symbol]),
            self._load_hl_xyz_specs([symbol]),
        )
        if hl_coin not in self._hl_xyz_indices:
            log.error(f"{symbol}: not found in HL XYZ universe after dynamic load")
            return False
        if symbol not in self.aster_specs:
            log.error(f"{symbol}: not found on Aster after dynamic load")
            return False
        return True

    # ── Spec loading ──

    async def _load_aster_specs(self, symbols: list[str]):
        for attempt in range(5):
            try:
                async with self.session.get(ASTER_EXCHANGE_INFO_URL, timeout=self.timeout) as r:
                    if r.status == 429:
                        wait = 2 ** attempt
                        log.warning(f"Aster exchangeInfo rate-limited, retrying in {wait}s...")
                        await asyncio.sleep(wait)
                        continue
                    data = await r.json()
                break
            except Exception as e:
                wait = 2 ** attempt
                log.warning(f"Aster exchangeInfo attempt {attempt+1} failed ({e}), retrying in {wait}s...")
                await asyncio.sleep(wait)
        else:
            log.error("Failed to load Aster specs after 5 attempts")
            return
        for sym_info in data.get("symbols", []):
            raw = sym_info.get("symbol", "")
            base = None
            for suffix in ("USDT", "USDC", "USD"):
                if raw.endswith(suffix):
                    base = raw[: -len(suffix)]
                    break
            # Cross-venue aliases: SAMSUNG/SKHYNIX are Aster's tradeable books for
            # canonical SMSN/SKHX. The name-matching SMSNUSDT/SKHXUSDT are dead
            # listings — skip them so the real book wins the canonical spec slot.
            if base in ASTER_BASE_TO_CANON:
                base = ASTER_BASE_TO_CANON[base]
            elif base in ASTER_BASE_ALIAS:
                continue
            if base not in symbols:
                continue
            tick = step = min_qty = min_notional = 0.0
            for f in sym_info.get("filters", []):
                ft = f.get("filterType")
                if ft == "PRICE_FILTER":
                    tick = float(f.get("tickSize") or f.get("minPrice") or 0.01)
                elif ft == "LOT_SIZE":
                    step = float(f.get("stepSize") or 0.001)
                    min_qty = float(f.get("minQty") or 0.001)
                elif ft == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional") or 5.0)
            pp = sym_info.get("pricePrecision", 2)
            qp = sym_info.get("quantityPrecision", 3)
            self.aster_specs[base] = ContractSpec(
                tick_size=tick or 0.01,
                step_size=step or 0.001,
                min_qty=min_qty or 0.001,
                min_notional=min_notional or 5.0,
                price_precision=int(pp),
                qty_precision=int(qp),
            )
        loaded = [s for s in symbols if s in self.aster_specs]
        log.info(f"Aster specs loaded for: {loaded}")

    async def _load_hl_xyz_specs(self, symbols: list[str]):
        """Load XYZ asset universe from metaAndAssetCtxs to get correct asset indices.

        HL builder-deployed perp dexs use an offset of 110000 + dex_index * 10000.
        'xyz' is the first builder dex (index 0), so asset_index = 110000 + universe_pos.

        We verify the offset dynamically by querying perpDexs so the code stays correct
        even if HL ever changes the dex ordering.
        """
        try:
            # Step 1: find the XYZ dex offset from perpDexs
            async with self.session.post(
                HYPERLIQUID_API,
                json={"type": "perpDexs"},
                timeout=self.timeout,
            ) as r:
                perp_dexs = await r.json()
            # perp_dexs[0] is None (default mainnet), builder dexs start at index 1
            # offset formula: 110000 + (builder_position) * 10000
            xyz_offset = 110000  # default / fallback
            for builder_idx, dex in enumerate(perp_dexs[1:]):
                if dex and dex.get("name") == "xyz":
                    xyz_offset = 110000 + builder_idx * 10000
                    break
            log.info(f"HL XYZ dex offset: {xyz_offset}")

            # Step 2: load universe + contexts
            async with self.session.post(
                HYPERLIQUID_API,
                json={"type": "metaAndAssetCtxs", "dex": "xyz"},
                timeout=self.timeout,
            ) as r:
                data = await r.json()
            meta, ctxs = data[0], data[1]
            sym_set = set(symbols)
            now = now_ms()

            for i, asset in enumerate(meta.get("universe", [])):
                coin_name = asset.get("name", "")
                base = coin_name.split(":")[-1]
                if base not in sym_set:
                    continue
                sz_dec = asset.get("szDecimals", 3)
                step = round(10 ** -sz_dec, sz_dec)
                asset_idx = xyz_offset + i  # correct index for order actions
                self._hl_xyz_indices[coin_name] = asset_idx

                # Price precision: HL allows at most (6 - szDecimals) significant
                # figures, but never fewer than 1. The tick size may also come from
                # the asset metadata (tickSize field) if the deployer set one.
                px_dec = max(6 - sz_dec, 1)
                tick = float(asset.get("tickSize", 0) or 0)
                if tick <= 0:
                    tick = round(10 ** -px_dec, px_dec)

                self.hl_specs[base] = ContractSpec(
                    tick_size=tick,
                    step_size=step,
                    min_qty=step,
                    min_notional=10.0,
                    price_precision=px_dec,
                    qty_precision=sz_dec,
                )
                log.info(
                    f"HL spec {base}: szDec={sz_dec} pxDec={px_dec} "
                    f"tick={tick} step={step} raw_keys={list(asset.keys())}"
                )
                # Populate oracle cache while we have the data
                if i < len(ctxs):
                    oracle_px = float(ctxs[i].get("oraclePx") or 0)
                    if oracle_px > 0:
                        self._hl_oracle_cache[base] = (oracle_px, now)

            loaded = [s for s in symbols if s in self.hl_specs]
            log.info(f"HL XYZ specs loaded for: {loaded} (indices: {self._hl_xyz_indices})")
        except Exception as e:
            log.error(f"Failed to load HL XYZ specs: {e}")

        # Infer actual tick sizes from the live orderbook — the metadata doesn't
        # include tick size for builder dex perps, and the formula-derived default
        # (10^-pxDec) is often wrong. The minimum price increment between
        # orderbook levels IS the tick.
        await self._infer_hl_tick_sizes(symbols)

    async def _infer_hl_tick_sizes(self, symbols: list[str]):
        """Probe each symbol's HL orderbook to discover the real tick size."""
        for symbol in symbols:
            hl_coin = f"xyz:{symbol}"
            try:
                async with self.session.post(
                    HYPERLIQUID_API,
                    json={"type": "l2Book", "coin": hl_coin},
                    timeout=self.timeout,
                ) as r:
                    data = await r.json()
                levels = data.get("levels", [[], []])
                # Gather all prices from both sides
                prices = []
                for side_levels in levels:
                    for lvl in side_levels[:10]:
                        prices.append(float(lvl["px"]))
                prices.sort()
                # Find the minimum non-zero difference between adjacent prices
                min_diff = None
                for j in range(len(prices) - 1):
                    diff = round(prices[j + 1] - prices[j], 10)
                    if diff > 1e-12:
                        if min_diff is None or diff < min_diff:
                            min_diff = diff
                if min_diff and symbol in self.hl_specs:
                    spec = self.hl_specs[symbol]
                    # Determine price_precision from the tick
                    tick_str = f"{min_diff:.10f}".rstrip("0")
                    if "." in tick_str:
                        px_prec = len(tick_str.split(".")[1])
                    else:
                        px_prec = 0
                    old_tick = spec.tick_size
                    spec.tick_size = min_diff
                    spec.price_precision = px_prec
                    if abs(old_tick - min_diff) > 1e-12:
                        log.info(
                            f"HL tick {symbol}: inferred {min_diff} "
                            f"(pxPrec={px_prec}) from orderbook "
                            f"(was {old_tick})"
                        )
            except Exception as e:
                log.warning(f"HL tick inference failed for {symbol}: {e}")

    # ── Orderbook ──

    async def get_orderbook(self, exchange: str, symbol: str) -> OrderBook:
        if exchange == "aster":
            return await self._get_aster_book(symbol)
        elif exchange == "hl":
            return await self._get_hl_book(symbol)
        return OrderBook()

    async def get_both_books(self, symbol: str) -> tuple[OrderBook, OrderBook]:
        """Fetch Aster and HL orderbooks concurrently."""
        aster_book, hl_book = await asyncio.gather(
            self._get_aster_book(symbol),
            self._get_hl_book(symbol),
            return_exceptions=True,
        )
        if isinstance(aster_book, Exception):
            log.error(f"Aster book error for {symbol}: {aster_book}")
            aster_book = OrderBook()
        if isinstance(hl_book, Exception):
            log.error(f"HL book error for {symbol}: {hl_book}")
            hl_book = OrderBook()
        return aster_book, hl_book

    async def _get_aster_mark(self, symbol: str) -> float:
        """Return cached mark price, refreshing if stale. Also caches indexPrice."""
        cached = self._mark_cache.get(symbol)
        now = now_ms()
        if cached and now - cached[2] < self._mark_cache_ttl_ms:
            return cached[0]
        aster_sym = aster_symbol_for(symbol)
        try:
            async with self.session.get(
                "https://fapi.asterdex.com/fapi/v1/premiumIndex",
                params={"symbol": aster_sym},
                timeout=self.timeout,
            ) as r:
                data = await r.json()
            mark = float(data.get("markPrice") or 0)
            index = float(data.get("indexPrice") or 0)
            if mark > 0:
                self._mark_cache[symbol] = (mark, index, now)
            # Aster funding settles every 8h; lastFundingRate is the per-8h rate.
            try:
                rate = float(data.get("lastFundingRate") or 0)
                self._aster_funding_cache[symbol] = (rate, now)
            except (TypeError, ValueError):
                pass
            return mark
        except Exception:
            return cached[0] if cached else 0.0

    def get_aster_index(self, symbol: str) -> float:
        """Return cached Aster index price (populated alongside mark price)."""
        cached = self._mark_cache.get(symbol)
        return cached[1] if cached else 0.0

    async def refresh_hl_oracles(self, symbols: list[str]):
        """Fetch HL XYZ oracle prices via metaAndAssetCtxs and cache them."""
        try:
            async with self.session.post(
                HYPERLIQUID_API,
                json={"type": "metaAndAssetCtxs", "dex": "xyz"},
                timeout=self.timeout,
            ) as r:
                data = await r.json()
            meta, ctxs = data[0], data[1]
            now = now_ms()
            sym_set = set(symbols)
            for i, asset in enumerate(meta.get("universe", [])):
                base = asset.get("name", "").split(":")[-1]
                if base in sym_set and i < len(ctxs):
                    oracle_px = float(ctxs[i].get("oraclePx") or 0)
                    if oracle_px > 0:
                        self._hl_oracle_cache[base] = (oracle_px, now)
                    # HL funding is published as an hourly rate.
                    try:
                        self._hl_funding_cache[base] = (float(ctxs[i].get("funding") or 0), now)
                    except (TypeError, ValueError):
                        pass
            log.debug(f"HL oracle prices refreshed for {len(self._hl_oracle_cache)} symbols")
        except Exception as e:
            log.warning(f"Failed to refresh HL oracle prices: {e}")

    def get_hl_oracle(self, symbol: str) -> float:
        """Return cached HL XYZ oracle price."""
        cached = self._hl_oracle_cache.get(symbol)
        return cached[0] if cached else 0.0

    def get_hl_funding_rate(self, symbol: str) -> float:
        """Cached HL funding rate (per 1h). Positive = longs pay shorts."""
        cached = self._hl_funding_cache.get(symbol)
        return cached[0] if cached else 0.0

    def get_aster_funding_rate(self, symbol: str) -> float:
        """Cached Aster funding rate (per 8h). Positive = longs pay shorts."""
        cached = self._aster_funding_cache.get(symbol)
        return cached[0] if cached else 0.0

    def record_oracle_delta(self, symbol: str, delta_bps: float):
        """
        Append a fresh oracle delta observation, deduped by source freshness.
        Only records when the underlying oracle timestamps have changed since
        the last recording — otherwise we'd accumulate duplicates of the same
        5-minute-stale HL oracle value, and the median would track a noisy
        point-in-time spike rather than smooth it out.
        """
        hl_cached = self._hl_oracle_cache.get(symbol)
        aster_cached = self._mark_cache.get(symbol)
        if not hl_cached or not aster_cached:
            return  # source not loaded yet
        hl_ts = hl_cached[1]
        aster_ts = aster_cached[2]
        last = self._last_recorded_source_ts.get(symbol)
        if last == (hl_ts, aster_ts):
            return  # neither feed has updated since last recording
        self._last_recorded_source_ts[symbol] = (hl_ts, aster_ts)
        if symbol not in self._oracle_delta_history:
            self._oracle_delta_history[symbol] = deque(maxlen=200)
        self._oracle_delta_history[symbol].append((now_ms(), delta_bps))

    def get_smoothed_oracle_delta(self, symbol: str, current: float) -> float:
        """
        Median of oracle-delta observations from the last 30 minutes.
        Falls back to the current point-in-time value until we have at least 5
        fresh observations — paired with the MIN_EXECUTABLE_PREMIUM_BPS floor
        in executor, so cold-start entries still need to clear the fee floor.
        """
        hist = self._oracle_delta_history.get(symbol)
        if not hist:
            return current
        cutoff = now_ms() - self._oracle_delta_window_ms
        recent = [d for ts, d in hist if ts >= cutoff]
        if len(recent) < 5:
            return current
        return statistics.median(recent)

    def oracle_delta_sample_count(self, symbol: str) -> int:
        """Number of fresh observations in the current window (for logging/health)."""
        hist = self._oracle_delta_history.get(symbol)
        if not hist:
            return 0
        cutoff = now_ms() - self._oracle_delta_window_ms
        return sum(1 for ts, _ in hist if ts >= cutoff)

    # ── Rolling book mid-spread baseline ──

    def record_book_spread(self, symbol: str, spread_bps: float):
        """Append a book mid-spread observation, deduped to ~1/min.

        Sampling at 1m granularity matches the candle resolution the baseline
        window was validated on and keeps the deque bounded regardless of how
        often we poll the books.
        """
        now = now_ms()
        last = self._last_book_spread_ts.get(symbol, 0)
        if now - last < BASELINE_SAMPLE_INTERVAL_SECONDS * 1000:
            return
        self._last_book_spread_ts[symbol] = now
        if symbol not in self._book_spread_history:
            # window/min samples + headroom
            self._book_spread_history[symbol] = deque(maxlen=BASELINE_WINDOW_MINUTES + 120)
        self._book_spread_history[symbol].append((now, spread_bps))

    def get_book_spread_baseline(self, symbol: str) -> float | None:
        """Median book mid-spread over the window, or None if too few samples."""
        hist = self._book_spread_history.get(symbol)
        if not hist:
            return None
        cutoff = now_ms() - self._book_spread_window_ms
        recent = [s for ts, s in hist if ts >= cutoff]
        if len(recent) < BASELINE_MIN_SAMPLES:
            return None
        return statistics.median(recent)

    def book_spread_sample_count(self, symbol: str) -> int:
        """Samples within the current baseline window (for logging/health)."""
        hist = self._book_spread_history.get(symbol)
        if not hist:
            return 0
        cutoff = now_ms() - self._book_spread_window_ms
        return sum(1 for ts, _ in hist if ts >= cutoff)

    async def warmup_book_spread(self, symbols: list[str]):
        """Seed book-spread history from 1m candles so baselines are usable at startup.

        Fetches the last BASELINE_WINDOW_MINUTES of 1m candles from both venues
        per symbol, computes the mid-to-mid spread at each common minute, and
        loads it into the rolling deque. Without this the bot would need to run
        for BASELINE_MIN_SAMPLES minutes before placing any trade.
        """
        end_ms = now_ms()
        start_ms = end_ms - self._book_spread_window_ms

        async def warm_one(canon: str):
            hl_coin = f"xyz:{canon}"
            ast_sym = aster_symbol_for(canon)
            try:
                hl_data, ast_data = await asyncio.gather(
                    history.hl_candles(self.session, hl_coin, start_ms, end_ms, interval="1m"),
                    history.aster_candles(self.session, ast_sym, start_ms, end_ms, interval="1m"),
                )
            except Exception as e:
                log.debug(f"warmup {canon}: candle fetch failed ({e})")
                return canon, 0
            if not hl_data or not ast_data:
                return canon, 0
            common = sorted(set(hl_data) & set(ast_data))
            dq = deque(maxlen=BASELINE_WINDOW_MINUTES + 120)
            for t in common:
                hl_px = float(hl_data[t])
                ast_px = float(ast_data[t])
                mid = (hl_px + ast_px) / 2
                if mid <= 0:
                    continue
                spread_bps = (ast_px - hl_px) / mid * 10000
                dq.append((t, spread_bps))
            if dq:
                self._book_spread_history[canon] = dq
                self._last_book_spread_ts[canon] = dq[-1][0]
            return canon, len(dq)

        results = await asyncio.gather(*[warm_one(s) for s in symbols])
        warmed = [c for c, n in results if n >= BASELINE_MIN_SAMPLES]
        log.info(
            f"Book-spread baseline warmed: {len(warmed)}/{len(symbols)} symbols "
            f"have >= {BASELINE_MIN_SAMPLES} samples ({BASELINE_WINDOW_MINUTES}m window)"
        )

    async def _get_aster_book(self, symbol: str) -> OrderBook:
        aster_sym = aster_symbol_for(symbol)
        try:
            async with self.session.get(
                "https://fapi.asterdex.com/fapi/v1/depth",
                params={"symbol": aster_sym, "limit": 5},
                timeout=self.timeout,
            ) as r:
                depth = await r.json()

            bids = depth.get("bids", [])
            asks = depth.get("asks", [])
            if not bids or not asks:
                return OrderBook()

            bid = float(bids[0][0])
            ask = float(asks[0][0])

            # Validate against cached mark price — rejects stale/phantom top-of-book levels
            mark_price = await self._get_aster_mark(symbol)
            if mark_price > 0:
                if abs(bid - mark_price) / mark_price > 0.30:
                    log.debug(f"Aster {symbol}: depth bid {bid} vs mark {mark_price:.2f} — stale, skipping")
                    return OrderBook()
                if abs(ask - mark_price) / mark_price > 0.30:
                    log.debug(f"Aster {symbol}: depth ask {ask} vs mark {mark_price:.2f} — stale, skipping")
                    return OrderBook()

            return OrderBook(
                bid=bid, ask=ask,
                bid_size=float(bids[0][1]), ask_size=float(asks[0][1]),
            )
        except Exception as e:
            log.debug(f"Aster book error {symbol}: {e}")
            return OrderBook()

    async def _get_hl_book(self, symbol: str) -> OrderBook:
        hl_coin = f"xyz:{symbol}"
        try:
            async with self.session.post(
                HYPERLIQUID_API,
                json={"type": "l2Book", "coin": hl_coin},
                timeout=self.timeout,
            ) as r:
                data = await r.json()
            levels = data.get("levels", [[], []])
            bids_raw = levels[0] if len(levels) > 0 else []
            asks_raw = levels[1] if len(levels) > 1 else []
            if not bids_raw or not asks_raw:
                return OrderBook()
            # HL returns bids descending (index 0 = best bid) and asks ascending
            b0 = float(bids_raw[0]["px"])
            a0 = float(asks_raw[0]["px"])
            if b0 > a0:
                # levels[0] is actually asks (ascending), levels[1] is bids
                b0, a0 = a0, b0
                bids_raw, asks_raw = asks_raw, bids_raw
            return OrderBook(
                bid=b0, ask=a0,
                bid_size=float(bids_raw[0]["sz"]),
                ask_size=float(asks_raw[0]["sz"]),
            )
        except Exception as e:
            log.debug(f"HL book error {symbol}: {e}")
            return OrderBook()

    # ── Quantity helpers ──

    def snap_aster_qty(self, symbol: str, base_qty: float) -> float:
        spec = self.aster_specs.get(symbol, ContractSpec())
        qty = math.floor(base_qty / spec.step_size) * spec.step_size
        qty = round(qty, spec.qty_precision)
        return qty if qty >= spec.min_qty else 0.0

    def snap_aster_price(self, symbol: str, price: float) -> float:
        """Snap price to the nearest tick boundary to satisfy Aster's PRICE_FILTER."""
        spec = self.aster_specs.get(symbol, ContractSpec())
        tick = spec.tick_size
        return round(round(price / tick) * tick, spec.price_precision)

    def format_aster_price(self, symbol: str, price: float) -> str:
        spec = self.aster_specs.get(symbol, ContractSpec())
        snapped = self.snap_aster_price(symbol, price)
        return f"{snapped:.{spec.price_precision}f}"

    def format_aster_qty(self, symbol: str, qty: float) -> str:
        spec = self.aster_specs.get(symbol, ContractSpec())
        return f"{qty:.{spec.qty_precision}f}"

    @staticmethod
    def _to_wire(rounded: str) -> str:
        """Canonical Hyperliquid wire form: strip trailing zeros (245.50 -> 245.5,
        0.40 -> 0.4, 100 -> 100). HL normalises the order to this form before
        verifying the action signature, so a non-canonical string (e.g. a
        trailing zero) makes HL recover a phantom signer and reject the order
        as "User or API Wallet 0x... does not exist". Matches the official
        SDK's float_to_wire."""
        norm = Decimal(rounded).normalize()
        if norm == 0:
            return "0"
        return f"{norm:f}"

    def format_hl_price(self, symbol: str, price: float) -> str:
        spec = self.hl_specs.get(symbol, ContractSpec())
        # Snap to tick boundary first, then format for the wire.
        tick = spec.tick_size
        snapped = round(round(price / tick) * tick, spec.price_precision)
        return self._to_wire(f"{snapped:.{spec.price_precision}f}")

    def format_hl_qty(self, symbol: str, qty: float) -> str:
        spec = self.hl_specs.get(symbol, ContractSpec())
        return self._to_wire(f"{qty:.{spec.qty_precision}f}")

    # ── AsterDEX orders ──

    def _sign_aster(self, params: dict) -> dict:
        return sign_aster_request(
            params,
            private_key=self.api_keys["aster_api_secret"],
            user_address=self.api_keys["aster_wallet_address"],
            signer_address=self.api_keys["aster_signer_address"],
        )

    async def place_aster_gtx(
        self, symbol: str, side: str, qty: float, price: float
    ) -> OrderResult:
        """
        Place a GTX (post-only) limit order on Aster.
        GTX = Good Till Crossing: posts as maker at the given price.
        Rejected immediately if it would cross (take) — use to stay passive.
        """
        aster_sym = aster_symbol_for(symbol)
        params = {
            "symbol": aster_sym,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": "GTX",
            "price": self.format_aster_price(symbol, price),
            "quantity": self.format_aster_qty(symbol, qty),
        }
        signed = self._sign_aster(params)
        try:
            async with self.session.post(
                ASTER_ORDER_URL, data=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            if "orderId" in data:
                oid = str(data["orderId"])
                filled_qty = float(data.get("executedQty", 0) or 0)
                fill_price = float(data.get("avgPrice", 0) or 0)
                log.info(
                    f"Aster GTX posted: {side.upper()} {qty} {aster_sym} @ {price} -> {oid}"
                )
                return OrderResult(
                    success=True, order_id=oid,
                    filled_qty=filled_qty, fill_price=fill_price or price,
                    raw=data,
                )
            else:
                err = data.get("msg", str(data))
                log.error(f"Aster GTX failed: {err}")
                return OrderResult(success=False, error=err, raw=data)
        except Exception as e:
            log.error(f"Aster GTX exception: {e}")
            return OrderResult(success=False, error=str(e), ambiguous=True)

    async def place_aster_ioc(
        self, symbol: str, side: str, qty: float, price: float
    ) -> OrderResult:
        """
        Place an IOC (taker) limit order on Aster to force-fill — used to escape
        a stuck maker exit. Pays Aster taker fee (~0.9bps); use sparingly.
        """
        aster_sym = aster_symbol_for(symbol)
        is_buy = side.lower() == "buy"
        buffer = price * ASTER_IOC_BUFFER_BPS / 10000
        limit_px = price + buffer if is_buy else price - buffer
        params = {
            "symbol": aster_sym,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": "IOC",
            "price": self.format_aster_price(symbol, limit_px),
            "quantity": self.format_aster_qty(symbol, qty),
        }
        signed = self._sign_aster(params)
        try:
            async with self.session.post(
                ASTER_ORDER_URL, data=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            if "orderId" in data:
                oid = str(data["orderId"])
                filled_qty = float(data.get("executedQty", 0) or 0)
                fill_price = float(data.get("avgPrice", 0) or 0)
                order_status = data.get("status", "")
                log.info(
                    f"Aster IOC: {side.upper()} {qty} {aster_sym} @ {limit_px} -> "
                    f"filled {filled_qty} @ {fill_price} (oid {oid}, status={order_status})"
                )
                # Safety: if Aster returned a non-terminal status (e.g. "NEW"),
                # the order may be resting instead of IOC. Cancel it immediately
                # to prevent phantom resting orders that fill later.
                if order_status not in ("FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"):
                    log.warning(
                        f"Aster IOC order {oid} has status {order_status!r} — "
                        f"cancelling to prevent resting order"
                    )
                    await self.cancel_aster_order(symbol, oid)
                return OrderResult(
                    success=filled_qty > 0, order_id=oid,
                    filled_qty=filled_qty, fill_price=fill_price or limit_px,
                    raw=data,
                )
            else:
                err = data.get("msg", str(data))
                log.error(f"Aster IOC failed: {err}")
                return OrderResult(success=False, error=err, raw=data)
        except Exception as e:
            log.error(f"Aster IOC exception: {e}")
            return OrderResult(success=False, error=str(e), ambiguous=True)

    async def cancel_aster_order(self, symbol: str, order_id: str) -> bool:
        aster_sym = aster_symbol_for(symbol)
        params = {"symbol": aster_sym, "orderId": int(order_id)}
        signed = self._sign_aster(params)
        try:
            async with self.session.delete(
                ASTER_ORDER_URL, data=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            ok = data.get("status") in ("CANCELED", "CANCELLED") or "orderId" in data
            log.info(f"Aster cancel {order_id}: {'ok' if ok else data}")
            return ok
        except Exception as e:
            log.error(f"Aster cancel error {order_id}: {e}")
            return False

    async def query_aster_order(self, symbol: str, order_id: str) -> dict:
        aster_sym = aster_symbol_for(symbol)
        params = {"symbol": aster_sym, "orderId": int(order_id)}
        signed = self._sign_aster(params)
        try:
            async with self.session.get(
                ASTER_ORDER_URL, params=signed, timeout=self.timeout
            ) as r:
                return await r.json()
        except Exception as e:
            log.warning(f"Aster query error {order_id}: {e}")
            return {}

    async def get_aster_open_orders(self, symbol: str) -> list:
        aster_sym = aster_symbol_for(symbol)
        params = {"symbol": aster_sym}
        signed = self._sign_aster(params)
        try:
            async with self.session.get(
                ASTER_OPEN_ORDERS_URL, params=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            log.warning(f"Aster open orders error: {e}")
            return []

    async def get_aster_position(self, symbol: str) -> dict:
        aster_sym = aster_symbol_for(symbol)
        params = {"symbol": aster_sym}
        signed = self._sign_aster(params)
        try:
            async with self.session.get(
                ASTER_POSITION_URL, params=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            if isinstance(data, list):
                for p in data:
                    if p.get("symbol") == aster_sym:
                        return p
            return {}
        except Exception as e:
            log.error(f"Aster position query error: {e}")
            return {}

    # ── Hyperliquid XYZ orders ──

    def _hl_sign_action(self, action: dict, nonce: int, vault_address: str | None = None) -> dict:
        """Sign a Hyperliquid L1 action using the official phantom-agent scheme.

        From hyperliquid-python-sdk/hyperliquid/utils/signing.py:
          1. msgpack-serialize the action
          2. Append 8-byte big-endian nonce
          3. Append vault address flag (0x00 for None, 0x01+addr for address)
          4. keccak256 the concatenation -> connectionId (bytes32)
          5. Build phantom agent: {source: "a", connectionId: bytes}
          6. EIP-712 sign with type Agent, domain name="Exchange", chainId=1337
        """
        import msgpack
        from eth_utils import keccak

        # Steps 1-4: compute connectionId
        data = msgpack.packb(action)
        data += nonce.to_bytes(8, "big")
        if vault_address is None:
            data += b"\x00"
        else:
            addr = vault_address[2:] if vault_address.startswith("0x") else vault_address
            data += b"\x01" + bytes.fromhex(addr)
        connection_id = keccak(primitive=data)  # returns bytes

        # Steps 5-6: EIP-712 sign the phantom agent
        typed_data = {
            "domain": {
                "chainId": 1337,
                "name": "Exchange",
                "verifyingContract": "0x0000000000000000000000000000000000000000",
                "version": "1",
            },
            "types": {
                "Agent": [
                    {"name": "source", "type": "string"},
                    {"name": "connectionId", "type": "bytes32"},
                ],
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
            },
            "primaryType": "Agent",
            "message": {"source": "a", "connectionId": connection_id},
        }
        signable = encode_typed_data(full_message=typed_data)
        signed = Account.sign_message(signable, private_key=self.api_keys["hl_private_key"])

        # Normalise v to 27/28 (some eth_account versions return 0/1 parity).
        v = signed.v
        if v in (0, 1):
            v += 27

        # Self-verify: recover the signer from the signature we're about to send.
        # If it doesn't match our wallet, Hyperliquid would recover a phantom
        # address and reject the action as "User or API Wallet 0x... does not
        # exist". Catch it here instead of leaking a bad order onto the venue.
        expected = self.api_keys["hl_wallet_address"]
        try:
            recovered = Account.recover_message(signable, vrs=(v, signed.r, signed.s))
            if expected and recovered.lower() != expected.lower():
                log.error(
                    f"HL signature self-check FAILED: recovers {recovered} but "
                    f"wallet is {expected} — refusing to send (would phantom-reject)"
                )
                raise ValueError(
                    f"HL signature recovers {recovered}, expected {expected}"
                )
        except ValueError:
            raise
        except Exception as e:
            log.warning(f"HL signature self-check skipped ({e})")

        return {
            "action": action,
            "nonce": nonce,
            "signature": {
                "r": "0x" + format(signed.r, "064x"),
                "s": "0x" + format(signed.s, "064x"),
                "v": v,
            },
            "vaultAddress": vault_address,
            "expiresAfter": None,
        }

    async def place_hl_ioc(
        self, symbol: str, side: str, qty: float, price: float
    ) -> OrderResult:
        """
        Place an IOC limit order on Hyperliquid XYZ DEX.
        Price includes a buffer (HL_IOC_BUFFER_BPS) to maximise fill probability.
        """
        hl_coin = f"xyz:{symbol}"
        asset_idx = self._hl_xyz_indices.get(hl_coin)
        if asset_idx is None:
            return OrderResult(success=False, error=f"HL asset index not found for {hl_coin}")

        is_buy = side.lower() == "buy"
        buffer = price * HL_IOC_BUFFER_BPS / 10000
        limit_px = price + buffer if is_buy else price - buffer

        order = {
            "a": asset_idx,
            "b": is_buy,
            "p": self.format_hl_price(symbol, limit_px),
            "s": self.format_hl_qty(symbol, qty),
            "r": False,
            "t": {"limit": {"tif": "Ioc"}},
        }
        action = {"type": "order", "orders": [order], "grouping": "na"}
        nonce = int(time.time() * 1000)

        try:
            payload = self._hl_sign_action(action, nonce)
            async with self.session.post(
                HL_EXCHANGE_URL, json=payload, timeout=self.timeout
            ) as r:
                if r.content_type != "application/json":
                    text = await r.text()
                    log.error(f"HL non-JSON response ({r.status}): {text}")
                    # Treat as ambiguous: the venue saw the request but we
                    # can't parse the outcome. Order may have filled.
                    return OrderResult(
                        success=False, error=f"HTTP {r.status}: {text}", ambiguous=True
                    )
                data = await r.json()

            status = data.get("status", "")
            if status == "ok":
                statuses = data.get("response", {}).get("data", {}).get("statuses", [{}])
                s = statuses[0] if statuses else {}
                if "filled" in s:
                    f = s["filled"]
                    filled_qty = float(f.get("totalSz", 0))
                    avg_px = float(f.get("avgPx", 0))
                    oid = str(f.get("oid", ""))
                    log.info(
                        f"HL IOC filled: {side.upper()} {filled_qty} {hl_coin} @ {avg_px} -> {oid}"
                    )
                    return OrderResult(
                        success=True, order_id=oid,
                        filled_qty=filled_qty, fill_price=avg_px, raw=data,
                    )
                elif "error" in s:
                    err_msg = s["error"]
                    # "could not immediately match" = IOC expired unfilled — that's expected
                    if "could not immediately match" in err_msg or "ioc" in err_msg.lower():
                        log.info(f"HL IOC expired unfilled (expected): {err_msg}")
                        return OrderResult(success=False, error="ioc_not_filled", raw=data)
                    log.error(f"HL IOC error: {err_msg}")
                    return OrderResult(success=False, error=err_msg, raw=data)
                else:
                    log.info(f"HL IOC not filled (price moved): {s}")
                    return OrderResult(success=False, error="ioc_not_filled", raw=data)
            else:
                err = str(data.get("response", data))
                log.error(f"HL order failed: {err}")
                return OrderResult(success=False, error=err, raw=data)
        except Exception as e:
            log.error(f"HL IOC exception: {e}")
            return OrderResult(success=False, error=str(e), ambiguous=True)

    async def place_hl_alo(
        self, symbol: str, side: str, qty: float, price: float
    ) -> OrderResult:
        """
        Place a post-only (Alo = Add Liquidity Only) limit order on HL XYZ.
        Rests as a maker at exactly `price` — rejected if it would cross.
        On success returns the resting order id (filled_qty=0); if it somehow
        fills immediately, returns the fill. Used for maker-leg-first entries.
        """
        hl_coin = f"xyz:{symbol}"
        asset_idx = self._hl_xyz_indices.get(hl_coin)
        if asset_idx is None:
            return OrderResult(success=False, error=f"HL asset index not found for {hl_coin}")

        is_buy = side.lower() == "buy"
        order = {
            "a": asset_idx,
            "b": is_buy,
            "p": self.format_hl_price(symbol, price),
            "s": self.format_hl_qty(symbol, qty),
            "r": False,
            "t": {"limit": {"tif": "Alo"}},
        }
        action = {"type": "order", "orders": [order], "grouping": "na"}
        nonce = int(time.time() * 1000)
        try:
            payload = self._hl_sign_action(action, nonce)
            async with self.session.post(
                HL_EXCHANGE_URL, json=payload, timeout=self.timeout
            ) as r:
                if r.content_type != "application/json":
                    text = await r.text()
                    log.error(f"HL ALO non-JSON response ({r.status}): {text}")
                    return OrderResult(success=False, error=f"HTTP {r.status}: {text}", ambiguous=True)
                data = await r.json()

            if data.get("status") != "ok":
                err = str(data.get("response", data))
                log.error(f"HL ALO failed: {err}")
                return OrderResult(success=False, error=err, raw=data)

            statuses = data.get("response", {}).get("data", {}).get("statuses", [{}])
            s = statuses[0] if statuses else {}
            if "resting" in s:
                oid = str(s["resting"].get("oid", ""))
                log.info(f"HL ALO resting: {side.upper()} {qty} {hl_coin} @ {price} -> {oid}")
                return OrderResult(success=True, order_id=oid, filled_qty=0.0,
                                   fill_price=price, raw=data)
            if "filled" in s:
                f = s["filled"]
                log.info(f"HL ALO filled immediately: {side.upper()} {hl_coin} -> {f.get('oid')}")
                return OrderResult(
                    success=True, order_id=str(f.get("oid", "")),
                    filled_qty=float(f.get("totalSz", 0)),
                    fill_price=float(f.get("avgPx", 0)), raw=data,
                )
            err = s.get("error", str(s))
            # Post-only that would cross is rejected — caller can reprice/retry.
            log.warning(f"HL ALO not rested: {err}")
            return OrderResult(success=False, error=err, raw=data)
        except Exception as e:
            log.error(f"HL ALO exception: {e}")
            return OrderResult(success=False, error=str(e), ambiguous=True)

    async def query_hl_order(self, order_id: str) -> dict:
        """Return HL order status dict (info `orderStatus`). Empty on failure."""
        try:
            async with self.session.post(
                HYPERLIQUID_API,
                json={
                    "type": "orderStatus",
                    "user": self.api_keys["hl_account_address"],
                    "oid": int(order_id),
                },
                timeout=self.timeout,
            ) as r:
                return await r.json()
        except Exception as e:
            log.warning(f"HL order query error {order_id}: {e}")
            return {}

    async def cancel_hl_order(self, symbol: str, order_id: str) -> bool:
        """Cancel a resting HL XYZ order by oid."""
        hl_coin = f"xyz:{symbol}"
        asset_idx = self._hl_xyz_indices.get(hl_coin)
        if asset_idx is None:
            return False
        action = {"type": "cancel", "cancels": [{"a": asset_idx, "o": int(order_id)}]}
        nonce = int(time.time() * 1000)
        try:
            payload = self._hl_sign_action(action, nonce)
            async with self.session.post(
                HL_EXCHANGE_URL, json=payload, timeout=self.timeout
            ) as r:
                data = await r.json()
            ok = data.get("status") == "ok"
            log.info(f"HL cancel {order_id}: {'ok' if ok else data}")
            return ok
        except Exception as e:
            log.error(f"HL cancel error {order_id}: {e}")
            return False

    async def reconcile_hl_position_delta(
        self, symbol: str, baseline_szi: float, expected_signed_qty: float, tolerance_frac: float = 0.05,
    ) -> tuple[bool, float, float]:
        """
        After an ambiguous HL order, check whether the position actually moved.

        baseline_szi: signed size BEFORE the order (from get_hl_position pre-call)
        expected_signed_qty: positive for buy, negative for sell — what we tried to fill
        Returns (filled, actual_filled_signed, current_szi).
        'filled' is True if the position moved by at least (1-tolerance) of expected.
        """
        await asyncio.sleep(1.0)  # let the venue settle
        for attempt in range(3):
            pos = await self.get_hl_position(symbol)
            try:
                current = float(pos.get("szi", 0) or 0)
            except (TypeError, ValueError):
                current = 0.0
            delta = current - baseline_szi
            # Same-sign and at least (1-tol) of expected magnitude → it filled
            same_sign = (delta * expected_signed_qty) > 0
            magnitude_ok = abs(delta) >= abs(expected_signed_qty) * (1 - tolerance_frac)
            if same_sign and magnitude_ok:
                return True, delta, current
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
        return False, current - baseline_szi, current

    async def get_hl_position(self, symbol: str) -> dict:
        hl_coin = f"xyz:{symbol}"
        try:
            async with self.session.post(
                HYPERLIQUID_API,
                json={
                    "type": "clearinghouseState",
                    "user": self.api_keys["hl_account_address"],
                    "dex": "xyz",
                },
                timeout=self.timeout,
            ) as r:
                data = await r.json()
            for pos in data.get("assetPositions", []):
                p = pos.get("position", {})
                if p.get("coin") == hl_coin:
                    return p
            return {}
        except Exception as e:
            log.error(f"HL position query error: {e}")
            return {}

    async def get_all_hl_positions(self) -> list[dict]:
        """Return all open HL XYZ positions (non-zero szi)."""
        try:
            async with self.session.post(
                HYPERLIQUID_API,
                json={
                    "type": "clearinghouseState",
                    "user": self.api_keys["hl_account_address"],
                    "dex": "xyz",
                },
                timeout=self.timeout,
            ) as r:
                data = await r.json()
            results = []
            for pos in data.get("assetPositions", []):
                p = pos.get("position", {})
                szi = float(p.get("szi", 0) or 0)
                if abs(szi) > 1e-12:
                    results.append(p)
            return results
        except Exception as e:
            log.error(f"HL all positions query error: {e}")
            return []

    async def get_all_aster_positions(self) -> list[dict]:
        """Return all open Aster positions (non-zero positionAmt)."""
        params = {}
        signed = self._sign_aster(params)
        try:
            async with self.session.get(
                ASTER_POSITION_URL, params=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            if not isinstance(data, list):
                return []
            return [p for p in data if abs(float(p.get("positionAmt", 0) or 0)) > 1e-12]
        except Exception as e:
            log.error(f"Aster all positions query error: {e}")
            return []

    async def set_hl_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        """Set leverage for a symbol on HL XYZ. updateLeverage wants the integer
        asset index (same id used for order placement), not the coin name."""
        hl_coin = f"xyz:{symbol}"
        asset_idx = self._hl_xyz_indices.get(hl_coin)
        if asset_idx is None:
            log.error(f"HL set leverage: asset index not found for {hl_coin}")
            return False
        action = {
            "type": "updateLeverage",
            "asset": asset_idx,
            "isCross": cross,
            "leverage": leverage,
        }
        nonce = int(time.time() * 1000)
        try:
            payload = self._hl_sign_action(action, nonce)
            async with self.session.post(
                HL_EXCHANGE_URL, json=payload, timeout=self.timeout
            ) as r:
                if r.content_type != "application/json":
                    text = await r.text()
                    log.error(f"HL non-JSON response ({r.status}): {text}")
                    return False
                data = await r.json()
            ok = data.get("status") == "ok"
            log.info(f"HL leverage set {hl_coin} {leverage}x cross={cross}: {ok} ({data})")
            return ok
        except Exception as e:
            log.error(f"HL set leverage error: {e}")
            return False

    async def set_aster_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage on Aster for a symbol (POST /fapi/v1/leverage)."""
        params = {"symbol": aster_symbol_for(symbol), "leverage": int(leverage)}
        signed = self._sign_aster(params)
        try:
            async with self.session.post(
                ASTER_LEVERAGE_URL, data=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            ok = "leverage" in data
            code = data.get("code")
            if not ok and code == -2014:
                log.info(f"Aster leverage {params['symbol']} {leverage}x: "
                         f"skipped (API-key issue, existing position) ({data})")
                return True
            log.info(f"Aster leverage set {params['symbol']} {leverage}x: {ok} ({data})")
            return ok
        except Exception as e:
            log.error(f"Aster set leverage error: {e}")
            return False

    async def set_aster_margin_type(self, symbol: str, margin_type: str) -> bool:
        """Set margin type on Aster (POST /fapi/v1/marginType). Treats the
        'no need to change' response as success (already set)."""
        params = {"symbol": aster_symbol_for(symbol), "marginType": margin_type.upper()}
        signed = self._sign_aster(params)
        try:
            async with self.session.post(
                ASTER_MARGIN_TYPE_URL, data=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            code = data.get("code")
            msg = str(data.get("msg", "")).lower()
            ok = code in (200, None) or "no need to change" in msg
            # Treat "can't change with open position" or API-key errors as
            # non-fatal — the position already exists with whatever margin type
            # was set, and we shouldn't block trading on this.
            if not ok and (code == -2014 or "position" in msg or "api-key" in msg):
                log.info(f"Aster margin type {params['symbol']} {margin_type}: "
                         f"skipped (existing position or API issue) ({data})")
                return True
            log.info(f"Aster margin type {params['symbol']} {margin_type}: {ok} ({data})")
            return ok
        except Exception as e:
            log.error(f"Aster set margin type error: {e}")
            return False

    async def ensure_perp_margin(self, symbol: str) -> bool:
        """Set leverage + isolated margin on both venues, once per symbol per run.

        Best-effort and idempotent: marks the symbol ready only if both venues
        accepted, so a transient failure retries on the next entry attempt. HL
        HIP-3 markets are isolated-only, so cross is forced False there.
        """
        if symbol in self._margin_ready:
            return True
        cross = ASTER_MARGIN_TYPE.upper() != "ISOLATED"
        hl_ok = await self.set_hl_leverage(symbol, LEVERAGE, cross=cross)
        # Set margin type before leverage (Binance rejects margin-type change with
        # an open position; on a fresh symbol this is fine).
        ast_mt_ok = await self.set_aster_margin_type(symbol, ASTER_MARGIN_TYPE)
        ast_lev_ok = await self.set_aster_leverage(symbol, LEVERAGE)
        if hl_ok and ast_mt_ok and ast_lev_ok:
            self._margin_ready.add(symbol)
            return True
        log.warning(
            f"{symbol}: ensure_perp_margin incomplete "
            f"(hl={hl_ok} aster_mt={ast_mt_ok} aster_lev={ast_lev_ok})"
        )
        return False

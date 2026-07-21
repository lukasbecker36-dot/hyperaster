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
    ASTER_BALANCE_URL, ASTER_INCOME_URL,
    ORDER_TIMEOUT_SECONDS, HL_IOC_BUFFER_BPS, HL_EXIT_IOC_BUFFER_BPS,
    ASTER_IOC_BUFFER_BPS, ASTER_BASE,
    aster_symbol_for, ASTER_BASE_ALIAS, ASTER_BASE_TO_CANON,
    BASELINE_WINDOW_MINUTES, BASELINE_MIN_SAMPLES, BASELINE_SAMPLE_INTERVAL_SECONDS,
    ASTER_LEVERAGE_URL, ASTER_MARGIN_TYPE_URL, LEVERAGE, ASTER_MARGIN_TYPE,
    ORACLE_CORRECTION_ENABLED, ORACLE_BASELINE_MIN_SAMPLES,
    ORACLE_STALENESS_GUARD_ENABLED, ORACLE_STALE_MINUTES,
    EXECUTABLE_SIGNAL_ENABLED, ASTER_REDUCE_ONLY,
)
from src import history

log = logging.getLogger(__name__)


def _hl_sigfig_tick(price: float) -> float:
    """HL's price grid allows up to 5 significant figures, so the tick for a
    price is 10^(floor(log10(price)) - 4): e.g. ~1212 → 0.1, ~200 → 0.01,
    ~45 → 0.001. Used as a deterministic fallback when the tick can't be read
    off a thin orderbook. Coarser-than-real is safe (still divisible); this rule
    matches HL for the equity-perp price ranges we trade."""
    if price <= 0:
        return 0.01
    exp = math.floor(math.log10(price)) - 4
    return 10.0 ** exp


@dataclass
class OrderBook:
    bid: float = 0.0
    ask: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0
    bids: list = field(default_factory=list)  # [(price, size), ...] best first
    asks: list = field(default_factory=list)  # [(price, size), ...] best first

    def vwap_sell(self, qty: float) -> float:
        """VWAP for selling qty into bids (hitting bids)."""
        remaining = qty
        total = 0.0
        for px, sz in self.bids:
            fill = min(remaining, sz)
            total += fill * px
            remaining -= fill
            if remaining <= 0:
                break
        filled = qty - remaining
        return total / filled if filled > 0 else self.bid

    def vwap_buy(self, qty: float) -> float:
        """VWAP for buying qty from asks (lifting offers)."""
        remaining = qty
        total = 0.0
        for px, sz in self.asks:
            fill = min(remaining, sz)
            total += fill * px
            remaining -= fill
            if remaining <= 0:
                break
        filled = qty - remaining
        return total / filled if filled > 0 else self.ask

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
        # Actually-applied Aster leverage per symbol (Aster caps equity perps
        # below the requested LEVERAGE). Used by the margin pre-flight.
        self._aster_leverage: dict[str, int] = {}

        # Mark/index price cache: symbol -> (mark_price, index_price, fetched_at_ms)
        self._mark_cache: dict[str, tuple[float, float, int]] = {}
        self._mark_cache_ttl_ms = 60_000  # refresh mark price every 60s

        # HL oracle price cache: symbol -> (oracle_px, fetched_at_ms)
        self._hl_oracle_cache: dict[str, tuple[float, int]] = {}

        # Symbols whose real HL tick has been inferred from the live book (the
        # metadata tick is often wrong for builder-dex perps). Prevents a
        # dynamically-loaded name keeping the default 0.01 tick.
        self._hl_tick_inferred: set[str] = set()

        # Funding rate caches.
        #   HL:    hourly rate, refreshed alongside oracles via metaAndAssetCtxs.
        #   Aster: 8h rate, refreshed alongside mark price via premiumIndex.
        # symbol -> (rate, fetched_at_ms)
        self._hl_funding_cache: dict[str, tuple[float, int]] = {}
        self._aster_funding_cache: dict[str, tuple[float, int]] = {}
        # Aster funding settlement window per symbol: (hours, detected_at_ms)
        self._aster_window_cache: dict[str, tuple[float, int]] = {}

        # Rolling oracle-delta history per symbol: deque of (ts_ms, delta_bps),
        # delta_bps = (aster_index - hl_oracle) / mid * 10000. Its 8h median is
        # the STRUCTURAL feed offset; the deviation of the live delta from that
        # median is a zero-lag fair-value correction to the book baseline (see
        # get_oracle_correction). Sampled ~1/min from the live oracle caches.
        self._oracle_delta_history: dict[str, deque] = {}
        self._last_oracle_sample_ts: dict[str, int] = {}       # dedupe to ~1/min
        self._oracle_delta_window_ms: int = BASELINE_WINDOW_MINUTES * 60 * 1000
        # Staleness detection: last HL-oracle price seen and the last time it
        # actually MOVED. A frozen oracle (market closed) means a book
        # "dislocation" has no arbitrageable anchor — the entry guard skips it.
        self._hl_oracle_last_px: dict[str, float] = {}
        self._hl_oracle_last_move: dict[str, int] = {}

        # Rolling book mid-spread history per symbol: deque of (ts_ms, spread_bps).
        # spread_bps = (aster_mid - hl_mid) / mid * 10000. The median over the
        # last BASELINE_WINDOW_MINUTES is the "structural" gap we baseline entries
        # against — this is what we actually trade (the books), unlike the oracle
        # feeds. Seeded from 1m candles on startup via warmup_book_spread().
        self._book_spread_history: dict[str, deque] = {}
        self._book_spread_window_ms: int = BASELINE_WINDOW_MINUTES * 60 * 1000
        self._last_book_spread_ts: dict[str, int] = {}  # dedupe to ~1/min

        # Rolling half-spread-differential history: deque of (ts_ms, half_diff_bps),
        # half_diff = (hl_spread − aster_spread)/2. The transient, book-positioning
        # component of the executable HL-maker/Aster-taker entry basis. Live-only
        # (can't be warmed from candles); its 8h median is the structural norm the
        # entry signal measures deviations from. Same 1/min cadence as book spread.
        self._half_spread_diff_history: dict[str, deque] = {}
        self._last_half_spread_ts: dict[str, int] = {}

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
        loaded = (hl_coin in self._hl_xyz_indices and symbol in self.hl_specs
                  and symbol in self.aster_specs)
        if not loaded:
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
        # The metadata HL tick is often wrong for builder-dex perps; startup
        # infers the real tick from the book for the universe, but a dynamically
        # -loaded name (manual /enter on a non-universe symbol) would otherwise
        # keep the default 0.01 and get its maker rejected ("Price must be
        # divisible by tick size"). Infer once here too.
        if symbol not in self._hl_tick_inferred:
            await self._infer_hl_tick_sizes([symbol])
        return True

    async def discover_overlap_bases(self) -> set[str]:
        """Live-query both venues and return the set of canonical bases listed on
        BOTH (HL XYZ ∩ Aster). Applies cross-venue aliases (SAMSUNG→SMSN etc.).

        No equity filtering here — the caller applies BLOCKED/NON_EQUITY excludes.
        Returns an empty set on any fetch failure so the caller can skip this round
        without mutating the universe."""
        hl_bases: set[str] = set()
        aster_bases: set[str] = set()
        try:
            async with self.session.post(
                HYPERLIQUID_API,
                json={"type": "metaAndAssetCtxs", "dex": "xyz"},
                timeout=self.timeout,
            ) as r:
                data = await r.json()
            for asset in data[0].get("universe", []):
                name = asset.get("name", "")
                if name:
                    hl_bases.add(name.split(":")[-1])
        except Exception as e:
            log.warning(f"discover_overlap: HL universe fetch failed ({e})")
            return set()
        try:
            async with self.session.get(ASTER_EXCHANGE_INFO_URL, timeout=self.timeout) as r:
                info = await r.json()
            for sym_info in info.get("symbols", []):
                raw = sym_info.get("symbol", "")
                for suffix in ("USDT", "USDC", "USD"):
                    if raw.endswith(suffix):
                        base = raw[: -len(suffix)]
                        # Map Aster's tradeable book back to the canonical base;
                        # skip the dead name-matching listings (SMSNUSDT etc.).
                        if base in ASTER_BASE_TO_CANON:
                            base = ASTER_BASE_TO_CANON[base]
                        elif base in ASTER_BASE_ALIAS:
                            break
                        aster_bases.add(base)
                        break
        except Exception as e:
            log.warning(f"discover_overlap: Aster exchangeInfo fetch failed ({e})")
            return set()
        return hl_bases & aster_bases

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
                # Thin book (no two adjacent-tick levels) → fall back to HL's
                # 5-significant-figure price grid from the current level, so we
                # never keep the default 0.01 and get the maker tick-rejected.
                if min_diff is None and prices:
                    min_diff = _hl_sigfig_tick(prices[len(prices) // 2])
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
                    self._hl_tick_inferred.add(symbol)
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

    async def get_funding_rates_fresh(self, symbol: str) -> tuple[float, float, float]:
        """Funding snapshot for an entry: (hl_per_1h, aster_per_window,
        aster_window_hours), refreshing the source feeds if the caches are cold.
        A silently-zero snapshot poisons the position's funding P&L for its
        whole life, so this actively fetches instead of trusting the scan-loop
        caches (which only cover the scanned universe and may be cold right
        after restart or for blocked/manual symbols). The window is detected
        from settlement timestamps (most 8h, some 4h e.g. SKHX) and stored on
        the position so funding accrual normalises correctly."""
        if symbol not in self._hl_funding_cache:
            await self.refresh_hl_oracles([symbol])
        # premiumIndex populates the Aster funding cache as a side effect;
        # force a fetch if the cache has never seen this symbol.
        if symbol not in self._aster_funding_cache:
            await self._get_aster_mark(symbol)
        hl_fr = self.get_hl_funding_rate(symbol)
        aster_fr = self.get_aster_funding_rate(symbol)
        aster_win = await self.get_aster_funding_window(symbol)
        if hl_fr == 0.0 and aster_fr == 0.0:
            log.warning(
                f"{symbol}: funding rates both 0.0 at entry snapshot — "
                f"funding P&L will read $0 for this position"
            )
        return hl_fr, aster_fr, aster_win

    async def get_aster_funding_window(self, symbol: str) -> float:
        """Aster funding settlement window (hours) for a name, detected from the
        gaps between actual settlement timestamps in the funding history. Most
        names settle every 8h but some use 4h (e.g. SKHX); assuming 8h halves
        their real funding accrual. Cached ~12h per symbol; falls back to 8.0
        on failure or sparse history (fresh listings)."""
        cached = self._aster_window_cache.get(symbol)
        now = now_ms()
        if cached and now - cached[1] < 12 * 3_600_000:
            return cached[0]
        window = 8.0
        try:
            async with self.session.get(
                f"{ASTER_BASE}/fapi/v1/fundingRate",
                params={"symbol": aster_symbol_for(symbol),
                        "startTime": now - 48 * 3_600_000},
                timeout=self.timeout,
            ) as r:
                rows = await r.json(content_type=None)
            times = sorted(int(x.get("fundingTime") or 0)
                           for x in (rows or []) if isinstance(x, dict))
            gaps = [(t2 - t1) / 3_600_000 for t1, t2 in zip(times, times[1:]) if t2 > t1]
            if gaps:
                window = statistics.median(gaps)
        except Exception as e:
            log.debug(f"{symbol}: Aster funding window detect failed ({e}) — using 8h")
        self._aster_window_cache[symbol] = (window, now)
        if abs(window - 8.0) > 0.5:
            log.info(f"{symbol}: Aster funding window detected as {window:.0f}h (not 8h)")
        return window

    def record_oracle_delta(self, symbol: str):
        """Sample the live oracle/index delta (~1/min) into the rolling window
        and track HL-oracle staleness.

        delta_bps = (aster_index - hl_oracle) / mid * 10000 — same orientation
        as the book spread (positive = Aster rich). Computed from the live
        oracle caches (HL oraclePx refreshed each slow scan; Aster indexPrice
        refreshed on each book fetch). No-op if either source is missing."""
        hl_oracle = self.get_hl_oracle(symbol)
        aster_index = self.get_aster_index(symbol)
        if hl_oracle <= 0 or aster_index <= 0:
            return
        now = now_ms()
        # Staleness: advance last_move only when the HL oracle price changes.
        prev_px = self._hl_oracle_last_px.get(symbol)
        if prev_px is None or abs(hl_oracle - prev_px) > 1e-9:
            self._hl_oracle_last_px[symbol] = hl_oracle
            self._hl_oracle_last_move[symbol] = now
        elif symbol not in self._hl_oracle_last_move:
            self._hl_oracle_last_move[symbol] = now
        # Sample the delta ~1/min (same cadence as the book baseline).
        last = self._last_oracle_sample_ts.get(symbol, 0)
        if now - last < BASELINE_SAMPLE_INTERVAL_SECONDS * 1000:
            return
        self._last_oracle_sample_ts[symbol] = now
        mid = (hl_oracle + aster_index) / 2
        if mid <= 0:
            return
        delta_bps = (aster_index - hl_oracle) / mid * 10000
        if symbol not in self._oracle_delta_history:
            self._oracle_delta_history[symbol] = deque(maxlen=BASELINE_WINDOW_MINUTES + 120)
        self._oracle_delta_history[symbol].append((now, delta_bps))

    def get_oracle_correction(self, symbol: str) -> float:
        """Fair-value correction to add to the book baseline: how far the live
        oracle delta sits from its own 8h structural median. 0.0 if the
        correction is disabled or there isn't enough oracle history yet, so the
        signal degrades cleanly to the pure book baseline."""
        if not ORACLE_CORRECTION_ENABLED:
            return 0.0
        hist = self._oracle_delta_history.get(symbol)
        if not hist:
            return 0.0
        cutoff = now_ms() - self._oracle_delta_window_ms
        recent = [d for ts, d in hist if ts >= cutoff]
        if len(recent) < ORACLE_BASELINE_MIN_SAMPLES:
            return 0.0
        return recent[-1] - statistics.median(recent)

    def oracle_is_stale(self, symbol: str) -> bool:
        """True when the HL oracle price hasn't moved in ORACLE_STALE_MINUTES —
        i.e. the market is likely closed and any book dislocation lacks an
        arbitrageable anchor. Only fires once staleness tracking exists for the
        symbol; an untracked name is never blocked here."""
        if not ORACLE_STALENESS_GUARD_ENABLED:
            return False
        last_move = self._hl_oracle_last_move.get(symbol)
        if last_move is None:
            return False
        return (now_ms() - last_move) > ORACLE_STALE_MINUTES * 60 * 1000

    def oracle_delta_sample_count(self, symbol: str) -> int:
        """Number of oracle-delta samples in the current window (health/logging)."""
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

    def _record_half_spread_diff(self, symbol: str, half_diff_bps: float):
        """Append a half-spread-differential sample, deduped to ~1/min."""
        now = now_ms()
        last = self._last_half_spread_ts.get(symbol, 0)
        if now - last < BASELINE_SAMPLE_INTERVAL_SECONDS * 1000:
            return
        self._last_half_spread_ts[symbol] = now
        if symbol not in self._half_spread_diff_history:
            self._half_spread_diff_history[symbol] = deque(maxlen=BASELINE_WINDOW_MINUTES + 120)
        self._half_spread_diff_history[symbol].append((now, half_diff_bps))

    def executable_deviation_bps(self, symbol: str, aster_book, hl_book, mid: float) -> float:
        """Deviation of the executable HL-maker/Aster-taker basis from its
        structural norm, i.e. the part of the entry edge the MID spread can't see.

        The executable basis decomposes as ±mid_spread + half_diff, where
        half_diff = (hl_spread − aster_spread)/2. The mid part is already handled
        by the (candle-warmed) book baseline, so this returns only the deviation
        of half_diff from its own 8h median — added identically to both direction
        excesses (you always maker-on-HL / taker-on-Aster). Records the live
        sample as a side effect. Returns 0.0 when disabled or the baseline isn't
        warm yet, so the signal degrades cleanly to the pure mid-spread basis."""
        if mid <= 0:
            return 0.0
        hl_spread = (hl_book.ask - hl_book.bid) / mid * 10000
        aster_spread = (aster_book.ask - aster_book.bid) / mid * 10000
        half_diff = (hl_spread - aster_spread) / 2.0
        self._record_half_spread_diff(symbol, half_diff)
        if not EXECUTABLE_SIGNAL_ENABLED:
            return 0.0
        hist = self._half_spread_diff_history.get(symbol)
        if not hist:
            return 0.0
        cutoff = now_ms() - self._book_spread_window_ms
        recent = [d for ts, d in hist if ts >= cutoff]
        if len(recent) < BASELINE_MIN_SAMPLES:
            return 0.0
        return half_diff - statistics.median(recent)

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
                bids=[(float(b[0]), float(b[1])) for b in bids],
                asks=[(float(a[0]), float(a[1])) for a in asks],
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
                bids=[(float(l["px"]), float(l["sz"])) for l in bids_raw],
                asks=[(float(l["px"]), float(l["sz"])) for l in asks_raw],
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
        self, symbol: str, side: str, qty: float, price: float,
        reduce_only: bool = False,
    ) -> OrderResult:
        """
        Place a GTX (post-only) limit order on Aster.
        GTX = Good Till Crossing: posts as maker at the given price.
        Rejected immediately if it would cross (take) — use to stay passive.
        reduce_only: set on closing orders so an accounting slip can never
        flip the position past flat (exchange truncates/rejects the excess).
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
        if reduce_only and ASTER_REDUCE_ONLY:
            params["reduceOnly"] = "true"
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
        self, symbol: str, side: str, qty: float, price: float,
        reduce_only: bool = False,
    ) -> OrderResult:
        """
        Place an IOC (taker) limit order on Aster to force-fill — used to escape
        a stuck maker exit. Pays Aster taker fee (~0.9bps); use sparingly.
        reduce_only: set on closing orders so an accounting slip can never
        flip the position past flat.
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
        if reduce_only and ASTER_REDUCE_ONLY:
            params["reduceOnly"] = "true"
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
                # Aster matches IOC orders asynchronously — the POST response
                # usually returns status=NEW with executedQty=0 BEFORE the match
                # runs, then the marketable order fills a moment later. Reading
                # the immediate ack as "no fill" is the bug that stranded exits
                # and left naked legs (SNDK). Poll the order once to capture the
                # real fill; a genuine miss still comes back 0 (EXPIRED/CANCELED).
                if filled_qty <= 0 and order_status not in (
                        "FILLED", "EXPIRED", "CANCELED", "CANCELLED", "REJECTED"):
                    await asyncio.sleep(0.5)
                    q = await self.query_aster_order(symbol, oid)
                    filled_qty = float(q.get("executedQty", filled_qty) or filled_qty)
                    fill_price = float(q.get("avgPrice", 0) or 0) or fill_price
                    order_status = q.get("status", order_status)
                log.info(
                    f"Aster IOC: {side.upper()} {qty} {aster_sym} @ {limit_px} -> "
                    f"filled {filled_qty} @ {fill_price} (oid {oid}, status={order_status})"
                )
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

    async def reconcile_position_costs(self, symbol: str, start_ms: int,
                                       end_ms: int) -> dict:
        """Sum the ACTUAL funding and commission paid on both legs of a position
        over [start_ms, end_ms], from the venues' own records (not the entry-rate
        estimate). Returns a dict:
            {"funding": signed USD (+received/-paid), "fees": USD cost (>=0),
             "ok": bool, "detail": str}
        Best-effort: if either venue query fails, ok=False and the caller keeps
        the estimate. Aster income (FUNDING_FEE/COMMISSION) + HL userFunding +
        HL fill fees. HL is USDC, Aster USDT — summed 1:1 (a stablecoin basis
        move over the hold is unpriced, same assumption as the estimate)."""
        pad = 90_000  # catch settlement/commission indexing just past the close
        ast, hl_fund, hl_fee = await asyncio.gather(
            self._aster_income_sum(symbol, start_ms - pad, end_ms + pad),
            self._hl_user_funding_sum(symbol, start_ms - pad, end_ms + pad),
            self._hl_fill_fee_sum(symbol, start_ms - pad, end_ms + pad),
            return_exceptions=True,
        )
        if (isinstance(ast, Exception) or isinstance(hl_fund, Exception)
                or isinstance(hl_fee, Exception) or ast is None
                or hl_fund is None or hl_fee is None):
            # Log WHICH leg failed at WARNING — otherwise the caller silently
            # falls back to the estimate and /positions is stuck on "(est)".
            bad = []
            if isinstance(ast, Exception) or ast is None:
                bad.append(f"aster_income={ast!r}")
            if isinstance(hl_fund, Exception) or hl_fund is None:
                bad.append(f"hl_funding={hl_fund!r}")
            if isinstance(hl_fee, Exception) or hl_fee is None:
                bad.append(f"hl_fees={hl_fee!r}")
            detail = "reconcile query failed: " + "; ".join(bad)
            log.warning(f"{symbol}: {detail}")
            return {"funding": 0.0, "fees": 0.0, "ok": False, "detail": detail}
        funding = hl_fund + ast["funding"]          # both signed
        fees = hl_fee + (-ast["commission"])        # commission is negative → positive cost
        return {
            "funding": funding, "fees": fees, "ok": True,
            "detail": (f"HL fund {hl_fund:+.4f} fee {hl_fee:.4f} | "
                       f"Ast fund {ast['funding']:+.4f} comm {ast['commission']:.4f}"),
        }

    async def _aster_income_sum(self, symbol: str, start_ms: int, end_ms: int) -> dict:
        """Signed GET /fapi/v1/income for one symbol → {"funding": sum
        FUNDING_FEE, "commission": sum COMMISSION} (both signed as account
        deltas: funding +received/-paid, commission negative = cost)."""
        aster_sym = aster_symbol_for(symbol)
        params = {"symbol": aster_sym, "startTime": int(start_ms),
                  "endTime": int(end_ms), "limit": 1000}
        signed = self._sign_aster(params)
        async with self.session.get(
            ASTER_INCOME_URL, params=signed, timeout=self.timeout
        ) as r:
            data = await r.json(content_type=None)
        if not isinstance(data, list):
            raise ValueError(f"income not a list: {str(data)[:150]}")
        funding = commission = 0.0
        for row in data:
            if not isinstance(row, dict) or row.get("symbol") != aster_sym:
                continue
            t = row.get("incomeType")
            amt = float(row.get("income") or 0)
            if t == "FUNDING_FEE":
                funding += amt
            elif t == "COMMISSION":
                commission += amt
        return {"funding": funding, "commission": commission}

    @staticmethod
    def _hl_coin_matches(reported: str, base: str) -> bool:
        """HL may report the builder-dex coin as 'xyz:SKHX' or bare 'SKHX' in
        userFunding/userFills — match on the last path segment either way."""
        if not reported:
            return False
        return reported.split(":")[-1] == base

    async def _hl_info_list(self, req_type: str, base: str, start_ms: int,
                            end_ms: int) -> list:
        """POST /info for a user history query. XYZ perps live on the builder
        dex, and user queries there need dex='xyz' (same as clearinghouseState).
        Try WITH the dex first; if HL errors (non-list), retry WITHOUT so a
        main-dex-only account still works. Raises if both shapes fail."""
        body = {"type": req_type,
                "user": self.api_keys["hl_account_address"],
                "startTime": int(start_ms), "endTime": int(end_ms)}
        last = None
        for extra in ({"dex": "xyz"}, {}):
            try:
                async with self.session.post(
                    HYPERLIQUID_API, json={**body, **extra}, timeout=self.timeout,
                ) as r:
                    data = await r.json(content_type=None)
                if isinstance(data, list):
                    return data
                last = f"{req_type} not a list (dex={extra or 'none'}): {str(data)[:120]}"
            except Exception as e:
                last = f"{req_type} error (dex={extra or 'none'}): {e}"
        raise ValueError(last or f"{req_type} failed")

    async def _hl_user_funding_sum(self, base: str, start_ms: int, end_ms: int) -> float:
        """userFunding → summed USDC funding delta for the coin (+recv / -paid)."""
        data = await self._hl_info_list("userFunding", base, start_ms, end_ms)
        total = 0.0
        for row in data:
            delta = (row or {}).get("delta") or {}
            if self._hl_coin_matches(delta.get("coin", ""), base):
                total += float(delta.get("usdc") or 0)
        return total

    async def _hl_fill_fee_sum(self, base: str, start_ms: int, end_ms: int) -> float:
        """userFillsByTime → summed fee for the coin (USDC cost, >=0)."""
        data = await self._hl_info_list("userFillsByTime", base, start_ms, end_ms)
        return sum(float((f or {}).get("fee") or 0)
                   for f in data if self._hl_coin_matches((f or {}).get("coin", ""), base))

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
        self, symbol: str, side: str, qty: float, price: float,
        buffer_bps: float | None = None,
    ) -> OrderResult:
        """
        Place an IOC limit order on Hyperliquid XYZ DEX.
        The limit is offset from `price` by buffer_bps (default HL_IOC_BUFFER_BPS)
        to maximise fill probability. The buffer is only a CAP — the order still
        fills at the resting book price — so a wider buffer never worsens the
        fill, it just makes the order marketable if the book moved. Exits pass a
        wider buffer (HL_EXIT_IOC_BUFFER_BPS) so a jumpy book can't dodge the cross.
        """
        hl_coin = f"xyz:{symbol}"
        asset_idx = self._hl_xyz_indices.get(hl_coin)
        if asset_idx is None:
            return OrderResult(success=False, error=f"HL asset index not found for {hl_coin}")

        is_buy = side.lower() == "buy"
        buf_bps = HL_IOC_BUFFER_BPS if buffer_bps is None else buffer_bps
        buffer = price * buf_bps / 10000
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

            if not isinstance(data, dict) or data.get("status") != "ok":
                err = str((data or {}).get("response", data) if isinstance(data, dict) else data)
                log.error(f"HL ALO failed: {err}")
                return OrderResult(success=False, error=err, raw=data if isinstance(data, dict) else {})

            # Null-safe navigation: HL can return {"status":"ok","response":null}
            # (or a null data/statuses) — chained .get with a {} default still
            # throws when a present key holds None, so coerce each level.
            resp = data.get("response") or {}
            inner = resp.get("data") or {} if isinstance(resp, dict) else {}
            statuses = inner.get("statuses") if isinstance(inner, dict) else None
            if not statuses:
                log.error(f"HL ALO ok but no statuses in response: {data}")
                return OrderResult(success=False, error=f"empty status: {resp}",
                                   ambiguous=True, raw=data)
            s = statuses[0] or {}
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

    async def get_hl_balance(self) -> dict:
        """HL XYZ-dex account balance (USDC). HIP-3 margin is isolated to the
        builder dex, so this queries the xyz-dex clearinghouse state — the funds
        the bot actually trades these perps against. Returns {} on failure."""
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
            ms = data.get("marginSummary", {}) or {}
            return {
                "account_value": float(ms.get("accountValue", 0) or 0),
                "margin_used": float(ms.get("totalMarginUsed", 0) or 0),
                "withdrawable": float(data.get("withdrawable", 0) or 0),
                "asset": "USDC",
            }
        except Exception as e:
            log.error(f"HL balance query error: {e}")
            return {}

    async def get_aster_balance(self, asset: str = "USDT") -> dict:
        """Aster futures wallet balance for `asset` (default USDT). Signed
        GET /fapi/v3/balance returns a per-asset list. Returns {} on failure."""
        signed = self._sign_aster({})
        try:
            async with self.session.get(
                ASTER_BALANCE_URL, params=signed, timeout=self.timeout
            ) as r:
                data = await r.json()
            if not isinstance(data, list):
                log.error(f"Aster balance unexpected response: {str(data)[:200]}")
                return {}
            for a in data:
                if str(a.get("asset", "")).upper() == asset.upper():
                    return {
                        "balance": float(a.get("balance", 0) or 0),
                        "available": float(a.get("availableBalance", 0) or 0),
                        "unrealized_pnl": float(a.get("crossUnPnl", 0) or 0),
                        "asset": asset,
                    }
            return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0, "asset": asset}
        except Exception as e:
            log.error(f"Aster balance query error: {e}")
            return {}

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
        """Set leverage on Aster for a symbol (POST /fapi/v1/leverage).

        Aster caps leverage per symbol (equity perps often max at 3x), and
        silently APPLIES the cap while returning success. We record the actually
        -applied leverage so the margin pre-flight uses the real number (a 3x
        cap needs ~33% margin, not the 20% a 5x request implies) — otherwise the
        check under-provisions and the hedge can still fail 'insufficient margin'.
        """
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
            if ok:
                applied = int(float(data.get("leverage", leverage)))
                self._aster_leverage[symbol] = applied
                if applied < leverage:
                    log.warning(
                        f"Aster CAPPED leverage {params['symbol']}: requested "
                        f"{leverage}x, applied {applied}x (this leg needs more "
                        f"margin than the HL {leverage}x leg)"
                    )
            log.info(f"Aster leverage set {params['symbol']} {leverage}x: {ok} ({data})")
            return ok
        except Exception as e:
            log.error(f"Aster set leverage error: {e}")
            return False

    def get_aster_leverage(self, symbol: str) -> int:
        """Actually-applied Aster leverage for a symbol (Aster may have capped
        below the requested LEVERAGE). Falls back to the configured LEVERAGE
        until set_aster_leverage has run for the symbol this session."""
        return self._aster_leverage.get(symbol, LEVERAGE)

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

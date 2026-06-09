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
import time
from dataclasses import dataclass, field

import aiohttp
from eth_account import Account
from eth_account.messages import encode_typed_data

from auth import sign_aster_request, now_ms
from config import (
    HYPERLIQUID_API, HL_EXCHANGE_URL,
    ASTER_ORDER_URL, ASTER_OPEN_ORDERS_URL, ASTER_POSITION_URL, ASTER_EXCHANGE_INFO_URL,
    ORDER_TIMEOUT_SECONDS, HL_IOC_BUFFER_BPS, ASTER_IOC_BUFFER_BPS,
)

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

        # Mark/index price cache: symbol -> (mark_price, index_price, fetched_at_ms)
        self._mark_cache: dict[str, tuple[float, float, int]] = {}
        self._mark_cache_ttl_ms = 60_000  # refresh mark price every 60s

        # HL oracle price cache: symbol -> (oracle_px, fetched_at_ms)
        self._hl_oracle_cache: dict[str, tuple[float, int]] = {}

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
                self.hl_specs[base] = ContractSpec(
                    tick_size=0.01,
                    step_size=step,
                    min_qty=step,
                    min_notional=10.0,
                    price_precision=2,
                    qty_precision=sz_dec,
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
        aster_sym = f"{symbol}USDT"
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
            log.debug(f"HL oracle prices refreshed for {len(self._hl_oracle_cache)} symbols")
        except Exception as e:
            log.warning(f"Failed to refresh HL oracle prices: {e}")

    def get_hl_oracle(self, symbol: str) -> float:
        """Return cached HL XYZ oracle price."""
        cached = self._hl_oracle_cache.get(symbol)
        return cached[0] if cached else 0.0

    async def _get_aster_book(self, symbol: str) -> OrderBook:
        aster_sym = f"{symbol}USDT"
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

    def format_hl_price(self, symbol: str, price: float) -> str:
        spec = self.hl_specs.get(symbol, ContractSpec())
        return f"{price:.{spec.price_precision}f}"

    def format_hl_qty(self, symbol: str, qty: float) -> str:
        spec = self.hl_specs.get(symbol, ContractSpec())
        return f"{qty:.{spec.qty_precision}f}"

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
        aster_sym = f"{symbol}USDT"
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
            return OrderResult(success=False, error=str(e))

    async def place_aster_ioc(
        self, symbol: str, side: str, qty: float, price: float
    ) -> OrderResult:
        """
        Place an IOC (taker) limit order on Aster to force-fill — used to escape
        a stuck maker exit. Pays Aster taker fee (~0.9bps); use sparingly.
        """
        aster_sym = f"{symbol}USDT"
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
                log.info(
                    f"Aster IOC: {side.upper()} {qty} {aster_sym} @ {limit_px} -> "
                    f"filled {filled_qty} @ {fill_price} (oid {oid})"
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
            return OrderResult(success=False, error=str(e))

    async def cancel_aster_order(self, symbol: str, order_id: str) -> bool:
        aster_sym = f"{symbol}USDT"
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
        aster_sym = f"{symbol}USDT"
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
        aster_sym = f"{symbol}USDT"
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
        aster_sym = f"{symbol}USDT"
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
        return {
            "action": action,
            "nonce": nonce,
            "signature": {
                "r": "0x" + format(signed.r, "064x"),
                "s": "0x" + format(signed.s, "064x"),
                "v": signed.v,
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
        limit_px = round(limit_px, self.hl_specs.get(symbol, ContractSpec()).price_precision)

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
                    return OrderResult(success=False, error=f"HTTP {r.status}: {text}")
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
            return OrderResult(success=False, error=str(e))

    async def get_hl_position(self, symbol: str) -> dict:
        hl_coin = f"xyz:{symbol}"
        try:
            async with self.session.post(
                HYPERLIQUID_API,
                json={
                    "type": "clearinghouseState",
                    "user": self.api_keys["hl_wallet_address"],
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

    async def set_hl_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        """Set leverage for a symbol on HL XYZ."""
        hl_coin = f"xyz:{symbol}"
        action = {
            "type": "updateLeverage",
            "asset": hl_coin,
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
                    return OrderResult(success=False, error=f"HTTP {r.status}: {text}")
                data = await r.json()
            ok = data.get("status") == "ok"
            log.info(f"HL leverage set {hl_coin} {leverage}x cross={cross}: {ok}")
            return ok
        except Exception as e:
            log.error(f"HL set leverage error: {e}")
            return False

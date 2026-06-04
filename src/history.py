"""Historical data fetchers — candles and funding history from both venues.

NOTE: neither venue serves historical orderbook depth, so bid/offer cost must
be *estimated* for backtests. Close prices and funding rates ARE available.
"""

import asyncio
import time
from decimal import Decimal

import aiohttp

from .aster import ASTER_URL, _get
from .hyperliquid import HL_URL, _post

HOUR_MS = 3_600_000


# ── Hyperliquid ──────────────────────────────────────────────────────────────

async def hl_candles(
    session: aiohttp.ClientSession, coin: str, start_ms: int, end_ms: int,
    interval: str = "1h",
) -> dict[int, Decimal]:
    """Returns {open_time_ms: close_price} for an HL coin."""
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms},
    }
    try:
        data = await _post(session, payload)
        return {int(c["t"]): Decimal(str(c["c"])) for c in data}
    except Exception:
        return {}


async def hl_funding_history(
    session: aiohttp.ClientSession, coin: str, start_ms: int,
) -> dict[int, Decimal]:
    """Returns {time_ms: hourly_funding_rate} for an HL coin (already hourly)."""
    payload = {"type": "fundingHistory", "coin": coin, "startTime": start_ms}
    try:
        data = await _post(session, payload)
        return {int(f["time"]): Decimal(str(f["fundingRate"])) for f in data}
    except Exception:
        return {}


# ── Aster ────────────────────────────────────────────────────────────────────

async def aster_candles(
    session: aiohttp.ClientSession, symbol: str, start_ms: int, end_ms: int,
    interval: str = "1h",
) -> dict[int, Decimal]:
    """Returns {open_time_ms: close_price} for an Aster symbol."""
    params = {
        "symbol": symbol, "interval": interval,
        "startTime": start_ms, "endTime": end_ms, "limit": 1500,
    }
    try:
        data = await _get(session, "/fapi/v1/klines", params)
        # kline: [openTime, open, high, low, close, volume, closeTime, ...]
        return {int(k[0]): Decimal(str(k[4])) for k in data}
    except Exception:
        return {}


async def aster_funding_history(
    session: aiohttp.ClientSession, symbol: str, start_ms: int,
) -> dict[int, Decimal]:
    """Returns {funding_time_ms: 8h_funding_rate} for an Aster symbol."""
    params = {"symbol": symbol, "startTime": start_ms, "limit": 1000}
    try:
        data = await _get(session, "/fapi/v1/fundingRate", params)
        return {int(f["fundingTime"]): Decimal(str(f["fundingRate"])) for f in data}
    except Exception:
        return {}


# ── USDC/USDT basis (Binance spot klines) ─────────────────────────────────────

async def usdc_usdt_history(
    session: aiohttp.ClientSession, start_ms: int, end_ms: int, interval: str = "1h",
) -> dict[int, Decimal]:
    """Returns {open_time_ms: USDCUSDT close}. Empty dict falls back to 1.0 upstream."""
    params = {
        "symbol": "USDCUSDT", "interval": interval,
        "startTime": start_ms, "endTime": end_ms, "limit": 1500,
    }
    try:
        async with session.get(
            "https://api.binance.com/api/v3/klines", params=params
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return {int(k[0]): Decimal(str(k[4])) for k in data}
    except Exception:
        return {}


def now_ms() -> int:
    return int(time.time() * 1000)

"""Aster DEX read-only client — Binance-compatible REST API, no auth for market data."""

from decimal import Decimal

import aiohttp

from .types import BookTicker, EquityPerp

ASTER_URL = "https://fapi.asterdex.com"


async def _get(
    session: aiohttp.ClientSession, path: str, params: dict | None = None
) -> dict | list:
    url = f"{ASTER_URL}{path}"
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        return await resp.json()


async def get_all_perps(session: aiohttp.ClientSession) -> dict[str, EquityPerp]:
    """
    Returns {canonical: EquityPerp} for all active USDT-margined perps on Aster.
    We don't filter by underlyingType here — the intersection with HL's xyz: list
    naturally isolates equity perps.
    """
    data = await _get(session, "/fapi/v1/exchangeInfo")
    result: dict[str, EquityPerp] = {}
    for sym in data.get("symbols", []):
        if sym.get("status") != "TRADING":
            continue
        symbol: str = sym["symbol"]
        if not symbol.endswith("USDT"):
            continue
        canonical = symbol[:-4]
        result[canonical] = EquityPerp(
            canonical=canonical,
            venue_symbol=symbol,
        )
    return result


async def get_all_book_tickers(session: aiohttp.ClientSession) -> dict[str, BookTicker]:
    """Returns {canonical: BookTicker} for all USDT perps."""
    data = await _get(session, "/fapi/v1/ticker/bookTicker")
    result: dict[str, BookTicker] = {}
    for t in data:
        symbol: str = t.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        try:
            bid = Decimal(t["bidPrice"])
            ask = Decimal(t["askPrice"])
            if bid <= 0 or ask <= 0:
                continue
            canonical = symbol[:-4]
            result[canonical] = BookTicker(symbol=symbol, bid=bid, ask=ask)
        except Exception:
            continue
    return result


async def get_all_funding_rates(session: aiohttp.ClientSession) -> dict[str, Decimal]:
    """Returns {canonical: 8h_funding_rate} from premiumIndex."""
    data = await _get(session, "/fapi/v1/premiumIndex")
    result: dict[str, Decimal] = {}
    for item in data:
        symbol: str = item.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        try:
            rate_str = item.get("lastFundingRate") or item.get("fundingRate", "0")
            canonical = symbol[:-4]
            result[canonical] = Decimal(rate_str)
        except Exception:
            continue
    return result

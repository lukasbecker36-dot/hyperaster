"""Aster DEX read-only client — Binance-compatible REST API, no auth for market data."""

from decimal import Decimal

import asyncio

import aiohttp

from .types import BookTicker, EquityPerp

ASTER_URL = "https://fapi.asterdex.com"

# Cross-venue aliases: Aster's tradeable base -> HL's canonical base.
# HL uses short/GDR tickers (SMSN, SKHX), Aster's name-matching contracts are
# dead (400), and the live books use the long names (SAMSUNG, SKHYNIX).
_ASTER_TO_CANON = {
    "SAMSUNG": "SMSN",
    "SKHYNIX": "SKHX",
}
_CANON_PHANTOMS = set(_ASTER_TO_CANON.values())  # dead listings to skip


async def _get(
    session: aiohttp.ClientSession, path: str, params: dict | None = None,
    retries: int = 5,
) -> dict | list:
    """GET with 429/418 backoff — same reasoning as the HL side: without it a
    single throttle became a silently-dropped symbol in every backtest."""
    url = f"{ASTER_URL}{path}"
    delay = 1.0
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params) as resp:
                if resp.status in (429, 418):
                    if attempt == retries:
                        resp.raise_for_status()
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 20.0)
                    continue
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientResponseError:
            raise
        except Exception:
            if attempt == retries:
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 2, 20.0)
    raise RuntimeError("unreachable")


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
        raw_base = symbol[:-4]
        if raw_base in _CANON_PHANTOMS:
            continue
        canonical = _ASTER_TO_CANON.get(raw_base, raw_base)
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
            raw_base = symbol[:-4]
            if raw_base in _CANON_PHANTOMS:
                continue
            canonical = _ASTER_TO_CANON.get(raw_base, raw_base)
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
            raw_base = symbol[:-4]
            if raw_base in _CANON_PHANTOMS:
                continue
            canonical = _ASTER_TO_CANON.get(raw_base, raw_base)
            result[canonical] = Decimal(rate_str)
        except Exception:
            continue
    return result

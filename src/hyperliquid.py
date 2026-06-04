"""Hyperliquid read-only client — no auth required for market data."""

import asyncio
from decimal import Decimal
from typing import Optional

import aiohttp

from .types import BookTicker, EquityPerp

HL_URL = "https://api.hyperliquid.xyz/info"


async def _post(session: aiohttp.ClientSession, payload: dict) -> dict | list:
    async with session.post(HL_URL, json=payload) as resp:
        resp.raise_for_status()
        return await resp.json()


async def get_all_equity_perps(session: aiohttp.ClientSession) -> dict[str, EquityPerp]:
    """
    Returns {canonical: EquityPerp} for all xyz: HIP-3 equity perps.
    Funding rate is the current 1-hour rate from metaAndAssetCtxs.
    """
    data = await _post(session, {"type": "metaAndAssetCtxs"})
    meta, ctxs = data[0], data[1]

    result: dict[str, EquityPerp] = {}
    for asset, ctx in zip(meta["universe"], ctxs):
        coin: str = asset["name"]
        if not coin.startswith("xyz:"):
            continue
        canonical = coin[4:]  # strip "xyz:"
        mark_px_str = ctx.get("markPx") or ctx.get("midPx")
        funding_str = ctx.get("funding", "0")
        result[canonical] = EquityPerp(
            canonical=canonical,
            venue_symbol=coin,
            mark_price=Decimal(mark_px_str) if mark_px_str else None,
            funding_rate_hourly=Decimal(funding_str),
        )
    return result


async def _get_book_ticker(session: aiohttp.ClientSession, coin: str) -> Optional[BookTicker]:
    try:
        book = await _post(session, {"type": "l2Book", "coin": coin})
        levels = book["levels"]
        if not levels[0] or not levels[1]:
            return None
        best_bid = Decimal(levels[0][0]["px"])
        best_ask = Decimal(levels[1][0]["px"])
        if best_bid <= 0 or best_ask <= 0:
            return None
        return BookTicker(symbol=coin, bid=best_bid, ask=best_ask)
    except Exception:
        return None


async def get_book_tickers(
    session: aiohttp.ClientSession, coins: list[str]
) -> dict[str, BookTicker]:
    """Fetch L2 books for multiple coins concurrently. Returns {canonical: BookTicker}."""
    tasks = [_get_book_ticker(session, coin) for coin in coins]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return {
        coin[4:]: ticker
        for coin, ticker in zip(coins, results)
        if isinstance(ticker, BookTicker)
    }

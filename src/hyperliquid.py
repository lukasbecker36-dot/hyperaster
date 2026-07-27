"""Hyperliquid read-only client — no auth required for market data."""

import asyncio
from decimal import Decimal
from typing import Optional

import aiohttp

from .types import BookTicker, EquityPerp

HL_URL = "https://api.hyperliquid.xyz/info"


# Opt-in live-gate guard. OFF by default so the trader's own use of this module
# (warmup_book_spread -> history.hl_candles) can never be blocked by a flag the
# trader itself writes. Heavy offline scripts turn it on.
_GATE_GUARD = False
_GATE_WAIT_MAX_S = 300


def set_gate_guard(enabled: bool = True):
    """Pause (then abort) requests while the live trader has a gate armed.

    For multi-symbol analysis run on the trading box. HL rate-limits per IP, so
    a backtest and the trader share one budget — and the loser is the gate, which
    goes blind and cannot fire.
    """
    global _GATE_GUARD
    _GATE_GUARD = enabled


async def _await_gate_clear():
    if not _GATE_GUARD:
        return
    from config import gate_is_armed
    waited = 0
    while gate_is_armed():
        if waited == 0:
            print("[gate-guard] live basis gate armed — holding HL requests so the "
                  "trader's book read isn't starved…", flush=True)
        if waited >= _GATE_WAIT_MAX_S:
            raise RuntimeError(
                f"gate still armed after {_GATE_WAIT_MAX_S}s — aborting rather than "
                f"competing with the live trader for HL's per-IP budget. Re-run when "
                f"the gate clears, run it off-box, or pass --ignore-gate.")
        await asyncio.sleep(5)
        waited += 5
    if waited:
        print(f"[gate-guard] gate cleared after {waited}s — resuming", flush=True)


async def _post(session: aiohttp.ClientSession, payload: dict,
                retries: int = 5) -> dict | list:
    """POST /info with 429 backoff.

    Had no retry at all: raise_for_status() turned a single throttle into a hard
    failure, and every caller here swallows exceptions into {} — so a backtest
    either crashed on the first 429 or silently dropped the symbols that got
    throttled (observed: "Built panels for 1 symbols, skipped 32", which looked
    like missing history rather than rate limiting). HL's info limit is 1200
    weight/min per IP and candleSnapshot is charged per 60 candles, so a
    multi-symbol fetch trips it easily.
    """
    delay = 1.0
    for attempt in range(retries + 1):
        await _await_gate_clear()
        try:
            async with session.post(HL_URL, json=payload) as resp:
                if resp.status == 429:
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


async def get_all_equity_perps(session: aiohttp.ClientSession) -> dict[str, EquityPerp]:
    """
    Returns {canonical: EquityPerp} for all xyz: HIP-3 equity perps.
    Funding rate is the current 1-hour rate from metaAndAssetCtxs.

    The xyz builder dex must be requested explicitly via the "dex" param —
    without it the default mainnet universe is returned, which has no xyz coins.
    Universe names may come back bare ("AAPL") or prefixed ("xyz:AAPL"); we
    normalise to a bare canonical and rebuild the full "xyz:" coin for /info
    history queries.
    """
    data = await _post(session, {"type": "metaAndAssetCtxs", "dex": "xyz"})
    meta, ctxs = data[0], data[1]

    result: dict[str, EquityPerp] = {}
    for asset, ctx in zip(meta["universe"], ctxs):
        name: str = asset["name"]
        canonical = name.split(":")[-1]  # bare base, prefix-agnostic
        mark_px_str = ctx.get("markPx") or ctx.get("midPx")
        funding_str = ctx.get("funding", "0")
        result[canonical] = EquityPerp(
            canonical=canonical,
            venue_symbol=f"xyz:{canonical}",  # full coin name for history endpoints
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

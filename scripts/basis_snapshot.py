#!/usr/bin/env python3
"""
Live executable-basis snapshot for one name — the numbers the /enter and /close
basis gates actually check, so you can pick gate targets by eye.

Convention (matches live_monitor._carry_basis_bps, maker-HL / taker-Aster):
    buy-HL leg : buy HL @ bid (maker), sell Aster @ bid (taker)
                 = (aster_bid − hl_bid) / mid × 10⁴
                 → this IS the gate basis for: ENTER L-HL/S-AST, EXIT L-AST/S-HL
    buy-AST leg: sell HL @ ask (maker), buy Aster @ ask (taker)
                 = (hl_ask − aster_ask) / mid × 10⁴
                 → this IS the gate basis for: ENTER L-AST/S-HL, EXIT L-HL/S-AST

Higher = better (in your favour); a gate fires when basis ≥ target.
The two legs' sum = the gross round trip you'd lock crossing in AND out right
now (before fees/funding) — usually negative; funding carry is what you're
harvesting while you wait for a better exit level.

24h averages come from the capture DB (data/orderbook_capture.db) when it has
rows for the name; otherwise approximated from 1m candle mids (marked ~).

Usage: python scripts/basis_snapshot.py SYMBOL
"""

import asyncio
import os
import sqlite3
import statistics
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    HYPERLIQUID_API, ASTER_BASE, DATA_DIR, aster_symbol_for,
)
from src import history

ASTER_DEPTH_URL = f"{ASTER_BASE}/fapi/v1/depth"
ASTER_PREMIUM_URL = f"{ASTER_BASE}/fapi/v1/premiumIndex"
CAPTURE_DB = os.path.join(DATA_DIR, "orderbook_capture.db")
DAY_MS = 24 * 3_600_000


async def _hl_book(session, symbol):
    async with session.post(HYPERLIQUID_API,
                            json={"type": "l2Book", "coin": f"xyz:{symbol}"}) as r:
        data = await r.json(content_type=None)
    levels = (data or {}).get("levels", [[], []])
    bids, asks = levels[0] or [], levels[1] or []
    if not bids or not asks:
        return None
    b, a = float(bids[0]["px"]), float(asks[0]["px"])
    if b > a:
        b, a = a, b
    return b, a


async def _aster_book(session, symbol):
    async with session.get(ASTER_DEPTH_URL,
                           params={"symbol": aster_symbol_for(symbol), "limit": 5}) as r:
        d = await r.json(content_type=None)
    bids, asks = (d or {}).get("bids", []), (d or {}).get("asks", [])
    if not bids or not asks:
        return None
    return float(bids[0][0]), float(asks[0][0])


async def _funding(session, symbol):
    """(hl_rate_per_1h, aster_rate_per_8h) — positive = longs pay shorts."""
    hl_r = ast_r = None
    try:
        async with session.post(HYPERLIQUID_API,
                                json={"type": "metaAndAssetCtxs", "dex": "xyz"}) as r:
            data = await r.json(content_type=None)
        meta, ctxs = data[0], data[1]
        for i, asset in enumerate(meta.get("universe", [])):
            if asset.get("name", "").split(":")[-1] == symbol and i < len(ctxs):
                hl_r = float(ctxs[i].get("funding") or 0)
                break
    except Exception:
        pass
    try:
        async with session.get(ASTER_PREMIUM_URL,
                               params={"symbol": aster_symbol_for(symbol)}) as r:
            d = await r.json(content_type=None)
        ast_r = float((d or {}).get("lastFundingRate") or 0)
    except Exception:
        pass
    return hl_r, ast_r


def _legs(hl_bid, hl_ask, ast_bid, ast_ask):
    """(buy_hl_leg_bps, buy_ast_leg_bps) or None."""
    mid = (hl_bid + hl_ask + ast_bid + ast_ask) / 4
    if mid <= 0:
        return None
    return ((ast_bid - hl_bid) / mid * 10000,
            (hl_ask - ast_ask) / mid * 10000)


def _avg_from_capture(symbol):
    """Exact 24h mean of both leg bases from the capture DB, or None."""
    if not os.path.exists(CAPTURE_DB):
        return None
    try:
        conn = sqlite3.connect(f"file:{CAPTURE_DB}?mode=ro", uri=True, timeout=10)
        cutoff = int(time.time() * 1000) - DAY_MS
        rows = conn.execute(
            "SELECT hl_bid, hl_ask, aster_bid, aster_ask FROM book_snaps "
            "WHERE symbol=? AND ts>=? AND hl_bid>0 AND hl_ask>0 "
            "AND aster_bid>0 AND aster_ask>0", (symbol, cutoff)).fetchall()
        conn.close()
    except Exception:
        return None
    if len(rows) < 20:
        return None
    buy_hl, buy_ast = [], []
    for hb, ha, ab, aa in rows:
        legs = _legs(hb, ha, ab, aa)
        if legs:
            buy_hl.append(legs[0])
            buy_ast.append(legs[1])
    if not buy_hl:
        return None
    return statistics.mean(buy_hl), statistics.mean(buy_ast), len(buy_hl), True


async def _avg_from_candles(session, symbol):
    """Fallback: 24h mean mid-spread from 1m candles → buy_hl ≈ +S̄, buy_ast ≈ −S̄
    (ignores each venue's own bid-ask width — marked approximate)."""
    end = int(time.time() * 1000)
    try:
        hl_d, ast_d = await asyncio.gather(
            history.hl_candles(session, f"xyz:{symbol}", end - DAY_MS, end, interval="1m"),
            history.aster_candles(session, aster_symbol_for(symbol), end - DAY_MS, end,
                                  interval="1m"),
        )
    except Exception:
        return None
    common = sorted(set(hl_d or {}) & set(ast_d or {}))
    if len(common) < 20:
        return None
    sp = []
    for t in common:
        h, a = float(hl_d[t]), float(ast_d[t])
        m = (h + a) / 2
        if m > 0:
            sp.append((a - h) / m * 10000)
    if not sp:
        return None
    s = statistics.mean(sp)
    return s, -s, len(sp), False


async def main():
    if len(sys.argv) < 2:
        print("Usage: basis_snapshot.py SYMBOL")
        return
    symbol = sys.argv[1].upper()
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        hl, ast, (hl_fr, ast_fr) = await asyncio.gather(
            _hl_book(session, symbol), _aster_book(session, symbol),
            _funding(session, symbol),
        )
        if not hl or not ast:
            print(f"📐 {symbol}: book unavailable "
                  f"(HL={'ok' if hl else 'empty'}, Aster={'ok' if ast else 'empty'})")
            return
        avg = _avg_from_capture(symbol) or await _avg_from_candles(session, symbol)

    hl_bid, hl_ask = hl
    ast_bid, ast_ask = ast
    legs = _legs(hl_bid, hl_ask, ast_bid, ast_ask)
    if not legs:
        print(f"📐 {symbol}: bad prices")
        return
    buy_hl_now, buy_ast_now = legs

    lines = [f"📐 {symbol} executable basis (maker-HL/taker-Ast, bps)"]
    if avg:
        a_hl, a_ast, n, exact = avg
        src = f"24h avg ({n} snaps)" if exact else f"~24h avg (candle mids, {n})"
    else:
        a_hl = a_ast = None
        src = "24h avg: no data"
    lines.append(f"{'':14}{'now':>8}  {src}")
    lines.append(f"{'buy-HL leg':<14}{buy_hl_now:>+8.1f}"
                 + (f"  {a_hl:>+8.1f}" if a_hl is not None else ""))
    lines.append("  = ENTER L-HL/S-AST · EXIT L-AST/S-HL")
    lines.append(f"{'buy-AST leg':<14}{buy_ast_now:>+8.1f}"
                 + (f"  {a_ast:>+8.1f}" if a_ast is not None else ""))
    lines.append("  = ENTER L-AST/S-HL · EXIT L-HL/S-AST")
    lines.append(f"round trip now (enter+exit): {buy_hl_now + buy_ast_now:+.1f}bps")
    lines.append(f"books  HL {hl_bid:.2f}/{hl_ask:.2f}  Ast {ast_bid:.2f}/{ast_ask:.2f}")

    if hl_fr is not None and ast_fr is not None:
        # Net carry per day (bps) for L-AST/S-HL: short HL earns hl_rate (if +),
        # long Aster pays ast_rate (if +). Other direction is the negative.
        carry_last = (hl_fr * 24 - ast_fr * 3) * 10000
        lines.append(f"funding  HL {hl_fr*100:+.4f}%/1h  Ast {ast_fr*100:+.4f}%/8h")
        lines.append(f"net carry ≈ {carry_last:+.1f}bps/day L-AST/S-HL "
                     f"({-carry_last:+.1f} L-HL/S-AST)")

    lines.append("")
    lines.append("gates: /enter waits for basis ≥ target, /close likewise.")
    lines.append(f"e.g. /enter {symbol} buy_aster 1000 {round(buy_ast_now) + 5}")
    lines.append(f"     /close {symbol} {round(buy_hl_now) + 5}")
    print("\n".join(lines))


if __name__ == "__main__":
    asyncio.run(main())

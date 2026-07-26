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
    fmt_px,
)
from src import history

ASTER_DEPTH_URL = f"{ASTER_BASE}/fapi/v1/depth"
ASTER_PREMIUM_URL = f"{ASTER_BASE}/fapi/v1/premiumIndex"
CAPTURE_DB = os.path.join(DATA_DIR, "orderbook_capture.db")
DAY_MS = 24 * 3_600_000


# symbol -> HL dex ("xyz" equity / "" main-dex crypto), resolved once per run.
_HL_DEX_CACHE: dict = {}


# Why the last HL book read failed, for the caller's message. HL returns a
# 200-with-null-body under pressure as often as a 429, so "empty" alone is not a
# diagnosis — and blaming rate limits when the real cause was a wrong dex cost
# real debugging time.
_HL_BOOK_ERR: dict = {}


async def _hl_dex(session, symbol, retries=2):
    """Which HL dex lists this symbol: 'xyz' (HIP-3 equity) or '' (main-dex
    crypto). Checks the xyz universe first, then main.

    Retries: a single null/429 body used to fall straight through to the 'xyz'
    default, which is silently WRONG for a main-dex crypto name — the resulting
    'xyz:BTC' read comes back empty and got reported as a rate limit. Returns
    None when both dexes are genuinely unreachable so the caller can say so
    instead of guessing.
    """
    if symbol in _HL_DEX_CACHE:
        return _HL_DEX_CACHE[symbol]
    reachable = False
    for attempt in range(retries + 1):
        for dex in ("xyz", None):
            body = {"type": "metaAndAssetCtxs"}
            if dex:
                body["dex"] = dex
            try:
                async with session.post(HYPERLIQUID_API, json=body) as r:
                    data = await r.json(content_type=None)
                if not (isinstance(data, list) and data
                        and isinstance(data[0], dict)):
                    continue          # null/error body — not an answer
                reachable = True
                names = {a.get("name", "").split(":")[-1]
                         for a in data[0].get("universe", [])}
                if symbol in names:
                    _HL_DEX_CACHE[symbol] = "xyz" if dex else ""
                    return _HL_DEX_CACHE[symbol]
            except Exception:
                pass
        if reachable:
            break                     # both universes answered; name isn't listed
        if attempt < retries:
            await asyncio.sleep(0.4 * (attempt + 1))
    if reachable:
        # Both universes answered and neither lists it — default xyz so the
        # caller still attempts a read (a brand-new listing can lag the meta).
        _HL_DEX_CACHE[symbol] = "xyz"
        return "xyz"
    return None


async def _hl_coin(session, symbol):
    """HL l2Book/candle coin string for a symbol (dex-aware). None if the dex
    could not be determined — do NOT guess, the wrong prefix reads as empty."""
    dex = await _hl_dex(session, symbol)
    if dex is None:
        return None
    return f"xyz:{symbol}" if dex == "xyz" else symbol


async def _hl_book(session, symbol, retries=2):
    """HL top-of-book, dex-aware, with retries.

    Had no retry at all: one 429 or null body returned None and /basis announced
    'book unavailable (HL=empty)' for a name whose book was live (observed on
    SKHX with bid/ask 1219.9/1220.1 on the venue at that moment).
    """
    coin = await _hl_coin(session, symbol)
    if coin is None:
        _HL_BOOK_ERR[symbol] = ("could not determine which HL dex lists this name "
                                "(metaAndAssetCtxs unreachable) — not a book problem")
        return None
    last = "no levels returned"
    for attempt in range(retries + 1):
        try:
            async with session.post(HYPERLIQUID_API,
                                    json={"type": "l2Book", "coin": coin}) as r:
                status = r.status
                data = await r.json(content_type=None)
            if status == 429:
                last = f"HL rate-limited (429) on {coin}"
            elif not isinstance(data, dict):
                last = f"HL returned a {type(data).__name__} body for {coin}"
            else:
                levels = data.get("levels") or [[], []]
                bids = (levels[0] if len(levels) > 0 else []) or []
                asks = (levels[1] if len(levels) > 1 else []) or []
                if bids and asks:
                    b, a = float(bids[0]["px"]), float(asks[0]["px"])
                    if b > a:
                        b, a = a, b
                    _HL_BOOK_ERR.pop(symbol, None)
                    return b, a
                last = f"{coin} has no resting {'bids' if not bids else 'asks'}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        if attempt < retries:
            await asyncio.sleep(0.3 * (attempt + 1))
    _HL_BOOK_ERR[symbol] = f"{last} (after {retries + 1} attempts)"
    return None


async def _aster_book(session, symbol):
    async with session.get(ASTER_DEPTH_URL,
                           params={"symbol": aster_symbol_for(symbol), "limit": 5}) as r:
        d = await r.json(content_type=None)
    bids, asks = (d or {}).get("bids", []), (d or {}).get("asks", [])
    if not bids or not asks:
        return None
    return float(bids[0][0]), float(asks[0][0])


def _percentile(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


async def _funding_avg_24h(session, symbol):
    """24h average funding from the venues' own history endpoints:
    (avg_hl_per_1h, avg_ast_per_window, ast_window_h) — actual settled rates,
    not predictions. The Aster funding WINDOW is detected from the gaps between
    settlement timestamps rather than assumed 8h — some names settle every 4h
    (e.g. SKHX). Falls back to 8h when fewer than 2 settlements are visible.
    Rate sides are None on failure."""
    start = int(time.time() * 1000) - DAY_MS
    hl_avg = ast_avg = None
    ast_window_h = 8.0
    try:
        async with session.post(HYPERLIQUID_API,
                                json={"type": "fundingHistory",
                                      "coin": await _hl_coin(session, symbol),
                                      "startTime": start}) as r:
            rows = await r.json(content_type=None)
        rates = [float(x.get("fundingRate") or 0) for x in (rows or [])]
        if rates:
            hl_avg = statistics.mean(rates)
    except Exception:
        pass
    try:
        async with session.get(f"{ASTER_BASE}/fapi/v1/fundingRate",
                               params={"symbol": aster_symbol_for(symbol),
                                       "startTime": start}) as r:
            rows = await r.json(content_type=None)
        rows = [x for x in (rows or []) if isinstance(x, dict)]
        rates = [float(x.get("fundingRate") or 0) for x in rows]
        if rates:
            ast_avg = statistics.mean(rates)
        times = sorted(int(x.get("fundingTime") or 0) for x in rows)
        gaps = [(t2 - t1) / 3_600_000 for t1, t2 in zip(times, times[1:])
                if t2 > t1]
        if gaps:
            ast_window_h = statistics.median(gaps)
    except Exception:
        pass
    return hl_avg, ast_avg, ast_window_h


async def _funding(session, symbol):
    """(hl_rate_per_1h, aster_rate_per_WINDOW) — positive = longs pay shorts.
    The Aster window varies by name; _funding_avg_24h detects it."""
    hl_r = ast_r = None
    try:
        body = {"type": "metaAndAssetCtxs"}
        if await _hl_dex(session, symbol) == "xyz":
            body["dex"] = "xyz"
        async with session.post(HYPERLIQUID_API, json=body) as r:
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
    return {
        "avg_hl": statistics.mean(buy_hl), "avg_ast": statistics.mean(buy_ast),
        "p90_hl": _percentile(buy_hl, 90), "p90_ast": _percentile(buy_ast, 90),
        "n": len(buy_hl), "exact": True,
    }


async def _avg_from_candles(session, symbol):
    """Fallback: 24h mean mid-spread from 1m candles → buy_hl ≈ +S̄, buy_ast ≈ −S̄
    (ignores each venue's own bid-ask width — marked approximate)."""
    end = int(time.time() * 1000)
    try:
        hl_coin = await _hl_coin(session, symbol)
        hl_d, ast_d = await asyncio.gather(
            history.hl_candles(session, hl_coin, end - DAY_MS, end, interval="1m"),
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
    # buy_hl leg ≈ +spread, buy_ast leg ≈ −spread (book widths ignored), so the
    # buy_ast p90 is the 90th pct of −spread = −(10th pct of spread).
    return {
        "avg_hl": s, "avg_ast": -s,
        "p90_hl": _percentile(sp, 90), "p90_ast": -_percentile(sp, 10),
        "n": len(sp), "exact": False,
    }


async def main():
    if len(sys.argv) < 2:
        print("Usage: basis_snapshot.py SYMBOL")
        return
    symbol = sys.argv[1].upper()
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Resolve the dex ONCE, before the gather. Three of the four coroutines
        # below call _hl_coin, and on a cold cache each raced to resolve it — up
        # to 6 concurrent metaAndAssetCtxs (weight 20 apiece) fired at an IP
        # already carrying the capture sweep. Any one that got throttled took the
        # old silent 'xyz' fallback. One call up front removes the race and the
        # burst.
        await _hl_dex(session, symbol)
        hl, ast, (hl_fr, ast_fr), (hl_fr24, ast_fr24, ast_win_h) = await asyncio.gather(
            _hl_book(session, symbol), _aster_book(session, symbol),
            _funding(session, symbol), _funding_avg_24h(session, symbol),
        )
        if not hl or not ast:
            why = []
            if not hl:
                why.append("HL: " + _HL_BOOK_ERR.get(symbol, "no book"))
            if not ast:
                why.append(f"Aster: {aster_symbol_for(symbol)} returned no "
                           f"top-of-book")
            print(f"📐 {symbol}: book unavailable\n  " + "\n  ".join(why))
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
        src = (f"24h from {avg['n']} capture snaps" if avg["exact"]
               else f"~24h from {avg['n']} candle mids (book width ignored)")
        lines.append(f"{'':11}{'now':>8}{'24h avg':>9}{'24h p90':>9}   ({src})")
        lines.append(f"{'buy-HL leg':<11}{buy_hl_now:>+8.1f}"
                     f"{avg['avg_hl']:>+9.1f}{avg['p90_hl']:>+9.1f}")
        lines.append("  = ENTER L-HL/S-AST · EXIT L-AST/S-HL")
        lines.append(f"{'buy-AST leg':<11}{buy_ast_now:>+8.1f}"
                     f"{avg['avg_ast']:>+9.1f}{avg['p90_ast']:>+9.1f}")
        lines.append("  = ENTER L-AST/S-HL · EXIT L-HL/S-AST")
        lines.append("p90 = level reached only ~10% of the last 24h — a gate "
                     "there fills on spikes, not the sit-level")
    else:
        lines.append(f"{'':11}{'now':>8}   (no 24h history yet)")
        lines.append(f"{'buy-HL leg':<11}{buy_hl_now:>+8.1f}")
        lines.append("  = ENTER L-HL/S-AST · EXIT L-AST/S-HL")
        lines.append(f"{'buy-AST leg':<11}{buy_ast_now:>+8.1f}")
        lines.append("  = ENTER L-AST/S-HL · EXIT L-HL/S-AST")
    lines.append(f"round trip now (enter+exit): {buy_hl_now + buy_ast_now:+.1f}bps")
    lines.append(f"books  HL {fmt_px(hl_bid)}/{fmt_px(hl_ask)}  Ast {fmt_px(ast_bid)}/{fmt_px(ast_ask)}")

    # Funding: Aster rates are PER WINDOW and the window varies by name (SKHX
    # settles every 4h, most 8h) — detected above from settlement timestamps.
    # Both venues shown normalised to per-1h so they compare at a glance.
    settles_day = 24.0 / ast_win_h if ast_win_h > 0 else 3.0
    win_str = f"{ast_win_h:.0f}h window" if ast_win_h else "8h window"
    if hl_fr is not None and ast_fr is not None:
        ast_1h = ast_fr / ast_win_h if ast_win_h > 0 else ast_fr / 8
        # Net carry per day (bps) for L-AST/S-HL: short HL earns hl_rate (if +),
        # long Aster pays ast_rate (if +). Other direction is the negative.
        carry_last = (hl_fr * 24 - ast_fr * settles_day) * 10000
        lines.append(f"funding now (per 1h)  HL {hl_fr*100:+.4f}%  "
                     f"Ast {ast_1h*100:+.4f}% ({win_str})")
        lines.append(f"  net carry ≈ {carry_last:+.1f}bps/day L-AST/S-HL "
                     f"({-carry_last:+.1f} L-HL/S-AST)")
    if hl_fr24 is not None and ast_fr24 is not None:
        ast24_1h = ast_fr24 / ast_win_h if ast_win_h > 0 else ast_fr24 / 8
        carry_24h = (hl_fr24 * 24 - ast_fr24 * settles_day) * 10000
        lines.append(f"funding 24h avg, settled (per 1h)  HL {hl_fr24*100:+.4f}%  "
                     f"Ast {ast24_1h*100:+.4f}%")
        lines.append(f"  net carry ≈ {carry_24h:+.1f}bps/day L-AST/S-HL "
                     f"({-carry_24h:+.1f} L-HL/S-AST)")

    lines.append("")
    lines.append("gates: /enter waits for basis ≥ target, /close likewise.")
    # Suggest the 24h p90 as the gate (fill on spikes); fall back to now+5.
    ast_gate = round(avg["p90_ast"]) if avg else round(buy_ast_now) + 5
    hl_gate = round(avg["p90_hl"]) if avg else round(buy_hl_now) + 5
    # CARRY-AWARE suggestion: while you sit between entry and exit you HOLD a
    # direction, and its funding carry can pay you or bleed you. Suggest the
    # positive-carry direction (settled 24h rates preferred over the live tick).
    carry_ashl = None   # net bps/day for holding L-AST/S-HL
    if hl_fr24 is not None and ast_fr24 is not None:
        carry_ashl = (hl_fr24 * 24 - ast_fr24 * settles_day) * 10000
    elif hl_fr is not None and ast_fr is not None:
        carry_ashl = (hl_fr * 24 - ast_fr * settles_day) * 10000
    if carry_ashl is None:
        lines.append(f"e.g. /enter {symbol} buy_aster 1000 {ast_gate}")
        lines.append(f"     /close {symbol} {hl_gate}")
        lines.append("(no funding data — carry direction unknown)")
    elif carry_ashl >= 0:
        # Hold L-AST/S-HL: enter on the buy-AST leg, exit on the buy-HL leg.
        lines.append(f"e.g. /enter {symbol} buy_aster 1000 {ast_gate}")
        lines.append(f"     /close {symbol} {hl_gate}")
        lines.append(f"→ holds L-AST/S-HL earning {carry_ashl:+.1f}bp/day while you wait")
        if carry_ashl > 0:
            lines.append(f"  (the other direction PAYS {-carry_ashl:+.1f}bp/day — avoid)")
    else:
        # Hold L-HL/S-AST: enter on the buy-HL leg, exit on the buy-AST leg.
        lines.append(f"e.g. /enter {symbol} buy_hl 1000 {hl_gate}")
        lines.append(f"     /close {symbol} {ast_gate}")
        lines.append(f"→ holds L-HL/S-AST earning {-carry_ashl:+.1f}bp/day while you wait")
        lines.append(f"  (the other direction PAYS {carry_ashl:+.1f}bp/day — avoid)")
    print("\n".join(lines))


if __name__ == "__main__":
    asyncio.run(main())

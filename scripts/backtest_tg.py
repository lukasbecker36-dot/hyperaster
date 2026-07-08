#!/usr/bin/env python3
"""
Telegram-friendly convergence backtest.

Fetches 1m candles for the overlapping equity-perp universe (or one symbol) and
simulates the LIVE strategy on close-to-close book spreads: rolling-median
baseline, excess-vs-baseline entry with a persistence streak, and the
target / stop-loss / converge / timeout exits — using the same config params
the live bot uses. Prints a compact per-symbol table + summary for /backtest.

Caveats (kept in the output): uses candle CLOSES as mids, so it does NOT model
the bid-ask you cross on each leg (real fills are worse by ~the Aster spread),
excludes funding, and does not apply the oracle correction. It's a directional
read on which names mean-revert enough to matter, not a P&L guarantee.

Usage:
    python scripts/backtest_tg.py [--hours 48] [--symbol SNDK] [--top 20]
"""

import argparse
import asyncio
import statistics
import sys
from collections import deque
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import history
from config import (
    OUTPUT_DIR, ENTRY_THRESHOLD_BPS_BY_SYMBOL, ENTRY_THRESHOLD_BPS,
    BLOCKED_SYMBOLS, NON_EQUITY_SYMBOLS, NOTIONAL_PER_LEG, CARRY_ROUND_TRIP_FEE,
    ROUND_TRIP_FEE, BASELINE_WINDOW_MINUTES, BASELINE_MIN_SAMPLES, ENTRY_CONFIRM_TICKS,
    EXIT_TARGET_NET_USD, EXIT_TARGET_NET_USD_BY_SYMBOL, BASIS_ADVERSE_STOP_USD,
    MAX_HOLD_HOURS, aster_symbol_for,
)

MIN_MS = 60_000
ASTER_DEPTH_URL = "https://fapi.asterdex.com/fapi/v1/depth"
HL_INFO_URL = "https://api.hyperliquid.xyz/info"
_CANON_TO_ASTER = {"SMSN": "SAMSUNG", "SKHX": "SKHYNIX"}


def _load_universe() -> list[str]:
    import csv
    path = Path(OUTPUT_DIR) / "overlap_symbols.csv"
    if not path.exists():
        return []
    exclude = BLOCKED_SYMBOLS | NON_EQUITY_SYMBOLS
    with open(path) as fh:
        return [r["coin"] for r in csv.DictReader(fh) if r["coin"] not in exclude]


def _backtest_one(spreads: list[tuple[int, float]], symbol: str,
                  cost_bps: float) -> dict | None:
    """spreads: sorted [(ts_ms, spread_bps)]. cost_bps: round-trip fee + the
    bid-ask you cross (both legs for taker, Aster only for maker). Returns
    per-symbol stats, net of that cost."""
    if len(spreads) < BASELINE_MIN_SAMPLES + 5:
        return None
    thr = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(symbol, ENTRY_THRESHOLD_BPS)
    target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(symbol, EXIT_TARGET_NET_USD)
    win_ms = BASELINE_WINDOW_MINUTES * MIN_MS

    hist = deque()  # (ts, spread) within the baseline window
    streak_dir, streak = "", 0
    pos = None      # dict: dir_sign, entry_spread, entry_base, entry_ts
    trades = []     # (net_bps, hold_h, reason)

    for ts, spread in spreads:
        hist.append((ts, spread))
        while hist and hist[0][0] < ts - win_ms:
            hist.popleft()
        recent = [s for _, s in hist]
        if len(recent) < BASELINE_MIN_SAMPLES:
            continue
        base = statistics.median(recent)

        if pos is not None:
            # Mark to market on close-to-close spread convergence.
            gross_bps = pos["sign"] * (pos["entry_spread"] - spread)
            net_bps = gross_bps - cost_bps
            net_usd = net_bps / 10_000 * NOTIONAL_PER_LEG
            hold_h = (ts - pos["entry_ts"]) / 3_600_000
            own_excess = pos["sign"] * (spread - pos["entry_base"])
            reason = None
            if BASIS_ADVERSE_STOP_USD > 0 and net_usd <= -BASIS_ADVERSE_STOP_USD:
                reason = "stop_loss"
            elif net_usd >= target:
                reason = "target"
            elif own_excess <= 0 and hold_h >= 0.5 and net_usd >= 0:
                reason = "converge"
            elif hold_h >= MAX_HOLD_HOURS:
                reason = "timeout"
            if reason:
                trades.append((net_bps, hold_h, reason))
                pos = None
            continue

        # Flat — look for an entry.
        aster_excess = spread - base          # long_hl_short_aster
        hl_excess = -(spread - base)          # long_aster_short_hl
        if aster_excess >= hl_excess:
            excess, sign, d = aster_excess, 1.0, "L-HL/S-AST"
        else:
            excess, sign, d = hl_excess, -1.0, "L-AST/S-HL"
        if excess >= thr:
            streak = streak + 1 if streak_dir == d else 1
            streak_dir = d
            if streak >= ENTRY_CONFIRM_TICKS:
                pos = {"sign": sign, "entry_spread": spread, "entry_base": base, "entry_ts": ts}
                streak, streak_dir = 0, ""
        else:
            streak, streak_dir = 0, ""

    if not trades:
        return None
    nets = [t[0] for t in trades]
    total_usd = sum(n / 10_000 * NOTIONAL_PER_LEG for n in nets)
    wins = sum(1 for n in nets if n > 0)
    reasons = {}
    for _, _, r in trades:
        reasons[r] = reasons.get(r, 0) + 1
    return {
        "symbol": symbol, "n": len(trades), "win_pct": 100 * wins / len(trades),
        "total_usd": total_usd, "avg_bps": statistics.mean(nets),
        "avg_hold": statistics.mean(t[1] for t in trades), "reasons": reasons,
    }


def _pick_interval(hours: int) -> str:
    """Choose the finest candle interval whose count fits one API call (~1500
    cap on Aster klines), so both venues return the SAME aligned recent window.
    A 168h @ 1m request would blow the cap and misalign → no overlapping data."""
    mins = hours * 60
    if mins <= 1450:            # ~24h at 1m (Aster kline cap ~1500)
        return "1m"
    if mins // 5 <= 1450:       # ~120h at 5m
        return "5m"
    if mins // 15 <= 1450:      # ~360h at 15m
        return "15m"
    return "1h"


async def _current_cross_bps(session, symbol, mode) -> float:
    """Estimate the round-trip bid-ask you cross, from the CURRENT books.
    taker: both legs cross (hl_spread + aster_spread). maker: only the Aster
    taker leg (aster_spread). Approximation — current spreads stand in for
    historical. Returns 0 on failure (falls back to fee-only)."""
    ast_sym = _CANON_TO_ASTER.get(symbol, symbol) + "USDT"
    try:
        async def _hl():
            async with session.post(HL_INFO_URL,
                                    json={"type": "l2Book", "coin": f"xyz:{symbol}"}) as r:
                lv = (await r.json()).get("levels") or [[], []]
                return float(lv[0][0]["px"]), float(lv[1][0]["px"])  # bid, ask

        async def _ast():
            async with session.get(ASTER_DEPTH_URL,
                                   params={"symbol": ast_sym, "limit": 5}) as r:
                d = await r.json()
                return float(d["bids"][0][0]), float(d["asks"][0][0])

        (hb, ha), (ab, aa) = await asyncio.gather(_hl(), _ast())
        mid = (hb + ha + ab + aa) / 4
        if mid <= 0:
            return 0.0
        hl_sp = (ha - hb) / mid * 10_000
        ast_sp = (aa - ab) / mid * 10_000
        return (hl_sp + ast_sp) if mode == "taker" else ast_sp
    except Exception:
        return 0.0


async def _fetch_spreads(session, symbol, start_ms, end_ms, interval) -> list[tuple[int, float]]:
    hl_coin = f"xyz:{symbol}"
    ast_sym = aster_symbol_for(symbol)
    hl_d, ast_d = await asyncio.gather(
        history.hl_candles(session, hl_coin, start_ms, end_ms, interval=interval),
        history.aster_candles(session, ast_sym, start_ms, end_ms, interval=interval),
    )
    if not hl_d or not ast_d:
        return []
    out = []
    for t in sorted(set(hl_d) & set(ast_d)):
        hl_px, ast_px = float(hl_d[t]), float(ast_d[t])
        mid = (hl_px + ast_px) / 2
        if mid > 0:
            out.append((t, (ast_px - hl_px) / mid * 10_000))
    return out


async def run(hours: int, symbol: str | None, top: int, cost_mode: str) -> str:
    syms = [symbol.upper()] if symbol else _load_universe()
    if not syms:
        return "📊 Backtest: no universe (run fetch_data.py) or symbol given."
    end_ms = int(__import__("time").time() * 1000)
    start_ms = end_ms - hours * 3_600_000
    interval = _pick_interval(hours)
    fee_bps = (ROUND_TRIP_FEE if cost_mode == "taker" else CARRY_ROUND_TRIP_FEE) * 10_000
    timeout = aiohttp.ClientTimeout(total=120)
    results = []
    with_data = 0
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # small concurrency so we don't hammer the venues
        sem = asyncio.Semaphore(6)

        async def one(s):
            async with sem:
                try:
                    sp, cross = await asyncio.gather(
                        _fetch_spreads(session, s, start_ms, end_ms, interval),
                        _current_cross_bps(session, s, cost_mode),
                    )
                except Exception:
                    return None, 0
                # cost = fee + the bid-ask crossed. 'none' mode leaves cross=0.
                cost = fee_bps + (cross if cost_mode != "none" else 0.0)
                return _backtest_one(sp, s, cost), len(sp)

        for r, nsp in await asyncio.gather(*[one(s) for s in syms]):
            if nsp > 0:
                with_data += 1
            if r:
                results.append(r)
    if not results:
        if with_data == 0:
            return (f"📊 Backtest ({hours}h, {interval}): no candle data returned for "
                    f"{len(syms)} name(s). Try a shorter window.")
        return (f"📊 Backtest ({hours}h, {interval}): {with_data}/{len(syms)} names had "
                f"data but none produced a qualifying trade (spread never cleared threshold).")

    results.sort(key=lambda r: r["total_usd"], reverse=True)
    shown = results[:top]
    cost_desc = {"taker": "taker-taker (cross both)",
                 "maker": "maker-first (cross Aster)",
                 "none": "fee only (optimistic)"}[cost_mode]
    lines = [
        f"📊 Backtest {hours}h · {interval} · ${NOTIONAL_PER_LEG:.0f}/leg",
        f"cost: {cost_desc}",
        f"{'SYM':<7}{'n':>3} {'win%':>4} {'net$':>7} {'bp/t':>6}",
    ]
    for r in shown:
        lines.append(
            f"{r['symbol']:<7}{r['n']:>3} {r['win_pct']:>3.0f}% "
            f"{r['total_usd']:>+7.2f} {r['avg_bps']:>+6.1f}"
        )
    tot = sum(r["total_usd"] for r in results)
    n = sum(r["n"] for r in results)
    losers = [r["symbol"] for r in results if r["total_usd"] < 0]
    lines += [
        "─" * 28,
        f"TOTAL {n} trades  net ${tot:+.2f}  ({len(results)} names)",
    ]
    if losers:
        lines.append("negative: " + ", ".join(losers[:12]))
    lines.append("⚠️ still optimistic: assumes maker fills, no slippage, "
                 "current spreads applied to history. Live is the real test.")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--cost", choices=("taker", "maker", "none"), default="taker",
                    help="round-trip cost model (default taker = conservative)")
    a = ap.parse_args()
    print(asyncio.run(run(max(1, min(a.hours, 336)), a.symbol, a.top, a.cost)))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Funding-carry backtest over the past week (hourly candles + realised funding).

For every overlapping equity perp it simulates the delta-neutral funding-carry
trade: enter at the start of the window in the funding-favourable direction
(long the venue that PAYS you, short the venue that CHARGES you), hold to the
end, and decompose the P&L into:

  - funding  : realised net carry over the hold (HL hourly + Aster 8h funding,
               summed in the chosen direction)
  - basis    : mark-to-market of the basis move while held — POSITIVE means the
               basis converged in your favour (you earned it), NEGATIVE means it
               moved against you (a cost). This is the "minimise basis cost (or
               earn it)" term.
  - costs    : round-trip fees + crossing the HL touch once (maker/taker entry)

  total = funding + basis - costs

Ranked by total net P&L on the configured notional. Funding APR is annualised
from the realised net carry. "stable" = how many Aster settlements kept the
window's net-funding sign (a flip-prone rate is a trap).

Basis uses mid/close prices (no historical order book exists); crossing cost is
estimated from the CURRENT HL touch spread. HL (USDC) is normalised to USDT with
the current USDC/USDT rate held constant across the window.

Run on the server (needs the venv's aiohttp + live API egress):
    .venv/bin/python scripts/funding_backtest.py
    .venv/bin/python scripts/funding_backtest.py --days 7 --notional 1000 --top 25
"""

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import aster, hyperliquid as hl
from src.history import (
    hl_candles, aster_candles, hl_funding_history, aster_funding_history, now_ms,
)
from config import (
    BLOCKED_SYMBOLS, NON_EQUITY_SYMBOLS, HL_TAKER_FEE, ASTER_MAKER_FEE,
)

BPS = Decimal("10000")
HOUR_MS = 3_600_000
ZERO = Decimal("0")
ROUND_TRIP_FEE_BPS = (Decimal(str(HL_TAKER_FEE)) * 2 + Decimal(str(ASTER_MAKER_FEE)) * 2) * BPS


async def fetch_usdc_usdt_rate(session) -> Decimal:
    try:
        async with session.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "USDCUSDT"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            return Decimal((await r.json())["price"])
    except Exception:
        return Decimal("1")


def _sum_window(hist: dict, start_ms: int, end_ms: int) -> tuple[Decimal, list]:
    """Sum realised funding rates whose timestamp falls in [start, end]."""
    vals = [v for t, v in hist.items() if start_ms <= t <= end_ms]
    return (sum(vals) if vals else ZERO), vals


async def fetch_one(session, sym, hl_coin, aster_sym, start_ms, end_ms):
    hl_c, ast_c, hl_f, ast_f = await asyncio.gather(
        hl_candles(session, hl_coin, start_ms, end_ms, interval="1h"),
        aster_candles(session, aster_sym, start_ms, end_ms, interval="1h"),
        hl_funding_history(session, hl_coin, start_ms),
        aster_funding_history(session, aster_sym, start_ms),
    )
    return sym, hl_c, ast_c, hl_f, ast_f


async def run(days: int, notional_usd: float, top: int) -> str:
    notional = Decimal(str(notional_usd))
    end_ms = now_ms()
    start_ms = end_ms - days * 24 * HOUR_MS

    connector = aiohttp.TCPConnector(limit=30)
    timeout = aiohttp.ClientTimeout(total=40)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        hl_perps, aster_perps, aster_tickers, usdc_rate = await asyncio.gather(
            hl.get_all_equity_perps(session),
            aster.get_all_perps(session),
            aster.get_all_book_tickers(session),
            fetch_usdc_usdt_rate(session),
        )
        universe = sorted((set(hl_perps) & set(aster_perps) & set(aster_tickers)) - NON_EQUITY_SYMBOLS)
        if not universe:
            return "No overlapping equity symbols found."

        hl_coins = [hl_perps[c].venue_symbol for c in universe]
        hl_tickers = await hl.get_book_tickers(session, hl_coins)

        results = await asyncio.gather(*[
            fetch_one(session, c, hl_perps[c].venue_symbol, aster_perps[c].venue_symbol,
                      start_ms, end_ms)
            for c in universe
        ])

    rows = []
    for sym, hl_c, ast_c, hl_f, ast_f in results:
        common = sorted(set(hl_c) & set(ast_c))
        if len(common) < 12:
            continue
        t0, t1 = common[0], common[-1]
        hl_entry = hl_c[t0] * usdc_rate
        hl_exit = hl_c[t1] * usdc_rate
        ast_entry = ast_c[t0]
        ast_exit = ast_c[t1]
        ref_entry = (hl_entry + ast_entry) / 2
        ref_exit = (hl_exit + ast_exit) / 2
        if ref_entry <= 0 or ref_exit <= 0:
            continue
        entry_basis = (ast_entry - hl_entry) / ref_entry * BPS   # + = Aster richer
        exit_basis = (ast_exit - hl_exit) / ref_exit * BPS

        hl_sum, _ = _sum_window(hl_f, t0, t1)                    # hourly rates summed
        ast_sum, ast_vals = _sum_window(ast_f, t0, t1)          # 8h rates summed

        # Net carry FRACTION for each direction over the hold:
        #   long_hl_short_aster: pay HL funding, receive Aster funding
        #   long_aster_short_hl: receive HL funding, pay Aster funding
        net_lhsa = (-hl_sum) + ast_sum
        if net_lhsa >= ZERO:
            direction, net_fraction = "long_hl_short_aster", net_lhsa
        else:
            direction, net_fraction = "long_aster_short_hl", -net_lhsa

        funding_pnl = net_fraction * notional
        # Basis P&L: convergence captured while held (mark-to-market).
        if direction == "long_hl_short_aster":
            basis_pnl_bps = entry_basis - exit_basis
        else:
            basis_pnl_bps = exit_basis - entry_basis
        basis_pnl = basis_pnl_bps / BPS * notional

        hl_bk = hl_tickers.get(sym)
        crossing_bps = hl_bk.spread_bps if hl_bk else Decimal("10")
        costs = (ROUND_TRIP_FEE_BPS + crossing_bps) / BPS * notional
        total = funding_pnl + basis_pnl - costs

        hours = Decimal(len(common) - 1) or Decimal(1)
        apr = net_fraction / hours * Decimal("24") * Decimal("365") * Decimal("100")

        if ast_vals:
            avg_pos = ast_sum >= ZERO
            same = sum(1 for v in ast_vals if (v >= ZERO) == avg_pos)
            stable = f"{same}/{len(ast_vals)}"
        else:
            stable = "n/a"

        # entry basis in the chosen direction's favour (>0 = entered with a credit)
        entry_fav = entry_basis if direction == "long_hl_short_aster" else -entry_basis

        rows.append({
            "sym": sym, "dir": direction, "total": total,
            "funding": funding_pnl, "basis": basis_pnl, "costs": costs,
            "apr": apr, "entry_fav": entry_fav, "basis_move": basis_pnl_bps,
            "stable": stable, "blocked": sym in BLOCKED_SYMBOLS,
            "hours": int(hours),
        })

    if not rows:
        return "No symbols had enough candle history."

    rows.sort(key=lambda r: r["total"], reverse=True)
    shown = rows[:top]

    def d(x):
        return float(x)

    out = [
        f"💰 Funding-carry backtest — last {days}d ({rows[0]['hours']}h), ${notional_usd:.0f}/leg",
        f"USDC/USDT held at current rate. Ranked by total net (funding + basis - costs).",
        "",
        f"{'Sym':<8} {'Dir':<11} {'total$':>8} {'fund$':>8} {'basis$':>8} "
        f"{'APR%':>6} {'entryBp':>8} {'bMoveBp':>8} {'stbl':>5}",
        "-" * 80,
    ]
    for r in shown:
        dir_lbl = "L-HL/S-AST" if r["dir"] == "long_hl_short_aster" else "L-AST/S-HL"
        flag = "*" if r["blocked"] else " "
        out.append(
            f"{r['sym']:<7}{flag} {dir_lbl:<11} {d(r['total']):>8.2f} {d(r['funding']):>8.2f} "
            f"{d(r['basis']):>8.2f} {d(r['apr']):>6.0f} {d(r['entry_fav']):>+8.0f} "
            f"{d(r['basis_move']):>+8.0f} {r['stable']:>5}"
        )

    pos = [r for r in rows if r["total"] > 0]
    fund_pos = [r for r in rows if r["funding"] > 0]
    out += [
        "-" * 80,
        f"{len(pos)}/{len(rows)} names net-positive over the week; "
        f"{len(fund_pos)} had a funding tailwind.",
        "* = currently in BLOCKED_SYMBOLS (blocked for the convergence strategy, "
        "but funding carry is a different trade).",
        "entryBp = basis in the chosen direction's favour at entry (>0 = entered with a credit).",
        "bMoveBp = basis P&L over the hold (>0 = basis converged in your favour).",
        "stbl = Aster settlements matching the window's net-funding sign.",
    ]
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description="Funding-carry backtest (hourly, past week)")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--notional", type=float, default=1000)
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    print(asyncio.run(run(args.days, args.notional, args.top)))


if __name__ == "__main__":
    main()

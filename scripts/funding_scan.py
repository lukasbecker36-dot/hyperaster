#!/usr/bin/env python3
"""
Funding-carry opportunity scanner for the equity-perp universe (HL xyz vs Aster).

A delta-neutral funding-carry trade holds long one venue / short the other and
earns the NET funding differential while waiting. HL funding settles hourly,
Aster every 8h; both are normalised to bps/hour. To smooth out single-print
spikes we rank on the ROLLING 24h AVERAGE of each venue's realised funding, not
the instantaneous rate.

For each name we show:
  - net carry (24h avg, bps/hr) and its annualised APR
  - current (instantaneous) net carry, with a flip flag if it disagrees with the avg
  - basis in the carry-implied direction (a tailwind credit or a headwind cost)
  - bid/offer spread on each venue (HL is the taker leg, Aster the maker leg)
  - estimated hours-to-profit = (fees + crossing - basis_credit) / net_carry
  - funding stability (how many of the last Aster settlements kept the avg's sign)

Only names whose 24h-avg net carry is a TAILWIND are shown — a funding headwind
is not a funding opportunity. Ranked by least hours-to-profit.

Run on the server (needs the venv's aiohttp + live API egress):
    .venv/bin/python scripts/funding_scan.py
    .venv/bin/python scripts/funding_scan.py --top 15 --hours 24
"""

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import aster, hyperliquid as hl
from src.history import hl_funding_history, aster_funding_history, now_ms
from config import (
    BLOCKED_SYMBOLS, NON_EQUITY_SYMBOLS,
    HL_MAKER_FEE, ASTER_TAKER_FEE,
)

BPS = Decimal("10000")
HOUR_MS = 3_600_000
ZERO = Decimal("0")

# Carry trades: HL maker on both entry+exit, Aster taker on both.
ROUND_TRIP_FEE_BPS = (Decimal(str(HL_MAKER_FEE)) * 2 + Decimal(str(ASTER_TAKER_FEE)) * 2) * BPS


async def fetch_usdc_usdt_rate(session: aiohttp.ClientSession) -> Decimal:
    try:
        async with session.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "USDCUSDT"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            data = await resp.json()
            return Decimal(data["price"])
    except Exception:
        return Decimal("1")


def _mean(vals: list[Decimal]) -> Decimal:
    return sum(vals) / Decimal(len(vals)) if vals else ZERO


class Opp:
    __slots__ = (
        "sym", "direction", "net_carry_hr", "cur_carry_hr", "apr",
        "basis_credit", "hl_bo", "aster_bo", "hours", "stability", "flip",
    )


def build_opp(
    sym: str, direction: str, net_carry_hr: Decimal, cur_carry_hr: Decimal,
    basis_credit: Decimal, hl_bo: Decimal, aster_bo: Decimal,
    stability: str, hl_coin_taker: bool,
) -> Opp:
    o = Opp()
    o.sym = sym
    o.direction = direction
    o.net_carry_hr = net_carry_hr
    o.cur_carry_hr = cur_carry_hr
    # Annualised: bps/hr -> %/yr.  (bps/100 = %), * 24 * 365 hours.
    o.apr = net_carry_hr / Decimal("100") * Decimal("24") * Decimal("365")
    o.basis_credit = basis_credit
    o.hl_bo = hl_bo
    o.aster_bo = aster_bo
    # Crossing cost: Aster is the taker leg so we cross its spread once per
    # round trip; HL rests as maker (no crossing cost).
    crossing = aster_bo
    hurdle = ROUND_TRIP_FEE_BPS + crossing - basis_credit
    if hurdle <= ZERO:
        o.hours = ZERO  # basis alone already covers costs — carry is pure gravy
    else:
        o.hours = hurdle / net_carry_hr  # net_carry_hr > 0 by construction
    o.stability = stability
    o.flip = (cur_carry_hr < ZERO)  # current carry has flipped against the 24h avg
    return o


async def fetch_funding_hist(session, sym, hl_coin, aster_sym, start_ms):
    hl_hist, ast_hist = await asyncio.gather(
        hl_funding_history(session, hl_coin, start_ms),
        aster_funding_history(session, aster_sym, start_ms),
    )
    return sym, hl_hist, ast_hist


async def scan(top: int, lookback_h: int) -> str:
    connector = aiohttp.TCPConnector(limit=30)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        hl_perps, aster_perps, aster_tickers, aster_funding_now, usdc_rate = await asyncio.gather(
            hl.get_all_equity_perps(session),
            aster.get_all_perps(session),
            aster.get_all_book_tickers(session),
            aster.get_all_funding_rates(session),
            fetch_usdc_usdt_rate(session),
        )

        exclude = BLOCKED_SYMBOLS | NON_EQUITY_SYMBOLS
        universe = sorted(
            (set(hl_perps) & set(aster_perps) & set(aster_tickers)) - exclude
        )
        if not universe:
            return "No tradeable overlapping symbols found."

        hl_coins = [hl_perps[c].venue_symbol for c in universe]
        hl_tickers = await hl.get_book_tickers(session, hl_coins)

        start_ms = now_ms() - (lookback_h + 1) * HOUR_MS
        hist_results = await asyncio.gather(*[
            fetch_funding_hist(
                session, c, hl_perps[c].venue_symbol, aster_perps[c].venue_symbol, start_ms
            )
            for c in universe
        ])
        hist = {sym: (h, a) for sym, h, a in hist_results}

    opps: list[Opp] = []
    for sym in universe:
        hl_book = hl_tickers.get(sym)
        aster_book = aster_tickers.get(sym)
        if not hl_book or not aster_book or hl_book.mid == ZERO or aster_book.mid == ZERO:
            continue

        hl_hist, ast_hist = hist.get(sym, ({}, {}))

        # 24h-avg funding, both normalised to bps/hour.
        hl_rates = list(hl_hist.values())                    # hourly, raw
        ast_rates = list(ast_hist.values())                  # 8h, raw
        cur_hl = hl_perps[sym].funding_rate_hourly or ZERO   # hourly, raw
        cur_ast_8h = aster_funding_now.get(sym, ZERO)        # 8h, raw

        hl_avg_hr = (_mean(hl_rates) if hl_rates else cur_hl) * BPS
        ast_avg_hr = (_mean(ast_rates) if ast_rates else cur_ast_8h) / Decimal("8") * BPS
        cur_hl_hr = cur_hl * BPS
        cur_ast_hr = cur_ast_8h / Decimal("8") * BPS

        # Carry in the long-HL/short-Aster convention; flip sign for the other.
        carry_buy_hl_avg = ast_avg_hr - hl_avg_hr
        carry_buy_hl_cur = cur_ast_hr - cur_hl_hr
        if carry_buy_hl_avg > ZERO:
            direction = "BUY_HL"        # long HL, short Aster
            net_carry = carry_buy_hl_avg
            cur_carry = carry_buy_hl_cur
        elif carry_buy_hl_avg < ZERO:
            direction = "BUY_ASTER"     # long Aster, short HL
            net_carry = -carry_buy_hl_avg
            cur_carry = -carry_buy_hl_cur
        else:
            continue  # no carry edge

        # Executable basis (USDC->USDT normalised). HL-maker / Aster-taker:
        # HL is more liquid so we rest there. Both legs end up on the same
        # side of the book (bid-bid for buy-HL, ask-ask for buy-AST).
        hl_bid_usdt = hl_book.bid * usdc_rate
        hl_ask_usdt = hl_book.ask * usdc_rate
        ref = (hl_book.mid * usdc_rate + aster_book.mid) / 2
        if direction == "BUY_HL":
            # buy HL @ bid (maker), sell Aster @ bid (taker)
            basis_credit = (aster_book.bid - hl_bid_usdt) / ref * BPS
        else:
            # sell HL @ ask (maker), buy Aster @ ask (taker)
            basis_credit = (hl_ask_usdt - aster_book.ask) / ref * BPS

        # Funding stability: how many recent Aster settlements share the avg's sign.
        if ast_rates:
            avg_pos = ast_avg_hr >= ZERO
            same = sum(1 for r in ast_rates if (r >= ZERO) == avg_pos)
            stability = f"{same}/{len(ast_rates)}"
        else:
            stability = "n/a"

        opps.append(build_opp(
            sym, direction, net_carry, cur_carry, basis_credit,
            hl_book.spread_bps, aster_book.spread_bps, stability,
            hl_coin_taker=True,
        ))

    if not opps:
        return "No funding-carry tailwinds right now (all names are headwinds)."

    # Rank by least hours-to-profit, then by richest carry as a tiebreak.
    opps.sort(key=lambda o: (o.hours, -o.net_carry_hr))
    opps = opps[:top]

    def fmt_hours(h: Decimal) -> str:
        return "immed" if h <= ZERO else f"{float(h):.1f}h"

    lines = [
        f"💰 Funding carry — top {len(opps)} (24h-avg net, ranked by hrs-to-profit)",
        "",
    ]
    for i, o in enumerate(opps, 1):
        dir_lbl = "L-HL/S-AST" if o.direction == "BUY_HL" else "L-AST/S-HL"
        flip = "  ⚠cur-flip" if o.flip else ""
        lines.append(
            f"{i}. {o.sym}  {dir_lbl}  net {float(o.net_carry_hr):+.3f}b/h "
            f"({float(o.apr):.0f}% APR)  ~{fmt_hours(o.hours)}"
        )
        lines.append(
            f"    cur {float(o.cur_carry_hr):+.3f}b/h{flip}  "
            f"basis {float(o.basis_credit):+.0f}b  "
            f"b/o HL {float(o.hl_bo):.0f}b/Ast {float(o.aster_bo):.0f}b  "
            f"stable {o.stability}"
        )
    lines.append("")
    lines.append(
        "net = 24h-avg net funding (bps/hr) in the shown direction. "
        "basis>0 = convergence tailwind, <0 = entry cost. "
        "hrs-to-profit = (fees+Ast b/o-basis)/net. "
        "stable = Aster settlements matching the avg sign."
    )
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description="Funding-carry opportunity scanner")
    ap.add_argument("--top", type=int, default=12, help="how many to show (default 12)")
    ap.add_argument("--hours", type=int, default=24, help="rolling funding window in hours (default 24)")
    args = ap.parse_args()
    print(asyncio.run(scan(args.top, args.hours)))


if __name__ == "__main__":
    main()

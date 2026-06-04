#!/usr/bin/env python3
"""
Equity perps arbitrage scanner: Hyperliquid <-> Aster DEX.

Usage:
    pip install aiohttp
    python scripts/analyze_arbs.py

No API keys required — uses public read-only endpoints only.
"""

import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from typing import Optional

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import aster, hyperliquid as hl
from src.fees import (
    ASTER_FEE_BPS,
    HL_MAKER_BPS,
    HL_TAKER_BPS,
    ROUND_TRIP_MAKER_BPS,
    ROUND_TRIP_TAKER_BPS,
)
from src.types import ArbResult

ZERO = Decimal("0")


async def fetch_usdc_usdt_rate(session: aiohttp.ClientSession) -> Decimal:
    """Live USDC/USDT spot rate from Binance. Falls back to 1.0."""
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


def _breakeven(gross_bps: Decimal, cost_bps: Decimal, carry_bps_hr: Decimal) -> Optional[Decimal]:
    if gross_bps >= cost_bps:
        return ZERO
    if carry_bps_hr <= ZERO:
        return None
    return (cost_bps - gross_bps) / carry_bps_hr


def _net_pnl(gross_bps: Decimal, cost_bps: Decimal, carry_bps_hr: Decimal, hours: int) -> Decimal:
    return gross_bps - cost_bps + carry_bps_hr * hours


def _fmt_bps(val: Decimal, width: int = 7) -> str:
    return f"{val:>+{width}.1f}b"


def _fmt_be(val: Optional[Decimal]) -> str:
    if val is None:
        return "  never"
    if val == ZERO:
        return " immed."
    return f"{val:>6.1f}h"


async def main() -> None:
    connector = aiohttp.TCPConnector(limit=30)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        print("Fetching data from Hyperliquid and Aster DEX...")

        # All top-level fetches fire concurrently
        hl_perps, aster_perps, aster_tickers, aster_funding, usdc_rate = await asyncio.gather(
            hl.get_all_equity_perps(session),
            aster.get_all_perps(session),
            aster.get_all_book_tickers(session),
            aster.get_all_funding_rates(session),
            fetch_usdc_usdt_rate(session),
        )

        print(f"  Hyperliquid xyz: equity perps : {len(hl_perps)}")
        print(f"  Aster DEX USDT perps          : {len(aster_perps)}")

        intersection = (
            set(hl_perps)
            & set(aster_perps)
            & set(aster_tickers)
        )

        if not intersection:
            print("\nNo overlapping symbols found. Raw lists:")
            print(f"  HL    : {sorted(hl_perps)[:20]}")
            print(f"  Aster : {sorted(aster_perps)[:20]}")
            return

        print(f"  Intersection                  : {len(intersection)} → {sorted(intersection)}")

        # Fetch HL L2 books for intersecting symbols concurrently
        print(f"\nFetching Hyperliquid orderbooks for {len(intersection)} symbols...")
        hl_coins = [hl_perps[c].venue_symbol for c in intersection]
        hl_tickers = await hl.get_book_tickers(session, hl_coins)
        print(f"  Got books: {len(hl_tickers)} / {len(intersection)}")

    # ── Compute arb metrics ───────────────────────────────────────────────────
    results: list[ArbResult] = []

    for canonical in sorted(intersection):
        if canonical not in hl_tickers:
            continue

        hl_book = hl_tickers[canonical]
        aster_book = aster_tickers[canonical]

        if hl_book.mid == ZERO or aster_book.mid == ZERO:
            continue

        # Normalise HL (USDC) price to USDT for cross-venue comparison
        hl_mid_usdt = hl_book.mid * usdc_rate
        aster_mid = aster_book.mid
        ref = (hl_mid_usdt + aster_mid) / 2

        mid_spread_bps = (aster_mid - hl_mid_usdt) / ref * Decimal("10000")
        abs_spread_bps = abs(mid_spread_bps)
        direction = "BUY_HL" if mid_spread_bps > ZERO else "BUY_ASTER"

        # Bid-offer cost: full spread on each venue × 2 (entry ask + exit bid)
        hl_sp = hl_book.spread_bps
        aster_sp = aster_book.spread_bps
        bo_cost = hl_sp + aster_sp

        # Funding — normalise Aster 8h rate to per-hour
        hl_fund_hr = (hl_perps[canonical].funding_rate_hourly or ZERO) * Decimal("10000")
        aster_fund_8h = aster_funding.get(canonical, ZERO) * Decimal("10000")
        aster_fund_hr = aster_fund_8h / 8

        # Net carry in chosen direction:
        # BUY_HL  = long HL, short Aster → earn aster_fund, pay hl_fund
        # BUY_ASTER = long Aster, short HL → earn hl_fund, pay aster_fund
        if direction == "BUY_HL":
            carry = aster_fund_hr - hl_fund_hr
        else:
            carry = hl_fund_hr - aster_fund_hr

        total_taker = bo_cost + ROUND_TRIP_TAKER_BPS
        total_maker = bo_cost + ROUND_TRIP_MAKER_BPS

        results.append(ArbResult(
            canonical=canonical,
            hl_mid=hl_book.mid,
            aster_mid=aster_book.mid,
            mid_spread_bps=mid_spread_bps,
            abs_spread_bps=abs_spread_bps,
            direction=direction,
            hl_spread_bps=hl_sp,
            aster_spread_bps=aster_sp,
            bo_cost_bps=bo_cost,
            taker_fee_cost_bps=ROUND_TRIP_TAKER_BPS,
            maker_fee_cost_bps=ROUND_TRIP_MAKER_BPS,
            total_taker_cost_bps=total_taker,
            total_maker_cost_bps=total_maker,
            hl_funding_bps_hr=hl_fund_hr,
            aster_funding_bps_hr=aster_fund_hr,
            net_carry_bps_hr=carry,
            net_pnl_taker_1h=_net_pnl(abs_spread_bps, total_taker, carry, 1),
            net_pnl_taker_8h=_net_pnl(abs_spread_bps, total_taker, carry, 8),
            net_pnl_taker_24h=_net_pnl(abs_spread_bps, total_taker, carry, 24),
            net_pnl_taker_1wk=_net_pnl(abs_spread_bps, total_taker, carry, 168),
            net_pnl_maker_1h=_net_pnl(abs_spread_bps, total_maker, carry, 1),
            net_pnl_maker_8h=_net_pnl(abs_spread_bps, total_maker, carry, 8),
            breakeven_taker_hours=_breakeven(abs_spread_bps, total_taker, carry),
            breakeven_maker_hours=_breakeven(abs_spread_bps, total_maker, carry),
        ))

    if not results:
        print("\nNo arb results computed.")
        return

    results.sort(key=lambda r: r.abs_spread_bps, reverse=True)

    # ── Output ────────────────────────────────────────────────────────────────
    print(f"""
{'='*110}
 EQUITY PERPS ARB SNAPSHOT  |  Hyperliquid (USDC) <-> Aster DEX (USDT)
 USDC/USDT: {usdc_rate:.6f}  |  Fees: HL taker {HL_TAKER_BPS}bps / maker {HL_MAKER_BPS}bps per trade, Aster {ASTER_FEE_BPS}bps
 Round-trip fee: taker {ROUND_TRIP_TAKER_BPS}bps  /  maker {ROUND_TRIP_MAKER_BPS}bps
 All values in basis points (bps). 1 bps = 0.01%.
{'='*110}""")

    hdr = (
        f"{'Sym':<6}  {'HL Mid':>9}  {'Ast Mid':>9}  {'Dir':<10}  "
        f"{'Spread':>7}  {'HL B/O':>7}  {'Ast B/O':>7}  {'B/O+Fee':>8}  "
        f"{'Carry/h':>8}  {'8h(tkr)':>8}  {'8h(mkr)':>8}  "
        f"{'BE tkr':>8}  {'BE mkr':>8}"
    )
    print(hdr)
    print("-" * len(hdr))

    for r in results:
        flag = ""
        if r.net_pnl_taker_8h > ZERO:
            flag = " ◄◄ TAKER"
        elif r.net_pnl_maker_8h > ZERO:
            flag = " ◄ maker"
        elif r.breakeven_taker_hours is not None:
            flag = " (carry)"

        print(
            f"{r.canonical:<6}  "
            f"{r.hl_mid:>9.2f}  "
            f"{r.aster_mid:>9.2f}  "
            f"{r.direction:<10}  "
            f"{r.abs_spread_bps:>6.1f}b  "
            f"{r.hl_spread_bps:>6.1f}b  "
            f"{r.aster_spread_bps:>6.1f}b  "
            f"{r.total_taker_cost_bps:>7.1f}b  "
            f"{r.net_carry_bps_hr:>+7.3f}b  "
            f"{_fmt_bps(r.net_pnl_taker_8h)}  "
            f"{_fmt_bps(r.net_pnl_maker_8h)}  "
            f"{_fmt_be(r.breakeven_taker_hours):>8}  "
            f"{_fmt_be(r.breakeven_maker_hours):>8}"
            f"{flag}"
        )

    print()

    # ── Summary ───────────────────────────────────────────────────────────────
    taker_viable = [r for r in results if r.net_pnl_taker_8h > ZERO]
    maker_viable = [r for r in results if r.net_pnl_maker_8h > ZERO]
    carry_viable = [r for r in results if r.breakeven_taker_hours is not None and r.breakeven_taker_hours > ZERO]

    print(f"SUMMARY ({len(results)} symbols analysed)")
    print(f"  Profitable within 8h — taker entry: {len(taker_viable)}")
    print(f"  Profitable within 8h — maker entry: {len(maker_viable)}")
    print(f"  Profitable with carry (taker entry, any hold): {len(carry_viable)}")

    if taker_viable or maker_viable:
        print("\n  TOP OPPORTUNITIES:")
        shown: set[str] = set()
        for r in sorted(taker_viable + maker_viable, key=lambda x: x.net_pnl_maker_8h, reverse=True):
            if r.canonical in shown:
                continue
            shown.add(r.canonical)
            print(
                f"    {r.canonical}: {r.direction}  spread {r.abs_spread_bps:.1f}bps  "
                f"carry {r.net_carry_bps_hr:+.3f}bps/h  "
                f"8h taker {r.net_pnl_taker_8h:+.1f}bps  "
                f"8h maker {r.net_pnl_maker_8h:+.1f}bps"
            )
    else:
        print("\n  No immediately profitable arbs found at this snapshot.")
        print("  Consider: (1) re-run at different times (US market hours may differ)")
        print("            (2) watch for temporary dislocations")

    print(f"""
NOTES:
  • HL prices are USDC-margined, Aster is USDT-margined. Basis adjusted above.
  • HL funding settles hourly; Aster every 8h. Both normalised to bps/hour here.
  • Aster 0% fee is a Dec-2025 promotion — verify before trading.
  • Bid-offer costs are from live book snapshot; may widen at entry.
  • These figures assume instant convergence at exit. Real hold time varies.
  • This is a single point-in-time snapshot. Run repeatedly to see distribution.
""")


if __name__ == "__main__":
    asyncio.run(main())

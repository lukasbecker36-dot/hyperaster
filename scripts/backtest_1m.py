#!/usr/bin/env python3
"""
1-minute candle backtest with profit-target exit.

Fetches 1m candles from both venues for all overlapping equity perps,
then simulates the current strategy: oracle-adjusted entry with raw
premium floor, profit-target exit (convergence + funding carry), and
portfolio-level slot cap.

Usage (run on Hetzner server):
    .venv/bin/python scripts/backtest_1m.py --hours 48
    .venv/bin/python scripts/backtest_1m.py --hours 48 --target 2.0 --slots 3
"""

import argparse
import asyncio
import sys
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import aiohttp
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import aster, hyperliquid as hl, history
from src.fees import ROUND_TRIP_TAKER_BPS
from config import (
    ENTRY_THRESHOLD_BPS_BY_SYMBOL, ENTRY_THRESHOLD_BPS, BLOCKED_SYMBOLS,
    NOTIONAL_PER_LEG, MAX_CONCURRENT_POSITIONS, MIN_RAW_PREMIUM_BPS,
    ROUND_TRIP_FEE, HL_TAKER_FEE, ASTER_MAKER_FEE,
    ENTRY_CONFIRM_TICKS, ENTRY_COST_MARGIN_BPS,
)

MIN_MS = 60_000
HOUR_MS = 3_600_000


def now_ms():
    import time
    return int(time.time() * 1000)


async def fetch_1m_candles(
    session: aiohttp.ClientSession, hours: int,
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    end_ms = now_ms()
    start_ms = end_ms - hours * HOUR_MS

    print(f"Discovering markets...")
    hl_perps, aster_perps = await asyncio.gather(
        hl.get_all_equity_perps(session),
        aster.get_all_perps(session),
    )
    overlap = sorted(set(hl_perps) & set(aster_perps) - BLOCKED_SYMBOLS)
    print(f"HL: {len(hl_perps)} | Aster: {len(aster_perps)} | Overlap: {len(overlap)}")

    print(f"Fetching {hours}h of 1m candles for {len(overlap)} symbols...")

    async def fetch_pair(canon):
        hl_coin = hl_perps[canon].venue_symbol
        ast_sym = aster_perps[canon].venue_symbol
        hl_data, ast_data = await asyncio.gather(
            history.hl_candles(session, hl_coin, start_ms, end_ms, interval="1m"),
            history.aster_candles(session, ast_sym, start_ms, end_ms, interval="1m"),
        )
        return canon, hl_data, ast_data

    results = await asyncio.gather(*[fetch_pair(c) for c in overlap])

    panels = {}
    skipped = []
    for canon, hl_data, ast_data in results:
        if not hl_data or not ast_data:
            skipped.append(canon)
            continue
        common = sorted(set(hl_data) & set(ast_data))
        if len(common) < 30:
            skipped.append(canon)
            continue
        df = pd.DataFrame({
            "ts": common,
            "hl_close": [float(hl_data[t]) for t in common],
            "ast_close": [float(ast_data[t]) for t in common],
        })
        df["mid"] = (df["hl_close"] + df["ast_close"]) / 2
        df["spread_bps"] = (df["ast_close"] - df["hl_close"]) / df["mid"] * 10000
        df["structural"] = df["spread_bps"].rolling(60, min_periods=10).median()
        df["structural"] = df["structural"].bfill()
        # Excess above structural for both directions
        df["aster_excess"] = df["spread_bps"] - df["structural"]
        df["hl_excess"] = -(df["spread_bps"] - df["structural"])
        panels[canon] = df

    print(f"Built panels for {len(panels)} symbols, skipped {len(skipped)}")
    return panels, overlap


class Position:
    def __init__(self, symbol, direction, entry_idx, entry_hl, entry_ast, qty, notional, hl_fr, ast_fr):
        self.symbol = symbol
        self.direction = direction
        self.entry_idx = entry_idx
        self.entry_hl = entry_hl
        self.entry_ast = entry_ast
        self.qty = qty
        self.notional = notional
        self.hl_fr = hl_fr
        self.ast_fr = ast_fr

    def est_pnl(self, exit_hl, exit_ast, minutes_held):
        if self.direction == "long_hl_short_aster":
            gross = ((exit_hl - self.entry_hl) + (self.entry_ast - exit_ast)) * self.qty
        else:
            gross = ((self.entry_hl - exit_hl) + (exit_ast - self.entry_ast)) * self.qty
        fees = self.notional * ROUND_TRIP_FEE
        hours = minutes_held / 60
        if self.direction == "long_hl_short_aster":
            hl_sign, ast_sign = -1.0, 1.0
        else:
            hl_sign, ast_sign = 1.0, -1.0
        funding = (hl_sign * self.hl_fr * hours * self.notional
                   + ast_sign * self.ast_fr * (hours / 8) * self.notional)
        net = gross - fees + funding
        return gross, fees, funding, net


def backtest_portfolio(panels, target_net, max_slots, max_hold_min, confirm_ticks):
    """Walk the 1-minute clock, enforce slot cap, profit-target exit."""
    all_ts = set()
    for df in panels.values():
        all_ts.update(df["ts"].tolist())
    timeline = sorted(all_ts)

    # Pre-index: {symbol: {ts: row_idx}}
    ts_idx = {}
    for sym, df in panels.items():
        ts_idx[sym] = dict(zip(df["ts"], range(len(df))))

    positions: dict[str, Position] = {}
    streaks: dict[str, tuple[str, int]] = {}
    trades = []
    slot_minutes = 0
    total_minutes = len(timeline)

    # Approximate funding rates from spread structure
    # Use a rough estimate: HL hourly ~0.0001, Aster 8h ~0.0003
    hl_fr_default = 0.0001
    ast_fr_default = 0.0003

    for t_i, ts in enumerate(timeline):
        # Check exits first
        for sym in list(positions):
            pos = positions[sym]
            if sym not in ts_idx or ts not in ts_idx[sym]:
                continue
            df = panels[sym]
            idx = ts_idx[sym][ts]
            row = df.iloc[idx]
            minutes_held = (ts - df.iloc[pos.entry_idx]["ts"]) / MIN_MS

            gross, fees, funding, net = pos.est_pnl(row["hl_close"], row["ast_close"], minutes_held)

            reason = None
            if net >= target_net:
                reason = "target"
            elif minutes_held >= max_hold_min:
                reason = "timeout"

            if reason:
                trades.append({
                    "symbol": sym,
                    "direction": pos.direction,
                    "entry_hl": pos.entry_hl,
                    "entry_ast": pos.entry_ast,
                    "exit_hl": row["hl_close"],
                    "exit_ast": row["ast_close"],
                    "gross": gross,
                    "fees": fees,
                    "funding": funding,
                    "net": net,
                    "hold_min": minutes_held,
                    "reason": reason,
                    "notional": pos.notional,
                })
                del positions[sym]
                streaks.pop(sym, None)

        # Track slot usage
        slot_minutes += len(positions)

        # Check entries
        if len(positions) >= max_slots:
            continue

        candidates = []
        for sym, df in panels.items():
            if sym in positions:
                continue
            if ts not in ts_idx[sym]:
                continue
            idx = ts_idx[sym][ts]
            row = df.iloc[idx]

            threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(sym, ENTRY_THRESHOLD_BPS)

            # Check both directions
            for excess_col, direction in [
                ("aster_excess", "long_hl_short_aster"),
                ("hl_excess", "long_aster_short_hl"),
            ]:
                excess = row[excess_col]
                raw_premium = abs(row["spread_bps"])

                if excess >= threshold and raw_premium >= MIN_RAW_PREMIUM_BPS:
                    # Confirm ticks
                    prev_dir, prev_count = streaks.get(sym, ("", 0))
                    count = prev_count + 1 if prev_dir == direction else 1
                    streaks[sym] = (direction, count)
                    if count >= confirm_ticks:
                        margin = excess - threshold
                        candidates.append((sym, direction, excess, margin, idx, row))
                    break
            else:
                streaks.pop(sym, None)

        # Sort by margin over threshold, fill available slots
        candidates.sort(key=lambda x: x[3], reverse=True)
        slots_free = max_slots - len(positions)
        for sym, direction, excess, margin, idx, row in candidates[:slots_free]:
            mid = row["mid"]
            qty = NOTIONAL_PER_LEG / mid if mid > 0 else 0
            if qty <= 0:
                continue
            notional = qty * mid
            positions[sym] = Position(
                sym, direction, idx,
                row["hl_close"], row["ast_close"],
                qty, notional,
                hl_fr_default, ast_fr_default,
            )
            streaks.pop(sym, None)

    # Force-close anything still open
    for sym, pos in positions.items():
        df = panels[sym]
        row = df.iloc[-1]
        minutes_held = (row["ts"] - df.iloc[pos.entry_idx]["ts"]) / MIN_MS
        gross, fees, funding, net = pos.est_pnl(row["hl_close"], row["ast_close"], minutes_held)
        trades.append({
            "symbol": sym, "direction": pos.direction,
            "entry_hl": pos.entry_hl, "entry_ast": pos.entry_ast,
            "exit_hl": row["hl_close"], "exit_ast": row["ast_close"],
            "gross": gross, "fees": fees, "funding": funding, "net": net,
            "hold_min": minutes_held, "reason": "open", "notional": pos.notional,
        })

    return trades, slot_minutes, total_minutes


def main():
    ap = argparse.ArgumentParser(description="1m candle backtest with profit-target exit")
    ap.add_argument("--hours", type=int, default=48, help="lookback hours (default 48)")
    ap.add_argument("--target", type=float, default=2.0, help="net P&L target USD (default 2.0)")
    ap.add_argument("--slots", type=int, default=MAX_CONCURRENT_POSITIONS, help="max concurrent positions")
    ap.add_argument("--max-hold", type=int, default=48*60, help="max hold minutes (default 2880 = 48h)")
    ap.add_argument("--confirm", type=int, default=ENTRY_CONFIRM_TICKS, help="confirm ticks (default from config)")
    args = ap.parse_args()

    async def run():
        async with aiohttp.ClientSession() as session:
            panels, overlap = await fetch_1m_candles(session, args.hours)

        if not panels:
            print("No data — check API connectivity")
            return

        trades, slot_min, total_min = backtest_portfolio(
            panels, args.target, args.slots, args.max_hold, args.confirm,
        )

        if not trades:
            print("No trades generated")
            return

        df = pd.DataFrame(trades)
        n = len(df)
        completed = df[df["reason"] != "open"]
        still_open = df[df["reason"] == "open"]

        targets = completed[completed["reason"] == "target"]
        timeouts = completed[completed["reason"] == "timeout"]

        total_net = df["net"].sum()
        total_gross = df["gross"].sum()
        total_fees = df["fees"].sum()
        total_funding = df["funding"].sum()

        print(f"\n{'='*60}")
        print(f"BACKTEST: {args.hours}h of 1m data | target=${args.target} | {args.slots} slots")
        print(f"{'='*60}")
        print(f"Symbols with data: {len(panels)}")
        print(f"Total trades: {n} ({len(completed)} closed, {len(still_open)} still open)")
        print(f"  Target hits: {len(targets)}")
        print(f"  Timeouts:    {len(timeouts)}")
        if len(completed) > 0:
            win_rate = len(completed[completed["net"] > 0]) / len(completed) * 100
            print(f"  Win rate:    {win_rate:.0f}%")
            print(f"  Avg hold:    {completed['hold_min'].mean():.0f}min ({completed['hold_min'].mean()/60:.1f}h)")
        print()
        print(f"P&L breakdown:")
        print(f"  Gross:    ${total_gross:+.2f}")
        print(f"  Fees:     ${total_fees:.2f}")
        print(f"  Funding:  ${total_funding:+.2f}")
        print(f"  Net:      ${total_net:+.2f}")
        print()

        if args.slots > 0 and total_min > 0:
            util = slot_min / (total_min * args.slots) * 100
            print(f"Slot usage: {slot_min}/{total_min * args.slots} = {util:.0f}%")

        # Per-symbol breakdown
        print(f"\nPer-symbol:")
        sym_pnl = df.groupby("symbol").agg(
            trades=("net", "count"),
            net=("net", "sum"),
            gross=("gross", "sum"),
            funding=("funding", "sum"),
            avg_hold=("hold_min", "mean"),
        ).sort_values("net", ascending=False)
        for sym, row in sym_pnl.iterrows():
            print(f"  {sym:8s}  {int(row['trades'])} trades  net=${row['net']:+6.2f}  "
                  f"gross=${row['gross']:+6.2f}  fund=${row['funding']:+5.2f}  "
                  f"avg={row['avg_hold']:.0f}min")

    asyncio.run(run())


if __name__ == "__main__":
    main()

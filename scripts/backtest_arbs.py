#!/usr/bin/env python3
"""
Historical arb backtest: Hyperliquid <-> Aster DEX equity perps.

Reconstructs the historical mid-to-mid spread from hourly close prices and the
funding-carry series from funding history, then:
  1. reports the spread distribution per symbol,
  2. measures how often the spread exceeded round-trip costs,
  3. backtests a simple convergence strategy (enter on wide spread, exit on
     reversion or timeout), accounting for fees + funding carry.

LIMITATION: historical orderbook depth is NOT available from either venue, so
the bid/offer crossing cost is *estimated* via --bo-bps (round-trip, both legs).
Convergence P&L is therefore an upper-ish bound; real fills pay live spread.

Usage:
    pip install aiohttp pandas
    python scripts/backtest_arbs.py --days 30
    python scripts/backtest_arbs.py --days 60 --entry 40 --bo-bps 20
"""

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

import aiohttp
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import aster, history, hyperliquid as hl
from src.fees import ROUND_TRIP_MAKER_BPS, ROUND_TRIP_TAKER_BPS
from config import (
    ENTRY_THRESHOLD_BPS_BY_SYMBOL, ENTRY_THRESHOLD_BPS, BLOCKED_SYMBOLS,
    NOTIONAL_PER_LEG, MAX_CONCURRENT_POSITIONS,
)

HOUR_MS = history.HOUR_MS


def floor_hour(ms: int) -> int:
    return (ms // HOUR_MS) * HOUR_MS


async def build_panel(
    session: aiohttp.ClientSession, days: int
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """Build {canonical: hourly DataFrame} for every intersecting equity perp."""
    end_ms = history.now_ms()
    start_ms = end_ms - days * 24 * HOUR_MS

    print(f"Discovering markets and fetching {days}d of hourly history...")
    hl_perps, aster_perps, usdc_hist = await asyncio.gather(
        hl.get_all_equity_perps(session),
        aster.get_all_perps(session),
        history.usdc_usdt_history(session, start_ms, end_ms),
    )

    intersection = sorted(set(hl_perps) & set(aster_perps))
    print(f"  HL equity perps: {len(hl_perps)} | Aster USDT perps: {len(aster_perps)}")
    print(f"  Intersection: {len(intersection)} → {intersection}")
    if not intersection:
        return {}, []

    usdc_s = pd.Series(
        {floor_hour(k): float(v) for k, v in usdc_hist.items()}, dtype="float64"
    ).sort_index()

    panels: dict[str, pd.DataFrame] = {}
    for canonical in intersection:
        hl_coin = hl_perps[canonical].venue_symbol
        aster_sym = aster_perps[canonical].venue_symbol

        hl_px, aster_px, hl_fund, aster_fund = await asyncio.gather(
            history.hl_candles(session, hl_coin, start_ms, end_ms),
            history.aster_candles(session, aster_sym, start_ms, end_ms),
            history.hl_funding_history(session, hl_coin, start_ms),
            history.aster_funding_history(session, aster_sym, start_ms),
        )
        if not hl_px or not aster_px:
            print(f"  {canonical:<6} skipped (HL pts={len(hl_px)}, Aster pts={len(aster_px)})")
            continue

        hl_s = pd.Series({floor_hour(k): float(v) for k, v in hl_px.items()})
        aster_s = pd.Series({floor_hour(k): float(v) for k, v in aster_px.items()})
        hl_f = pd.Series({floor_hour(k): float(v) for k, v in hl_fund.items()})
        # Aster funding is 8h; spread it to an hourly-equivalent rate, ffill across window
        aster_f8 = pd.Series({floor_hour(k): float(v) / 8 for k, v in aster_fund.items()})

        df = pd.DataFrame({"hl_px": hl_s, "aster_px": aster_s}).dropna()
        if df.empty:
            print(f"  {canonical:<6} skipped (no overlapping timestamps)")
            continue

        df["usdc"] = usdc_s.reindex(df.index).ffill().bfill().fillna(1.0)
        df["hl_fund"] = hl_f.reindex(df.index).fillna(0.0)
        df["aster_fund_hr"] = aster_f8.reindex(df.index).ffill().bfill().fillna(0.0)

        # Mid-to-mid spread in bps (Aster - HL_in_usdt) / ref
        df["hl_usdt"] = df["hl_px"] * df["usdc"]
        df["ref"] = (df["hl_usdt"] + df["aster_px"]) / 2
        df["spread_bps"] = (df["aster_px"] - df["hl_usdt"]) / df["ref"] * 10000

        panels[canonical] = df.sort_index()
        print(f"  {canonical:<6} ok ({len(df)} hourly points)")

    return panels, intersection


def carry_bps_hr(row: pd.Series, direction: str) -> float:
    """Net funding carry (bps/hr) earned holding `direction`."""
    hl_f = row["hl_fund"] * 10000
    as_f = row["aster_fund_hr"] * 10000
    # BUY_HL = long HL / short Aster → receive Aster funding, pay HL funding
    return (as_f - hl_f) if direction == "BUY_HL" else (hl_f - as_f)


def backtest_symbol(
    df: pd.DataFrame, entry_bps: float, exit_bps: float,
    max_hold_h: int, total_cost_taker: float, total_cost_maker: float,
) -> dict:
    """State machine: flat → enter on |excess|>=entry → exit on |excess|<=exit or timeout.

    We trade the *excess* spread (raw minus the structural oracle/basis gap),
    matching the live executor (which subtracts the smoothed oracle delta) and
    compute_thresholds.py (whose p75 is on |raw - median|). Using raw spread
    here would over-fire on names with a large persistent basis (SMSN, SKHX,
    BIRD, ...). Structural gap = whole-window median (a lookahead approximation
    of the slowly-varying basis; the live bot uses a trailing estimate).
    """
    structural = float(df["spread_bps"].median())

    in_pos = False
    direction = ""
    entry_abs = 0.0
    hold = 0
    carry_acc = 0.0  # accumulated funding carry in bps

    trades: list[dict] = []
    rows = df.to_dict("records")

    for row in rows:
        excess = row["spread_bps"] - structural
        abs_spread = abs(excess)

        if not in_pos:
            if abs_spread >= entry_bps:
                in_pos = True
                direction = "BUY_HL" if excess > 0 else "BUY_ASTER"
                entry_abs = abs_spread
                hold = 0
                carry_acc = 0.0
        else:
            hold += 1
            carry_acc += carry_bps_hr(row, direction)
            converged = abs_spread <= exit_bps
            timed_out = hold >= max_hold_h
            if converged or timed_out:
                conv_profit = entry_abs - abs_spread  # bps captured from narrowing
                gross = conv_profit + carry_acc
                trades.append({
                    "direction": direction,
                    "entry_bps": entry_abs,
                    "exit_bps": abs_spread,
                    "conv_bps": conv_profit,
                    "carry_bps": carry_acc,
                    "hold_h": hold,
                    "net_taker": gross - total_cost_taker,
                    "net_maker": gross - total_cost_maker,
                    "exit_reason": "converged" if converged else "timeout",
                })
                in_pos = False

    if not trades:
        return {"n_trades": 0, "structural": structural}

    t = pd.DataFrame(trades)
    return {
        "n_trades": len(t),
        "structural": structural,
        "win_rate_taker": (t["net_taker"] > 0).mean() * 100,
        "win_rate_maker": (t["net_maker"] > 0).mean() * 100,
        "avg_hold_h": t["hold_h"].mean(),
        "total_net_taker": t["net_taker"].sum(),
        "total_net_maker": t["net_maker"].sum(),
        "avg_net_taker": t["net_taker"].mean(),
        "avg_net_maker": t["net_maker"].mean(),
        "avg_conv": t["conv_bps"].mean(),
        "avg_carry": t["carry_bps"].mean(),
        "pct_timeout": (t["exit_reason"] == "timeout").mean() * 100,
    }


def backtest_portfolio(
    panels: dict[str, pd.DataFrame], thresholds: dict[str, float],
    exit_bps: float, max_hold_h: int, max_slots: int,
    cost_taker: float, cost_maker: float,
) -> dict:
    """Capacity-aware sim across ALL symbols on one timeline with a shared slot cap.

    Unlike backtest_symbol (which runs each name independently with unlimited
    capital), this walks the union hourly clock and holds at most `max_slots`
    positions at once. When more entry signals fire than free slots, they are
    filled best-first by margin over threshold — the live executor's edge-rank
    intent. This is the realistic P&L ceiling the per-symbol sum cannot show.
    """
    # Precompute per-symbol structural gap + O(1) timestamp lookup.
    sym_data: dict[str, dict] = {}
    all_ts: set[int] = set()
    for sym, df in panels.items():
        structural = float(df["spread_bps"].median())
        recs: dict[int, dict] = {}
        for ts, r in df.iterrows():
            recs[int(ts)] = {
                "excess": float(r["spread_bps"]) - structural,
                "hl_fund": float(r["hl_fund"]),
                "aster_fund_hr": float(r["aster_fund_hr"]),
            }
        sym_data[sym] = recs
        all_ts.update(recs)

    open_pos: dict[str, dict] = {}
    trades: list[dict] = []
    slot_hours_used = 0

    for ts in sorted(all_ts):
        exited_now: set[str] = set()
        # 1. Exits — only evaluate a position on hours its symbol has data.
        for sym in list(open_pos.keys()):
            rec = sym_data[sym].get(ts)
            if rec is None:
                continue
            pos = open_pos[sym]
            pos["hold"] += 1
            pos["carry_acc"] += carry_bps_hr(rec, pos["direction"])
            slot_hours_used += 1
            abs_excess = abs(rec["excess"])
            if abs_excess <= exit_bps or pos["hold"] >= max_hold_h:
                conv = pos["entry_abs"] - abs_excess
                gross = conv + pos["carry_acc"]
                trades.append({
                    "sym": sym, "hold_h": pos["hold"],
                    "net_taker": gross - cost_taker,
                    "net_maker": gross - cost_maker,
                    "exit_reason": "converged" if abs_excess <= exit_bps else "timeout",
                })
                del open_pos[sym]
                exited_now.add(sym)

        # 2. Entries — rank free-slot candidates best-first by margin over threshold.
        free = max_slots - len(open_pos)
        if free <= 0:
            continue
        candidates = []
        for sym, recs in sym_data.items():
            if sym in open_pos or sym in exited_now:
                continue
            rec = recs.get(ts)
            if rec is None:
                continue
            thr = thresholds.get(sym, ENTRY_THRESHOLD_BPS)
            margin = abs(rec["excess"]) - thr
            if margin >= 0:
                candidates.append((margin, sym, rec))
        candidates.sort(key=lambda c: c[0], reverse=True)
        for _margin, sym, rec in candidates[:free]:
            open_pos[sym] = {
                "direction": "BUY_HL" if rec["excess"] > 0 else "BUY_ASTER",
                "entry_abs": abs(rec["excess"]),
                "hold": 0,
                "carry_acc": 0.0,
            }

    if not trades:
        return {"n_trades": 0}
    t = pd.DataFrame(trades)
    horizon_h = (max(all_ts) - min(all_ts)) / history.HOUR_MS if all_ts else 0
    return {
        "n_trades": len(t),
        "total_net_taker": t["net_taker"].sum(),
        "total_net_maker": t["net_maker"].sum(),
        "win_rate_taker": (t["net_taker"] > 0).mean() * 100,
        "win_rate_maker": (t["net_maker"] > 0).mean() * 100,
        "avg_hold_h": t["hold_h"].mean(),
        "pct_timeout": (t["exit_reason"] == "timeout").mean() * 100,
        "slot_hours_used": slot_hours_used,
        "slot_hours_avail": max_slots * horizon_h,
        "by_sym": t.groupby("sym")["net_taker"].agg(["count", "sum"]).sort_values("sum", ascending=False),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Historical HL<->Aster arb backtest")
    ap.add_argument("--days", type=int, default=30, help="lookback window (default 30)")
    ap.add_argument("--entry", type=float, default=None,
                    help="entry spread threshold bps (default: round-trip taker cost)")
    ap.add_argument("--exit", type=float, default=5.0,
                    help="convergence exit threshold bps (default 5)")
    ap.add_argument("--max-hold", type=int, default=168,
                    help="max hold hours before forced exit (default 168 = 1wk)")
    ap.add_argument("--bo-bps", type=float, default=15.0,
                    help="estimated round-trip bid/offer cost bps, both legs (default 15)")
    ap.add_argument("--slots", type=int, default=MAX_CONCURRENT_POSITIONS,
                    help=f"concurrent position cap for portfolio sim (default {MAX_CONCURRENT_POSITIONS})")
    args = ap.parse_args()

    cost_taker = float(ROUND_TRIP_TAKER_BPS) + args.bo_bps
    cost_maker = float(ROUND_TRIP_MAKER_BPS) + args.bo_bps
    use_per_symbol = args.entry is None
    global_entry = args.entry if args.entry is not None else cost_taker

    async def run():
        conn = aiohttp.TCPConnector(limit=20)
        to = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(connector=conn, timeout=to) as session:
            return await build_panel(session, args.days)

    panels, _ = asyncio.run(run())
    if not panels:
        print("\nNo data to analyse.")
        return

    # ── Spread distribution ───────────────────────────────────────────────────
    print(f"""
{'='*108}
 HISTORICAL SPREAD DISTRIBUTION  ({args.days}d hourly)  |  spread = (Aster - HL·USDC/USDT) in bps
 Round-trip cost: taker {cost_taker:.0f}bps / maker {cost_maker:.0f}bps  (fees + est. bid/offer {args.bo_bps:.0f}bps)
{'='*108}""")
    hdr = (f"{'Sym':<6} {'pts':>5} {'mean':>7} {'med':>7} {'std':>7} "
           f"{'p5':>7} {'p95':>7} {'maxAbs':>7} {'%>tkr':>6} {'%>mkr':>6} {'carry/h':>8}")
    print(hdr)
    print("-" * len(hdr))

    dist_rows = []
    for sym, df in panels.items():
        s = df["spread_bps"]
        a = s.abs()
        net_carry = (df["aster_fund_hr"] - df["hl_fund"]) * 10000  # for BUY_HL view
        dist_rows.append((sym, len(s)))
        print(
            f"{sym:<6} {len(s):>5} {s.mean():>7.1f} {s.median():>7.1f} {s.std():>7.1f} "
            f"{s.quantile(0.05):>7.1f} {s.quantile(0.95):>7.1f} {a.max():>7.1f} "
            f"{(a > cost_taker).mean()*100:>5.0f}% {(a > cost_maker).mean()*100:>5.0f}% "
            f"{net_carry.abs().mean():>7.3f}b"
        )

    # ── Backtest ──────────────────────────────────────────────────────────────
    mode = "per-symbol from config" if use_per_symbol else f"|spread|>={global_entry:.0f}bps"
    print(f"""
{'='*108}
 CONVERGENCE BACKTEST  |  entry: {mode}, exit<={args.exit:.0f}bps or {args.max_hold}h timeout
{'='*108}""")
    hdr2 = (f"{'Sym':<6} {'entry':>5} {'basis':>6} {'trades':>6} {'win%tkr':>7} {'win%mkr':>7} {'avgHold':>7} "
            f"{'avgConv':>7} {'avgCarry':>8} {'avgNet(t)':>9} {'avgNet(m)':>9} "
            f"{'totNet(t)':>9} {'totNet(m)':>9} {'timeout%':>8}")
    print(hdr2)
    print("-" * len(hdr2))

    summary = []
    for sym, df in panels.items():
        if sym in BLOCKED_SYMBOLS:
            continue
        entry = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(sym, ENTRY_THRESHOLD_BPS) if use_per_symbol else global_entry
        r = backtest_symbol(df, entry, args.exit, args.max_hold, cost_taker, cost_maker)
        if r["n_trades"] == 0:
            print(f"{sym:<6} {entry:>5.0f} {r['structural']:>+5.0f}b {'0':>6}  (excess never reached entry threshold)")
            continue
        summary.append((sym, r, entry))
        print(
            f"{sym:<6} {entry:>5.0f} {r['structural']:>+5.0f}b {r['n_trades']:>6} {r['win_rate_taker']:>6.0f}% {r['win_rate_maker']:>6.0f}% "
            f"{r['avg_hold_h']:>6.1f}h {r['avg_conv']:>7.1f} {r['avg_carry']:>+7.2f}b "
            f"{r['avg_net_taker']:>+8.1f}b {r['avg_net_maker']:>+8.1f}b "
            f"{r['total_net_taker']:>+8.1f}b {r['total_net_maker']:>+8.1f}b "
            f"{r['pct_timeout']:>7.0f}%"
        )

    # ── Verdict ───────────────────────────────────────────────────────────────
    print(f"\n{'='*108}\n VERDICT\n{'='*108}")
    taker_pos = [(s, r, e) for s, r, e in summary if r["total_net_taker"] > 0]
    maker_pos = [(s, r, e) for s, r, e in summary if r["total_net_maker"] > 0]

    print(f"  Symbols with POSITIVE total backtest P&L — taker entry: {len(taker_pos)}/{len(summary)}")
    print(f"  Symbols with POSITIVE total backtest P&L — maker entry: {len(maker_pos)}/{len(summary)}")

    if maker_pos:
        print("\n  Best (by total maker P&L over window):")
        for s, r, e in sorted(maker_pos, key=lambda x: x[1]["total_net_maker"], reverse=True)[:8]:
            print(f"    {s:<6} thr={e:.0f}  {r['n_trades']:>3} trades  "
                  f"total {r['total_net_maker']:+.0f}bps (maker) / {r['total_net_taker']:+.0f}bps (taker)  "
                  f"win {r['win_rate_maker']:.0f}%/{r['win_rate_taker']:.0f}%  "
                  f"avg hold {r['avg_hold_h']:.0f}h")
    else:
        print("\n  No symbol was net-profitable over the window under these assumptions.")

    # ── Portfolio sim (capacity-aware) ────────────────────────────────────────
    sim_panels = {s: df for s, df in panels.items() if s not in BLOCKED_SYMBOLS}
    thr_map = {
        s: (ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(s, ENTRY_THRESHOLD_BPS) if use_per_symbol else global_entry)
        for s in sim_panels
    }
    pf = backtest_portfolio(
        sim_panels, thr_map, args.exit, args.max_hold, args.slots, cost_taker, cost_maker
    )
    print(f"""
{'='*108}
 PORTFOLIO SIMULATION  |  {args.slots} concurrent slots, best-first by edge  (the per-symbol sum above ignores this cap)
{'='*108}""")
    if pf["n_trades"] == 0:
        print("  No trades.")
    else:
        util = 100 * pf["slot_hours_used"] / pf["slot_hours_avail"] if pf["slot_hours_avail"] else 0
        notional = float(NOTIONAL_PER_LEG)
        print(
            f"  Trades:        {pf['n_trades']}  (vs {len(summary)} symbols' independent runs above)\n"
            f"  Win rate:      {pf['win_rate_taker']:.0f}% taker / {pf['win_rate_maker']:.0f}% maker\n"
            f"  Avg hold:      {pf['avg_hold_h']:.1f}h   |  timeout exits: {pf['pct_timeout']:.0f}%\n"
            f"  Slot usage:    {pf['slot_hours_used']:.0f} / {pf['slot_hours_avail']:.0f} position-hours "
            f"({util:.0f}% of {args.slots}-slot capacity)\n"
            f"  ── Total P&L over window (notional ${notional:,.0f}/leg) ──\n"
            f"     TAKER entry:  {pf['total_net_taker']:+,.0f} bps  →  ${pf['total_net_taker']/10000*notional:+,.0f}\n"
            f"     MAKER entry:  {pf['total_net_maker']:+,.0f} bps  →  ${pf['total_net_maker']/10000*notional:+,.0f}"
        )
        print("\n  Top contributors (taker, captured under the cap):")
        for sym, row in pf["by_sym"].head(8).iterrows():
            print(f"    {sym:<6} {int(row['count']):>3} trades  {row['sum']/10000*notional:+,.0f}$ "
                  f"({row['sum']:+.0f}bps)")

    print(f"""
CAVEATS:
  • Bid/offer cost is ESTIMATED ({args.bo_bps:.0f}bps round trip) — historical depth unavailable.
    Equity-perp books are thin; real slippage may exceed this. Tune with --bo-bps.
  • Backtest assumes you always fill at the candle close mid. No partial fills, no leg risk.
  • Funding carry uses settled historical rates; Aster 8h rate spread to hourly-equivalent.
  • Close-to-close ignores intra-hour spread spikes — true opportunity count is higher,
    but so is the noise. Treat this as a feasibility screen, not a P&L promise.
  • Overnight/weekend candles may reflect stale oracle prices (equity markets closed).
""")


if __name__ == "__main__":
    main()

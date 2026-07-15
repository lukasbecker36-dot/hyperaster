#!/usr/bin/env python3
"""
Analyse the captured order-book / oracle time series (from capture_orderbooks.py)
to answer, from real data, the questions the live approach kept getting wrong:

  1. COST — what does a round trip actually cost? The executable bid-ask you
     cross on each leg is the thing that turned "converged" trades into losses.
     Reports each venue's spread distribution and the round-trip crossing cost.
  2. REVERSION — does the cross-venue mid spread actually mean-revert, and how
     fast? Reports the excess-vs-rolling-baseline half-life per name.
  3. EXPECTANCY — is there ANY entry threshold / holding horizon where a
     maker-HL / taker-Aster convergence round trip is net-positive after the
     real crossing cost? Walk-forward sim on the captured executable prices.

Two execution models (─-cost):
  mid   (default) HL leg fills at mid (maker, no HL spread paid), Aster leg
        crosses as taker → round-trip cost = Aster spread + fees. Matches the
        live est_net realism.
  taker (conservative) both legs cross → cost = HL spread + Aster spread + fees.

Usage:
    python scripts/analyze_capture.py                       # all names, mid model
    python scripts/analyze_capture.py --symbol AAPL         # one name, verbose
    python scripts/analyze_capture.py --cost taker          # conservative
    python scripts/analyze_capture.py --baseline-min 60     # baseline window (min)
"""

import argparse
import math
import os
import sqlite3
import statistics
import sys
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    DATA_DIR, CARRY_ROUND_TRIP_FEE, ROUND_TRIP_FEE, NOTIONAL_PER_LEG,
)

THR_GRID = [20, 30, 45, 60, 80, 110, 150, 200]


def _mid(bid, ask):
    if bid and ask and bid > 0 and ask > 0:
        return (bid + ask) / 2
    return None


def _pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo = int(math.floor(k))
    hi = int(math.ceil(k))
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _load(db, symbol=None):
    """symbol -> list of dict rows (time-ordered) with the fields we need."""
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    q = ("SELECT ts, symbol, hl_bid, hl_ask, hl_mid, hl_oracle, "
         "aster_bid, aster_ask, aster_index FROM book_snaps ")
    args = ()
    if symbol:
        q += "WHERE symbol=? "
        args = (symbol.upper(),)
    q += "ORDER BY symbol, ts"
    out = defaultdict(list)
    for r in conn.execute(q, args):
        out[r["symbol"]].append(dict(r))
    conn.close()
    return out


def _spread_series(rows):
    """Return time-aligned lists: ts, spread_bps (aster_mid-hl_mid), hl_sp_bps,
    aster_sp_bps. Uses top-of-book mids; falls back to ctx mid / oracle-index
    when a venue's book is missing for that row."""
    ts, sp, hl_sp, ast_sp = [], [], [], []
    for r in rows:
        hlm = _mid(r["hl_bid"], r["hl_ask"]) or r["hl_mid"] or r["hl_oracle"]
        astm = _mid(r["aster_bid"], r["aster_ask"]) or r["aster_index"]
        if not hlm or not astm or hlm <= 0 or astm <= 0:
            continue
        mid = (hlm + astm) / 2
        ts.append(r["ts"])
        sp.append((astm - hlm) / mid * 10000)
        hl_sp.append(((r["hl_ask"] - r["hl_bid"]) / mid * 10000)
                     if r["hl_bid"] and r["hl_ask"] else float("nan"))
        ast_sp.append(((r["aster_ask"] - r["aster_bid"]) / mid * 10000)
                      if r["aster_bid"] and r["aster_ask"] else float("nan"))
    return ts, sp, hl_sp, ast_sp


def _half_life(x):
    """Lag-1 AR(1) half-life of the demeaned series, in samples. inf if it
    doesn't mean-revert (rho >= 1), None if too short."""
    x = [v for v in x if v == v]
    if len(x) < 30:
        return None
    m = statistics.mean(x)
    d = [v - m for v in x]
    num = sum(d[i] * d[i - 1] for i in range(1, len(d)))
    den = sum(v * v for v in d[:-1])
    if den <= 0:
        return None
    rho = num / den
    if rho <= 0:
        return 0.0
    if rho >= 1:
        return float("inf")
    return -math.log(2) / math.log(rho)


STOP_BUFFER_BPS = 50.0   # diverge this far BEYOND entry excess = adverse stop


def _backtest(ts, sp, hl_sp, ast_sp, symbol, thr, baseline_n, cost_mode,
              max_hold_n):
    """Walk-forward convergence sim on the captured spread series. Enter when the
    excess (spread − rolling-median baseline) exceeds thr; FADE it (bet it
    reverts toward baseline). Exit on convergence (excess crosses baseline in our
    favour), max hold, or an adverse stop (diverges STOP_BUFFER beyond entry).
    Net of the real per-tick crossing cost. Returns stats.

    Convention: sign = -1 short-spread (entered excess>0, Aster rich → long HL /
    short Aster), +1 long-spread. For both:
        current_excess = spread − baseline
        own_excess     = -sign * current_excess   (starts at |entry_excess|, →0 as it reverts)
        gross_bps      = sign * (current_excess − entry_excess)
    """
    hist = deque(maxlen=baseline_n)
    pos = None            # (sign, entry_excess, entry_i)
    trades = []           # (net_bps, hold_samples)
    fee_bps = (ROUND_TRIP_FEE if cost_mode == "taker" else CARRY_ROUND_TRIP_FEE) * 10000

    for i, s in enumerate(sp):
        hist.append(s)
        if len(hist) < baseline_n:
            continue
        base = statistics.median(hist)
        current_excess = s - base
        if pos is not None:
            sign, entry_excess, entry_i = pos
            own_excess = -sign * current_excess
            # HONEST P&L = the actual spread change between entry and now, NOT
            # the change in excess. Excess subtracts a TRAILING baseline, so its
            # change smuggles in the baseline's drift — on a trending/random-walk
            # spread the median chases the price and "reversion to baseline"
            # books fictional profit even when the price never reverted. You
            # trade the spread, not the baseline; the baseline belongs only in
            # the entry/exit DECISION (own_excess), never in the accounting.
            gross = sign * (s - sp[entry_i])
            hold = i - entry_i
            # Round-trip crossing you actually pay, sampled at the exit tick.
            a_sp = ast_sp[i] if ast_sp[i] == ast_sp[i] else 0.0
            h_sp = hl_sp[i] if hl_sp[i] == hl_sp[i] else 0.0
            cost = fee_bps + (a_sp + h_sp if cost_mode == "taker" else a_sp)
            net = gross - cost
            converged = own_excess <= 0
            adverse = own_excess >= abs(entry_excess) + STOP_BUFFER_BPS
            if converged or adverse or hold >= max_hold_n:
                trades.append((net, hold))
                pos = None
            continue
        # flat — enter when the deviation clears the threshold, fading it.
        if abs(current_excess) >= thr:
            sign = -1.0 if current_excess > 0 else 1.0
            pos = (sign, current_excess, i)
    if not trades:
        return None
    nets = [t[0] for t in trades]
    wins = sum(1 for n in nets if n > 0)
    return {
        "n": len(trades), "win": 100 * wins / len(trades),
        "net_bps": statistics.mean(nets),
        "total_usd": sum(n / 10000 * NOTIONAL_PER_LEG for n in nets),
        "avg_hold": statistics.mean(t[1] for t in trades),
    }


def analyze(db, symbol, cost_mode, baseline_min, interval_sec, max_hold_hours):
    data = _load(db, symbol)
    if not data:
        print("No data in capture DB yet.")
        return
    baseline_n = max(10, int(baseline_min * 60 / interval_sec))
    max_hold_n = max(5, int(max_hold_hours * 3600 / interval_sec))

    print(f"cost model: {cost_mode} | baseline: {baseline_min}min "
          f"({baseline_n} samples) | assumed interval: {interval_sec}s\n")
    rows_hdr = (f"{'SYM':<8}{'n':>7} {'hrs':>5} {'sprd_p50':>8} {'sprd_std':>8} "
                f"{'aXcost':>7} {'hlXcost':>7} {'half_life':>9}")
    print(rows_hdr)
    print("─" * len(rows_hdr))

    summary = []
    for sym in sorted(data):
        rows = data[sym]
        ts, sp, hl_sp, ast_sp = _spread_series(rows)
        if len(sp) < 30:
            continue
        hrs = (ts[-1] - ts[0]) / 3_600_000 if len(ts) > 1 else 0
        hl_cost = statistics.median([v for v in hl_sp if v == v] or [float("nan")])
        ast_cost = statistics.median([v for v in ast_sp if v == v] or [float("nan")])
        hl_ = _half_life(sp)
        hl_str = ("∞" if hl_ == float("inf") else
                  f"{hl_*interval_sec/60:.0f}m" if hl_ else "—")
        print(f"{sym:<8}{len(sp):>7} {hrs:>5.1f} "
              f"{statistics.median(sp):>+8.1f} {statistics.pstdev(sp):>8.1f} "
              f"{ast_cost:>7.0f} {hl_cost:>7.0f} {hl_str:>9}")
        summary.append((sym, ts, sp, hl_sp, ast_sp, hrs))

    # Expectancy grid per name.
    print("\n=== Net-of-cost expectancy grid (total $ over the window) ===")
    print(f"{'SYM':<8}" + "".join(f"{t:>8}" for t in THR_GRID))
    print("─" * (8 + 8 * len(THR_GRID)))
    for sym, ts, sp, hl_sp, ast_sp, hrs in summary:
        cells = []
        best = None
        for thr in THR_GRID:
            r = _backtest(ts, sp, hl_sp, ast_sp, sym, float(thr),
                          baseline_n, cost_mode, max_hold_n)
            if not r or r["n"] < 3:
                cells.append(f"{'·':>8}")
                continue
            cells.append(f"{r['total_usd']:>+8.1f}")
            # Only flag a ★ on a meaningful sample — a lucky low-n threshold on a
            # non-reverting name is not an edge (see the NOISE control).
            if r["n"] >= 10 and (best is None or r["total_usd"] > best[1]):
                best = (thr, r["total_usd"], r["win"], r["n"])
        tag = ""
        if best and best[1] > 0:
            tag = f"  ★ {best[0]}bp ${best[1]:+.0f} {best[2]:.0f}%win n={best[3]}"
        print(f"{sym:<8}" + "".join(cells) + tag)

    print("\nReading it: aXcost/hlXcost = median bid-ask you cross per venue "
          "(bps). half_life = how fast the spread reverts. ★ = best net-positive "
          "threshold. A name with no ★ never paid its way after real costs — "
          "don't trade it. Re-run on a later/second window before trusting any ★.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(DATA_DIR, "orderbook_capture.db"))
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--cost", choices=("mid", "taker"), default="mid")
    ap.add_argument("--baseline-min", type=float, default=480, help="baseline window (minutes)")
    ap.add_argument("--interval-sec", type=float, default=3.0,
                    help="capture cadence, to convert sample counts to time")
    ap.add_argument("--max-hold-hours", type=float, default=12.0)
    a = ap.parse_args()
    if not os.path.exists(a.db):
        print(f"No capture DB at {a.db} — run capture_orderbooks.py first.")
        return
    analyze(a.db, a.symbol, a.cost, a.baseline_min, a.interval_sec, a.max_hold_hours)


if __name__ == "__main__":
    main()

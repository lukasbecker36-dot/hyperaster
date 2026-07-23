#!/usr/bin/env python3
"""
Rank basis-trade opportunities by the p90 round trip PLUS the funding carry of
the direction you'd hold — surfacing names with both a spread to earn and
positive carry while you wait.

For each name, from the capture DB (orderbook_capture.db):
    buy_hl_leg  = (aster_bid − hl_bid)/mid × 10⁴   (ENTER L-HL/S-AST · EXIT L-AST/S-HL)
    buy_ast_leg = (hl_ask − aster_ask)/mid × 10⁴   (ENTER L-AST/S-HL · EXIT L-HL/S-AST)
    sum   = p90(buy_hl) + p90(buy_ast)   — round trip if you catch both spikes
Then, for the leading candidates, the 24h SETTLED funding is fetched from both
venues (real settlements, correct Aster window) and the net carry per day is
computed for each hold direction:
    carry(L-AST/S-HL) = hl_1h·24 − ast_window·settles_per_day   (bps/day)
    carry(L-HL/S-AST) = the negative
The displayed carry/hold is the BETTER direction — the one you'd sit in while
waiting between entry and exit spikes. Final ranking:
    score = sum + best_carry   (round trip + one day of carry)
★ marks the "might work straight away" names: sum clears round-trip fees AND
the hold direction has positive carry.

Still an optimistic screener (assumes you time both spikes) — /basis SYM for
the full pre-trade picture on one name.

Usage: python scripts/basis_opportunities.py [N]     (default 5)
"""

import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DATA_DIR, NON_EQUITY_SYMBOLS, CARRY_ROUND_TRIP_FEE

CAPTURE_DB = os.path.join(DATA_DIR, "orderbook_capture.db")
DAY_MS = 24 * 3_600_000
MIN_SAMPLES = 200          # need decent 24h coverage before a p90 is meaningful
FEE_BPS = CARRY_ROUND_TRIP_FEE * 10000   # maker-HL/taker-Ast round trip ≈ 4.8bp


def _percentile(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _fmt_n(n):
    return f"{n/1000:.1f}k" if n >= 1000 else str(n)


def _load_p90s():
    """capture DB → [(sym, p90_buy_hl, p90_buy_ast, sum, n)] sorted by sum."""
    cutoff = int(time.time() * 1000) - DAY_MS
    conn = sqlite3.connect(f"file:{CAPTURE_DB}?mode=ro", uri=True, timeout=15)
    rows = conn.execute(
        "SELECT symbol, hl_bid, hl_ask, aster_bid, aster_ask, hl_funding, "
        "aster_funding FROM book_snaps "
        "WHERE ts >= ? AND hl_bid > 0 AND hl_ask > 0 "
        "AND aster_bid > 0 AND aster_ask > 0", (cutoff,)).fetchall()
    conn.close()
    buy_hl = defaultdict(list)
    buy_ast = defaultdict(list)
    hl_fund = defaultdict(list)
    ast_fund = defaultdict(list)
    for sym, hb, ha, ab, aa, hf, af in rows:
        if sym in NON_EQUITY_SYMBOLS:
            continue
        mid = (hb + ha + ab + aa) / 4
        if mid <= 0:
            continue
        buy_hl[sym].append((ab - hb) / mid * 10000)
        buy_ast[sym].append((ha - aa) / mid * 10000)
        if hf is not None:
            hl_fund[sym].append(float(hf))
        if af is not None:
            ast_fund[sym].append(float(af))
    ranked = []
    for sym, hl_list in buy_hl.items():
        if len(hl_list) < MIN_SAMPLES:
            continue
        p90_hl = _percentile(hl_list, 90)
        p90_ast = _percentile(buy_ast[sym], 90)
        # Net carry (bps/day) of holding L-AST/S-HL, from the captured rates.
        # HL funding is per-1h; Aster per settlement window. The window isn't
        # stored, so assume the common 8h (3 settlements/day) — the SIGN and
        # rough size are right for ranking; /basis SYM gives the exact per-name
        # number with the detected window. None when funding wasn't captured.
        carry = None
        if hl_fund.get(sym) and ast_fund.get(sym):
            hl_avg = sum(hl_fund[sym]) / len(hl_fund[sym])
            ast_avg = sum(ast_fund[sym]) / len(ast_fund[sym])
            carry = (hl_avg * 24 - ast_avg * 3) * 10000
        ranked.append((sym, p90_hl, p90_ast, p90_hl + p90_ast, len(hl_list), carry))
    ranked.sort(key=lambda r: r[3], reverse=True)
    return ranked


def run(top: int):
    if not os.path.exists(CAPTURE_DB):
        print("🎯 No capture DB yet — start scripts/capture_orderbooks.py and let "
              "it run a while first.")
        return
    try:
        ranked = _load_p90s()
    except Exception as e:
        print(f"🎯 capture DB read failed: {e}")
        return
    if not ranked:
        print(f"🎯 Not enough 24h data yet (need ≥{MIN_SAMPLES} snaps/name ≈ "
              f"{MIN_SAMPLES*3//60}min at 3s). Let the capture run longer.")
        return

    candidates = ranked[:max(top * 3, 12)]
    scored = []
    for sym, ph, pa, s, n, c_ashl in candidates:  # carry of holding L-AST/S-HL
        if c_ashl is None:
            scored.append((sym, s, None, "?", s, n))
            continue
        # The better hold direction and its carry (what you'd earn waiting).
        if c_ashl >= -c_ashl:
            best_carry, hold = c_ashl, "L-AST"
        else:
            best_carry, hold = -c_ashl, "L-HL"
        scored.append((sym, s, best_carry, hold, s + best_carry, n))
    scored.sort(key=lambda r: r[4], reverse=True)

    hdr = f"{'#':>2} {'SYM':<7}{'sum':>6}{'c/d':>7} {'hold':<6}{'n':>6}"
    lines = ["🎯 Top basis opportunities (bps)", hdr, "─" * len(hdr)]
    for i, (sym, s, carry, hold, score, n) in enumerate(scored[:top], 1):
        star = ""
        if carry is not None and carry > 0 and s > FEE_BPS:
            star = " ★"
        c_str = f"{carry:>+7.1f}" if carry is not None else f"{'?':>7}"
        lines.append(f"{i:>2} {sym:<7}{s:>+6.0f}{c_str} {hold:<6}{_fmt_n(n):>6}{star}")
    lines.append("─" * len(hdr))
    lines.append("sum = p90 round trip (catch both legs' spikes). c/d = ~net")
    lines.append("carry bps/day of the BETTER hold direction (from captured")
    lines.append("rates, 8h Aster window assumed); hold = which (L-AST = long")
    lines.append("Aster/short HL). Ranked by sum + carry.")
    lines.append(f"★ = sum > fees ({FEE_BPS:.0f}bp) AND positive carry — spread to")
    lines.append("earn and paid to wait. /basis SYM for exact carry + window.")
    print("\n".join(lines))


def main():
    top = 5
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        top = max(1, min(int(sys.argv[1]), 20))
    run(top)


if __name__ == "__main__":
    main()

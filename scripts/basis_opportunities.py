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
# Floor (sit-level round trip) below this = wide/one-sided book (a missed exit
# spike is a loss). LLY's floor was ~-3.5 (fine); ZHIPU's ~-24 (trap).
FLOOR_MIN_BPS = -10.0


def _percentile(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _fmt_n(n):
    return f"{n/1000:.1f}k" if n >= 1000 else str(n)


def _load_p90s():
    """capture DB → [(sym, p90sum, n, carry, floor)] per name.
    floor = avg(buy_hl) + avg(buy_ast) = the sit-level round trip (≈ HL spread −
    Aster spread). Near 0 = tight symmetric books, so a missed exit spike bails
    ~breakeven; deep negative = wide/one-sided book (Aster book much wider), so
    a missed spike is a loss — the ZHIPU trap. Used to gate ★ and penalise
    ranking so high-amplitude-but-broken-book names don't float to the top."""
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
        floor = (sum(buy_hl[sym]) / len(hl_list)
                 + sum(buy_ast[sym]) / len(buy_ast[sym]))
        ranked.append((sym, p90_hl + p90_ast, len(hl_list), carry, floor))
    ranked.sort(key=lambda r: r[1], reverse=True)
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
    for sym, s, n, c_ashl, floor in candidates:  # c_ashl = carry holding L-AST/S-HL
        if c_ashl is None:
            best_carry, hold = None, "?"
        elif c_ashl >= -c_ashl:   # better hold direction + its carry
            best_carry, hold = c_ashl, "L-AST"
        else:
            best_carry, hold = -c_ashl, "L-HL"
        # Rank score rewards amplitude + carry but PENALISES a negative floor
        # (a wide/one-sided book where a missed exit spike is a loss), so
        # ZHIPU-shaped traps sink instead of topping the list on amplitude alone.
        score = s + (best_carry or 0) + min(0.0, floor)
        scored.append((sym, s, best_carry, hold, floor, score, n))
    scored.sort(key=lambda r: r[5], reverse=True)

    hdr = f"{'#':>2} {'SYM':<7}{'sum':>6}{'floor':>7}{'c/d':>7} {'hold':<6}{'n':>6}"
    lines = ["🎯 Top basis opportunities (bps)", hdr, "─" * len(hdr)]
    for i, (sym, s, carry, hold, floor, score, n) in enumerate(scored[:top], 1):
        # ★ = spread clears fees AND positive carry AND a sound floor (tight
        # symmetric book). ⚠ = deep-negative floor → wide-book trap, no ★.
        bad_floor = floor < FLOOR_MIN_BPS
        star = ""
        if not bad_floor and carry is not None and carry > 0 and s > FEE_BPS:
            star = " ★"
        elif bad_floor:
            star = " ⚠"
        c_str = f"{carry:>+7.1f}" if carry is not None else f"{'?':>7}"
        lines.append(f"{i:>2} {sym:<7}{s:>+6.0f}{floor:>+7.0f}{c_str} "
                     f"{hold:<6}{_fmt_n(n):>6}{star}")
    lines.append("─" * len(hdr))
    lines.append("sum = p90 round trip (catch both spikes). floor = sit-level")
    lines.append("round trip (avg of both legs) — near 0 = tight symmetric book,")
    lines.append(f"a missed exit bails ~breakeven; ⚠ = < {FLOOR_MIN_BPS:.0f} = "
                 f"wide/one-sided")
    lines.append("book, missed exit is a LOSS (no ★). c/d = ~net carry/day of the")
    lines.append("BETTER hold (8h Aster window assumed). Rank = sum + carry + floor.")
    lines.append(f"★ = sum > fees ({FEE_BPS:.0f}bp) + positive carry + sound floor —")
    lines.append("spread to earn, paid to wait, breakeven if the exit's slow.")
    lines.append("/basis SYM for exact carry + window before trading.")
    print("\n".join(lines))


def main():
    top = 5
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        top = max(1, min(int(sys.argv[1]), 20))
    run(top)


if __name__ == "__main__":
    main()

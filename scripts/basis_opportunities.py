#!/usr/bin/env python3
"""
Rank basis-trade opportunities across the whole universe by the round trip you
could capture if you caught each leg at its 24h 90th-percentile level.

For each name, from the capture DB (orderbook_capture.db):
    buy_hl_leg  = (aster_bid − hl_bid)/mid × 10⁴   (ENTER L-HL/S-AST · EXIT L-AST/S-HL)
    buy_ast_leg = (hl_ask − aster_ask)/mid × 10⁴   (ENTER L-AST/S-HL · EXIT L-HL/S-AST)
    p90 each over the last 24h, then
    opportunity = p90(buy_hl) + p90(buy_ast)

That sum is the round trip because a full cycle spans BOTH legs — you enter on
one leg's favourable spike and exit on the other's. e.g. SKHX buy_hl p90 +62,
buy_ast p90 −21 → 41bps of oscillation to harvest (before funding).

It's an optimistic upper bound (assumes you time both spikes), so treat it as a
"where's the widest oscillation" screener, not a guaranteed edge. Pure DB read,
no network. Same leg convention as /basis and the live gates.

Usage: python scripts/basis_opportunities.py [N]     (default 5)
"""

import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DATA_DIR, NON_EQUITY_SYMBOLS

CAPTURE_DB = os.path.join(DATA_DIR, "orderbook_capture.db")
DAY_MS = 24 * 3_600_000
MIN_SAMPLES = 200          # need decent 24h coverage before a p90 is meaningful


def _percentile(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _fmt_n(n):
    return f"{n/1000:.1f}k" if n >= 1000 else str(n)


def main():
    top = 5
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        top = max(1, min(int(sys.argv[1]), 20))

    if not os.path.exists(CAPTURE_DB):
        print("🎯 No capture DB yet — start scripts/capture_orderbooks.py and let "
              "it run a while first.")
        return

    cutoff = int(time.time() * 1000) - DAY_MS
    try:
        conn = sqlite3.connect(f"file:{CAPTURE_DB}?mode=ro", uri=True, timeout=15)
        rows = conn.execute(
            "SELECT symbol, hl_bid, hl_ask, aster_bid, aster_ask FROM book_snaps "
            "WHERE ts >= ? AND hl_bid > 0 AND hl_ask > 0 "
            "AND aster_bid > 0 AND aster_ask > 0", (cutoff,)).fetchall()
        conn.close()
    except Exception as e:
        print(f"🎯 capture DB read failed: {e}")
        return
    if not rows:
        print("🎯 No fresh capture rows in the last 24h — is capture running?")
        return

    buy_hl = defaultdict(list)
    buy_ast = defaultdict(list)
    for sym, hb, ha, ab, aa in rows:
        if sym in NON_EQUITY_SYMBOLS:
            continue
        mid = (hb + ha + ab + aa) / 4
        if mid <= 0:
            continue
        buy_hl[sym].append((ab - hb) / mid * 10000)
        buy_ast[sym].append((ha - aa) / mid * 10000)

    ranked = []
    for sym, hl_list in buy_hl.items():
        if len(hl_list) < MIN_SAMPLES:
            continue
        p90_hl = _percentile(hl_list, 90)
        p90_ast = _percentile(buy_ast[sym], 90)
        ranked.append((sym, p90_hl, p90_ast, p90_hl + p90_ast, len(hl_list)))
    if not ranked:
        need = MIN_SAMPLES
        print(f"🎯 Not enough 24h data yet (need ≥{need} snaps/name ≈ "
              f"{need*3//60}min at 3s). Let the capture run longer.")
        return

    ranked.sort(key=lambda r: r[3], reverse=True)
    hdr = f"{'#':>2} {'SYM':<7}{'buyHL':>7}{'buyAST':>8}{'sum':>7}{'n':>7}"
    lines = ["🎯 Top basis opportunities (24h p90 round-trip, bps)", hdr,
             "─" * len(hdr)]
    for i, (sym, ph, pa, s, n) in enumerate(ranked[:top], 1):
        lines.append(f"{i:>2} {sym:<7}{ph:>+7.0f}{pa:>+8.0f}{s:>+7.0f}{_fmt_n(n):>7}")
    lines.append("─" * len(hdr))
    lines.append("sum = p90(buy-HL) + p90(buy-AST) = round trip if you catch both")
    lines.append("legs' 10%-best spikes (before funding). Optimistic — a screener,")
    lines.append("not a guarantee. /basis SYM for the full picture on one name.")
    print("\n".join(lines))


if __name__ == "__main__":
    main()

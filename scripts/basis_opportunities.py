#!/usr/bin/env python3
"""
Rank basis-trade opportunities by the p90 round trip PLUS the funding carry of
the direction you'd hold — surfacing names with both a spread to earn and
positive carry while you wait.

For each name, from the capture DB (orderbook_capture.db):
    buy_hl_leg  = (aster_bid − hl_bid)/mid × 10⁴   (ENTER L-HL/S-AST · EXIT L-AST/S-HL)
    buy_ast_leg = (hl_ask − aster_ask)/mid × 10⁴   (ENTER L-AST/S-HL · EXIT L-HL/S-AST)
    sum   = p90(buy_hl) + p90(buy_ast)   — round trip if you catch both spikes
Carry uses the 24h average funding from the capture DB, normalised with each
candidate's REAL Aster settlement window (detected from settlement timestamps,
shown as `win`):
    carry(L-AST/S-HL) = hl_1h·24 − ast_per_window·(24/window)   (bps/day)
    carry(L-HL/S-AST) = the negative
The displayed carry/hold is the BETTER direction — the one you'd sit in while
waiting between entry and exit spikes. Final ranking:
    score = sum + best_carry + min(0, floor)
★ marks the "might work straight away" names: sum clears round-trip fees AND
the hold direction has positive carry AND the floor is sound.

Scope: NON_EQUITY is always excluded; BLOCKED is excluded unless --blocked (the
capture DB keeps blocked names so they can be re-evaluated from data, but they
must not appear in a "what should I trade" ranking); crypto is excluded when
EQUITY_ONLY is set.

Still an optimistic screener (assumes you time both spikes) — /basis SYM for
the full pre-trade picture on one name, and check book DEPTH before sizing: a
name can clear fees on paper and have $86 at the touch.

Usage: python scripts/basis_opportunities.py [N] [--blocked]     (default 5)
"""

import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (DATA_DIR, NON_EQUITY_SYMBOLS, BLOCKED_SYMBOLS,
                    CARRY_ROUND_TRIP_FEE, carry_round_trip_fee,
                    aster_symbol_for, ASTER_BASE, EQUITY_ONLY, is_crypto_symbol)

CAPTURE_DB = os.path.join(DATA_DIR, "orderbook_capture.db")
DAY_MS = 24 * 3_600_000
MIN_SAMPLES = 200          # need decent 24h coverage before a p90 is meaningful
# Hard cap on rows pulled into Python. The DB grows unbounded across restarts
# (no retention prune), so a 24h window can still be huge; above this we sample
# every k-th snapshot so a p90 stays stable while peak memory stays bounded on
# the 4GB box. ~400k narrow rows ≈ 10k/name over 40 names — plenty for a p90.
MAX_ROWS = 400_000
FEE_BPS = CARRY_ROUND_TRIP_FEE * 10000   # maker-HL/taker-Ast round trip ≈ 4.8bp
# Floor (sit-level round trip) below this = wide/one-sided book (a missed exit
# spike is a loss). LLY's floor was ~-3.5 (fine); ZHIPU's ~-24 (trap).
FLOOR_MIN_BPS = -10.0


def _aster_windows(symbols, workers=8):
    """{symbol: Aster funding settlement window in hours}, detected from the gaps
    between real settlement timestamps — the same method /basis uses.

    The window is NOT stored in the capture DB, and this used to assume 8h
    (3 settlements/day). Every name checked actually settles HOURLY, which
    understated the Aster leg 8x. That matters most where the two venues' rates
    nearly cancel — i.e. it manufactured carry out of near-neutral funding: ACE
    displayed +43.1bps/day against a true +2.6. Names with a genuinely one-sided
    skew (SAGA, +89.5) were right either way, so the error quietly reordered the
    ranking instead of breaking it visibly.

    A symbol is ABSENT from the result if detection failed, so the caller shows
    carry as unknown rather than guessing. Aster-side calls only — this does not
    touch the HL weight budget.
    """
    import json as _json
    import urllib.request
    from concurrent.futures import ThreadPoolExecutor

    def one(sym):
        url = (f"{ASTER_BASE}/fapi/v1/fundingRate?symbol={aster_symbol_for(sym)}"
               f"&startTime={int(time.time() * 1000) - 48 * 3_600_000}")
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                rows = _json.loads(r.read())
            times = sorted(int(x.get("fundingTime") or 0)
                           for x in (rows or []) if isinstance(x, dict))
            gaps = [(b - a) / 3_600_000 for a, b in zip(times, times[1:]) if b > a]
            if not gaps:
                return sym, None
            gaps.sort()
            return sym, gaps[len(gaps) // 2]        # median gap
        except Exception:
            return sym, None

    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sym, win in ex.map(one, symbols):
            if win and win > 0:
                out[sym] = win
    return out


def _median(xs):
    """Median of a list, or None if empty."""
    if not xs:
        return None
    ys = sorted(xs)
    m = len(ys) // 2
    return ys[m] if len(ys) % 2 else (ys[m - 1] + ys[m]) / 2


def _percentile(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _fmt_n(n):
    return f"{n/1000:.1f}k" if n >= 1000 else str(n)


def _ensure_cover_index():
    """Build the covering index on an existing DB if the capture service hasn't
    yet (older DBs predate it). Index-only scans are what keep /opps fast: the
    query never touches the fat hl_levels/aster_levels blob pages. Opened
    read-write briefly; if that fails (read-only FS, locked), we just fall back
    to the slower path rather than erroring — best-effort."""
    try:
        c = sqlite3.connect(CAPTURE_DB, timeout=15)
        c.execute("PRAGMA busy_timeout=10000")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_snap_cover ON book_snaps("
            "ts, symbol, hl_bid, hl_ask, aster_bid, aster_ask, "
            "hl_funding, aster_funding)")
        c.commit()
        c.close()
    except Exception:
        pass


def _load_p90s(include_blocked: bool = False):
    """capture DB → [(sym, p90sum, n, hl_funding_avg, aster_funding_avg, floor)]
    per name. Carry is NOT computed here — it needs each name's real Aster
    settlement window, which run() detects.

    floor = avg(buy_hl) + avg(buy_ast) = the sit-level round trip (≈ HL spread −
    Aster spread). Near 0 = tight symmetric books, so a missed exit spike bails
    ~breakeven; deep negative = wide/one-sided book (Aster book much wider), so
    a missed spike is a loss — the ZHIPU trap. Used to gate ★ and penalise
    ranking so high-amplitude-but-broken-book names don't float to the top."""
    cutoff = int(time.time() * 1000) - DAY_MS
    _ensure_cover_index()
    conn = sqlite3.connect(f"file:{CAPTURE_DB}?mode=ro", uri=True, timeout=15)
    # Count first (index-only, fast) so we can stride-sample huge windows down to
    # MAX_ROWS instead of materialising millions of fat-book rows into Python.
    n_win = conn.execute(
        "SELECT count(*) FROM book_snaps WHERE ts >= ?", (cutoff,)).fetchone()[0]
    stride = max(1, n_win // MAX_ROWS)
    sql = ("SELECT symbol, hl_bid, hl_ask, aster_bid, aster_ask, hl_funding, "
           "aster_funding FROM book_snaps "
           "WHERE ts >= ? AND hl_bid > 0 AND hl_ask > 0 "
           "AND aster_bid > 0 AND aster_ask > 0")
    params = [cutoff]
    if stride > 1:
        # Sample whole capture cycles by time bucket (each cycle shares a ~ts, so
        # this keeps every symbol represented) rather than by rowid, which could
        # systematically drop a symbol.
        sql += " AND ((ts / 3000) % ?) = 0"
        params.append(stride)
    buy_hl = defaultdict(list)
    buy_ast = defaultdict(list)
    hl_fund = defaultdict(list)
    ast_fund = defaultdict(list)
    # Stream the cursor — don't fetchall — so we never hold the full result set
    # AND the per-symbol lists at once.
    for sym, hb, ha, ab, aa, hf, af in conn.execute(sql, params):
        # NON_EQUITY is always dropped. BLOCKED is dropped unless the caller asks
        # to re-evaluate them (the capture DB keeps them on purpose for that) —
        # without this the screener recommended names the trader refuses to touch
        # (observed: SPCX, in BLOCKED_SYMBOLS, ranked 11th). Crypto is dropped
        # under EQUITY_ONLY, using the same classifier that picks the fee, so a
        # name's inclusion and its displayed cost can never disagree.
        if sym in NON_EQUITY_SYMBOLS:
            continue
        if not include_blocked and sym in BLOCKED_SYMBOLS:
            continue
        if EQUITY_ONLY and is_crypto_symbol(sym):
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
    conn.close()
    ranked = []
    for sym, hl_list in buy_hl.items():
        if len(hl_list) < MIN_SAMPLES:
            continue
        p90_hl = _percentile(hl_list, 90)
        p90_ast = _percentile(buy_ast[sym], 90)
        # MEDIAN, not mean, of the captured rate snapshots. These are samples of
        # lastFundingRate, so each settlement appears once per snapshot it
        # survived — a mean lets one extreme settlement drag the whole figure
        # (CXMT's single -2.0000% print produced a +830bps/day carry here). The
        # median is the rate that persisted longest, which is what you'd actually
        # accrue sitting in the position. Still per-1h for HL and PER WINDOW for
        # Aster, so run() normalises with each name's detected window.
        hl_avg = _median(hl_fund.get(sym))
        ast_avg = _median(ast_fund.get(sym))
        floor = (sum(buy_hl[sym]) / len(hl_list)
                 + sum(buy_ast[sym]) / len(buy_ast[sym]))
        ranked.append((sym, p90_hl + p90_ast, len(hl_list), hl_avg, ast_avg, floor))
    ranked.sort(key=lambda r: r[1], reverse=True)
    return ranked


def run(top: int, include_blocked: bool = False):
    if not os.path.exists(CAPTURE_DB):
        print("🎯 No capture DB yet — start scripts/capture_orderbooks.py and let "
              "it run a while first.")
        return
    try:
        ranked = _load_p90s(include_blocked)
    except Exception as e:
        print(f"🎯 capture DB read failed: {e}")
        return
    if not ranked:
        print(f"🎯 Not enough 24h data yet (need ≥{MIN_SAMPLES} snaps/name ≈ "
              f"{MIN_SAMPLES*3//60}min at 3s). Let the capture run longer.")
        return

    candidates = ranked[:max(top * 3, 12)]
    # Detect each candidate's real Aster settlement window before computing carry.
    windows = _aster_windows([c[0] for c in candidates])
    scored = []
    for sym, s, n, hl_avg, ast_avg, floor in candidates:
        # carry(L-AST/S-HL) = short HL earns hl_1h*24, long Aster pays
        # ast_per_window * settlements_per_day. Unknown (not guessed) if funding
        # wasn't captured or the window couldn't be detected.
        win = windows.get(sym)
        if hl_avg is None or ast_avg is None or not win:
            c_ashl = None
        else:
            c_ashl = (hl_avg * 24 - ast_avg * (24.0 / win)) * 10000
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

    hdr = f"{'#':>2} {'SYM':<7}{'sum':>6}{'floor':>7}{'c/d':>7} {'hold':<6}{'win':>5}{'n':>6}"
    lines = ["🎯 Top basis opportunities (bps)", hdr, "─" * len(hdr)]
    for i, (sym, s, carry, hold, floor, score, n) in enumerate(scored[:top], 1):
        # ★ = spread clears fees AND positive carry AND a sound floor (tight
        # symmetric book). ⚠ = deep-negative floor → wide-book trap, no ★.
        bad_floor = floor < FLOOR_MIN_BPS
        fee_bps = carry_round_trip_fee(sym) * 10000  # crypto pays more than equity
        star = ""
        if not bad_floor and carry is not None and carry > 0 and s > fee_bps:
            star = " ★"
        elif bad_floor:
            star = " ⚠"
        c_str = f"{carry:>+7.1f}" if carry is not None else f"{'?':>7}"
        w = windows.get(sym)
        w_str = f"{w:>4.0f}h" if w else f"{'?':>5}"
        lines.append(f"{i:>2} {sym:<7}{s:>+6.0f}{floor:>+7.0f}{c_str} "
                     f"{hold:<6}{w_str}{_fmt_n(n):>6}{star}")
    lines.append("─" * len(hdr))
    lines.append("sum = p90 round trip (catch both spikes). floor = sit-level")
    lines.append("round trip (avg of both legs) — near 0 = tight symmetric book,")
    lines.append(f"a missed exit bails ~breakeven; ⚠ = < {FLOOR_MIN_BPS:.0f} = "
                 f"wide/one-sided")
    lines.append("book, missed exit is a LOSS (no ★). c/d = ~net carry/day of the")
    lines.append("BETTER hold, using each name's DETECTED Aster window (win).")
    lines.append("Rank = sum + carry + floor. '?' carry = funding or window")
    lines.append("unknown — not guessed.")
    lines.append(f"★ = sum > fees (equity {FEE_BPS:.0f}bp / crypto higher) + "
                 f"positive carry + sound floor —")
    lines.append("spread to earn, paid to wait, breakeven if the exit's slow.")
    scope = []
    if EQUITY_ONLY:
        scope.append("equity only (EQUITY_ONLY)")
    scope.append("BLOCKED shown" if include_blocked else "BLOCKED excluded")
    lines.append("scope: " + ", ".join(scope) + ".")
    lines.append("/basis SYM for exact carry + window before trading.")
    print("\n".join(lines))


def main():
    top = 5
    args = sys.argv[1:]
    # --blocked re-includes BLOCKED_SYMBOLS: the capture DB keeps them so they can
    # be re-evaluated from data, but they must not show up in the normal ranking.
    include_blocked = any(a in ("--blocked", "--include-blocked") for a in args)
    for a in args:
        if a.isdigit():
            top = max(1, min(int(a), 20))
            break
    run(top, include_blocked)


if __name__ == "__main__":
    main()

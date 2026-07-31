#!/usr/bin/env python3
"""
Mean-reversion screen: names whose CURRENT executable basis has swung to the
OPPOSITE sign of its 24h average — i.e. the spread is stretched away from where
it's been sitting, so a revert-to-mean round trip is available right now.

For each name, from the capture DB (orderbook_capture.db):
    buy_hl_leg  = (aster_bid − hl_bid)/mid × 10⁴   (ENTER L-HL/S-AST · EXIT L-AST/S-HL)
    buy_ast_leg = (hl_ask − aster_ask)/mid × 10⁴   (ENTER L-AST/S-HL · EXIT L-HL/S-AST)
    now = the latest snapshot's legs;  avg = the 24h mean of each leg.

Filter: the buy-HL leg's `now` and `avg` must have OPPOSITE signs (both above a
small floor so a near-zero wobble doesn't count). The buy-AST leg mirrors it.

Score (rank, higher = more stretched, matching the requested formula):
    hl_stretch  = |hl_now| + |hl_avg|
    ast_stretch = |ast_now| + |ast_avg|
    score = (hl_stretch + ast_stretch) / 2
This ≈ the revert-to-mean round trip you could capture: enter now on the leg
that's currently favourable, exit when it reverts through its average.

`enter` = the direction whose leg is favourable NOW (positive) — the one you'd
open here and hold for the reversion. `fund24h` = the net funding actually
SETTLED over the last 24h for that enter direction (+ = you'd have been paid to
hold, − = you'd have paid), fetched from both venues for the displayed names.
`/basis SYM` for the exact levels, live-vs-realised carry and book depth.

Scope mirrors /opps: NON_EQUITY always excluded; BLOCKED excluded unless
--blocked; crypto excluded under EQUITY_ONLY.

Usage: python scripts/basis_reversion.py [N] [--blocked]     (default 5)
"""

import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (DATA_DIR, NON_EQUITY_SYMBOLS, BLOCKED_SYMBOLS,
                    EQUITY_ONLY, is_crypto_symbol, HYPERLIQUID_API, ASTER_BASE,
                    aster_symbol_for, load_hl_dex_map)

CAPTURE_DB = os.path.join(DATA_DIR, "orderbook_capture.db")
DAY_MS = 24 * 3_600_000
MIN_SAMPLES = 200          # need decent 24h coverage before now/avg are meaningful
MAX_ROWS = 400_000         # cap rows pulled into Python (see basis_opportunities)
# Both legs of the flip must clear this magnitude, so a name whose basis just
# jitters either side of ~0 (sign flips that mean nothing) isn't surfaced.
MIN_FLIP_BPS = 3.0


def _ensure_cover_index():
    """Build the covering index if the capture service hasn't (older DBs); keeps
    the scan index-only so it never touches the fat level blobs. Best-effort."""
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


def _fmt_n(n):
    return f"{n/1000:.1f}k" if n >= 1000 else str(n)


def _realised_funding(symbols, workers=8):
    """{sym: (hl_sum, ast_sum)} — funding ACTUALLY SETTLED over the last 24h,
    summed per venue (fractions). Same source + method /basis uses: HL
    fundingHistory + Aster fundingRate, summed (no window rescaling, so a one-off
    print can't be multiplied across the day). Network, but bounded to the names
    being displayed and run in a thread pool. A venue is None if its fetch failed
    → the caller shows the funding column as unknown rather than guessing.

    Net realised carry for a direction (bps):
        L-AST/S-HL = (hl_sum - ast_sum) * 1e4   (short HL receives, long Aster pays)
        L-HL/S-AST = the negative.
    """
    import json as _json
    import urllib.request
    from concurrent.futures import ThreadPoolExecutor

    dex_map = load_hl_dex_map()
    start = int(time.time() * 1000) - DAY_MS
    now = int(time.time() * 1000)

    def one(sym):
        hl_sum = ast_sum = None
        # HL fundingHistory (POST, dex-aware coin from the persisted map).
        try:
            coin = sym if dex_map.get(sym, "xyz") == "" else f"xyz:{sym}"
            body = _json.dumps({"type": "fundingHistory", "coin": coin,
                                "startTime": start}).encode()
            req = urllib.request.Request(
                HYPERLIQUID_API, data=body,
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as r:
                rows = _json.loads(r.read())
            rates = [float(x.get("fundingRate") or 0)
                     for x in (rows or []) if isinstance(x, dict)]
            if rates:
                hl_sum = sum(rates)
        except Exception:
            pass
        # Aster fundingRate (GET), only settlements inside the 24h window.
        try:
            url = (f"{ASTER_BASE}/fapi/v1/fundingRate?"
                   f"symbol={aster_symbol_for(sym)}&startTime={start}")
            with urllib.request.urlopen(url, timeout=10) as r:
                rows = _json.loads(r.read())
            rates = [float(x.get("fundingRate") or 0)
                     for x in (rows or []) if isinstance(x, dict)
                     and start <= int(x.get("fundingTime") or 0) <= now]
            if rates:
                ast_sum = sum(rates)
        except Exception:
            pass
        return sym, (hl_sum, ast_sum)

    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sym, val in ex.map(one, symbols):
            out[sym] = val
    return out


def _load_reversion(include_blocked: bool = False):
    """capture DB → [(sym, hl_now, hl_avg, ast_now, ast_avg, score, enter, n)]
    for names whose buy-HL leg flipped sign vs its 24h mean, ranked by score.

    Uses running sums for the 24h mean (memory-light) and tracks each name's
    legs at its MAX ts as `now` — with EQUITY_ONLY (~40 names) the window fits
    under MAX_ROWS so stride=1 and that max-ts row is the genuine latest snap."""
    cutoff = int(time.time() * 1000) - DAY_MS
    _ensure_cover_index()
    conn = sqlite3.connect(f"file:{CAPTURE_DB}?mode=ro", uri=True, timeout=15)
    n_win = conn.execute(
        "SELECT count(*) FROM book_snaps WHERE ts >= ?", (cutoff,)).fetchone()[0]
    stride = max(1, n_win // MAX_ROWS)
    sql = ("SELECT ts, symbol, hl_bid, hl_ask, aster_bid, aster_ask "
           "FROM book_snaps WHERE ts >= ? AND hl_bid > 0 AND hl_ask > 0 "
           "AND aster_bid > 0 AND aster_ask > 0")
    params = [cutoff]
    if stride > 1:
        sql += " AND ((ts / 3000) % ?) = 0"
        params.append(stride)
    hl_sum = defaultdict(float)
    ast_sum = defaultdict(float)
    cnt = defaultdict(int)
    now_ts = defaultdict(int)
    hl_now: dict = {}
    ast_now: dict = {}
    for ts, sym, hb, ha, ab, aa in conn.execute(sql, params):
        if sym in NON_EQUITY_SYMBOLS:
            continue
        if not include_blocked and sym in BLOCKED_SYMBOLS:
            continue
        if EQUITY_ONLY and is_crypto_symbol(sym):
            continue
        mid = (hb + ha + ab + aa) / 4
        if mid <= 0:
            continue
        bh = (ab - hb) / mid * 10000
        ba = (ha - aa) / mid * 10000
        hl_sum[sym] += bh
        ast_sum[sym] += ba
        cnt[sym] += 1
        if ts > now_ts[sym]:
            now_ts[sym] = ts
            hl_now[sym] = bh
            ast_now[sym] = ba
    conn.close()
    out = []
    for sym, c in cnt.items():
        if c < MIN_SAMPLES:
            continue
        hl_avg = hl_sum[sym] / c
        ast_avg = ast_sum[sym] / c
        hn = hl_now[sym]
        an = ast_now[sym]
        # Opposite signs on the buy-HL leg, both sides above the noise floor.
        if not (abs(hn) >= MIN_FLIP_BPS and abs(hl_avg) >= MIN_FLIP_BPS
                and (hn > 0) != (hl_avg > 0)):
            continue
        score = (abs(hn) + abs(hl_avg) + abs(an) + abs(ast_avg)) / 2
        # Enter now on whichever leg is currently favourable (positive).
        enter = "L-AST" if an >= hn else "L-HL"
        out.append((sym, hn, hl_avg, an, ast_avg, score, enter, c))
    out.sort(key=lambda r: r[5], reverse=True)
    return out


def run(top: int, include_blocked: bool = False):
    if not os.path.exists(CAPTURE_DB):
        print("🔁 No capture DB yet — start scripts/capture_orderbooks.py first.")
        return
    try:
        ranked = _load_reversion(include_blocked)
    except Exception as e:
        print(f"🔁 capture DB read failed: {e}")
        return
    if not ranked:
        print("🔁 No names currently flipped (now basis vs 24h avg, opposite "
              f"signs, both ≥{MIN_FLIP_BPS:.0f}bp). Nothing stretched right now.")
        return

    shown = ranked[:top]
    # 24h realised funding for the names we'll display, for their enter direction.
    funds = _realised_funding([r[0] for r in shown])

    hdr = (f"{'#':>2} {'SYM':<7}{'hl n/avg':>13}{'ast n/avg':>13}"
           f"{'score':>7} {'enter':<6}{'fund24h':>8}{'n':>6}")
    lines = ["🔁 Basis reversion — now flipped vs 24h avg (bps)", hdr,
             "─" * len(hdr)]
    for i, (sym, hn, ha, an, aa, score, enter, n) in enumerate(shown, 1):
        hl_str = f"{hn:+.0f}/{ha:+.0f}"
        ast_str = f"{an:+.0f}/{aa:+.0f}"
        hl_sum, ast_sum = funds.get(sym, (None, None))
        if hl_sum is None or ast_sum is None:
            f_str = f"{'?':>8}"
        else:
            ashl = (hl_sum - ast_sum) * 10000            # L-AST/S-HL realised
            fund = ashl if enter == "L-AST" else -ashl   # for THIS enter dir
            f_str = f"{fund:>+8.1f}"
        lines.append(f"{i:>2} {sym:<7}{hl_str:>13}{ast_str:>13}"
                     f"{score:>7.0f} {enter:<6}{f_str}{_fmt_n(n):>6}")
    lines.append("─" * len(hdr))
    lines.append("Shows names whose CURRENT basis sits on the OPPOSITE side of")
    lines.append("its 24h average — stretched, so a revert-to-mean round trip is")
    lines.append("on. hl/ast n/avg = each leg's now vs 24h mean. score =")
    lines.append("(|hl now|+|hl avg| + |ast now|+|ast avg|)/2 ≈ the reversion")
    lines.append("round trip available. enter = the leg favourable NOW (the")
    lines.append("direction you'd open here and hold for the revert).")
    lines.append("fund24h = net funding actually SETTLED over the last 24h for")
    lines.append("the enter direction (+ paid you / − you paid); realised, so a")
    lines.append("one-off print can skew it — /basis SYM for live vs realised.")
    scope = []
    if EQUITY_ONLY:
        scope.append("equity only (EQUITY_ONLY)")
    scope.append("BLOCKED shown" if include_blocked else "BLOCKED excluded")
    lines.append("scope: " + ", ".join(scope) + ".")
    lines.append("/basis SYM for exact levels, carry + book depth before trading.")
    print("\n".join(lines))


def main():
    top = 5
    args = sys.argv[1:]
    include_blocked = any(a in ("--blocked", "--include-blocked") for a in args)
    for a in args:
        if a.isdigit():
            top = max(1, min(int(a), 20))
            break
    run(top, include_blocked)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
1-minute candle backtest with profit-target exit.

Fetches 1m candles from both venues for all overlapping equity perps,
then simulates the current strategy: oracle-adjusted entry with raw
premium floor, profit-target exit (convergence + funding carry), and
portfolio-level slot cap.

Sizing is capped to current top-of-book liquidity per symbol.
Bid-ask crossing cost uses actual per-symbol spreads from live order books.

Usage (run on Hetzner server):
    .venv/bin/python scripts/backtest_1m.py --hours 48
    .venv/bin/python scripts/backtest_1m.py --hours 48 --target 2.0
"""

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

import aiohttp
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import aster, hyperliquid as hl, history
from src.fees import ROUND_TRIP_TAKER_BPS
from config import (
    ENTRY_THRESHOLD_BPS_BY_SYMBOL, ENTRY_THRESHOLD_BPS, BLOCKED_SYMBOLS,
    NOTIONAL_PER_LEG, MAX_CONCURRENT_POSITIONS, MIN_RAW_PREMIUM_BPS,
    ROUND_TRIP_FEE, ENTRY_CONFIRM_TICKS, ENTRY_COST_MARGIN_BPS,
)

MIN_MS = 60_000
HOUR_MS = 3_600_000


def now_ms():
    import time
    return int(time.time() * 1000)


async def fetch_book_sizes(
    session: aiohttp.ClientSession,
    hl_perps: dict, aster_perps: dict, overlap: list[str],
) -> dict[str, float]:
    """Fetch current top-of-book sizes and return {symbol: max_notional_usd}."""
    hl_books = await hl.get_book_tickers(
        session, [hl_perps[c].venue_symbol for c in overlap]
    )
    aster_books = await aster.get_all_book_tickers(session)

    sizes = {}
    for canon in overlap:
        hl_bt = hl_books.get(canon)
        ast_bt = aster_books.get(canon)
        if not hl_bt or not ast_bt:
            continue
        hl_mid = float((hl_bt.bid + hl_bt.ask) / 2)
        # We don't have sizes from bookTicker endpoints — fetch L2 for HL
        # and depth for Aster. For now, use a simpler approach: fetch the
        # actual book depth via the info APIs.
        sizes[canon] = hl_mid  # placeholder, will be replaced below
    return sizes


async def fetch_top_of_book(
    session: aiohttp.ClientSession,
    hl_perps: dict, aster_perps: dict, overlap: list[str],
) -> tuple[dict[str, float], dict[str, float]]:
    """Fetch current top-of-book depth and spreads.

    Returns:
        tob_notional: {symbol: max_notional_usd} — min of HL/Aster best-level size
        tob_spread_bps: {symbol: round_trip_crossing_bps} — HL spread + Aster spread
    """
    async def get_hl_top(canon):
        coin = hl_perps[canon].venue_symbol
        try:
            payload = {"type": "l2Book", "coin": coin}
            async with session.post("https://api.hyperliquid.xyz/info", json=payload) as r:
                data = await r.json()
            levels = data.get("levels", [[], []])
            bids = levels[0] if levels[0] else []
            asks = levels[1] if levels[1] else []
            if not bids or not asks:
                return canon, 0, 0, 0, 0
            bid_px = float(bids[0]["px"])
            ask_px = float(asks[0]["px"])
            bid_sz = float(bids[0]["sz"])
            ask_sz = float(asks[0]["sz"])
            mid = (bid_px + ask_px) / 2
            spread_bps = (ask_px - bid_px) / mid * 10000 if mid > 0 else 0
            return canon, bid_sz * mid, ask_sz * mid, mid, spread_bps
        except Exception:
            return canon, 0, 0, 0, 0

    async def get_aster_top(canon):
        sym = aster_perps[canon].venue_symbol
        try:
            url = f"https://fapi.asterdex.com/fapi/v1/depth"
            async with session.get(url, params={"symbol": sym, "limit": 5}) as r:
                data = await r.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if not bids or not asks:
                return canon, 0, 0, 0, 0
            bid_px = float(bids[0][0])
            ask_px = float(asks[0][0])
            bid_sz = float(bids[0][1])
            ask_sz = float(asks[0][1])
            mid = (bid_px + ask_px) / 2
            spread_bps = (ask_px - bid_px) / mid * 10000 if mid > 0 else 0
            return canon, bid_sz * mid, ask_sz * mid, mid, spread_bps
        except Exception:
            return canon, 0, 0, 0, 0

    hl_results = await asyncio.gather(*[get_hl_top(c) for c in overlap])
    ast_results = await asyncio.gather(*[get_aster_top(c) for c in overlap])

    hl_map = {c: (bid_n, ask_n, mid, sp) for c, bid_n, ask_n, mid, sp in hl_results}
    ast_map = {c: (bid_n, ask_n, mid, sp) for c, bid_n, ask_n, mid, sp in ast_results}

    sizes = {}
    spreads = {}
    for canon in overlap:
        hl_bid_n, hl_ask_n, hl_mid, hl_sp = hl_map.get(canon, (0, 0, 0, 0))
        ast_bid_n, ast_ask_n, ast_mid, ast_sp = ast_map.get(canon, (0, 0, 0, 0))
        avail = [x for x in [hl_bid_n, hl_ask_n, ast_bid_n, ast_ask_n] if x > 0]
        sizes[canon] = min(avail) if avail else 0
        # Round-trip crossing = pay full spread on each venue twice (entry + exit)
        spreads[canon] = (hl_sp + ast_sp) * 2
    return sizes, spreads


async def fetch_1m_candles(
    session: aiohttp.ClientSession, hours: int,
) -> tuple[dict[str, pd.DataFrame], list[str], dict, dict]:
    end_ms = now_ms()
    start_ms = end_ms - hours * HOUR_MS

    print(f"Discovering markets...")
    hl_perps, aster_perps = await asyncio.gather(
        hl.get_all_equity_perps(session),
        aster.get_all_perps(session),
    )
    overlap = sorted(set(hl_perps) & set(aster_perps) - BLOCKED_SYMBOLS)
    print(f"HL: {len(hl_perps)} | Aster: {len(aster_perps)} | Overlap: {len(overlap)}")

    print(f"Fetching top-of-book liquidity & spreads...")
    tob_notional, tob_spread_bps = await fetch_top_of_book(session, hl_perps, aster_perps, overlap)

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
        panels[canon] = df

    print(f"Built panels for {len(panels)} symbols, skipped {len(skipped)}")
    return panels, overlap, hl_perps, aster_perps, tob_notional, tob_spread_bps


def apply_baseline(panels: dict, window_min: int):
    """(Re)compute the rolling-baseline excess columns for a given window.

    Baseline = rolling median of the book-mid spread over `window_min` minutes
    (1m candles, so window in samples == window in minutes). This is the
    structural gap the live bot would track; excess is the tradeable deviation.

    Uses a backward-looking rolling window — no future data leaks in, matching
    what the live bot can compute from its own spread history.
    """
    for df in panels.values():
        df["structural"] = df["spread_bps"].rolling(window_min, min_periods=10).median()
        df["structural"] = df["structural"].bfill()
        df["aster_excess"] = df["spread_bps"] - df["structural"]
        df["hl_excess"] = -(df["spread_bps"] - df["structural"])


class Position:
    def __init__(self, symbol, direction, entry_idx, entry_ts, entry_hl, entry_ast, qty, notional, hl_fr, ast_fr):
        self.symbol = symbol
        self.direction = direction
        self.entry_idx = entry_idx
        self.entry_ts = entry_ts
        self.entry_hl = entry_hl
        self.entry_ast = entry_ast
        self.qty = qty
        self.notional = notional
        self.hl_fr = hl_fr
        self.ast_fr = ast_fr

    def est_pnl(self, exit_hl, exit_ast, minutes_held, bo_bps):
        if self.direction == "long_hl_short_aster":
            gross = ((exit_hl - self.entry_hl) + (self.entry_ast - exit_ast)) * self.qty
        else:
            gross = ((self.entry_hl - exit_hl) + (exit_ast - self.entry_ast)) * self.qty
        fees = self.notional * ROUND_TRIP_FEE
        crossing = self.notional * bo_bps / 10000
        hours = minutes_held / 60
        if self.direction == "long_hl_short_aster":
            hl_sign, ast_sign = -1.0, 1.0
        else:
            hl_sign, ast_sign = 1.0, -1.0
        funding = (hl_sign * self.hl_fr * hours * self.notional
                   + ast_sign * self.ast_fr * (hours / 8) * self.notional)
        net = gross - fees - crossing + funding
        return gross, fees, crossing, funding, net


def backtest_portfolio(panels, tob_notional, tob_spread_bps, target_net, max_slots,
                       max_hold_min, confirm_ticks):
    """Walk the 1-minute clock, enforce slot cap, profit-target exit."""
    all_ts = set()
    for df in panels.values():
        all_ts.update(df["ts"].tolist())
    timeline = sorted(all_ts)

    ts_idx = {}
    for sym, df in panels.items():
        ts_idx[sym] = dict(zip(df["ts"], range(len(df))))

    positions: dict[str, Position] = {}
    streaks: dict[str, tuple[str, int]] = {}
    trades = []
    slot_minutes = 0
    total_minutes = len(timeline)

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
            sym_bo_bps = tob_spread_bps.get(sym, 0)

            gross, fees, crossing, funding, net = pos.est_pnl(
                row["hl_close"], row["ast_close"], minutes_held, sym_bo_bps)

            reason = None
            if net >= target_net:
                reason = "target"
                # Cap P&L to target: live bot exits at ~$2 net (1s polling),
                # not at end-of-candle price. Attribute the overshoot back to
                # gross so fees/crossing/funding stay accurate.
                overshoot = net - target_net
                gross -= overshoot
                net = target_net
            elif minutes_held >= max_hold_min:
                reason = "timeout"

            if reason:
                trades.append({
                    "symbol": sym, "direction": pos.direction,
                    "entry_ts": pos.entry_ts,
                    "entry_hl": pos.entry_hl, "entry_ast": pos.entry_ast,
                    "exit_hl": row["hl_close"], "exit_ast": row["ast_close"],
                    "gross": gross, "fees": fees, "crossing": crossing,
                    "funding": funding, "net": net,
                    "hold_min": minutes_held, "reason": reason,
                    "notional": pos.notional,
                })
                del positions[sym]
                streaks.pop(sym, None)

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

            base_threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(sym, ENTRY_THRESHOLD_BPS)
            # Dynamic cost floor: must clear round-trip fees + both venue spreads
            # + margin, matching live executor's ENTRY_COST_MARGIN_BPS logic.
            sym_spread = tob_spread_bps.get(sym, 0) / 2  # RT spread / 2 = one-way HL+Aster
            cost_floor = ROUND_TRIP_FEE * 10000 + sym_spread + ENTRY_COST_MARGIN_BPS
            threshold = max(base_threshold, cost_floor)

            for excess_col, direction in [
                ("aster_excess", "long_hl_short_aster"),
                ("hl_excess", "long_aster_short_hl"),
            ]:
                excess = row[excess_col]
                raw_premium = abs(row["spread_bps"])

                if excess >= threshold and raw_premium >= MIN_RAW_PREMIUM_BPS:
                    prev_dir, prev_count = streaks.get(sym, ("", 0))
                    count = prev_count + 1 if prev_dir == direction else 1
                    streaks[sym] = (direction, count)
                    if count >= confirm_ticks:
                        margin = excess - threshold
                        candidates.append((sym, direction, excess, margin, idx, row))
                    break
            else:
                streaks.pop(sym, None)

        candidates.sort(key=lambda x: x[3], reverse=True)
        slots_free = max_slots - len(positions)
        for sym, direction, excess, margin, idx, row in candidates[:slots_free]:
            mid = row["mid"]
            if mid <= 0:
                continue
            # Cap notional to current top-of-book liquidity
            max_notional = tob_notional.get(sym, 0)
            if max_notional <= 0:
                continue
            notional = min(NOTIONAL_PER_LEG, max_notional)
            qty = notional / mid
            if qty <= 0:
                continue
            actual_notional = qty * mid
            positions[sym] = Position(
                sym, direction, idx, ts,
                row["hl_close"], row["ast_close"],
                qty, actual_notional,
                hl_fr_default, ast_fr_default,
            )
            streaks.pop(sym, None)

    # Force-close anything still open
    for sym, pos in positions.items():
        df = panels[sym]
        row = df.iloc[-1]
        minutes_held = (row["ts"] - df.iloc[pos.entry_idx]["ts"]) / MIN_MS
        sym_bo_bps = tob_spread_bps.get(sym, 0)
        gross, fees, crossing, funding, net = pos.est_pnl(
            row["hl_close"], row["ast_close"], minutes_held, sym_bo_bps)
        trades.append({
            "symbol": sym, "direction": pos.direction,
            "entry_ts": pos.entry_ts,
            "entry_hl": pos.entry_hl, "entry_ast": pos.entry_ast,
            "exit_hl": row["hl_close"], "exit_ast": row["ast_close"],
            "gross": gross, "fees": fees, "crossing": crossing,
            "funding": funding, "net": net,
            "hold_min": minutes_held, "reason": "open", "notional": pos.notional,
        })

    return trades, slot_minutes, total_minutes


def peak_analysis(panels, tob_notional, tob_spread_bps, max_hold_min, confirm_ticks):
    """Per-entry peak-excursion analysis to calibrate per-name profit targets.

    For every qualifying entry signal (same threshold/cost-floor/confirm logic
    as the live bot), holds INDEPENDENTLY — no slot cap, no early target exit —
    and records the maximum net P&L (gross - fees - crossing) reached before the
    spread reverts to baseline or times out. Trades are non-overlapping per name.

    The peak distribution per name is the empirical ceiling of reversion that
    name offers, which is what a sensible profit target should be set against.
    Funding is excluded here (negligible over the short pre-peak holds) so the
    number is pure convergence capture.
    """
    rows = []
    for sym, df in panels.items():
        max_notional = tob_notional.get(sym, 0)
        if max_notional <= 0:
            continue
        notional = min(NOTIONAL_PER_LEG, max_notional)
        bo_bps = tob_spread_bps.get(sym, 0)
        fees = notional * ROUND_TRIP_FEE
        crossing = notional * bo_bps / 10000

        base_threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(sym, ENTRY_THRESHOLD_BPS)
        cost_floor = ROUND_TRIP_FEE * 10000 + bo_bps / 2 + ENTRY_COST_MARGIN_BPS
        threshold = max(base_threshold, cost_floor)

        n = len(df)
        ts = df["ts"].values
        hl = df["hl_close"].values
        ast = df["ast_close"].values
        midv = df["mid"].values
        a_exc = df["aster_excess"].values
        h_exc = df["hl_excess"].values
        spread = df["spread_bps"].values

        i = 0
        streak_dir, streak_n = "", 0
        while i < n:
            entered = None
            for exc_arr, direction in [(a_exc, "long_hl_short_aster"),
                                       (h_exc, "long_aster_short_hl")]:
                if exc_arr[i] >= threshold and abs(spread[i]) >= MIN_RAW_PREMIUM_BPS:
                    streak_n = streak_n + 1 if streak_dir == direction else 1
                    streak_dir = direction
                    if streak_n >= confirm_ticks:
                        entered = direction
                    break
            else:
                streak_dir, streak_n = "", 0

            if not entered:
                i += 1
                continue

            entry_hl, entry_ast = hl[i], ast[i]
            qty = notional / midv[i] if midv[i] > 0 else 0
            entry_excess = a_exc[i] if entered == "long_hl_short_aster" else h_exc[i]
            exc_arr = a_exc if entered == "long_hl_short_aster" else h_exc

            max_net, t_peak, reverted = -1e9, 0.0, False
            j = i + 1
            last_min = 0.0
            while j < n:
                mins = (ts[j] - ts[i]) / MIN_MS
                if mins > max_hold_min:
                    break
                last_min = mins
                if entered == "long_hl_short_aster":
                    gross = ((hl[j] - entry_hl) + (entry_ast - ast[j])) * qty
                else:
                    gross = ((entry_hl - hl[j]) + (ast[j] - entry_ast)) * qty
                net = gross - fees - crossing
                if net > max_net:
                    max_net, t_peak = net, mins
                if exc_arr[j] <= 0:   # reverted to baseline — no edge left
                    reverted = True
                    break
                j += 1

            peak_gross_bps = (max_net + fees + crossing) / notional * 10000 if notional > 0 else 0
            rows.append({
                "symbol": sym, "direction": entered,
                "entry_excess_bps": entry_excess,
                "max_net": max_net, "peak_gross_bps": peak_gross_bps,
                "t_peak_min": t_peak, "notional": notional,
                "reverted": reverted, "hold_min": last_min,
            })
            i = j + 1
            streak_dir, streak_n = "", 0

    return rows


def tp_sweep(panels, tob_notional, tob_spread_bps, max_hold_min, confirm_ticks,
             tp_grid_bps, reclaim_bps=0.0):
    """Per-entry TP-level comparison on one shared set of entries.

    Fills the gap between the two existing exit models. peak_analysis records
    only max_net and stops the walk AT baseline reclaim, so it never records what
    a trade would actually have closed at — the "peaked, then gave it back, ended
    a loss" cohort is invisible. backtest_portfolio's --sweep does exit on a $
    target but never on reclaim, so it holds losers to the 48h timeout instead of
    closing them where the live bot's convergence exit would.

    Here each qualifying entry is walked ONCE, recording:
      * the first minute each candidate TP level is reached,
      * the peak (max_net) and when,
      * the TERMINAL net if you took no TP at all — exiting at baseline reclaim
        (excess <= reclaim_bps) or the hold cap, whichever comes first.
    Every TP level is then scored against that same path, so differences are the
    TP's doing and not a different entry set.

    A TP only counts as reached if it happens BEFORE the terminal exit: a level
    first touched after the spread has already reclaimed is unreachable, because
    the convergence exit would have closed the position first.

    Funding is excluded, matching peak_analysis — it's ~2-4bps/day on these names,
    negligible over the short holds a TP produces but material if you raise
    --max-hold into days, so read long-horizon rows with that in mind.
    """
    trades = []
    for sym, df in panels.items():
        max_notional = tob_notional.get(sym, 0)
        if max_notional <= 0:
            continue
        notional = min(NOTIONAL_PER_LEG, max_notional)
        bo_bps = tob_spread_bps.get(sym, 0)
        fees = notional * ROUND_TRIP_FEE
        crossing = notional * bo_bps / 10000

        base_threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(sym, ENTRY_THRESHOLD_BPS)
        cost_floor = ROUND_TRIP_FEE * 10000 + bo_bps / 2 + ENTRY_COST_MARGIN_BPS
        threshold = max(base_threshold, cost_floor)

        n = len(df)
        ts = df["ts"].values
        hl = df["hl_close"].values
        ast = df["ast_close"].values
        midv = df["mid"].values
        a_exc = df["aster_excess"].values
        h_exc = df["hl_excess"].values
        spread = df["spread_bps"].values

        # TP levels as dollars for this symbol's notional.
        tp_usd = [(bps, notional * bps / 10000) for bps in tp_grid_bps]

        i = 0
        streak_dir, streak_n = "", 0
        while i < n:
            entered = None
            for exc_arr, direction in [(a_exc, "long_hl_short_aster"),
                                       (h_exc, "long_aster_short_hl")]:
                if exc_arr[i] >= threshold and abs(spread[i]) >= MIN_RAW_PREMIUM_BPS:
                    streak_n = streak_n + 1 if streak_dir == direction else 1
                    streak_dir = direction
                    if streak_n >= confirm_ticks:
                        entered = direction
                    break
            else:
                streak_dir, streak_n = "", 0

            if not entered:
                i += 1
                continue

            entry_hl, entry_ast = hl[i], ast[i]
            qty = notional / midv[i] if midv[i] > 0 else 0
            entry_excess = a_exc[i] if entered == "long_hl_short_aster" else h_exc[i]
            exc_arr = a_exc if entered == "long_hl_short_aster" else h_exc

            first_hit = {bps: None for bps in tp_grid_bps}
            max_net, t_peak = -1e9, 0.0
            terminal_net, terminal_min, terminal_reason = None, 0.0, "cap"
            j = i + 1
            while j < n:
                mins = (ts[j] - ts[i]) / MIN_MS
                if mins > max_hold_min:
                    break
                if entered == "long_hl_short_aster":
                    gross = ((hl[j] - entry_hl) + (entry_ast - ast[j])) * qty
                else:
                    gross = ((entry_hl - hl[j]) + (ast[j] - entry_ast)) * qty
                net = gross - fees - crossing
                if net > max_net:
                    max_net, t_peak = net, mins
                for bps, usd in tp_usd:
                    if first_hit[bps] is None and net >= usd:
                        first_hit[bps] = mins
                # Terminal exit: the convergence exit the live bot would take.
                if exc_arr[j] <= reclaim_bps:
                    terminal_net, terminal_min, terminal_reason = net, mins, "reclaim"
                    break
                terminal_net, terminal_min = net, mins
                j += 1

            if terminal_net is None:      # no forward bars at all
                i += 1
                continue

            trades.append({
                "symbol": sym, "direction": entered, "notional": notional,
                "entry_excess_bps": entry_excess,
                "max_net": max_net, "t_peak_min": t_peak,
                "terminal_net": terminal_net, "terminal_min": terminal_min,
                "terminal_reason": terminal_reason,
                "first_hit": first_hit,
            })
            i = j + 1
            streak_dir, streak_n = "", 0

    return trades


def score_tp(trades, tp_bps):
    """Outcome of applying ONE take-profit level to the shared entry set.

    tp_bps None = the no-TP baseline: hold to reclaim or the cap.
    """
    if not trades:
        return None
    nets, holds = [], []
    hits = rescued = given_back = 0
    forgone = 0.0
    for t in trades:
        notional = t["notional"]
        tp_usd = notional * tp_bps / 10000 if tp_bps is not None else None
        hit_min = t["first_hit"].get(tp_bps) if tp_bps is not None else None
        # Unreachable if the level is first touched only after the terminal exit.
        reachable = hit_min is not None and hit_min <= t["terminal_min"]
        if reachable:
            hits += 1
            nets.append(tp_usd)
            holds.append(hit_min)
            forgone += max(0.0, t["max_net"] - tp_usd)
            if t["terminal_net"] < 0:
                rescued += 1        # this TP converted a loser into a winner
        else:
            nets.append(t["terminal_net"])
            holds.append(t["terminal_min"])
            # Peaked into profit but wasn't captured and ended negative.
            if tp_bps is not None and t["max_net"] > 0 and t["terminal_net"] < 0:
                given_back += 1
    n = len(nets)
    wins = sum(1 for v in nets if v > 0)
    holds_sorted = sorted(holds)
    return {
        "tp_bps": tp_bps, "n": n, "hits": hits, "hit_pct": hits / n * 100,
        "total": sum(nets), "mean": sum(nets) / n,
        "win_pct": wins / n * 100,
        "med_hold": holds_sorted[len(holds_sorted) // 2],
        "rescued": rescued, "given_back": given_back, "forgone": forgone,
    }


def main():
    ap = argparse.ArgumentParser(description="1m candle backtest with profit-target exit")
    ap.add_argument("--hours", type=int, default=48, help="lookback hours (default 48)")
    ap.add_argument("--target", type=float, default=2.0, help="net P&L target USD (default 2.0)")
    ap.add_argument("--slots", type=int, default=MAX_CONCURRENT_POSITIONS, help="max concurrent positions")
    ap.add_argument("--max-hold", type=int, default=48*60, help="max hold minutes (default 2880 = 48h)")
    ap.add_argument("--confirm", type=int, default=ENTRY_CONFIRM_TICKS, help="confirm ticks (default from config)")
    ap.add_argument("--window", type=int, default=480, help="rolling baseline window in minutes (default 480 = 8h, matches live)")
    ap.add_argument("--sweep", action="store_true", help="sweep target from $1-$20 and print comparison")
    ap.add_argument("--window-sweep", action="store_true",
                    help="sweep baseline window (30m-24h) at fixed target/slots")
    ap.add_argument("--peak-analysis", action="store_true",
                    help="measure per-name peak reversion to calibrate per-name targets")
    ap.add_argument("--tp-sweep", action="store_true",
                    help="compare take-profit LEVELS (bps of notional) on one shared "
                         "entry set, incl. how many losses each TP rescues")
    ap.add_argument("--tp-grid", default="2,4,6,8,10,12,15,20,25,30,40,50",
                    help="TP levels in bps of notional (net of fees+crossing)")
    ap.add_argument("--tp-max-hold", type=int, default=240,
                    help="hold cap for --tp-sweep in minutes (default 240 = 4h; "
                         "funding is excluded so long caps overstate)")
    ap.add_argument("--reclaim-bps", type=float, default=0.0,
                    help="excess level counted as reverted for the no-TP exit "
                         "(default 0 = full baseline reclaim)")
    ap.add_argument("--per-symbol", action="store_true",
                    help="with --tp-sweep, also break the best TP down per name")
    args = ap.parse_args()

    async def run():
        async with aiohttp.ClientSession() as session:
            panels, overlap, hl_perps, aster_perps, tob_notional, tob_spread_bps = \
                await fetch_1m_candles(session, args.hours)

        if not panels:
            print("No data — check API connectivity")
            return

        # Show liquidity & spread snapshot
        print(f"\nTop-of-book snapshot (current):")
        print(f"  {'Symbol':8s} {'Notional':>10s}  {'Size':>8s}  {'RT Spread':>10s}")
        for sym in sorted(tob_notional, key=tob_notional.get, reverse=True):
            n = tob_notional[sym]
            sp = tob_spread_bps.get(sym, 0)
            if n > 0 and sym in panels:
                cap = "full" if n >= NOTIONAL_PER_LEG else f"${n:.0f}"
                print(f"  {sym:8s} ${n:>8.0f}  {cap:>8s}  {sp:>8.1f}bps")

        if args.window_sweep:
            windows = [30, 60, 120, 240, 480, 720, 1440]
            print(f"\n{'='*72}")
            print(f"WINDOW SWEEP: {args.hours}h | target=${args.target} | {args.slots} slots")
            print(f"(rolling-median baseline of book spread, varying window)")
            print(f"{'='*72}")
            print(f"  {'Window':>8s}  {'Trades':>6s}  {'Wins':>5s}  {'Win%':>5s}  "
                  f"{'AvgHold':>8s}  {'Gross':>8s}  {'Costs':>8s}  {'Net':>8s}  {'$/day':>7s}")
            print(f"  {'-'*8}  {'-'*6}  {'-'*5}  {'-'*5}  "
                  f"{'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*7}")
            for win in windows:
                apply_baseline(panels, win)
                trades, slot_min, total_min = backtest_portfolio(
                    panels, tob_notional, tob_spread_bps, args.target, args.slots,
                    args.max_hold, args.confirm,
                )
                label = f"{win}m" if win < 120 else f"{win//60}h"
                if not trades:
                    print(f"  {label:>8s}    0 trades")
                    continue
                df = pd.DataFrame(trades)
                completed = df[df["reason"] != "open"]
                n_closed = len(completed)
                wins = len(completed[completed["net"] > 0]) if n_closed > 0 else 0
                win_pct = wins / n_closed * 100 if n_closed > 0 else 0
                avg_hold = completed["hold_min"].mean() if n_closed > 0 else 0
                total_net = df["net"].sum()
                total_gross = df["gross"].sum()
                total_costs = df["fees"].sum() + df["crossing"].sum()
                per_day = total_net / (args.hours / 24)
                print(f"  {label:>8s}  {len(df):>6d}  {wins:>5d}  {win_pct:>4.0f}%  "
                      f"{avg_hold:>6.0f}m   ${total_gross:>7.2f}  ${total_costs:>7.2f}  "
                      f"${total_net:>7.2f}  ${per_day:>6.2f}")
            return

        # Apply the configured baseline window for all non-window-sweep runs
        apply_baseline(panels, args.window)

        if args.tp_sweep:
            grid = [float(x) for x in args.tp_grid.split(",") if x.strip()]
            trades = tp_sweep(panels, tob_notional, tob_spread_bps,
                              args.tp_max_hold, args.confirm, grid, args.reclaim_bps)
            if not trades:
                print("\nNo entry signals found for the TP sweep")
                return
            base = score_tp(trades, None)
            print(f"\n{'='*100}")
            print(f"TAKE-PROFIT LEVEL SWEEP: {args.hours}h | {args.window}m baseline | "
                  f"{len(trades)} entries | hold cap {args.tp_max_hold}m")
            print(f"no-TP exit = baseline reclaim (excess <= {args.reclaim_bps:g}bps) "
                  f"or hold cap. Funding excluded.")
            print(f"{'='*100}")
            hdr = (f"  {'TP':>6}  {'hit%':>6}  {'win%':>6}  {'total$':>9}  "
                   f"{'mean$':>8}  {'medHold':>8}  {'rescued':>8}  {'gaveBack':>9}  "
                   f"{'forgone$':>9}")
            print(hdr)
            print("  " + "-" * (len(hdr) - 2))
            rows = [base] + [score_tp(trades, tp) for tp in grid]
            for r in rows:
                lbl = "none" if r["tp_bps"] is None else f"{r['tp_bps']:g}bp"
                resc = "—" if r["tp_bps"] is None else f"{r['rescued']:d}"
                gb = f"{r['given_back']:d}" if r["tp_bps"] is not None else f"{sum(1 for t in trades if t['max_net'] > 0 and t['terminal_net'] < 0)}"
                forg = "—" if r["tp_bps"] is None else f"{r['forgone']:.2f}"
                print(f"  {lbl:>6}  {r['hit_pct']:>5.0f}%  {r['win_pct']:>5.0f}%  "
                      f"${r['total']:>8.2f}  ${r['mean']:>7.3f}  {r['med_hold']:>7.0f}m  "
                      f"{resc:>8}  {gb:>9}  {forg:>9}")
            print("  " + "-" * (len(hdr) - 2))
            n_gb = sum(1 for t in trades if t["max_net"] > 0 and t["terminal_net"] < 0)
            print(f"  {n_gb}/{len(trades)} entries ({n_gb/len(trades)*100:.0f}%) went "
                  f"POSITIVE at some point and still closed negative with no TP —")
            print(f"  that cohort is the entire prize a take-profit is competing for.")
            print(f"  rescued = losses this TP converts to wins | gaveBack = still "
                  f"peaked-positive-then-lost at this TP")
            print(f"  forgone = profit left on the table by capping at this TP")
            best = max(rows, key=lambda r: r["total"])
            bl = "no TP" if best["tp_bps"] is None else f"{best['tp_bps']:g}bps"
            print(f"\n  BEST TOTAL: {bl}  ${best['total']:.2f} "
                  f"(vs no-TP ${base['total']:.2f}, "
                  f"{best['total'] - base['total']:+.2f})")
            if args.per_symbol and best["tp_bps"] is not None:
                print(f"\n  Per-name at TP={bl}:")
                print(f"    {'SYM':<9}{'n':>4}{'hit%':>7}{'total$':>10}{'noTP$':>10}"
                      f"{'delta$':>9}")
                syms = sorted({t["symbol"] for t in trades})
                per = []
                for sym in syms:
                    sub = [t for t in trades if t["symbol"] == sym]
                    a = score_tp(sub, best["tp_bps"])
                    b = score_tp(sub, None)
                    per.append((sym, a, b))
                per.sort(key=lambda x: x[1]["total"] - x[2]["total"], reverse=True)
                for sym, a, b in per:
                    print(f"    {sym:<9}{a['n']:>4}{a['hit_pct']:>6.0f}%"
                          f"{a['total']:>10.2f}{b['total']:>10.2f}"
                          f"{a['total']-b['total']:>+9.2f}")
            return

        if args.peak_analysis:
            rows = peak_analysis(panels, tob_notional, tob_spread_bps,
                                 args.max_hold, args.confirm)
            if not rows:
                print("\nNo entry signals found for peak analysis")
                return
            pdf = pd.DataFrame(rows)
            print(f"\n{'='*86}")
            print(f"PEAK REVERSION ANALYSIS: {args.hours}h | {args.window}m baseline "
                  f"| {len(pdf)} independent entries")
            print(f"(max net P&L each trade reaches before reverting to baseline; "
                  f"funding excluded)")
            print(f"{'='*86}")
            print(f"  {'Symbol':8s} {'N':>3s}  {'peak$ med':>9s} {'p75':>7s} {'max':>7s}  "
                  f"{'peakbps':>8s}  {'t-peak':>7s}  {'rev%':>5s}  {'$ target':>9s}  {'size':>6s}")
            print(f"  {'-'*8} {'-'*3}  {'-'*9} {'-'*7} {'-'*7}  {'-'*8}  {'-'*7}  "
                  f"{'-'*5}  {'-'*9}  {'-'*6}")
            agg = pdf.groupby("symbol")
            suggested = {}
            # Sort by median peak descending
            order = agg["max_net"].median().sort_values(ascending=False).index
            for sym in order:
                g = pdf[pdf["symbol"] == sym]
                npk = len(g)
                med = g["max_net"].median()
                p75 = g["max_net"].quantile(0.75)
                mx = g["max_net"].max()
                pbps = g["peak_gross_bps"].median()
                tpk = g["t_peak_min"].median()
                revpct = g["reverted"].mean() * 100
                notional = g["notional"].iloc[0]
                # Target = 60% of median peak, floored at $3 (cost coverage),
                # rounded to nearest $0.50. Capture most of the move without
                # waiting so long that it un-reverts.
                tgt = max(3.0, round(med * 0.6 * 2) / 2)
                suggested[sym] = tgt
                print(f"  {sym:8s} {npk:>3d}  ${med:>7.2f} ${p75:>5.2f} ${mx:>5.2f}  "
                      f"{pbps:>6.0f}bp  {tpk:>5.0f}m  {revpct:>4.0f}%  ${tgt:>7.2f}  "
                      f"${notional:>5.0f}")

            print(f"\nSuggested per-name targets (EXIT_TARGET_NET_USD_BY_SYMBOL):")
            print("{")
            for sym in sorted(suggested, key=suggested.get, reverse=True):
                print(f'    "{sym}": {suggested[sym]:.1f},')
            print("}")
            return

        if args.sweep:
            sweep_targets = [1, 2, 3, 5, 8, 10, 15, 20]
            sweep_slots = [1, 2, 3, 5, 8]

            for slots in sweep_slots:
                print(f"\n{'='*70}")
                print(f"TARGET SWEEP: {args.hours}h | {slots} slots | per-symbol book spreads")
                print(f"{'='*70}")
                print(f"  {'Target':>7s}  {'Trades':>6s}  {'Wins':>5s}  {'Win%':>5s}  "
                      f"{'AvgHold':>8s}  {'Gross':>8s}  {'Costs':>8s}  {'Net':>8s}  {'$/day':>7s}")
                print(f"  {'-'*7}  {'-'*6}  {'-'*5}  {'-'*5}  "
                      f"{'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*7}")
                for tgt in sweep_targets:
                    trades, slot_min, total_min = backtest_portfolio(
                        panels, tob_notional, tob_spread_bps, tgt, slots,
                        args.max_hold, args.confirm,
                    )
                    if not trades:
                        print(f"  ${tgt:>5.0f}    0 trades")
                        continue
                    df = pd.DataFrame(trades)
                    completed = df[df["reason"] != "open"]
                    n_closed = len(completed)
                    wins = len(completed[completed["net"] > 0]) if n_closed > 0 else 0
                    win_pct = wins / n_closed * 100 if n_closed > 0 else 0
                    avg_hold = completed["hold_min"].mean() if n_closed > 0 else 0
                    total_net = df["net"].sum()
                    total_gross = df["gross"].sum()
                    total_costs = df["fees"].sum() + df["crossing"].sum()
                    per_day = total_net / (args.hours / 24)
                    print(f"  ${tgt:>5.0f}  {len(df):>6d}  {wins:>5d}  {win_pct:>4.0f}%  "
                          f"{avg_hold:>6.0f}m   ${total_gross:>7.2f}  ${total_costs:>7.2f}  "
                          f"${total_net:>7.2f}  ${per_day:>6.2f}")
            return

        trades, slot_min, total_min = backtest_portfolio(
            panels, tob_notional, tob_spread_bps, args.target, args.slots,
            args.max_hold, args.confirm,
        )

        if not trades:
            print("\nNo trades generated")
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
        total_crossing = df["crossing"].sum()
        total_funding = df["funding"].sum()
        avg_notional = df["notional"].mean()

        print(f"\n{'='*60}")
        print(f"BACKTEST: {args.hours}h of 1m data | target=${args.target}"
              f" | {args.slots} slots | {args.window}m baseline window")
        print(f"{'='*60}")
        print(f"Symbols with data: {len(panels)}")
        print(f"Total trades: {n} ({len(completed)} closed, {len(still_open)} still open)")
        print(f"  Target hits: {len(targets)}")
        print(f"  Timeouts:    {len(timeouts)}")
        if len(completed) > 0:
            win_rate = len(completed[completed["net"] > 0]) / len(completed) * 100
            print(f"  Win rate:    {win_rate:.0f}%")
            print(f"  Avg hold:    {completed['hold_min'].mean():.0f}min ({completed['hold_min'].mean()/60:.1f}h)")
        print(f"  Avg notional: ${avg_notional:.0f}")
        print()
        print(f"P&L breakdown:")
        print(f"  Gross:     ${total_gross:+.2f}")
        print(f"  Fees:      ${total_fees:.2f}")
        avg_bo = total_crossing / (avg_notional * n) * 10000 if (avg_notional * n) > 0 else 0
        print(f"  Crossing:  ${total_crossing:.2f}  (avg {avg_bo:.0f}bps from order books)")
        print(f"  Funding:   ${total_funding:+.2f}")
        print(f"  Net:       ${total_net:+.2f}")
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
            crossing=("crossing", "sum"),
            funding=("funding", "sum"),
            avg_hold=("hold_min", "mean"),
            avg_notional=("notional", "mean"),
        ).sort_values("net", ascending=False)
        for sym, row in sym_pnl.iterrows():
            sp = tob_spread_bps.get(sym, 0)
            print(f"  {sym:8s}  {int(row['trades'])}t  net=${row['net']:+7.2f}  "
                  f"gross=${row['gross']:+7.2f}  bo=${row['crossing']:5.2f}  "
                  f"sprd={sp:>5.0f}bp  ${row['avg_notional']:>5.0f}  avg={row['avg_hold']:.0f}min")

        # Time-of-day distribution (ET = UTC-4 during EDT)
        from datetime import datetime, timezone, timedelta
        ET = timezone(timedelta(hours=-4))
        df["entry_hour_et"] = df["entry_ts"].apply(
            lambda ms: datetime.fromtimestamp(ms / 1000, tz=ET).hour
        )
        hourly = df.groupby("entry_hour_et").agg(
            trades=("net", "count"),
            net=("net", "sum"),
        )
        print(f"\nEntry time-of-day (ET):")
        print(f"  US market hours: 09:30-16:00 ET")
        mkt_mask = df["entry_hour_et"].between(9, 15)
        mkt_trades = mkt_mask.sum()
        off_trades = len(df) - mkt_trades
        mkt_net = df.loc[mkt_mask, "net"].sum()
        off_net = df.loc[~mkt_mask, "net"].sum()
        print(f"  Market hours:  {mkt_trades} trades  ${mkt_net:+.2f} net")
        print(f"  Off hours:     {off_trades} trades  ${off_net:+.2f} net")
        print()
        max_bar = 30
        max_count = hourly["trades"].max() if len(hourly) > 0 else 1
        for hour in range(24):
            if hour in hourly.index:
                cnt = int(hourly.loc[hour, "trades"])
                net = hourly.loc[hour, "net"]
                bar_len = int(cnt / max_count * max_bar)
                bar = "█" * bar_len
                mkt = " *" if 9 <= hour <= 15 else "  "
                print(f"  {hour:02d}:00{mkt} {bar:>{max_bar}s}  {cnt:>2d}t  ${net:+.2f}")
            else:
                mkt = " *" if 9 <= hour <= 15 else "  "
                print(f"  {hour:02d}:00{mkt} {'':>{max_bar}s}   0t")

    asyncio.run(run())


if __name__ == "__main__":
    main()

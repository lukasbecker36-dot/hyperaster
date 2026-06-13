"""
Live monitor for equity perp arb: AsterDEX vs Hyperliquid XYZ.

Watches all overlapping equity perps simultaneously.
Uses Aster GTX (post-only maker) + Hyperliquid IOC (taker).

Usage:
    python live_monitor.py --paper       # paper mode (default)
    python live_monitor.py --live        # live mode (real orders)
    python live_monitor.py --symbols AAPL NVDA TSLA  # specific symbols only
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

# UTF-8 console output on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from auth import load_api_keys, now_ms
from config import (
    POLL_INTERVAL_SECONDS, ASTER_FILL_POLL_SECONDS,
    ENTRY_THRESHOLD_BPS, ENTRY_THRESHOLD_BPS_BY_SYMBOL,
    EXIT_THRESHOLD_BPS, MAX_HOLD_HOURS, MAX_CONCURRENT_POSITIONS,
    HEARTBEAT_INTERVAL_MINUTES, PAPER_MODE, DATA_DIR, OUTPUT_DIR,
    BLOCKED_SYMBOLS, ENTRY_CONFIRM_TICKS, ADVERSE_STOP_BPS,
    ROUND_TRIP_FEE, NOTIONAL_PER_LEG, EXIT_TARGET_NET_USD,
    EXIT_TARGET_NET_USD_BY_SYMBOL, aster_symbol_for,
)

SLOW_SCAN_INTERVAL_SECONDS = 300   # re-rank all symbols every 5 min
FAST_CANDIDATES = 3                # symbols to poll every tick between slow scans
from database import init_db
from exchange_client import ExchangeClient
from position_manager import PositionManager, estimate_funding_pnl
from executor import Executor
from notify import install_handler as install_alert_handler
from recovery import reconcile_incomplete_intents

# ── Logging ──

def setup_logging():
    os.makedirs(DATA_DIR, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = RotatingFileHandler(
        os.path.join(DATA_DIR, "monitor.log"),
        maxBytes=10 * 1024 * 1024, backupCount=7,
    )
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    ch.setLevel(logging.INFO)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(fh)
    root.addHandler(ch)


log = logging.getLogger(__name__)


def load_symbols(override: list[str] | None) -> list[dict]:
    """Load the overlapping equity perp universe from fetch_data output."""
    if override:
        return [{"coin": s, "hl_coin": f"xyz:{s}", "aster_symbol": aster_symbol_for(s)}
                for s in override]

    overlap_path = Path(OUTPUT_DIR) / "overlap_symbols.csv"
    if not overlap_path.exists():
        log.error(
            "No overlap_symbols.csv found. Run fetch_data.py first to discover "
            "overlapping equity perps."
        )
        sys.exit(1)

    import pandas as pd
    df = pd.read_csv(overlap_path)
    symbols = df.to_dict("records")
    log.info(f"Loaded {len(symbols)} symbols from overlap_symbols.csv")
    return symbols


_SPREADS_FILE = os.path.join(DATA_DIR, "latest_spreads.json")

def _write_latest_spreads(spreads: dict[str, tuple[float, str, float]]):
    """Atomically write current excess/baseline per symbol for control bot."""
    tmp = _SPREADS_FILE + ".tmp"
    try:
        data = {}
        for sym, (exc, d, b) in spreads.items():
            if d == "L-HL/S-AST":
                hl_short_aster_excess = exc
            else:
                hl_short_aster_excess = -exc
            data[sym] = {
                "excess": round(exc, 1),
                "hl_excess": round(hl_short_aster_excess, 1),
                "direction": d,
                "baseline": round(b, 1),
            }
        data["_ts"] = time.time()
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _SPREADS_FILE)
    except Exception:
        pass


async def run_monitor(paper_mode: bool, symbol_filter: list[str] | None):
    symbol_rows = load_symbols(symbol_filter)
    symbols = [r["coin"] for r in symbol_rows if r["coin"] not in BLOCKED_SYMBOLS]
    blocked = [r["coin"] for r in symbol_rows if r["coin"] in BLOCKED_SYMBOLS]
    if blocked:
        log.info(f"Excluded blocked symbols: {blocked}")

    log.info("=" * 70)
    log.info("EQUITY PERP ARB MONITOR — Aster vs Hyperliquid XYZ")
    log.info(f"Mode:         {'PAPER' if paper_mode else 'LIVE'}")
    log.info(f"Symbols ({len(symbols)}): {symbols}")
    log.info(f"Poll:         {POLL_INTERVAL_SECONDS}s main | {ASTER_FILL_POLL_SECONDS}s Aster fill poll")
    log.info(f"Max positions: {MAX_CONCURRENT_POSITIONS}")
    log.info("=" * 70)

    init_db()

    try:
        api_keys = load_api_keys()
    except EnvironmentError as e:
        if paper_mode:
            log.warning(f"API keys missing ({e}) — OK for paper mode")
            api_keys = {
                "aster_api_key": "", "aster_api_secret": "",
                "aster_wallet_address": "", "aster_signer_address": "",
                "hl_private_key": "", "hl_wallet_address": "",
            }
        else:
            log.error(f"Cannot start live: {e}")
            return

    # Include symbols with open positions even if blocked (need specs for exit)
    pm_preload = PositionManager(paper_mode=paper_mode)
    open_pos_syms = set(pm_preload.positions.keys())
    blocked_with_positions = open_pos_syms & BLOCKED_SYMBOLS
    if blocked_with_positions:
        log.info(f"Blocked symbols with open positions (will scan for exit): {blocked_with_positions}")
    all_load_syms = list(dict.fromkeys(symbols + list(blocked_with_positions)))

    client = ExchangeClient(api_keys)
    await client.start(all_load_syms)

    # Verify all specs loaded
    missing = [s for s in symbols if s not in client.aster_specs or s not in client.hl_specs]
    if missing:
        log.warning(f"Specs not loaded for: {missing} — these will be skipped")
        symbols = [s for s in symbols if s not in missing]

    if not symbols:
        log.error("No valid symbols. Exiting.")
        return

    # Seed the rolling book-spread baselines from 1m candles so entries can fire
    # immediately instead of waiting BASELINE_MIN_SAMPLES minutes of live data.
    await client.warmup_book_spread(all_load_syms)

    pm = pm_preload
    executor = Executor(client, pm, paper_mode=paper_mode)

    # Crash recovery: reconcile any uncompleted intents against venue state.
    # MUST run before the main loop so we don't start trading on top of an
    # unknown state. Refuses to start if anything truly ambiguous is found.
    if not paper_mode:
        try:
            report = await reconcile_incomplete_intents(client, pm, paper_mode)
        except Exception as e:
            log.critical(f"Crash recovery failed: {e} — REFUSING TO START")
            await client.close()
            return
        if report.auto_recovered:
            log.warning(f"Auto-recovered positions: {report.auto_recovered}")
        if report.no_fills:
            log.info(f"Intents closed as no_fill: {report.no_fills}")
        if report.requires_manual:
            log.critical(
                f"REFUSING TO START — symbols require manual intervention: "
                f"{report.requires_manual}. Run flatten.py --symbols "
                f"{' '.join(report.requires_manual)} --reconcile, then clear "
                f"'error' rows from positions table."
            )
            await client.close()
            return

    # Log any positions found in DB after recovery
    if pm.positions:
        for sym, pos in pm.positions.items():
            log.warning(
                f"Crash recovery: {sym} position #{pos.id} status={pos.status} "
                f"qty={pos.qty} direction={pos.direction}"
            )

    start_time = now_ms()
    last_heartbeat = start_time
    last_aster_poll = 0
    last_slow_scan = 0
    tick_count = 0
    consecutive_errors = 0
    # symbol -> (excess_bps, direction_str, baseline_bps) from last scan
    latest_spreads: dict[str, tuple[float, str, float]] = {}
    # top N candidates polled every fast tick
    candidates: list[str] = []

    async def scan_symbol(symbol: str) -> tuple[str, float, str, float, object, object] | None:
        """Fetch books and return (symbol, executable_excess_bps, direction, baseline_bps, aster_book, hl_book).

        executable_excess_bps is the book mid-spread's deviation from its rolling
        baseline — what try_entry actually checks. May be negative when the
        current spread sits at or below its own structural baseline.
        """
        try:
            aster_book, hl_book = await client.get_both_books(symbol)
            if aster_book.bid <= 0 or hl_book.bid <= 0:
                return None
            mid = (aster_book.mid + hl_book.mid) / 2
            if mid <= 0:
                return None

            # Match try_entry exactly: deviation of book mid-spread from its
            # rolling baseline. Record the sample, then read the baseline.
            spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
            client.record_book_spread(symbol, spread_bps)
            baseline = client.get_book_spread_baseline(symbol)
            # No baseline yet (cold start) — return None so the protocol skips it.
            if baseline is None:
                return None
            aster_excess = spread_bps - baseline    # long_hl_short_aster
            hl_excess    = -(spread_bps - baseline)  # long_aster_short_hl
            if aster_excess >= hl_excess:
                executable_excess = aster_excess
                direction = "L-HL/S-AST"
            else:
                executable_excess = hl_excess
                direction = "L-AST/S-HL"
            return symbol, executable_excess, direction, baseline, aster_book, hl_book
        except Exception as e:
            log.debug(f"scan_symbol {symbol}: {e}")
            return None

    async def process_result(result):
        """Act on a scan result — check entry/exit conditions."""
        if result is None:
            return
        symbol, executable_excess_bps, direction, baseline_bps, aster_book, hl_book = result
        latest_spreads[symbol] = (executable_excess_bps, direction, baseline_bps)
        pos = pm.get(symbol)
        nonlocal consecutive_errors
        try:
            if pos and pos.status == "open":
                elapsed_hours = (now_ms() - pos.entry_time) / 3_600_000
                # Evaluate exits on the POSITION'S OWN direction, not the scan's
                # best-of-both `executable_excess_bps`. Using the max meant a
                # reversed position wasn't exited until the *opposite* direction
                # also calmed down — letting losses run (CBRS ran to -24.7bps).
                mid = (aster_book.mid + hl_book.mid) / 2
                # Estimate net P&L at current book prices (gross - fees + funding)
                if mid > 0 and pos.direction == "long_hl_short_aster":
                    est_gross = ((hl_book.bid - pos.hl_entry_price)
                                 + (pos.aster_entry_price - aster_book.ask)) * pos.qty
                elif mid > 0:
                    est_gross = ((pos.hl_entry_price - hl_book.ask)
                                 + (aster_book.bid - pos.aster_entry_price)) * pos.qty
                else:
                    est_gross = 0.0
                est_fees = (pos.notional_usd or NOTIONAL_PER_LEG) * ROUND_TRIP_FEE
                est_funding = estimate_funding_pnl(
                    pos.direction, elapsed_hours,
                    pos.notional_usd or NOTIONAL_PER_LEG,
                    pos.hl_funding_rate, pos.aster_funding_rate,
                )
                est_net = est_gross - est_fees + est_funding

                should_exit, reason = False, ""
                sym_target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(symbol, EXIT_TARGET_NET_USD)
                if est_net >= sym_target:
                    should_exit, reason = True, "target"
                elif symbol in BLOCKED_SYMBOLS:
                    should_exit, reason = True, "blocked"
                elif elapsed_hours >= MAX_HOLD_HOURS:
                    should_exit, reason = True, "timeout"
                else:
                    # Convergence exit: spread has reverted to the baseline that
                    # existed AT ENTRY TIME. Using the current rolling baseline
                    # would let the goalposts shift — a spread that stays wide for
                    # 8h+ gets absorbed into the rolling median, making excess drop
                    # to 0 even though the spread never actually reverted.
                    entry_base = pos.entry_baseline_bps
                    if not entry_base:
                        log.warning(f"{symbol}: entry_baseline_bps=0 — skipping convergence check")
                    else:
                        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
                        raw_excess = spread_bps - entry_base
                        pos_dir = pos.direction or "long_hl_short_aster"
                        own_excess = raw_excess if pos_dir == "long_hl_short_aster" else -raw_excess
                        if own_excess <= 0 and elapsed_hours >= 0.5:
                            log.info(
                                f"CONVERGE {symbol}: own_excess={own_excess:.1f} "
                                f"spread={spread_bps:.1f} entry_base={entry_base:.1f} "
                                f"dir={pos_dir} held={elapsed_hours:.1f}h "
                                f"HL={hl_book.mid:.2f} Ast={aster_book.mid:.2f}"
                            )
                            should_exit, reason = True, "converge"
                if should_exit:
                    await executor.try_exit(symbol, aster_book, hl_book, reason)
            elif not pos:
                if pm.active_count < MAX_CONCURRENT_POSITIONS:
                    await executor.try_entry(symbol, aster_book, hl_book)
        except Exception as e:
            log.error(f"Error processing {symbol}: {e}")
            consecutive_errors += 1

    try:
        while True:
            tick_start = time.time()
            tick_count += 1
            now = now_ms()

            # ── 1. Poll Aster maker orders (entering/exiting) ──
            if now - last_aster_poll >= ASTER_FILL_POLL_SECONDS * 1000:
                last_aster_poll = now
                pending = [s for s, p in pm.positions.items()
                           if p.status in ("entering", "exiting")]
                if pending and not paper_mode:
                    await asyncio.gather(*[executor.poll_aster_maker(s) for s in pending])

            # ── 2. Slow scan: rank all symbols every 5 min ──
            if now - last_slow_scan >= SLOW_SCAN_INTERVAL_SECONDS * 1000:
                last_slow_scan = now
                await client.refresh_hl_oracles(symbols)
                all_results = await asyncio.gather(*[scan_symbol(s) for s in symbols])
                open_syms = set(pm.positions.keys())
                ranked = sorted(
                    [r for r in all_results if r and r[0] not in open_syms],
                    key=lambda r: r[1] / ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(r[0], ENTRY_THRESHOLD_BPS),
                    reverse=True,
                )
                candidates = [r[0] for r in ranked[:FAST_CANDIDATES]]
                # Candidates + open positions are re-processed in the fast tick
                # below; skip them here so the entry persistence streak isn't
                # double-counted within a single loop iteration.
                fast_set = set(candidates) | set(open_syms)
                for r in all_results:
                    if r and r[0] not in fast_set:
                        await process_result(r)
                thresh_strs = " | ".join(
                    f"{s} {spd:.0f}/{ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(s, ENTRY_THRESHOLD_BPS):.0f}bps base={base:+.0f}"
                    for s, spd, _, base, *_ in ranked[:5]
                )
                log.info(f"Slow scan | Watching: {candidates} | Top 5: {thresh_strs}")

            # ── 3. Fast tick: poll candidates + open positions ──
            open_syms = set(pm.positions.keys())
            fast_syms = list(dict.fromkeys(list(open_syms) + candidates))
            fast_results = await asyncio.gather(*[scan_symbol(s) for s in fast_syms])
            for r in fast_results:
                await process_result(r)

            # ── 3b. Persist latest spreads for control bot ──
            _write_latest_spreads(latest_spreads)

            # ── 4. Periodic tick log ──
            if tick_count % 20 == 0:
                pos_str = ""
                if pm.positions:
                    pos_str = " | " + ", ".join(
                        f"{s}[{p.status} {(now_ms()-p.entry_time)/3_600_000:.1f}h]"
                        for s, p in pm.positions.items()
                    )
                cand_parts = []
                for s in candidates:
                    spd = latest_spreads.get(s, (0, '', 0.0))[0]
                    thr = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(s, ENTRY_THRESHOLD_BPS)
                    _, streak = executor._entry_streak.get(s, ("", 0))
                    if spd >= thr and streak > 0:
                        cand_parts.append(f"{s} {spd:.0f}bps({streak}/{ENTRY_CONFIRM_TICKS})")
                    else:
                        pct = spd / thr * 100 if thr > 0 else 0
                        cand_parts.append(f"{s} {pct:.0f}%")
                cand_str = "  ".join(cand_parts) or "pending scan"
                log.info(
                    f"Tick {tick_count} | Active: {pm.active_count}{pos_str} | "
                    f"Watching: {cand_str}"
                )
                consecutive_errors = 0

            # ── 5. Heartbeat ──
            elapsed_hb = (now_ms() - last_heartbeat) / 60_000
            if elapsed_hb >= HEARTBEAT_INTERVAL_MINUTES:
                uptime = (now_ms() - start_time) / 60_000

                # Open positions summary
                pos_lines = []
                for sym, pos in pm.positions.items():
                    elapsed_h = (now_ms() - pos.entry_time) / 3_600_000
                    cur_spread = latest_spreads.get(sym, (0.0, "", 0.0))[0]
                    pos_lines.append(
                        f"  {sym} [{pos.status}] entry={pos.entry_spread_bps:.1f}bps "
                        f"now={cur_spread:.1f}bps hold={elapsed_h:.1f}h"
                    )

                # Top 5 symbols closest to entry threshold (no open position)
                open_syms = set(pm.positions.keys())
                spread_ranking = [
                    (sym, spd, drn, base)
                    for sym, (spd, drn, base) in latest_spreads.items()
                    if sym not in open_syms
                ]
                spread_ranking.sort(key=lambda x: x[1], reverse=True)
                watch_lines = []
                for sym, spd, drn, base in spread_ranking[:5]:
                    thr = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(sym, ENTRY_THRESHOLD_BPS)
                    _, streak = executor._entry_streak.get(sym, ("", 0))
                    if spd >= thr and streak > 0:
                        proximity = f"{spd:.0f}bps({streak}/{ENTRY_CONFIRM_TICKS})"
                    else:
                        pct = spd / thr * 100 if thr > 0 else 0
                        proximity = f"{pct:.0f}%"
                    watch_lines.append(
                        f"  {sym}: {proximity}  base={base:+.0f}bps  ({drn})"
                    )

                log.info(
                    f"HEARTBEAT | uptime={uptime:.0f}min | ticks={tick_count} | "
                    f"positions={pm.active_count}/{MAX_CONCURRENT_POSITIONS} | "
                    f"threshold={ENTRY_THRESHOLD_BPS:.0f}bps\n"
                    + (("  Open positions:\n" + "\n".join(pos_lines) + "\n") if pos_lines else "  No open positions\n")
                    + "  Closest to entry:\n" + ("\n".join(watch_lines) if watch_lines else "  (no data yet)")
                )
                last_heartbeat = now_ms()

            elapsed = time.time() - tick_start
            await asyncio.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))

    except KeyboardInterrupt:
        log.info("Stopped by user (Ctrl+C)")
    finally:
        await client.close()
        log.info("Monitor shut down")


def main():
    parser = argparse.ArgumentParser(description="Equity Perp Arb Monitor (Aster vs HL)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--paper", action="store_true")
    mode.add_argument("--live", action="store_true")
    parser.add_argument("--symbols", nargs="*", help="Override symbols (e.g. AAPL NVDA)")
    args = parser.parse_args()

    if args.paper:
        paper_mode = True
    elif args.live:
        paper_mode = False
    else:
        # No explicit flag — fall back to HYPERASTER_MODE env var (set by the
        # control bot's mode file via systemd EnvironmentFile), then config.
        env_mode = os.getenv("HYPERASTER_MODE", "").strip().lower()
        if env_mode == "live":
            paper_mode = False
        elif env_mode == "paper":
            paper_mode = True
        else:
            paper_mode = PAPER_MODE

    setup_logging()
    install_alert_handler()

    try:
        asyncio.run(run_monitor(paper_mode, args.symbols))
    except Exception as e:
        log.exception(f"Fatal error: {e}")


if __name__ == "__main__":
    main()

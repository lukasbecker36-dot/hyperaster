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
    BLOCKED_SYMBOLS, ADVERSE_STOP_BPS, ENTRY_CONFIRM_TICKS,
    ROUND_TRIP_FEE, NOTIONAL_PER_LEG, aster_symbol_for,
)

SLOW_SCAN_INTERVAL_SECONDS = 300   # re-rank all symbols every 5 min
FAST_CANDIDATES = 3                # symbols to poll every tick between slow scans
from database import init_db
from exchange_client import ExchangeClient
from position_manager import PositionManager
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

    client = ExchangeClient(api_keys)
    await client.start(symbols)

    # Verify all specs loaded
    missing = [s for s in symbols if s not in client.aster_specs or s not in client.hl_specs]
    if missing:
        log.warning(f"Specs not loaded for: {missing} — these will be skipped")
        symbols = [s for s in symbols if s not in missing]

    if not symbols:
        log.error("No valid symbols. Exiting.")
        return

    pm = PositionManager(paper_mode=paper_mode)
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
    # symbol -> (excess_bps, direction_str, oracle_delta_bps) from last scan
    latest_spreads: dict[str, tuple[float, str, float]] = {}
    # top N candidates polled every fast tick
    candidates: list[str] = []

    async def scan_symbol(symbol: str) -> tuple[str, float, str, float, object, object] | None:
        """Fetch books and return (symbol, executable_excess_bps, direction, smoothed_delta_bps, aster_book, hl_book).

        executable_excess_bps uses bid-to-other-ask prices — what try_entry
        actually checks, not the optimistic mid-to-mid value. May be negative
        when neither direction has a positive edge after crossing cost.
        """
        try:
            aster_book, hl_book = await client.get_both_books(symbol)
            if aster_book.bid <= 0 or hl_book.bid <= 0:
                return None
            mid = (aster_book.mid + hl_book.mid) / 2
            if mid <= 0:
                return None

            aster_index = client.get_aster_index(symbol)
            hl_oracle = client.get_hl_oracle(symbol)
            # If either oracle is missing, return None — the scan-result protocol
            # already treats None as "skip". Far safer than substituting delta=0
            # and letting structural feed gap masquerade as tradeable edge.
            if aster_index <= 0 or hl_oracle <= 0:
                return None
            oracle_delta_bps = (aster_index - hl_oracle) / mid * 10000
            client.record_oracle_delta(symbol, oracle_delta_bps)
            smoothed_delta = client.get_smoothed_oracle_delta(symbol, oracle_delta_bps)

            # Match try_entry exactly: bid-to-other-ask, less smoothed delta.
            aster_premium_bps = (aster_book.bid - hl_book.ask) / mid * 10000
            hl_premium_bps    = (hl_book.bid - aster_book.ask) / mid * 10000
            aster_excess = aster_premium_bps - smoothed_delta   # long_hl_short_aster
            hl_excess    = hl_premium_bps    + smoothed_delta   # long_aster_short_hl
            if aster_excess >= hl_excess:
                executable_excess = aster_excess
                direction = "L-HL/S-AST"
            else:
                executable_excess = hl_excess
                direction = "L-AST/S-HL"
            return symbol, executable_excess, direction, smoothed_delta, aster_book, hl_book
        except Exception as e:
            log.debug(f"scan_symbol {symbol}: {e}")
            return None

    async def process_result(result):
        """Act on a scan result — check entry/exit conditions."""
        if result is None:
            return
        symbol, executable_excess_bps, direction, smoothed_delta_bps, aster_book, hl_book = result
        latest_spreads[symbol] = (executable_excess_bps, direction, smoothed_delta_bps)
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
                if mid > 0 and pos.direction == "long_hl_short_aster":
                    own_excess = (aster_book.bid - hl_book.ask) / mid * 10000 - smoothed_delta_bps
                elif mid > 0:
                    own_excess = (hl_book.bid - aster_book.ask) / mid * 10000 + smoothed_delta_bps
                else:
                    own_excess = executable_excess_bps
                should_exit, reason = False, ""
                if own_excess <= -ADVERSE_STOP_BPS:
                    # Edge inverted hard — phantom entry that reversed. Bail now.
                    should_exit, reason = True, "stop"
                elif own_excess <= EXIT_THRESHOLD_BPS:
                    # Oracle-adjusted spread compressed — but only exit if the
                    # actual P&L at current prices covers fees. Without this
                    # guard, oracle delta shifts create phantom "convergence" and
                    # the position is closed at a loss.
                    if mid > 0 and pos.direction == "long_hl_short_aster":
                        est_hl = (hl_book.bid - pos.hl_entry_price) * pos.qty
                        est_ast = (pos.aster_entry_price - aster_book.ask) * pos.qty
                    elif mid > 0:
                        est_hl = (pos.hl_entry_price - hl_book.ask) * pos.qty
                        est_ast = (aster_book.bid - pos.aster_entry_price) * pos.qty
                    else:
                        est_hl, est_ast = 0.0, 0.0
                    est_gross = est_hl + est_ast
                    est_fees = (pos.notional_usd or NOTIONAL_PER_LEG) * ROUND_TRIP_FEE
                    if est_gross >= est_fees:
                        should_exit, reason = True, "converged"
                    else:
                        log.debug(
                            f"{symbol}: excess={own_excess:.1f}bps below exit threshold "
                            f"but est net=${est_gross - est_fees:.2f} < 0 — holding"
                        )
                elif elapsed_hours >= MAX_HOLD_HOURS:
                    should_exit, reason = True, "timeout"
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
                    f"{s} {spd:.0f}/{ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(s, ENTRY_THRESHOLD_BPS):.0f}bps d={odelta:+.0f}"
                    for s, spd, _, odelta, *_ in ranked[:5]
                )
                log.info(f"Slow scan | Watching: {candidates} | Top 5: {thresh_strs}")

            # ── 3. Fast tick: poll candidates + open positions ──
            open_syms = set(pm.positions.keys())
            fast_syms = list(dict.fromkeys(list(open_syms) + candidates))
            fast_results = await asyncio.gather(*[scan_symbol(s) for s in fast_syms])
            for r in fast_results:
                await process_result(r)

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
                    (sym, spd, drn, odelta)
                    for sym, (spd, drn, odelta) in latest_spreads.items()
                    if sym not in open_syms
                ]
                spread_ranking.sort(key=lambda x: x[1], reverse=True)
                watch_lines = []
                for sym, spd, drn, odelta in spread_ranking[:5]:
                    thr = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(sym, ENTRY_THRESHOLD_BPS)
                    _, streak = executor._entry_streak.get(sym, ("", 0))
                    if spd >= thr and streak > 0:
                        proximity = f"{spd:.0f}bps({streak}/{ENTRY_CONFIRM_TICKS})"
                    else:
                        pct = spd / thr * 100 if thr > 0 else 0
                        proximity = f"{pct:.0f}%"
                    watch_lines.append(
                        f"  {sym}: {proximity}  d={odelta:+.0f}bps  ({drn})"
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

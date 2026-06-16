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
    BLOCKED_SYMBOLS, NON_EQUITY_SYMBOLS, ENTRY_CONFIRM_TICKS, ADVERSE_STOP_BPS,
    ROUND_TRIP_FEE, CARRY_ROUND_TRIP_FEE, NOTIONAL_PER_LEG, EXIT_TARGET_NET_USD,
    EXIT_TARGET_NET_USD_BY_SYMBOL, MAX_FUNDING_DRAG_USD,
    FUNDING_ADVERSE_STOP_USD,
    MANUAL_ENTRY_GATE_TIMEOUT_MIN, aster_symbol_for,
)

SLOW_SCAN_INTERVAL_SECONDS = 300   # re-rank all symbols every 5 min
FAST_CANDIDATES = 3                # symbols to poll every tick between slow scans
from database import init_db
from exchange_client import ExchangeClient
from position_manager import PositionManager, estimate_funding_pnl
from executor import Executor
from notify import install_handler as install_alert_handler, send_alert
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
# Runtime auto-entry kill switch. When this file contains "off" the monitor runs
# (and manages exits + manual entries) but never auto-opens a basis arb. Lets you
# go live for a manual funding trade without the auto-scanner also trading live.
_AUTO_ENTRY_FILE = os.path.join(DATA_DIR, "auto_entry")
# Manual command inbox: the control bot drops one JSON file per request here
# ({"action":"enter","symbol":..,"direction":..,"notional":..} or
# {"action":"close","symbol":..}). The monitor is the single order-placing
# process, so all manual entries/exits are funnelled through it (no races).
_MANUAL_CMD_DIR = os.path.join(DATA_DIR, "manual_cmds")
# Snapshot of basis-gated orders still waiting for their target, for /positions.
_PENDING_GATES_FILE = os.path.join(DATA_DIR, "pending_gates.json")


def _write_pending_gates(pending_entries: dict, pending_exits: dict):
    """Persist waiting basis-gated orders so the control bot can show them."""
    tmp = _PENDING_GATES_FILE + ".tmp"
    try:
        data = {
            "entries": {
                s: {"direction": r["direction"], "notional": r["notional"],
                    "orig_notional": r.get("orig_notional", r["notional"]),
                    "target_bps": r["target_bps"]}
                for s, r in pending_entries.items()
            },
            "exits": {s: {"target_bps": r["target_bps"]} for s, r in pending_exits.items()},
            "_ts": time.time(),
        }
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _PENDING_GATES_FILE)
    except Exception:
        pass


def _auto_entry_enabled() -> bool:
    """Read the runtime auto-entry flag. Missing/unreadable file = enabled (default)."""
    try:
        return Path(_AUTO_ENTRY_FILE).read_text().strip().lower() != "off"
    except FileNotFoundError:
        return True
    except Exception:
        return True


def _carry_basis_bps(direction: str, action: str, aster_book, hl_book):
    """Executable basis (bps) in the position's FAVOUR for the given action.

    Uses the HL-maker / Aster-taker convention (HL is more liquid so we
    rest there, cross on Aster). Matches /funding display. Higher = better.

      Maker buy  HL @ hl_bid  |  Maker sell HL @ hl_ask
      Taker buy Ast @ ast_ask |  Taker sell Ast @ ast_bid

      buy-HL leg  (enter L-HL/S-AST or exit L-AST/S-HL):
          buy HL @ bid (maker), sell Aster @ bid (taker)
          = (aster_bid - hl_bid) / mid
      buy-AST leg (enter L-AST/S-HL or exit L-HL/S-AST):
          sell HL @ ask (maker), buy Aster @ ask (taker)
          = (hl_ask - aster_ask) / mid
    """
    mid = (aster_book.mid + hl_book.mid) / 2
    if mid <= 0:
        return None
    buy_hl_leg = (direction == "long_hl_short_aster") == (action == "enter")
    if buy_hl_leg:
        return (aster_book.bid - hl_book.bid) / mid * 10000
    return (hl_book.ask - aster_book.ask) / mid * 10000

def _write_latest_spreads(spreads: dict[str, tuple[float, str, float]],
                          est_net: dict[str, float] | None = None):
    """Atomically write current excess/baseline per symbol for control bot."""
    tmp = _SPREADS_FILE + ".tmp"
    est_net = est_net or {}
    try:
        data = {}
        for sym, (exc, d, b) in spreads.items():
            if d == "L-HL/S-AST":
                hl_short_aster_excess = exc
            else:
                hl_short_aster_excess = -exc
            entry = {
                "excess": round(exc, 1),
                "hl_excess": round(hl_short_aster_excess, 1),
                "direction": d,
                "baseline": round(b, 1),
            }
            if sym in est_net:
                entry["est_net"] = round(est_net[sym], 2)
            data[sym] = entry
        data["_ts"] = time.time()
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _SPREADS_FILE)
    except Exception:
        pass


async def run_monitor(paper_mode: bool, symbol_filter: list[str] | None):
    symbol_rows = load_symbols(symbol_filter)
    exclude = BLOCKED_SYMBOLS | NON_EQUITY_SYMBOLS
    symbols = [r["coin"] for r in symbol_rows if r["coin"] not in exclude]
    blocked = [r["coin"] for r in symbol_rows if r["coin"] in exclude]
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
                "hl_account_address": "",
            }
        else:
            log.error(f"Cannot start live: {e}")
            return

    # Include symbols with open positions even if blocked (need specs for exit)
    pm_preload = PositionManager(paper_mode=paper_mode)
    open_pos_syms = set(pm_preload.positions.keys())
    blocked_with_positions = open_pos_syms & exclude
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
    # symbol -> est_net USD (executable bid/ask P&L) for open positions
    latest_est_net: dict[str, float] = {}
    # Runtime flags refreshed once per tick from their control files.
    runtime_flags = {"auto_entry": True}
    # Basis-gated manual orders waiting for a good fill level.
    #   pending_entries: symbol -> {direction, notional, target_bps, expires_ms}
    #   pending_exits:   symbol -> {target_bps}
    pending_entries: dict[str, dict] = {}
    pending_exits: dict[str, dict] = {}
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
                # Estimate net P&L at current book prices (gross - fees + funding).
                # Maker-first positions (carry + convergence) mark on HL-maker/
                # Aster-taker exit prices; legacy taker positions use taker-taker
                # (conservative, both legs cross).
                maker_first = pos.entry_maker_venue == "hl"
                if mid > 0 and pos.direction == "long_hl_short_aster":
                    hl_exit = hl_book.ask if maker_first else hl_book.bid
                    est_gross = ((hl_exit - pos.hl_entry_price)
                                 + (pos.aster_entry_price - aster_book.ask)) * pos.qty
                elif mid > 0:
                    hl_exit = hl_book.bid if maker_first else hl_book.ask
                    est_gross = ((pos.hl_entry_price - hl_exit)
                                 + (aster_book.bid - pos.aster_entry_price)) * pos.qty
                else:
                    est_gross = 0.0
                rt_fee = CARRY_ROUND_TRIP_FEE if maker_first else ROUND_TRIP_FEE
                est_fees = (pos.notional_usd or NOTIONAL_PER_LEG) * rt_fee
                est_funding = estimate_funding_pnl(
                    pos.direction, elapsed_hours,
                    pos.notional_usd or NOTIONAL_PER_LEG,
                    pos.hl_funding_rate, pos.aster_funding_rate,
                )
                est_net = est_gross - est_fees + est_funding
                latest_est_net[symbol] = est_net

                should_exit, reason = False, ""
                sym_target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(symbol, EXIT_TARGET_NET_USD)
                if pos.hold_for_funding:
                    # Manual funding-carry hold: held for carry, never the basis
                    # target/convergence exits (those would close it the moment the
                    # basis reverts). The blocklist is a convergence-strategy concern,
                    # so it does NOT apply here — a deliberately-entered funding hold
                    # on a "blocked" name (e.g. NOW, WDC) must persist. Only the hard
                    # safety exits apply.
                    if est_net <= -FUNDING_ADVERSE_STOP_USD:
                        log.warning(
                            f"FUNDING-STOP {symbol}: est_net=${est_net:.2f} <= "
                            f"-${FUNDING_ADVERSE_STOP_USD} — bailing | held={elapsed_hours:.1f}h"
                        )
                        should_exit, reason = True, "funding_stop"
                    # No timeout — funding-carry trades are held indefinitely
                    # (only the adverse stop closes them automatically).
                elif est_net >= sym_target:
                    should_exit, reason = True, "target"
                elif symbol in BLOCKED_SYMBOLS:
                    should_exit, reason = True, "blocked"
                elif est_funding < -MAX_FUNDING_DRAG_USD and elapsed_hours >= 1.0:
                    log.info(
                        f"FUNDING-DRAG {symbol}: funding=${est_funding:.2f} "
                        f"exceeds -${MAX_FUNDING_DRAG_USD} | est_net=${est_net:.2f} "
                        f"held={elapsed_hours:.1f}h"
                    )
                    should_exit, reason = True, "funding_drag"
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
                        # Gate on est_net (executable bid/ask P&L), NOT just the mid
                        # spread. A thin book can balloon at exit time: the mid says
                        # "converged, take profit" while the executable price would
                        # lose money crossing a blown-out bid/ask. We're mid-neutral
                        # once converged, so there's no directional urgency — hold
                        # until the book tightens (est_net >= 0) or the timeout fires.
                        if own_excess <= 0 and elapsed_hours >= 0.5 and est_net >= 0:
                            log.info(
                                f"CONVERGE {symbol}: own_excess={own_excess:.1f} "
                                f"spread={spread_bps:.1f} entry_base={entry_base:.1f} "
                                f"est_net=${est_net:.2f} dir={pos_dir} held={elapsed_hours:.1f}h "
                                f"HL={hl_book.mid:.2f} Ast={aster_book.mid:.2f}"
                            )
                            should_exit, reason = True, "converge"
                        elif own_excess <= 0 and elapsed_hours >= 0.5:
                            # Mids converged but executable P&L is negative (wide
                            # book) — hold for the book to tighten, don't dump at a loss.
                            log.debug(
                                f"CONVERGE-WAIT {symbol}: mids converged "
                                f"(own_excess={own_excess:.1f}) but est_net=${est_net:.2f} "
                                f"<0 — holding | held={elapsed_hours:.1f}h"
                            )
                if should_exit:
                    await executor.exit_position(symbol, aster_book, hl_book, reason)
            elif not pos:
                if runtime_flags["auto_entry"] and pm.active_count < MAX_CONCURRENT_POSITIONS:
                    await executor.try_entry(symbol, aster_book, hl_book)
        except Exception as e:
            log.error(f"Error processing {symbol}: {e}")
            consecutive_errors += 1

    async def process_manual_commands():
        """Consume manual /enter and /close requests dropped by the control bot.

        The monitor is the single order-placing process, so funnelling manual
        actions through here (rather than the stdlib control bot) keeps one
        writer on the venues and reuses the tested leg-risk flow. Each request
        file is consumed exactly once; the outcome is alerted to Telegram.
        """
        try:
            os.makedirs(_MANUAL_CMD_DIR, exist_ok=True)
            files = sorted(Path(_MANUAL_CMD_DIR).glob("*.json"))
        except Exception:
            return
        for f in files:
            try:
                cmd = json.loads(f.read_text())
            except Exception:
                cmd = None
            # Consume the request once, regardless of outcome, so a bad/looping
            # command can't be retried forever.
            try:
                f.unlink()
            except Exception:
                pass
            if not cmd:
                continue
            action = cmd.get("action")
            symbol = cmd.get("symbol", "")
            target = cmd.get("target_bps")  # None = execute immediately (market)
            try:
                if action == "enter":
                    direction = cmd.get("direction", "")
                    notional = float(cmd.get("notional", 0) or 0)
                    if target is not None:
                        # Basis-gated: hold until the executable entry basis clears
                        # the target instead of crossing now.
                        pending_entries[symbol] = {
                            "direction": direction, "notional": notional,
                            "orig_notional": notional,
                            "target_bps": float(target),
                            "expires_ms": now_ms() + MANUAL_ENTRY_GATE_TIMEOUT_MIN * 60_000,
                        }
                        send_alert(
                            f"/enter {symbol}: waiting for entry basis ≥ {float(target):.0f}bps "
                            f"(expires {MANUAL_ENTRY_GATE_TIMEOUT_MIN}min)"
                        )
                    else:
                        ok, msg = await executor.force_entry_maker(
                            symbol, direction, notional
                        )
                        send_alert(f"/enter {symbol}: {'OK' if ok else 'FAILED'} — {msg}")
                elif action == "close":
                    pos = pm.get(symbol)
                    if not pos:
                        pending_exits.pop(symbol, None)
                        send_alert(f"/close {symbol}: no open position")
                        continue
                    if target is not None:
                        pending_exits[symbol] = {"target_bps": float(target)}
                        send_alert(
                            f"/close {symbol}: waiting for exit basis ≥ {float(target):.0f}bps "
                            f"(safety stops still apply)"
                        )
                    else:
                        pending_exits.pop(symbol, None)
                        aster_book, hl_book = await client.get_both_books(symbol)
                        await executor.exit_position(symbol, aster_book, hl_book, "manual")
                        send_alert(f"/close {symbol}: exit submitted")
                elif action == "cancel":
                    had = pending_entries.pop(symbol, None) or pending_exits.pop(symbol, None)
                    send_alert(f"/cancel {symbol}: {'gate cleared' if had else 'nothing pending'}")
                else:
                    log.warning(f"manual cmd: unknown action {action!r}")
            except Exception as e:
                log.error(f"manual cmd {action} {symbol} failed: {e}")
                send_alert(f"/{action} {symbol}: ERROR {e}")

    async def evaluate_gated_orders():
        """Fire basis-gated manual entries/exits once their target level is met."""
        for symbol in list(pending_entries):
            req = pending_entries[symbol]
            pos = pm.get(symbol)
            if pos and pos.status not in ("open", None):
                pending_entries.pop(symbol, None)
                send_alert(
                    f"/enter {symbol}: cancelled — existing position in "
                    f"status '{pos.status}' (close/drop it first)"
                )
                continue
            if pos and pos.direction != req["direction"]:
                pending_entries.pop(symbol, None)
                send_alert(f"/enter {symbol}: cancelled — position open in opposite direction")
                continue
            if now_ms() >= req["expires_ms"]:
                pending_entries.pop(symbol, None)
                send_alert(f"/enter {symbol}: gate expired (basis never reached "
                           f"{req['target_bps']:.0f}bps) — not entered")
                continue
            try:
                aster_book, hl_book = await client.get_both_books(symbol)
            except Exception:
                continue
            basis = _carry_basis_bps(req["direction"], "enter", aster_book, hl_book)
            if basis is None:
                continue
            log.info(
                f"gate {symbol}: basis={basis:.1f}bps target={req['target_bps']:.0f}bps "
                f"HL={hl_book.bid:.2f}/{hl_book.ask:.2f} AST={aster_book.bid:.2f}/{aster_book.ask:.2f}"
            )
            if basis >= req["target_bps"]:
                ok, msg = await executor.force_entry_maker(
                    symbol, req["direction"], req["notional"]
                )
                pending_entries.pop(symbol, None)
                send_alert(f"/enter {symbol}: basis {basis:.0f}bps ≥ target — "
                           f"{'OK' if ok else 'FAILED'} — {msg}")

        # Exits: close when the executable exit basis clears the target. No expiry —
        # the position's own safety stops close it if the basis stays unfavourable.
        for symbol in list(pending_exits):
            pos = pm.get(symbol)
            if not pos or pos.status != "open":
                if not pos:
                    pending_exits.pop(symbol, None)
                continue
            try:
                aster_book, hl_book = await client.get_both_books(symbol)
            except Exception:
                continue
            basis = _carry_basis_bps(pos.direction, "exit", aster_book, hl_book)
            if basis is None:
                continue
            if basis >= pending_exits[symbol]["target_bps"]:
                pending_exits.pop(symbol, None)
                await executor.exit_position(symbol, aster_book, hl_book, "manual_target")
                send_alert(f"/close {symbol}: exit basis {basis:.0f}bps ≥ target — submitted")

    try:
        while True:
            tick_start = time.time()
            tick_count += 1
            now = now_ms()

            # ── 0. Runtime controls: auto-entry flag + manual command inbox ──
            prev_auto = runtime_flags["auto_entry"]
            runtime_flags["auto_entry"] = _auto_entry_enabled()
            if runtime_flags["auto_entry"] != prev_auto:
                state = "ENABLED" if runtime_flags["auto_entry"] else "DISABLED"
                log.warning(f"Auto-entry {state} (runtime flag changed)")
            await process_manual_commands()
            await evaluate_gated_orders()

            # ── 1. Poll resting maker orders (entering/exiting) ──
            if now - last_aster_poll >= ASTER_FILL_POLL_SECONDS * 1000:
                last_aster_poll = now
                if not paper_mode:
                    # Maker-first carry entries: HL post-only resting, hedge on fill.
                    hl_makers = [s for s, p in pm.positions.items()
                                 if p.status == "entering" and p.entry_maker_venue == "hl"]
                    # Maker-first exits (carry + convergence): HL post-only close
                    # resting, Aster taker closes each HL fill. Distinguished from a
                    # taker-fallback exit (which rests an Aster GTX) by the empty
                    # aster_exit_order_id.
                    hl_exit_makers = [s for s, p in pm.positions.items()
                                      if p.status == "exiting" and p.entry_maker_venue == "hl"
                                      and not p.aster_exit_order_id]
                    # Legacy Aster-GTX flow: convergence entries + any exit with a
                    # resting Aster GTX (convergence exits + carry taker fallbacks).
                    aster_makers = [s for s, p in pm.positions.items()
                                    if (p.status == "exiting" and p.aster_exit_order_id)
                                    or (p.status == "entering" and p.entry_maker_venue != "hl")]
                    tasks = ([executor.poll_hl_maker(s) for s in hl_makers]
                             + [executor.poll_hl_maker_exit(s) for s in hl_exit_makers]
                             + [executor.poll_aster_maker(s) for s in aster_makers])
                    if tasks:
                        await asyncio.gather(*tasks)

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
            # Drop est_net for symbols no longer holding a position
            for s in list(latest_est_net.keys()):
                if s not in pm.positions:
                    latest_est_net.pop(s, None)
            _write_latest_spreads(latest_spreads, latest_est_net)
            _write_pending_gates(pending_entries, pending_exits)

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

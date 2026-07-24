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
    BASIS_ADVERSE_STOP_USD,
    MANUAL_ENTRY_GATE_TIMEOUT_MIN, aster_symbol_for, ASTER_BASE_TO_CANON,
)

SLOW_SCAN_INTERVAL_SECONDS = 300   # re-rank all symbols every 5 min
FAST_CANDIDATES = 3                # symbols to poll every tick between slow scans
DISCOVERY_INTERVAL_SECONDS = 3600  # re-query both venues for newly-listed overlaps hourly
# Basis-gated manual orders: the target must hold for this many consecutive
# evaluations before firing (a one-tick book spike used to fire the gate and
# fill well through the level), and the executor re-validates the basis on
# fresh books just before placing, with this much tolerance for book noise.
GATE_CONFIRM_TICKS = 3
GATE_REVALIDATE_TOL_BPS = 5.0
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
_AUTO_EXIT_FILE = os.path.join(DATA_DIR, "auto_exit")
# Runtime per-leg notional override for AUTO convergence entries. When absent,
# NOTIONAL_PER_LEG (config) is used. Lets you dial size down for live testing
# without a restart; manual /enter and /drip size independently.
_AUTO_NOTIONAL_FILE = os.path.join(DATA_DIR, "auto_notional")
# Manual command inbox: the control bot drops one JSON file per request here
# ({"action":"enter","symbol":..,"direction":..,"notional":..} or
# {"action":"close","symbol":..}). The monitor is the single order-placing
# process, so all manual entries/exits are funnelled through it (no races).
_MANUAL_CMD_DIR = os.path.join(DATA_DIR, "manual_cmds")
# Snapshot of basis-gated orders still waiting for their target, for /positions.
_PENDING_GATES_FILE = os.path.join(DATA_DIR, "pending_gates.json")
# Signals to the capture daemon that a manual basis gate (or drip) is armed and
# needs a clean HL read. While this is fresh, capture skips its per-name HL
# l2Book fetches (its biggest HL-budget cost) so the gate isn't blinded by
# rate-limiting. Freshness-gated (mtime) so a crashed trader can't starve
# capture forever.
_GATE_ACTIVE_FILE = os.path.join(DATA_DIR, "gate_active")
# Last-fetched exchange balances, written on the /balance request so the control
# bot (which holds no API keys) can display them.
_BALANCES_FILE = os.path.join(DATA_DIR, "balances.json")
# Session health (cycle counts, auto-entry state) for /status — the control bot
# is a separate process and can't read the trader's in-memory counters.
_HEALTH_FILE = os.path.join(DATA_DIR, "bot_health.json")
# Actual accrued funding/fees per open position, reconciled from the venues
# periodically by the trader so /positions can show real funding (not the
# entry-rate estimate, which extrapolates a single snapshot and drifts badly).
_COSTS_FILE = os.path.join(DATA_DIR, "position_costs.json")


def _write_pending_gates(pending_entries: dict, pending_exits: dict,
                         drips: dict | None = None,
                         drip_exits: dict | None = None):
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
            "exits": {
                s: {k: v for k, v in r.items() if k in ("target_bps", "maker_venue", "close_notional")}
                for s, r in pending_exits.items()
            },
            "drips": {
                s: {"direction": d["direction"],
                    "filled_notional": d["filled_notional"],
                    "target_notional": d["target_notional"],
                    "min_basis_bps": d["min_basis_bps"],
                    "fills": d["fills"]}
                for s, d in (drips or {}).items()
            },
            "drip_exits": {
                s: {"direction": d["direction"],
                    "exited_qty": d["exited_qty"],
                    "total_qty": d["total_qty"],
                    "max_basis_bps": d["max_basis_bps"],
                    "bite_qty": d["bite_qty"],
                    "fills": d["fills"]}
                for s, d in (drip_exits or {}).items()
            },
            "_ts": time.time(),
        }
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _PENDING_GATES_FILE)
    except Exception:
        pass


def _write_gate_active(active: bool):
    """Touch/refresh the gate-active flag when any manual gate/drip is armed, or
    remove it when none are. The capture daemon reads it to back off HL while a
    gate needs a clean read. Written every loop iteration so its mtime doubles
    as a liveness signal for the capture daemon's freshness check."""
    try:
        if active:
            with open(_GATE_ACTIVE_FILE, "w") as f:
                f.write(str(time.time()))
        elif os.path.exists(_GATE_ACTIVE_FILE):
            os.remove(_GATE_ACTIVE_FILE)
    except Exception:
        pass


def _restore_pending_gates(pending_entries: dict, pending_exits: dict, pm, alert):
    """Reload waiting basis gates from pending_gates.json at startup so a restart
    doesn't silently drop them. Only restores entries with no open position and
    exits whose position is still open. Drips are NOT restored (they carry
    executor-internal fill state that can't be safely reconstructed) — any
    in-flight drip is surfaced by the intent/position reconcile instead."""
    from config import MANUAL_ENTRY_GATE_TIMEOUT_MIN
    try:
        with open(_PENDING_GATES_FILE) as f:
            data = json.load(f)
    except Exception:
        return
    restored = []
    for sym, r in (data.get("entries") or {}).items():
        if pm.get(sym):          # a position already exists → don't re-arm entry
            continue
        try:
            expires_ms = (now_ms() + MANUAL_ENTRY_GATE_TIMEOUT_MIN * 60_000
                          if MANUAL_ENTRY_GATE_TIMEOUT_MIN > 0 else 0)
            pending_entries[sym] = {
                "direction": r["direction"],
                "notional": float(r["notional"]),
                "orig_notional": float(r.get("orig_notional", r["notional"])),
                "target_bps": float(r["target_bps"]),
                "expires_ms": expires_ms,
            }
            restored.append(f"ENTER {sym} ≥{float(r['target_bps']):.0f}bps")
        except Exception:
            continue
    for sym, r in (data.get("exits") or {}).items():
        pos = pm.get(sym)
        if not pos or pos.status != "open":   # only re-arm exits on a live position
            continue
        try:
            pe = {"target_bps": float(r["target_bps"])}
            if r.get("maker_venue"):
                pe["maker_venue"] = r["maker_venue"]
            if r.get("close_notional"):
                pe["close_notional"] = float(r["close_notional"])
            pending_exits[sym] = pe
            restored.append(f"CLOSE {sym} ≥{float(r['target_bps']):.0f}bps")
        except Exception:
            continue
    if restored:
        log.warning(f"Restored {len(restored)} pending gate(s) after restart: "
                    + ", ".join(restored))
        try:
            alert("♻️ Restored gates after restart:\n  " + "\n  ".join(restored))
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


def _auto_exit_enabled() -> bool:
    """Read the runtime auto-exit flag. Missing/unreadable file = enabled (default)."""
    try:
        return Path(_AUTO_EXIT_FILE).read_text().strip().lower() != "off"
    except FileNotFoundError:
        return True
    except Exception:
        return True


def _auto_notional() -> float:
    """Per-leg notional (USD) for auto convergence entries. Runtime override from
    the auto_notional file, else NOTIONAL_PER_LEG. Clamped to a sane floor so a
    bad file can't size to ~0; falls back to the config default on any error."""
    try:
        v = float(Path(_AUTO_NOTIONAL_FILE).read_text().strip())
        if v < 1.0:
            return NOTIONAL_PER_LEG
        return v
    except FileNotFoundError:
        return NOTIONAL_PER_LEG
    except Exception:
        return NOTIONAL_PER_LEG


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
    # BOTH books must be fully populated. If one venue's book is empty (bid or
    # ask 0 — a transient fetch miss), the combined mid is still positive from
    # the other side and the basis math produces garbage (a missing HL bid made
    # (aster_bid-0)/mid ≈ 2 → ~20000bps, which cleared a gate and fired a bogus
    # order into an unplaceable price). No valid basis without both books.
    if (aster_book.bid <= 0 or aster_book.ask <= 0
            or hl_book.bid <= 0 or hl_book.ask <= 0):
        return None
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

    # Load specs for EVERY open position, even ones outside the startup universe
    # (blocked names, or auto-discovered symbols not yet in overlap_symbols.csv).
    # Without this, an open position on such a symbol can't be exited — the HL
    # asset index is never loaded (observed: DKNG "HL asset index not found").
    pm_preload = PositionManager(paper_mode=paper_mode)
    open_pos_syms = set(pm_preload.positions.keys())
    extra_pos_syms = open_pos_syms - set(symbols)
    if extra_pos_syms:
        log.info(f"Open positions outside startup universe (loading specs for exit): {extra_pos_syms}")
    all_load_syms = list(dict.fromkeys(symbols + list(open_pos_syms)))

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
    # Give the PM the client so a live close can reconcile actual funding +
    # commission from the venues before it sends the CLOSED alert.
    pm.client = client
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
    last_cost_reconcile = 0  # actual funding/fees for open positions → position_costs.json
    last_discovery = start_time  # first auto-discovery runs DISCOVERY_INTERVAL after start
    tick_count = 0
    consecutive_errors = 0
    # symbol -> (excess_bps, direction_str, baseline_bps) from last scan
    latest_spreads: dict[str, tuple[float, str, float]] = {}
    # symbol -> est_net USD (executable bid/ask P&L) for open positions
    latest_est_net: dict[str, float] = {}
    # symbol -> ACTUAL accrued funding (USD) reconciled from the venues every
    # ~5min. Used in place of the entry-rate estimate for est_net so the stop/
    # display P&L reflects real settled funding, not an extrapolated snapshot.
    actual_funding_by_sym: dict[str, float] = {}
    # Runtime flags refreshed once per tick from their control files.
    runtime_flags = {"auto_entry": True, "auto_exit": True,
                     "auto_notional": float(NOTIONAL_PER_LEG)}
    # Basis-gated manual orders waiting for a good fill level.
    #   pending_entries: symbol -> {direction, notional, target_bps, expires_ms}
    #   pending_exits:   symbol -> {target_bps}
    pending_entries: dict[str, dict] = {}
    pending_exits: dict[str, dict] = {}
    # Restore waiting gates from disk — they live only in memory otherwise, so a
    # restart (crash/OOM/deploy) silently dropped every armed gate while the
    # position stayed open (observed: an overnight SKHX exit gate vanished).
    _restore_pending_gates(pending_entries, pending_exits, pm, send_alert)
    # ("enter"|"exit", symbol) -> consecutive evaluations the basis held ≥ target
    gate_streaks: dict[tuple[str, str], int] = {}
    # symbol -> last ms we warned that a gate couldn't evaluate (books empty).
    # Throttles the "gate blind" warning to ~1/5min so a persistent HL rate-limit
    # is visible (silent skips looked like the gate "wasn't live").
    gate_blind_last: dict[str, int] = {}

    def _warn_gate_blind(symbol: str, which: str):
        now = now_ms()
        if now - gate_blind_last.get(symbol, 0) > 300_000:
            gate_blind_last[symbol] = now
            log.warning(
                f"gate {symbol}: cannot evaluate {which} — book unavailable "
                f"(HL/Aster fetch empty; likely rate-limited). Gate NOT firing "
                f"until books return."
            )
            try:
                send_alert(f"⚠️ {symbol} {which} gate can't read the book "
                           f"(rate-limited?) — not firing until it clears")
            except Exception:
                pass
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
            # oracle-corrected fair spread. Record both samples, then baseline.
            spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
            client.record_book_spread(symbol, spread_bps)
            client.record_oracle_delta(symbol)
            book_base = client.get_book_spread_baseline(symbol)
            # No baseline yet (cold start) — return None so the protocol skips it.
            if book_base is None:
                return None
            # Fair spread = book baseline + oracle fair-value correction (0 when
            # oracle history is insufficient → pure book baseline). exec_dev adds
            # the executable HL-maker/Aster-taker basis refinement (0 until warm),
            # matching try_entry so ranking/display track the actual decision.
            # PROTECTIVE ONLY: the correction can only suppress the excess, never
            # inflate it — a noisy oracle spike must not create phantom edges.
            correction = client.get_oracle_correction(symbol)
            exec_dev = client.executable_deviation_bps(symbol, aster_book, hl_book, mid)
            raw_dev = spread_bps - book_base
            raw_aster = raw_dev + exec_dev
            raw_hl = -raw_dev + exec_dev
            corr_aster = (raw_dev - correction) + exec_dev
            corr_hl = -(raw_dev - correction) + exec_dev
            aster_excess = min(raw_aster, corr_aster)
            hl_excess    = min(raw_hl, corr_hl)
            if aster_excess >= hl_excess:
                executable_excess = aster_excess
                direction = "L-HL/S-AST"
            else:
                executable_excess = hl_excess
                direction = "L-AST/S-HL"
            effective_base = book_base
            return symbol, executable_excess, direction, effective_base, aster_book, hl_book
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
                # Maker-first positions close the HL leg as a resting maker and
                # cross the Aster leg as a taker. The HL maker is marked at MID,
                # NOT the favorable touch (ask for a sell / bid for a buy): a
                # resting sell rarely gets lifted right at the ask on a converging
                # book — it reprices down to fill — so marking it at the touch was
                # optimistic and let the target/converge gates fire on P&L the exit
                # never realized (DELL 'converge' gate said est_net>=0 but the fill
                # booked -$0.61 gross). The Aster leg keeps its taker touch (a real
                # cross). Legacy taker positions cross both legs (taker-taker).
                maker_first = pos.entry_maker_venue == "hl"
                if mid > 0 and pos.direction == "long_hl_short_aster":
                    hl_exit = hl_book.mid if maker_first else hl_book.bid
                    est_gross = ((hl_exit - pos.hl_entry_price)
                                 + (pos.aster_entry_price - aster_book.ask)) * pos.qty
                elif mid > 0:
                    hl_exit = hl_book.mid if maker_first else hl_book.ask
                    est_gross = ((pos.hl_entry_price - hl_exit)
                                 + (aster_book.bid - pos.aster_entry_price)) * pos.qty
                else:
                    est_gross = 0.0
                rt_fee = CARRY_ROUND_TRIP_FEE if maker_first else ROUND_TRIP_FEE
                est_fees = (pos.notional_usd or NOTIONAL_PER_LEG) * rt_fee
                # Prefer the ACTUAL settled funding reconciled from the venues
                # (refreshed ~5min); the entry-rate estimate extrapolates one
                # snapshot linearly and drifts far (SKHX est -$4.43 vs ~flat).
                if symbol in actual_funding_by_sym:
                    est_funding = actual_funding_by_sym[symbol]
                else:
                    est_funding = estimate_funding_pnl(
                        pos.direction, elapsed_hours,
                        pos.notional_usd or NOTIONAL_PER_LEG,
                        pos.hl_funding_rate, pos.aster_funding_rate,
                        pos.aster_funding_window_h,
                    )
                est_net = est_gross - est_fees + est_funding
                latest_est_net[symbol] = est_net

                # Mid-marked MTM for the STOP triggers only. est_net marks the
                # exit at executable crossing prices, which is hypersensitive to
                # transient one-tick book width on thin venues — a single Aster
                # ask gap made est_net crater and trip the stop on book noise
                # rather than genuine divergence (DELL stopped at a -$0.15
                # realized loss because the executable mark spiked for one tick).
                # Marking the exit at mids measures real price divergence and
                # roughly cancels the entry crossing cost, so a freshly-opened,
                # unmoved position sits near zero. est_net (executable) stays for
                # the take-profit gate (don't bank profit you can't cross out to
                # realize) and for display.
                if mid > 0:
                    hl_leg_mid = hl_book.mid - pos.hl_entry_price
                    aster_leg_mid = pos.aster_entry_price - aster_book.mid
                    if pos.direction != "long_hl_short_aster":
                        hl_leg_mid = -hl_leg_mid
                        aster_leg_mid = -aster_leg_mid
                    est_gross_mid = (hl_leg_mid + aster_leg_mid) * pos.qty
                else:
                    est_gross_mid = 0.0
                est_net_mid = est_gross_mid - est_fees + est_funding

                should_exit, reason = False, ""
                # Profit target scales with THIS position's notional, so a trade
                # opened at reduced size still exits on the same bps-edge as a
                # full-size one (a fixed $3 target on a $200 position would need
                # ~150bps and almost never fire → rides to timeout instead).
                base_target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(symbol, EXIT_TARGET_NET_USD)
                size_frac = (pos.notional_usd / NOTIONAL_PER_LEG) if pos.notional_usd else 1.0
                sym_target = base_target * size_frac

                # Skip auto-exit when disabled, or when a drip/drip_exit is active
                if (not runtime_flags["auto_exit"]
                        or symbol in executor._drip_exits
                        or symbol in executor._drips):
                    pass
                elif pos.hold_for_funding:
                    # Manual funding-carry hold: held for carry, NEVER auto-closed.
                    # No basis/convergence exit (those would close it the moment the
                    # basis reverts), no mark-to-market safety stop, and no timeout.
                    # The operator opened it deliberately via /enter and owns the
                    # exit — it closes ONLY on a manual /close. The blocklist is a
                    # convergence concern and does not apply here either.
                    pass
                elif BASIS_ADVERSE_STOP_USD > 0 and est_net_mid <= -(BASIS_ADVERSE_STOP_USD * size_frac):
                    # Mid-marked stop: the ADVERSE_STOP_BPS guard only fires when
                    # the excess INVERTS, so a position that just diverges or
                    # never converges would otherwise bleed to the 12h timeout
                    # (RKLB −$15.68, STRC −$12.62). Cap that at a dollar loss,
                    # scaled to the position's size. Marked at mids so transient
                    # book width can't trip it (see est_net_mid).
                    log.warning(
                        f"BASIS-STOP {symbol}: est_net_mid=${est_net_mid:.2f} <= "
                        f"-${BASIS_ADVERSE_STOP_USD * size_frac:.2f} — bailing | "
                        f"held={elapsed_hours:.1f}h (executable est_net=${est_net:.2f})"
                    )
                    should_exit, reason = True, "stop_loss"
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
                    # Convergence exit: the EXECUTABLE entry-edge that motivated
                    # the trade has decayed to <= 0 — i.e. if we were flat we'd no
                    # longer enter this direction, so there's nothing left to
                    # capture. Same yardstick as the entry signal:
                    #   own_excess = (spread − fair)·dir + exec_dev
                    # fair = frozen entry book-median (so a persistent dislocation
                    # absorbed into the rolling median can't shift the goalposts)
                    # + LIVE oracle correction (a genuine fair-value move during the
                    # hold carries the target with it) + LIVE executable deviation
                    # (exit sooner if the executable basis deteriorates, hold if real
                    # edge remains). Mirrors try_entry exactly.
                    entry_base = pos.entry_baseline_bps
                    if not entry_base:
                        log.warning(f"{symbol}: entry_baseline_bps=0 — skipping convergence check")
                    else:
                        client.record_oracle_delta(symbol)
                        correction = client.get_oracle_correction(symbol)
                        exec_dev = client.executable_deviation_bps(symbol, aster_book, hl_book, mid)
                        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
                        raw_dev = spread_bps - entry_base
                        pos_dir = pos.direction or "long_hl_short_aster"
                        # Protective-only: correction can only reduce own_excess
                        # (make it look more converged / less edge), never inflate it.
                        raw_own = (raw_dev if pos_dir == "long_hl_short_aster"
                                   else -raw_dev) + exec_dev
                        corr_dev = raw_dev - correction
                        corr_own = (corr_dev if pos_dir == "long_hl_short_aster"
                                    else -corr_dev) + exec_dev
                        own_excess = min(raw_own, corr_own)
                        # Gate on est_net (executable bid/ask P&L), NOT just the
                        # trigger. A thin book can balloon at exit time: the signal
                        # says "converged, take profit" while the executable price
                        # would lose money crossing a blown-out bid/ask. We're mid-
                        # neutral once converged, so there's no directional urgency —
                        # hold until the book tightens (est_net >= 0) or timeout fires.
                        if own_excess <= 0 and elapsed_hours >= 0.5 and est_net >= 0:
                            log.info(
                                f"CONVERGE {symbol}: own_excess={own_excess:.1f} "
                                f"spread={spread_bps:.1f} entry_base={entry_base:.1f} "
                                f"corr={correction:+.1f} exec_dev={exec_dev:+.1f} "
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
                    await executor.try_entry(symbol, aster_book, hl_book,
                                             notional=runtime_flags["auto_notional"])
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
                        # 0 (or less) disables expiry — the gate waits
                        # indefinitely (e.g. to catch an overnight spike).
                        expires_ms = (now_ms() + MANUAL_ENTRY_GATE_TIMEOUT_MIN * 60_000
                                      if MANUAL_ENTRY_GATE_TIMEOUT_MIN > 0 else 0)
                        pending_entries[symbol] = {
                            "direction": direction, "notional": notional,
                            "orig_notional": notional,
                            "target_bps": float(target),
                            "expires_ms": expires_ms,
                        }
                        exp_str = (f"expires {MANUAL_ENTRY_GATE_TIMEOUT_MIN}min"
                                   if MANUAL_ENTRY_GATE_TIMEOUT_MIN > 0 else "no expiry")
                        send_alert(
                            f"/enter {symbol}: waiting for entry basis ≥ {float(target):.0f}bps "
                            f"({exp_str})"
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
                    maker_venue = cmd.get("maker_venue", "")
                    close_notional = float(cmd.get("close_notional", 0) or 0)
                    if target is not None:
                        pe = {"target_bps": float(target)}
                        if maker_venue:
                            pe["maker_venue"] = maker_venue
                        if close_notional:
                            pe["close_notional"] = close_notional
                        pending_exits[symbol] = pe
                        mv_str = f" maker={maker_venue}" if maker_venue else ""
                        n_str = f" ${close_notional:.0f}" if close_notional else ""
                        send_alert(
                            f"/close {symbol}{n_str}: waiting for exit basis ≥ {float(target):.0f}bps{mv_str} "
                            f"(safety stops still apply)"
                        )
                    else:
                        pending_exits.pop(symbol, None)
                        if close_notional and pos.qty > 0:
                            aster_book, hl_book = await client.get_both_books(symbol)
                            avg_price = (pos.hl_entry_price + pos.aster_entry_price) / 2
                            if avg_price <= 0:
                                avg_price = (aster_book.mid + hl_book.mid) / 2
                            close_qty = min(close_notional / avg_price, pos.qty) if avg_price > 0 else pos.qty
                            # exit_position routes partials to HL-maker or taker-taker
                            ok = await executor.exit_position(
                                symbol, aster_book, hl_book, "manual_partial",
                                maker_venue, close_qty)
                            send_alert(f"/close {symbol}: partial close {'submitted' if ok else 'FAILED'}")
                        else:
                            aster_book, hl_book = await client.get_both_books(symbol)
                            await executor.exit_position(
                                symbol, aster_book, hl_book, "manual", maker_venue)
                            send_alert(f"/close {symbol}: exit submitted")
                elif action == "cancel":
                    had = pending_entries.pop(symbol, None) or pending_exits.pop(symbol, None)
                    # Cancel any active drip or drip_exit
                    drip_ok, drip_msg = executor.cancel_drip(symbol)
                    dex_ok, dex_msg = executor.cancel_drip_exit(symbol)
                    pos = pm.get(symbol)
                    parts = []
                    if had:
                        parts.append("gate cleared")
                    if drip_ok:
                        parts.append(drip_msg)
                    if dex_ok:
                        parts.append(dex_msg)
                    # Abort a running maker ENTRY (status="entering") …
                    if pos and pos.status == "entering":
                        executor._abort_entering.add(symbol)
                        parts.append("aborting maker entry (finalizes next tick)")
                    # … or a stuck /close in progress (status="exiting").
                    elif pos and pos.status == "exiting":
                        ok, msg = await executor.abort_exit(symbol)
                        parts.append(msg if ok else "exit abort failed")
                    if not parts:
                        parts.append("nothing pending")
                    send_alert(f"/cancel {symbol}: {' + '.join(parts)}")
                elif action == "drip":
                    direction = cmd.get("direction", "")
                    notional = float(cmd.get("notional", 0) or 0)
                    min_basis = float(cmd.get("min_basis_bps", 0) or 0)
                    bite_qty = float(cmd.get("bite_qty", 0) or 0)
                    ok, msg = executor.start_drip(
                        symbol, direction, notional, min_basis, bite_qty)
                    send_alert(f"/drip {symbol}: {'OK' if ok else 'FAILED'} — {msg}")
                elif action == "drip_exit":
                    max_basis = float(cmd.get("max_basis_bps", 0) or 0)
                    bite_qty = float(cmd.get("bite_qty", 0) or 0)
                    target_notional = float(cmd.get("target_notional", 0) or 0)
                    ok, msg = executor.start_drip_exit(
                        symbol, max_basis, bite_qty, target_notional)
                    send_alert(f"/drip_exit {symbol}: {'OK' if ok else 'FAILED'} — {msg}")
                elif action == "import":
                    await _handle_import(symbol or None)
                elif action == "forget":
                    await _handle_forget(symbol)
                elif action == "balance":
                    await _handle_balance()
                else:
                    log.warning(f"manual cmd: unknown action {action!r}")
            except Exception as e:
                log.error(f"manual cmd {action} {symbol} failed: {e}")
                send_alert(f"/{action} {symbol}: ERROR {e}")

    async def _handle_balance():
        """Fetch balances on both venues, cache them for the control bot, and
        reply over the Telegram alert channel."""
        hl, aster = await asyncio.gather(
            client.get_hl_balance(),
            client.get_aster_balance(),
        )
        snap = {"hl": hl, "aster": aster, "_ts": time.time()}
        try:
            tmp = _BALANCES_FILE + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(snap, fh)
            os.replace(tmp, _BALANCES_FILE)
        except Exception:
            pass
        lines = ["💰 Balances"]
        if hl:
            lines.append(
                f"HL (xyz): ${hl['account_value']:.2f} {hl['asset']} "
                f"(free ${hl['withdrawable']:.2f}, margin used ${hl['margin_used']:.2f})"
            )
        else:
            lines.append("HL (xyz): query failed")
        if aster:
            upnl = aster.get("unrealized_pnl", 0.0)
            lines.append(
                f"Aster: ${aster['balance']:.2f} {aster['asset']} "
                f"(free ${aster['available']:.2f}, uPnL ${upnl:+.2f})"
            )
        else:
            lines.append("Aster: query failed")
        if hl and aster:
            lines.append(f"Total ≈ ${hl['account_value'] + aster['balance']:.2f} (USDC+USDT)")
        send_alert("\n".join(lines))

    async def _handle_forget(symbol: str):
        """Drop a tracked position from the DB/memory WITHOUT placing any orders —
        for a position already closed on the venue by hand, so /positions is out
        of sync. Verifies BOTH venues are flat first; refuses otherwise so we
        can never orphan a real (naked) leg."""
        pos = pm.get(symbol)
        if not pos:
            send_alert(f"/forget {symbol}: no such tracked position")
            return
        try:
            hlp = await client.get_hl_position(symbol)
            szi = abs(float(hlp.get("szi", 0) or 0))
        except Exception as e:
            send_alert(f"/forget {symbol}: HL position check failed ({e}) — aborting, retry")
            return
        try:
            ap = await client.get_aster_position(symbol)
            amt = abs(float(ap.get("positionAmt", 0) or 0))
        except Exception as e:
            send_alert(f"/forget {symbol}: Aster position check failed ({e}) — aborting, retry")
            return
        # "Flat" = under 5% of the tracked qty remaining on each venue (dust-safe).
        thresh = max(pos.qty * 0.05, 1e-9)
        if szi >= thresh or amt >= thresh:
            send_alert(
                f"/forget {symbol}: REFUSED — venue still shows a position "
                f"(HL szi={szi:.4f}, Aster amt={amt:.4f}; tracked qty={pos.qty:.4f}). "
                f"Close it on the venue (or /close) first, then /forget."
            )
            return
        pm.drop_entering(symbol, "manual_forget_venue_flat")
        send_alert(f"/forget {symbol}: removed from tracking — both venues confirmed flat.")

    async def _handle_import(filter_symbol: str | None = None):
        """Scan both venues for offsetting positions and import them into the DB."""
        hl_positions, aster_positions = await asyncio.gather(
            client.get_all_hl_positions(),
            client.get_all_aster_positions(),
        )
        # Build lookup: canonical_base -> (signed_qty, entry_price) per venue
        hl_map: dict[str, tuple[float, float]] = {}
        for p in hl_positions:
            coin = p.get("coin", "")
            if not coin.startswith("xyz:"):
                continue
            base = coin.split(":", 1)[1]
            szi = float(p.get("szi", 0) or 0)
            entry_px = float(p.get("entryPx", 0) or 0)
            if abs(szi) > 1e-12:
                hl_map[base] = (szi, entry_px)

        aster_map: dict[str, tuple[float, float]] = {}
        for p in aster_positions:
            raw_sym = p.get("symbol", "")
            base = None
            for suffix in ("USDT", "USDC", "USD"):
                if raw_sym.endswith(suffix):
                    base = raw_sym[:-len(suffix)]
                    break
            if not base:
                continue
            if base in ASTER_BASE_TO_CANON:
                base = ASTER_BASE_TO_CANON[base]
            amt = float(p.get("positionAmt", 0) or 0)
            entry_px = float(p.get("entryPrice", 0) or 0)
            if abs(amt) > 1e-12:
                aster_map[base] = (amt, entry_px)

        # Find offsetting pairs (opposite signs on each venue)
        common = set(hl_map) & set(aster_map)
        if filter_symbol:
            common = {filter_symbol} & common

        pairs = []
        for base in sorted(common):
            hl_szi, hl_px = hl_map[base]
            ast_amt, ast_px = aster_map[base]
            if hl_szi * ast_amt >= 0:
                continue  # same direction, not an arb pair
            if pm.has_position(base):
                continue  # already tracked
            qty = min(abs(hl_szi), abs(ast_amt))
            if hl_szi > 0:
                direction = "long_hl_short_aster"
            else:
                direction = "long_aster_short_hl"
            pairs.append((base, direction, qty, hl_px, ast_px, hl_szi, ast_amt))

        if not pairs:
            if filter_symbol:
                # Give detail on why nothing matched
                reasons = []
                if filter_symbol not in hl_map:
                    reasons.append("no HL position")
                if filter_symbol not in aster_map:
                    reasons.append("no Aster position")
                if filter_symbol in hl_map and filter_symbol in aster_map:
                    hl_s, _ = hl_map[filter_symbol]
                    ast_s, _ = aster_map[filter_symbol]
                    if hl_s * ast_s >= 0:
                        reasons.append("same direction on both venues (not offsetting)")
                    if pm.has_position(filter_symbol):
                        reasons.append("already tracked in DB")
                send_alert(f"/import {filter_symbol}: nothing to import ({', '.join(reasons) or '?'})")
            else:
                send_alert(
                    f"/import: no importable pairs found.\n"
                    f"HL positions: {list(hl_map.keys())}\n"
                    f"Aster positions: {list(aster_map.keys())}\n"
                    f"Already tracked: {list(pm.positions.keys())}"
                )
            return

        imported = []
        for base, direction, qty, hl_px, ast_px, hl_szi, ast_amt in pairs:
            hl_fr = client.get_hl_funding_rate(base)
            ast_fr = client.get_aster_funding_rate(base)
            ast_win = await client.get_aster_funding_window(base)
            pos = pm.import_position(
                symbol=base,
                hl_coin=f"xyz:{base}",
                aster_symbol=aster_symbol_for(base),
                direction=direction,
                qty=qty,
                hl_price=hl_px,
                aster_price=ast_px,
                hl_funding_rate=hl_fr,
                aster_funding_rate=ast_fr,
                aster_funding_window_h=ast_win,
            )
            short_dir = "HL↑ Ast↓" if "long_hl" in direction else "HL↓ Ast↑"
            imported.append(
                f"  • {base} #{pos.id} {short_dir} qty={qty} "
                f"HL@{hl_px:.2f} Ast@{ast_px:.2f} ${pos.notional_usd:.0f}"
            )
            log.warning(f"Imported {base}: {direction} qty={qty} HL@{hl_px:.2f} Ast@{ast_px:.2f}")
        send_alert("✅ Imported positions:\n" + "\n".join(imported))

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
            if req.get("expires_ms", 0) and now_ms() >= req["expires_ms"]:
                pending_entries.pop(symbol, None)
                send_alert(f"/enter {symbol}: gate expired (basis never reached "
                           f"{req['target_bps']:.0f}bps) — not entered")
                continue
            try:
                aster_book, hl_book = await client.get_both_books(symbol)
            except Exception:
                _warn_gate_blind(symbol, "ENTER")
                continue
            basis = _carry_basis_bps(req["direction"], "enter", aster_book, hl_book)
            if basis is None:
                _warn_gate_blind(symbol, "ENTER")
                continue
            log.info(
                f"gate {symbol}: basis={basis:.1f}bps target={req['target_bps']:.0f}bps "
                f"HL={hl_book.bid:.2f}/{hl_book.ask:.2f} AST={aster_book.bid:.2f}/{aster_book.ask:.2f}"
            )
            # Persistence: the basis must clear the target on GATE_CONFIRM_TICKS
            # consecutive evaluations. A single-tick spike (one wild print on
            # either book) used to fire the gate and fill well through the level.
            if basis >= req["target_bps"]:
                gate_streaks[("enter", symbol)] = gate_streaks.get(("enter", symbol), 0) + 1
            else:
                gate_streaks.pop(("enter", symbol), None)
            if gate_streaks.get(("enter", symbol), 0) >= GATE_CONFIRM_TICKS:
                gate_streaks.pop(("enter", symbol), None)
                ok, msg = await executor.force_entry_maker(
                    symbol, req["direction"], req["notional"]
                )
                pending_entries.pop(symbol, None)
                send_alert(f"/enter {symbol}: basis {basis:.0f}bps ≥ target "
                           f"({GATE_CONFIRM_TICKS} ticks) — "
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
                _warn_gate_blind(symbol, "CLOSE")
                continue
            basis = _carry_basis_bps(pos.direction, "exit", aster_book, hl_book)
            if basis is None:
                _warn_gate_blind(symbol, "CLOSE")
                continue
            target = pending_exits[symbol]["target_bps"]
            if basis >= target:
                gate_streaks[("exit", symbol)] = gate_streaks.get(("exit", symbol), 0) + 1
            else:
                gate_streaks.pop(("exit", symbol), None)
            if gate_streaks.get(("exit", symbol), 0) >= GATE_CONFIRM_TICKS:
                gate_streaks.pop(("exit", symbol), None)
                mv = pending_exits[symbol].get("maker_venue", "")
                cn = float(pending_exits[symbol].get("close_notional", 0) or 0)
                # The executor RE-VALIDATES the basis on its own fresh books just
                # before placing (min_exit_basis, small tolerance for book
                # noise). On failure the gate stays armed and re-fires later —
                # observed SKHX: fired at -30, filled at -44.7 without this.
                min_basis = target - GATE_REVALIDATE_TOL_BPS
                log.warning(f"gate {symbol}: EXIT firing at basis={basis:.1f}bps "
                            f"(target {target:.0f}, {GATE_CONFIRM_TICKS} ticks)")
                if cn and pos.qty > 0:
                    avg_price = (pos.hl_entry_price + pos.aster_entry_price) / 2
                    if avg_price <= 0:
                        avg_price = (aster_book.mid + hl_book.mid) / 2
                    close_qty = min(cn / avg_price, pos.qty) if avg_price > 0 else pos.qty
                    ok = await executor.exit_position(
                        symbol, aster_book, hl_book, "manual_target_partial",
                        mv, close_qty, min_exit_basis=min_basis)
                else:
                    ok = await executor.exit_position(
                        symbol, aster_book, hl_book, "manual_target", mv,
                        min_exit_basis=min_basis)
                if ok:
                    pending_exits.pop(symbol, None)
                    send_alert(f"/close {symbol}: exit basis {basis:.0f}bps ≥ "
                               f"{target:.0f}bps target — submitted")
                else:
                    # Re-validation (or placement) failed — keep the gate armed.
                    log.warning(f"gate {symbol}: exit submission failed after fire "
                                f"— gate stays armed")

    async def discover_new_symbols():
        """Re-query both venues for the current overlap and add any newly-listed
        equity perp that appears on BOTH exchanges to the live universe. Loads its
        specs and warms its baseline so the scanner can pick it up on the next slow
        scan. Excluded names (BLOCKED/NON_EQUITY) are skipped."""
        try:
            overlap = await client.discover_overlap_bases()
        except Exception as e:
            log.warning(f"auto-discovery failed: {e}")
            return
        if not overlap:
            return
        known = set(symbols)
        new = sorted(overlap - known - exclude)
        if not new:
            return
        log.info(f"Auto-discovery: {len(new)} candidate new overlap symbol(s): {new}")
        added = []
        for sym in new:
            try:
                if not await client.ensure_symbol_loaded(sym):
                    log.info(f"Auto-discovery: {sym} specs unavailable — skipping")
                    continue
                await client.warmup_book_spread([sym])
                symbols.append(sym)
                added.append(sym)
            except Exception as e:
                log.warning(f"Auto-discovery: failed to add {sym} ({e})")
        if added:
            # Persist to overlap_symbols.csv so the names survive a restart —
            # otherwise every restart re-discovers and RE-ANNOUNCES them (and an
            # open position on one can't be exited, its specs unloaded). Append
            # only genuinely-new rows.
            try:
                import csv as _csv
                overlap_path = Path(OUTPUT_DIR) / "overlap_symbols.csv"
                existing = set()
                if overlap_path.exists():
                    with open(overlap_path) as fh:
                        existing = {r["coin"] for r in _csv.DictReader(fh)}
                to_write = [s for s in added if s not in existing]
                if to_write:
                    new_file = not overlap_path.exists()
                    with open(overlap_path, "a", newline="") as fh:
                        w = _csv.writer(fh)
                        if new_file:
                            w.writerow(["coin", "hl_coin", "aster_symbol"])
                        for s in to_write:
                            w.writerow([s, f"xyz:{s}", aster_symbol_for(s)])
            except Exception as e:
                log.warning(f"Auto-discovery: failed to persist {added} to CSV ({e})")
            log.info(f"Auto-discovery: added {added} to live universe ({len(symbols)} total)")
            send_alert(
                f"🆕 Auto-added {len(added)} new equity perp(s) now on both venues: "
                f"{', '.join(added)}\nWatching for arbs (default {ENTRY_THRESHOLD_BPS:.0f}bps "
                f"threshold until calibrated)."
            )

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
                # Resume the full-universe scan immediately when re-enabled
                # (it's skipped while off), rather than waiting up to 5 min.
                if runtime_flags["auto_entry"]:
                    last_slow_scan = 0
            # Auto-entry off must also abort AUTO entries still in-flight (a
            # resting maker not yet filled) — not just stop new ones — or a
            # position keeps completing after you disabled it (ZM did). Manual
            # /enter (funding holds) are left alone. The poll finalizes each
            # aborted entry: nothing filled → dropped, partial → opened hedged.
            if not runtime_flags["auto_entry"]:
                for s, p in list(pm.positions.items()):
                    if (p.status == "entering" and p.entry_maker_venue == "hl"
                            and not p.hold_for_funding
                            and s not in executor._abort_entering):
                        executor._abort_entering.add(s)
                        log.warning(f"{s}: auto-entry OFF — aborting in-flight auto entry")
            prev_auto_exit = runtime_flags["auto_exit"]
            runtime_flags["auto_exit"] = _auto_exit_enabled()
            if runtime_flags["auto_exit"] != prev_auto_exit:
                state = "ENABLED" if runtime_flags["auto_exit"] else "DISABLED"
                log.warning(f"Auto-exit {state} (runtime flag changed)")
            prev_notional = runtime_flags["auto_notional"]
            runtime_flags["auto_notional"] = _auto_notional()
            if runtime_flags["auto_notional"] != prev_notional:
                log.warning(f"Auto-entry notional set to ${runtime_flags['auto_notional']:.0f}/leg "
                            f"(runtime override changed)")
            # An exception here must not kill the monitor — exits and safety
            # stops would silently stop being evaluated for live positions.
            try:
                await process_manual_commands()
            except Exception as e:
                log.error(f"process_manual_commands errored: {e}", exc_info=True)
            try:
                await evaluate_gated_orders()
            except Exception as e:
                log.error(f"evaluate_gated_orders errored: {e}", exc_info=True)

            # ── 1. Poll resting maker orders (entering/exiting) ──
            if now - last_aster_poll >= ASTER_FILL_POLL_SECONDS * 1000:
                last_aster_poll = now
                if not paper_mode:
                    # Maker-first carry entries: HL post-only resting, hedge on fill.
                    hl_makers = [s for s, p in pm.positions.items()
                                 if p.status == "entering" and p.entry_maker_venue == "hl"]
                    # HL-maker exits: HL post-only close resting, Aster taker closes
                    # each HL fill. Identified by exiting + a resting HL exit order +
                    # NO Aster GTX (vs the Aster-GTX taker-fallback below). Covers both
                    # carry exits (entered maker-first on HL) AND taker-entered
                    # positions closed with an explicit /close ... hl. Must NOT gate on
                    # entry_maker_venue or the latter's resting HL maker is never polled.
                    hl_exit_makers = [s for s, p in pm.positions.items()
                                      if p.status == "exiting" and p.hl_exit_order_id
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
                        # return_exceptions: one symbol's poll blowing up must
                        # not kill the others' polls (or the whole monitor).
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        poll_syms = hl_makers + hl_exit_makers + aster_makers
                        for s, r in zip(poll_syms, results):
                            if isinstance(r, Exception):
                                log.error(f"poll for {s} errored: {r}", exc_info=r)

            # ── 1b. Drip entries: one taker-taker bite per active drip per tick ──
            drip_syms = list(executor._drips.keys())
            for ds in drip_syms:
                try:
                    result = await executor.drip_tick(ds)
                    if result and isinstance(result, str):
                        send_alert(f"💧 {result}")
                except Exception as e:
                    log.error(f"drip tick {ds} error: {e}")
                    send_alert(f"💧 drip {ds}: ERROR {e}")

            # ── 1c. Drip exits: one taker-taker exit bite per active drip_exit ──
            dex_syms = list(executor._drip_exits.keys())
            for ds in dex_syms:
                try:
                    result = await executor.drip_exit_tick(ds)
                    if result and isinstance(result, str):
                        send_alert(f"💧 {result}")
                except Exception as e:
                    log.error(f"drip_exit tick {ds} error: {e}")
                    send_alert(f"💧 drip_exit {ds}: ERROR {e}")

            # ── 1d. Auto-discovery: add newly-listed cross-venue overlaps hourly ──
            if now - last_discovery >= DISCOVERY_INTERVAL_SECONDS * 1000:
                last_discovery = now
                await discover_new_symbols()

            # ── 2. Slow scan: rank all symbols every 5 min ──
            # The full-universe scan exists ONLY to find auto-entry candidates
            # (~40 HL l2Book fetches/5min + polling the top candidates every
            # tick). When auto-entry is OFF it's pure wasted HL bandwidth — skip
            # it so only open positions (fast tick) and armed gates (which fetch
            # their own books) hit HL. This is the big lever against rate-limiting
            # when you're only running manual carry trades.
            if now - last_slow_scan >= SLOW_SCAN_INTERVAL_SECONDS * 1000:
                last_slow_scan = now
                await client.refresh_hl_oracles(symbols)   # 1 cheap batch call
                if not runtime_flags["auto_entry"]:
                    candidates = []
                    log.info("Slow scan SKIPPED (auto-entry off) — only open "
                             "positions + armed gates fetch books")
                else:
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
            _write_pending_gates(pending_entries, pending_exits,
                                 executor._drips, executor._drip_exits)
            _write_gate_active(bool(pending_entries or pending_exits
                                    or executor._drips or executor._drip_exits))
            try:
                tmp = _HEALTH_FILE + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump({
                        "cycles_ok": pm.session_cycles_ok,
                        "cycles_error": pm.session_cycles_error,
                        "auto_entry": runtime_flags["auto_entry"],
                        "auto_exit": runtime_flags["auto_exit"],
                        "uptime_min": round((now_ms() - start_time) / 60_000, 1),
                        "_ts": time.time(),
                    }, fh)
                os.replace(tmp, _HEALTH_FILE)
            except Exception:
                pass

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

            # ── 4b. Reconcile actual funding/fees for open positions ──
            # /positions otherwise shows the entry-rate funding ESTIMATE, which
            # extrapolates a single snapshot linearly and drifts far from reality
            # (SKHX showed -$4.43 when actual was ~flat). Every 5 min, pull the
            # real settled funding + commission from the venues per open position
            # and write it for the control bot to display. Live only.
            if (not paper_mode
                    and now - last_cost_reconcile >= 300_000
                    and pm.positions):
                last_cost_reconcile = now
                costs = {}
                # Rebuild fresh so a closed position's funding can't leak into a
                # later position on the same symbol.
                actual_funding_by_sym.clear()
                for sym, p in list(pm.positions.items()):
                    try:
                        r = await client.reconcile_position_costs(
                            sym, p.entry_time, now_ms())
                        if r.get("ok"):
                            costs[sym] = {"funding": round(r["funding"], 4),
                                          "fees": round(r["fees"], 4),
                                          "ts": time.time()}
                            actual_funding_by_sym[sym] = r["funding"]
                    except Exception as e:
                        log.debug(f"cost reconcile {sym} failed: {e}")
                if costs:
                    try:
                        tmp = _COSTS_FILE + ".tmp"
                        with open(tmp, "w") as fh:
                            json.dump(costs, fh)
                        os.replace(tmp, _COSTS_FILE)
                    except Exception as e:
                        log.debug(f"position_costs write failed: {e}")

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
                    f"cycles: {pm.session_cycles_ok} ok / {pm.session_cycles_error} error | "
                    f"threshold={ENTRY_THRESHOLD_BPS:.0f}bps\n"
                    + (("  Open positions:\n" + "\n".join(pos_lines) + "\n") if pos_lines else "  No open positions\n")
                    + "  Closest to entry:\n" + ("\n".join(watch_lines) if watch_lines else "  (no data yet)")
                )
                last_heartbeat = now_ms()

            elapsed = time.time() - tick_start
            await asyncio.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))

    except KeyboardInterrupt:
        log.info("Stopped by user (Ctrl+C)")
    except Exception as e:
        # CRITICAL routes to the Telegram alert handler — the operator must
        # know the monitor died with live positions unmanaged.
        log.critical(f"MONITOR CRASHED: {e} — live positions are UNMANAGED "
                     f"until restart", exc_info=True)
        raise
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

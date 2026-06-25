#!/usr/bin/env python3
"""
Telegram control bot for the hyperaster arb monitor.

Always-on long-polling bot that lets you check status and control the trader
from your phone. Locked to an allowlist of chat IDs. Destructive commands
(/stop, /flatten) require a typed confirmation so a stray tap can't halt
trading or close the book.

Stdlib only — no extra deps. Reads the same SQLite DB the trader writes, and
shells out to systemctl / flatten.py for lifecycle + emergency actions.

Env (from .env, loaded by systemd EnvironmentFile or python-dotenv):
  ALERT_TELEGRAM_BOT_TOKEN        bot token (reused from alerting)
  ALERT_TELEGRAM_CHAT_ID          your chat id (single)
  CONTROL_TELEGRAM_CHAT_IDS       optional, comma-separated allowlist (overrides above)
  CONTROL_SERVICE_NAME            systemd unit to control (default: hyperaster)
  CONTROL_BRANCH                  git branch for /restart pull (default: current)

Commands:
  /status      service state + uptime + open position count
  /positions   open positions from the DB
  /pnl         realised P&L (today + all-time)
  /log [n]     last n journal lines (default 20)
  /start       start the trader service
  /stop        stop the trader  (requires: /stop YES)
  /restart     git pull + restart the trader
  /flatten     emergency close ALL positions (requires: /flatten YES)
  /help        command list
"""

import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parent
VENV_PY = BASE_DIR / ".venv" / "bin" / "python"
PYTHON = str(VENV_PY) if VENV_PY.exists() else "python3"

# Runtime mode file — written here, read by the trader via systemd
# EnvironmentFile=-/opt/hyperaster/data/mode.env. Not version controlled.
MODE_FILE = BASE_DIR / "data" / "mode.env"

# Runtime auto-entry kill switch + manual command inbox (consumed by the monitor).
# The monitor is the only process that places orders, so manual entries/exits are
# enqueued as files here rather than executed by this stdlib-only control bot.
AUTO_ENTRY_FILE = BASE_DIR / "data" / "auto_entry"
MANUAL_CMD_DIR = BASE_DIR / "data" / "manual_cmds"

TOKEN = os.getenv("ALERT_TELEGRAM_BOT_TOKEN", "")
SERVICE = os.getenv("CONTROL_SERVICE_NAME", "hyperaster")
BRANCH = os.getenv("CONTROL_BRANCH", "")

def _allowed_chat_ids() -> set[str]:
    raw = os.getenv("CONTROL_TELEGRAM_CHAT_IDS") or os.getenv("ALERT_TELEGRAM_CHAT_ID", "")
    return {c.strip() for c in raw.split(",") if c.strip()}

ALLOWED = _allowed_chat_ids()
API = f"https://api.telegram.org/bot{TOKEN}"

# Pending destructive confirmations: chat_id -> (command, expires_at)
_PENDING: dict[str, tuple[str, float]] = {}
# Pending manual live entries: chat_id -> (request_dict, expires_at). Kept
# separate because these carry args (symbol/direction/notional) that the simple
# action-name confirm flow can't round-trip.
_PENDING_ENTER: dict[str, tuple[dict, float]] = {}
_CONFIRM_TTL = 60  # seconds
# Monotonic counter so rapid manual-command enqueues get unique filenames.
_ENQUEUE_SEQ = 0


# ── Telegram I/O ──

def _api(method: str, params: dict, timeout: int = 35) -> dict:
    url = f"{API}/{method}"
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def send(chat_id: str, text: str, parse_mode: str | None = None):
    # Telegram caps messages at 4096 chars
    for i in range(0, len(text), 3900):
        params = {"chat_id": chat_id, "text": text[i:i + 3900]}
        if parse_mode:
            params["parse_mode"] = parse_mode
        try:
            _api("sendMessage", params, timeout=15)
        except Exception as e:
            print(f"send failed: {e}", flush=True)


# ── Shell helpers ──

def run(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        out = (p.stdout or "") + (p.stderr or "")
        return p.returncode, out.strip()
    except subprocess.TimeoutExpired:
        return 124, f"timed out after {timeout}s"
    except Exception as e:
        return 1, str(e)


def db_path() -> str:
    try:
        from config import DB_PATH
        return DB_PATH
    except Exception:
        return str(BASE_DIR / "data" / "positions.db")


def query_db(sql: str, args: tuple = ()) -> list[tuple]:
    import sqlite3
    conn = sqlite3.connect(db_path())
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def read_mode() -> str:
    """Current configured mode from the mode file; defaults to paper."""
    try:
        for line in MODE_FILE.read_text().splitlines():
            line = line.strip()
            if line.startswith("HYPERASTER_MODE="):
                v = line.split("=", 1)[1].strip().lower()
                if v in ("paper", "live"):
                    return v
    except FileNotFoundError:
        pass
    return "paper"


def write_mode(mode: str):
    MODE_FILE.parent.mkdir(parents=True, exist_ok=True)
    MODE_FILE.write_text(f"HYPERASTER_MODE={mode}\n")


def open_live_count() -> int:
    try:
        rows = query_db(
            "SELECT COUNT(*) FROM positions WHERE status NOT IN ('closed','error') AND paper=0"
        )
        return rows[0][0] if rows else 0
    except Exception:
        return 0


def _apply_mode(chat_id: str, mode: str):
    write_mode(mode)
    rc, out = run(["systemctl", "restart", SERVICE], timeout=30)
    time.sleep(2)
    _, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    send(chat_id, f"⚙️ mode → {mode.upper()} — service {active}\n{out}".strip())


# ── Command handlers ──

def _uptime_str() -> str:
    """Human-readable uptime from systemd."""
    _, ts = run(
        ["systemctl", "show", SERVICE, "--property=ExecMainStartTimestamp", "--value"],
        timeout=10,
    )
    if not ts or ts == "n/a":
        return "?"
    try:
        from datetime import datetime
        start = datetime.strptime(ts.strip(), "%a %Y-%m-%d %H:%M:%S %Z")
        delta = datetime.utcnow() - start
        hours, rem = divmod(int(delta.total_seconds()), 3600)
        mins = rem // 60
        if hours >= 24:
            return f"{hours // 24}d {hours % 24}h"
        return f"{hours}h {mins}m"
    except Exception:
        return ts.strip()


def _latest_spreads() -> str:
    """Parse the most recent tick log line from journalctl."""
    _, out = run(
        ["journalctl", "-u", SERVICE, "--no-pager", "-o", "cat",
         "--grep=Watching:", "-n", "1"],
        timeout=10,
    )
    if not out or "Watching:" not in out:
        return "(no spread data yet)"
    # Extract everything after "Watching:"
    parts = out.split("Watching:", 1)
    return parts[1].strip() if len(parts) > 1 else out.strip()


def cmd_status(chat_id: str, _arg: str):
    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    # Open positions
    try:
        rows = query_db(
            "SELECT COUNT(*) FROM positions WHERE status NOT IN ('closed','error') AND paper=0"
        )
        open_live = rows[0][0] if rows else 0
        rows = query_db(
            "SELECT COUNT(*) FROM positions WHERE status NOT IN ('closed','error') AND paper=1"
        )
        open_paper = rows[0][0] if rows else 0
    except Exception as e:
        open_live = open_paper = f"?({e})"
    mode = read_mode()
    uptime = _uptime_str()
    spreads = _latest_spreads()
    send(chat_id,
         f"🤖 {SERVICE}: {active.upper()} ({uptime})\n"
         f"mode: {mode}\n"
         f"open positions: {open_live} live / {open_paper} paper\n\n"
         f"📈 spreads: {spreads}")


def _load_live_spreads() -> dict:
    """Load latest_spreads.json written by the monitor."""
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "latest_spreads.json")
        with open(p) as f:
            return json.load(f)
    except Exception:
        return {}


def _load_pending_gates() -> dict:
    """Load pending_gates.json (basis-gated orders waiting for their target)."""
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "pending_gates.json")
        with open(p) as f:
            return json.load(f)
    except Exception:
        return {}


def _pending_gate_lines() -> list[str]:
    """Render the 'waiting for basis target' section for /positions."""
    g = _load_pending_gates()
    entries = g.get("entries", {}) or {}
    exits = g.get("exits", {}) or {}
    drips = g.get("drips", {}) or {}
    if not entries and not exits and not drips:
        return []
    out = ["⏳ Pending basis gates:"]
    for sym, r in entries.items():
        short = "L-HL/S-AST" if r.get("direction") == "long_hl_short_aster" else "L-AST/S-HL"
        remaining = r.get("notional", 0)
        orig = r.get("orig_notional", remaining)
        size_str = f"${remaining:.0f}" if abs(remaining - orig) < 1 else f"${remaining:.0f}/${orig:.0f} remaining"
        out.append(f"  • {sym} ENTER {short} {size_str} "
                   f"— waiting entry basis ≥ {r.get('target_bps', 0):+.0f}bps")
    for sym, r in exits.items():
        out.append(f"  • {sym} CLOSE — waiting exit basis ≥ {r.get('target_bps', 0):+.0f}bps")
    for sym, d in drips.items():
        short = "L-HL/S-AST" if d.get("direction") == "long_hl_short_aster" else "L-AST/S-HL"
        out.append(
            f"  • {sym} 💧DRIP {short} ${d.get('filled_notional',0):.0f}"
            f"/${d.get('target_notional',0):.0f} "
            f"({d.get('fills',0)} fills) min≥{d.get('min_basis_bps',0):.0f}bps"
        )
    out.append("  (/cancel SYM to clear a gate)")
    return out


def cmd_positions(chat_id: str, _arg: str):
    try:
        from config import (
            ROUND_TRIP_FEE, CARRY_ROUND_TRIP_FEE,
            EXIT_TARGET_NET_USD, EXIT_TARGET_NET_USD_BY_SYMBOL,
        )
        from position_manager import estimate_funding_pnl
    except Exception:
        send(chat_id, "Import error")
        return
    try:
        rows = query_db(
            "SELECT symbol, status, direction, entry_spread_bps, qty, entry_time, "
            "paper, notional_usd, hl_entry_price, aster_entry_price, "
            "hl_funding_rate, aster_funding_rate, "
            "COALESCE(entry_baseline_bps, 0), COALESCE(hold_for_funding, 0), "
            "COALESCE(entry_maker_venue, '') "
            "FROM positions WHERE status NOT IN ('closed','error') ORDER BY entry_time"
        )
    except Exception as e:
        send(chat_id, f"DB error: {e}")
        return
    gate_lines = _pending_gate_lines()
    if not rows:
        msg = "No open positions."
        if gate_lines:
            msg += "\n\n" + "\n".join(gate_lines)
        send(chat_id, msg)
        return
    now = time.time() * 1000
    live = _load_live_spreads()
    stale = ""
    if live.get("_ts") and time.time() - live["_ts"] > 30:
        stale = " ⚠️stale"
    lines = ["📊 Open positions:"]
    for (sym, status, direction, spread, qty, etime, paper, notional,
         hl_px, ast_px, hl_fr, ast_fr, entry_base, hold_for_funding,
         entry_maker_venue) in rows:
        held_h = (now - (etime or now)) / 3_600_000
        tag = " [paper]" if paper else ""
        if hold_for_funding:
            tag += " 💰carry"
        elif entry_maker_venue == "hl":
            tag += " 🅼maker"
        notional = notional or 1000
        # Maker-first (carry + convergence) pay HL-maker/Aster-taker; legacy
        # taker convergence pays HL-taker/Aster-maker.
        fees = notional * (CARRY_ROUND_TRIP_FEE if entry_maker_venue == "hl" else ROUND_TRIP_FEE)
        funding = estimate_funding_pnl(
            direction or "long_hl_short_aster", held_h, notional,
            hl_fr or 0, ast_fr or 0,
        )
        short_dir = "HL↑ Ast↓" if "long_hl" in (direction or "") else "HL↓ Ast↑"
        sym_target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(sym, EXIT_TARGET_NET_USD)

        # Current excess vs ENTRY baseline (what convergence exit uses)
        sym_live = live.get(sym, {})
        est_net_str = ""
        if sym_live and isinstance(sym_live, dict):
            # Recover raw spread: hl_excess = spread_bps - rolling_baseline
            raw_spread = sym_live.get("baseline", 0) + sym_live.get("hl_excess", 0)
            # Excess vs the baseline that existed at entry time
            excess_vs_entry = raw_spread - (entry_base or 0)
            pos_dir = direction or "long_hl_short_aster"
            own_excess = excess_vs_entry if "long_hl" in pos_dir else -excess_vs_entry
            excess_str = f"now={own_excess:+.0f}bps{stale}"
            if "est_net" in sym_live:
                est_net_str = f"  est_net=${sym_live['est_net']:+.2f} (executable)"
        else:
            excess_str = "now=?"

        qty_str = f"{qty or 0}"
        if hold_for_funding and notional:
            qty_str += f" (${notional:.0f})"

        if hold_for_funding:
            lines.append(
                f"• {sym}{tag} [{status}] {short_dir}\n"
                f"    HL:{hl_px:.2f}  Ast:{ast_px:.2f}  qty={qty_str}\n"
                f"    funding=${funding:+.2f}  fees=${fees:.2f}  held={held_h:.1f}h{est_net_str}"
            )
        else:
            lines.append(
                f"• {sym}{tag} [{status}] {short_dir}\n"
                f"    excess: entry={spread:+.0f}bps  {excess_str}  exit≤0bps\n"
                f"    HL:{hl_px:.2f}  Ast:{ast_px:.2f}  qty={qty_str}\n"
                f"    funding=${funding:+.2f}  fees=${fees:.2f}  held={held_h:.1f}h{est_net_str}\n"
                f"    target=${sym_target:.2f} net | need gross≥${sym_target - funding + fees:.2f}"
            )
    if gate_lines:
        lines.append("")
        lines.extend(gate_lines)
    send(chat_id, "\n".join(lines))


def _pnl_for_mode(paper: int) -> tuple[int, float, int, float, int, float]:
    """Return (n_all, pnl_all, n_today, pnl_today, n_err, funding_all) for a mode."""
    rows = query_db(
        "SELECT COUNT(*), COALESCE(SUM(net_pnl),0), COALESCE(SUM(funding_pnl),0) "
        "FROM positions WHERE status='closed' AND paper=?", (paper,),
    )
    n_all, pnl_all, funding_all = rows[0]
    midnight = int(time.time()) - (int(time.time()) % 86400)
    rows = query_db(
        "SELECT COUNT(*), COALESCE(SUM(net_pnl),0) FROM positions "
        "WHERE status='closed' AND paper=? AND exit_time >= ?",
        (paper, midnight * 1000),
    )
    n_today, pnl_today = rows[0]
    rows = query_db(
        "SELECT COUNT(*) FROM positions WHERE status='error' AND paper=?",
        (paper,),
    )
    n_err = rows[0][0]
    return n_all, pnl_all, n_today, pnl_today, n_err, funding_all


def cmd_pnl(chat_id: str, _arg: str):
    try:
        sections = []
        for label, paper_val in [("LIVE", 0), ("PAPER", 1)]:
            n_all, pnl_all, n_today, pnl_today, n_err, funding_all = _pnl_for_mode(paper_val)
            if n_all == 0 and n_today == 0 and n_err == 0:
                continue
            err_line = f"\n⚠️ {n_err} position(s) in ERROR state" if n_err else ""
            sections.append(
                f"💰 Realised P&L ({label})\n"
                f"today: ${pnl_today:.2f} ({n_today} trades)\n"
                f"all-time: ${pnl_all:.2f} ({n_all} trades)\n"
                f"  incl. funding carry: ${funding_all:+.2f}"
                f"{err_line}"
            )
        if not sections:
            send(chat_id, "No closed trades yet.")
            return
    except Exception as e:
        send(chat_id, f"DB error: {e}")
        return
    send(chat_id, "\n\n".join(sections))


def cmd_log(chat_id: str, arg: str):
    n = 20
    if arg.strip().isdigit():
        n = min(int(arg.strip()), 100)
    rc, out = run(["journalctl", "-u", SERVICE, "-n", str(n), "--no-pager", "-o", "cat"], timeout=15)
    send(chat_id, f"📜 last {n} lines:\n\n{out or '(empty)'}")


def cmd_start(chat_id: str, _arg: str):
    rc, out = run(["systemctl", "start", SERVICE], timeout=30)
    time.sleep(2)
    _, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    send(chat_id, f"▶️ start: {'ok' if rc == 0 else 'FAILED'} — now {active}\n{out}")


def cmd_stop(chat_id: str, arg: str):
    # Warn about open positions; require confirmation
    try:
        rows = query_db(
            "SELECT COUNT(*) FROM positions WHERE status NOT IN ('closed','error') AND paper=0"
        )
        n_open = rows[0][0]
    except Exception:
        n_open = "?"
    if arg.strip().upper() != "YES":
        _PENDING[chat_id] = ("stop", time.time() + _CONFIRM_TTL)
        send(chat_id,
             f"⚠️ Stop {SERVICE}? {n_open} live position(s) are open and will be "
             f"LEFT UNMANAGED on the exchanges (no exit monitoring).\n\n"
             f"Send /stop YES within {_CONFIRM_TTL}s to confirm.")
        return
    rc, out = run(["systemctl", "stop", SERVICE], timeout=30)
    send(chat_id, f"⏹️ stop: {'ok' if rc == 0 else 'FAILED'}\n{out}")


def cmd_restart(chat_id: str, _arg: str):
    send(chat_id, "🔄 pulling + restarting…")
    pull_cmd = ["git", "-C", str(BASE_DIR), "pull"]
    if BRANCH:
        pull_cmd += ["origin", BRANCH]
    rc, out = run(pull_cmd, timeout=60)
    send(chat_id, f"git pull:\n{out}")
    rc, out = run(["systemctl", "restart", SERVICE], timeout=30)
    time.sleep(2)
    _, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    send(chat_id, f"restart: {'ok' if rc == 0 else 'FAILED'} — now {active}\n{out}")


def cmd_flatten(chat_id: str, arg: str):
    if arg.strip().upper() != "YES":
        _PENDING[chat_id] = ("flatten", time.time() + _CONFIRM_TTL)
        send(chat_id,
             "🚨 FLATTEN — this market-closes all BOT-MANAGED positions "
             "(tracked in DB) on BOTH venues (taker fees, immediate).\n"
             "Unrelated positions (spot-perp, manual trades) are NOT touched.\n\n"
             f"Send /flatten YES within {_CONFIRM_TTL}s to confirm.")
        return
    send(chat_id, "🚨 flattening bot-managed positions…")
    rc, out = run(
        [PYTHON, str(BASE_DIR / "flatten.py"), "--yes"],
        timeout=180,
    )
    send(chat_id, f"flatten {'completed' if rc == 0 else 'FINISHED WITH ERRORS'}:\n\n{out[-3500:]}")


def cmd_mode(chat_id: str, _arg: str):
    send(chat_id, f"⚙️ configured mode: {read_mode().upper()}\n"
                  f"(use /paper or /live to switch — restarts the trader)")


def cmd_live(chat_id: str, arg: str):
    if arg.strip().upper() != "YES":
        _PENDING[chat_id] = ("live", time.time() + _CONFIRM_TTL)
        send(chat_id,
             "⚠️ Switch to LIVE mode? The trader will place REAL orders with REAL "
             "capital on both venues.\n\n"
             f"Send /live YES within {_CONFIRM_TTL}s to confirm.")
        return
    _apply_mode(chat_id, "live")


def cmd_paper(chat_id: str, arg: str):
    n_live = open_live_count()
    # Switching to paper while live positions are open abandons them (paper mode
    # won't manage real positions). Require confirmation in that case.
    if n_live and arg.strip().upper() != "YES":
        _PENDING[chat_id] = ("paper", time.time() + _CONFIRM_TTL)
        send(chat_id,
             f"⚠️ {n_live} live position(s) are open. Switching to PAPER will leave "
             f"them UNMANAGED on the exchanges. Consider /flatten YES first.\n\n"
             f"Send /paper YES within {_CONFIRM_TTL}s to switch anyway.")
        return
    _apply_mode(chat_id, "paper")


def cmd_spreads(chat_id: str, _arg: str):
    """Show latest fast-tick spreads + slow-scan Top 5 with baseline detail."""
    fast = _latest_spreads()
    # Grab last slow scan line (shows excess/threshold and rolling baseline)
    _, out = run(
        ["journalctl", "-u", SERVICE, "--no-pager", "-o", "cat",
         "--grep=Top 5:", "-n", "1"],
        timeout=10,
    )
    if out and "Top 5:" in out:
        top5 = out.split("Top 5:", 1)[1].strip()
    else:
        top5 = "(no slow scan yet)"
    send(chat_id,
         f"📈 Fast tick:  (excess% of threshold, or excessbps(streak))\n{fast}\n\n"
         f"🔍 Top 5  (excess / threshold bps · base = 8h-median spread):\n{top5}")


def cmd_trades(chat_id: str, arg: str):
    """Show last N closed trades with full P&L breakdown."""
    n = 5
    if arg.strip().isdigit():
        n = min(int(arg.strip()), 20)
    try:
        rows = query_db(
            "SELECT symbol, direction, entry_spread_bps, exit_spread_bps, "
            "hl_entry_price, hl_exit_price, aster_entry_price, aster_exit_price, "
            "qty, notional_usd, gross_pnl, fee_cost, "
            "COALESCE(funding_pnl,0), net_pnl, exit_reason, "
            "entry_time, exit_time, paper "
            "FROM positions WHERE status='closed' "
            "ORDER BY exit_time DESC LIMIT ?",
            (n,),
        )
    except Exception as e:
        send(chat_id, f"DB error: {e}")
        return
    if not rows:
        send(chat_id, "No closed trades yet.")
        return
    lines = [f"📋 Last {len(rows)} trade(s):"]
    for (sym, direction, entry_sp, exit_sp, hl_in, hl_out, ast_in, ast_out,
         qty, notional, gross, fees, funding, net, reason, etime, xtime, paper) in rows:
        tag = " [paper]" if paper else ""
        held_h = ((xtime or 0) - (etime or 0)) / 3_600_000
        short_dir = "HL↑ Ast↓" if "long_hl" in (direction or "") else "HL↓ Ast↑"
        lines.append(
            f"\n• {sym}{tag} {short_dir} — {reason}\n"
            f"  entry spread={entry_sp or 0:.1f}bps → exit={exit_sp or 0:.1f}bps\n"
            f"  HL: {hl_in:.2f}→{hl_out:.2f}  Ast: {ast_in:.2f}→{ast_out:.2f}\n"
            f"  qty={qty}  notional=${notional or 0:.0f}\n"
            f"  gross=${gross:.4f}  fees=${fees:.4f}  funding=${funding:+.4f}\n"
            f"  net=${net:.4f}  held={held_h:.1f}h"
        )
    send(chat_id, "\n".join(lines))


_DIR_ALIASES = {
    "long_hl_short_aster": "long_hl_short_aster",
    "buy_hl": "long_hl_short_aster", "long_hl": "long_hl_short_aster",
    "l-hl/s-ast": "long_hl_short_aster", "hl": "long_hl_short_aster",
    "long_aster_short_hl": "long_aster_short_hl",
    "buy_aster": "long_aster_short_hl", "long_aster": "long_aster_short_hl",
    "l-ast/s-hl": "long_aster_short_hl", "aster": "long_aster_short_hl",
}


def _enqueue_manual(cmd: dict):
    """Atomically drop a manual command file for the monitor to consume.

    Uses a nanosecond timestamp + pid so rapid enqueues can't collide on the
    same filename (which would silently drop a command).
    """
    MANUAL_CMD_DIR.mkdir(parents=True, exist_ok=True)
    global _ENQUEUE_SEQ
    _ENQUEUE_SEQ += 1
    cid = f"{time.time_ns()}_{os.getpid()}_{_ENQUEUE_SEQ}"
    dest = MANUAL_CMD_DIR / f"{cid}.json"
    tmp = MANUAL_CMD_DIR / f"{cid}.json.tmp"
    tmp.write_text(json.dumps(cmd))
    tmp.replace(dest)


def cmd_autoentry(chat_id: str, arg: str):
    """Toggle the auto basis-arb entry scanner. Exits/manual entries unaffected."""
    a = arg.strip().lower()
    if a not in ("on", "off"):
        cur = "off" if (AUTO_ENTRY_FILE.exists()
                        and AUTO_ENTRY_FILE.read_text().strip().lower() == "off") else "on"
        send(chat_id,
             f"auto-entry is currently {cur.upper()}.\n"
             "/autoentry off — stop auto-opening basis arbs (exits still run)\n"
             "/autoentry on — resume auto entry")
        return
    AUTO_ENTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTO_ENTRY_FILE.write_text(a + "\n")
    if a == "off":
        send(chat_id, "🛑 auto-entry DISABLED — the scanner won't open new basis arbs. "
                      "Exits and manual /enter still work. (takes effect within ~1s, no restart)")
    else:
        send(chat_id, "✅ auto-entry ENABLED — scanner will auto-open basis arbs again.")


def cmd_enter(chat_id: str, arg: str):
    """Manually open ONE delta-neutral funding-carry hold via the monitor.

    Usage: /enter SYMBOL DIRECTION NOTIONAL [BASIS_TARGET_BPS]
      DIRECTION: long_hl_short_aster | long_aster_short_hl
                 (aliases: buy_hl / buy_aster / L-HL/S-AST / L-AST/S-HL)
      NOTIONAL : USD per leg
      BASIS_TARGET_BPS (optional): only fill once the executable entry basis is
                 at or better than this (bps, in your favour). Omit = enter now.

    Held for funding carry — the bot won't close it on basis convergence, only
    on safety stops (mark-to-market loss / 1-week timeout) or manual /close.
    Leverage 5x + isolated margin are applied automatically on entry.
    In live mode this places REAL orders and requires a typed YES.
    """
    toks = arg.split()
    if len(toks) not in (3, 4):
        send(chat_id,
             "Usage: /enter SYMBOL DIRECTION NOTIONAL [BASIS_TARGET_BPS]\n"
             "e.g. /enter SMSN long_hl_short_aster 1000\n"
             "     /enter SMSN buy_hl 1000 -10   (wait until entry basis ≥ -10bps)\n"
             "DIRECTION aliases: buy_hl / buy_aster / L-HL/S-AST / L-AST/S-HL")
        return
    symbol = toks[0].upper()
    direction = _DIR_ALIASES.get(toks[1].lower())
    if not direction:
        send(chat_id, f"Bad direction {toks[1]!r}. Use long_hl_short_aster or long_aster_short_hl "
                      "(or buy_hl / buy_aster).")
        return
    try:
        notional = float(toks[2])
        if notional <= 0:
            raise ValueError
    except ValueError:
        send(chat_id, f"Bad notional {toks[2]!r} — must be a positive number of USD.")
        return
    target_bps = None
    if len(toks) == 4:
        try:
            target_bps = float(toks[3])
        except ValueError:
            send(chat_id, f"Bad basis target {toks[3]!r} — must be a number (bps).")
            return

    req = {"action": "enter", "symbol": symbol, "direction": direction, "notional": notional}
    if target_bps is not None:
        req["target_bps"] = target_bps
    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    if active.strip() != "active":
        send(chat_id, f"⚠️ trader service is {active.strip()} — start it first (/start), "
                      "the monitor is what places the order.")
        return

    short = "L-HL/S-AST" if direction == "long_hl_short_aster" else "L-AST/S-HL"
    gate = f" once basis ≥ {target_bps:.0f}bps" if target_bps is not None else ""
    if read_mode() == "live":
        _PENDING_ENTER[chat_id] = (req, time.time() + _CONFIRM_TTL)
        send(chat_id,
             f"⚠️ LIVE order: enter {symbol} {short} ${notional:.0f}/leg as a funding hold{gate}.\n"
             "5x isolated. This places REAL orders. Reply YES within 60s to confirm.")
        return
    _enqueue_manual(req)
    send(chat_id, f"📩 queued [PAPER] entry: {symbol} {short} ${notional:.0f}{gate}. "
                  "You'll get an alert when it's placed.")


def cmd_close(chat_id: str, arg: str):
    """Manually close one open position via the monitor.

    Usage: /close SYMBOL [BASIS_TARGET_BPS]
      Omit target = close now (cross the book).
      With target = wait until the executable exit basis is at or better than the
      target (bps, in your favour). Safety stops still close it if it stays bad.
    """
    toks = arg.split()
    if not toks:
        send(chat_id, "Usage: /close SYMBOL [BASIS_TARGET_BPS]\n"
                      "e.g. /close SMSN        (close now)\n"
                      "     /close SMSN 5       (wait until exit basis ≥ 5bps)")
        return
    symbol = toks[0].upper()
    req = {"action": "close", "symbol": symbol}
    gate = ""
    if len(toks) >= 2:
        try:
            req["target_bps"] = float(toks[1])
            gate = f" once exit basis ≥ {req['target_bps']:.0f}bps"
        except ValueError:
            send(chat_id, f"Bad basis target {toks[1]!r} — must be a number (bps).")
            return
    _enqueue_manual(req)
    send(chat_id, f"📩 queued close for {symbol}{gate}. You'll get an alert when the exit is submitted.")


def cmd_cancel(chat_id: str, arg: str):
    """Cancel a pending basis-gated /enter or /close that hasn't fired yet."""
    symbol = arg.strip().upper()
    if not symbol:
        send(chat_id, "Usage: /cancel SYMBOL")
        return
    _enqueue_manual({"action": "cancel", "symbol": symbol})
    send(chat_id, f"📩 cancel requested for any pending gate on {symbol}.")


def cmd_drip(chat_id: str, arg: str):
    """Taker-taker drip entry: small bites until target notional is reached.

    Usage: /drip SYMBOL DIRECTION NOTIONAL MIN_BASIS_BPS [BITE_USD]
      DIRECTION: long_hl_short_aster | long_aster_short_hl (or aliases)
      NOTIONAL:  total target USD per leg
      MIN_BASIS_BPS: minimum executable basis (bps) to place a bite
      BITE_USD (optional): notional per bite, default $200

    Each tick, if the executable spread ≥ MIN_BASIS_BPS, places one small
    taker-taker order (HL IOC + Aster IOC). Accumulates into one position.
    Use /cancel SYMBOL to stop early.
    """
    toks = arg.split()
    if len(toks) not in (4, 5):
        send(chat_id,
             "Usage: /drip SYMBOL DIRECTION NOTIONAL MIN_BASIS_BPS [BITE_USD]\n"
             "e.g. /drip ZHIPU buy_hl 2000 50\n"
             "     /drip ZHIPU buy_hl 2000 50 100   (bites of $100)\n"
             "DIRECTION aliases: buy_hl / buy_aster / L-HL/S-AST / L-AST/S-HL")
        return
    symbol = toks[0].upper()
    direction = _DIR_ALIASES.get(toks[1].lower())
    if not direction:
        send(chat_id, f"Bad direction {toks[1]!r}. Use long_hl_short_aster or long_aster_short_hl.")
        return
    try:
        notional = float(toks[2])
        if notional <= 0:
            raise ValueError
    except ValueError:
        send(chat_id, f"Bad notional {toks[2]!r} — must be a positive number.")
        return
    try:
        min_basis = float(toks[3])
    except ValueError:
        send(chat_id, f"Bad min basis {toks[3]!r} — must be a number (bps).")
        return
    bite = 0.0
    if len(toks) == 5:
        try:
            bite = float(toks[4])
            if bite <= 0:
                raise ValueError
        except ValueError:
            send(chat_id, f"Bad bite size {toks[4]!r} — must be a positive number.")
            return

    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    if active.strip() != "active":
        send(chat_id, f"⚠️ trader service is {active.strip()} — start it first (/start).")
        return

    req = {"action": "drip", "symbol": symbol, "direction": direction,
           "notional": notional, "min_basis_bps": min_basis}
    if bite:
        req["bite_notional"] = bite
    short = "L-HL/S-AST" if direction == "long_hl_short_aster" else "L-AST/S-HL"
    bite_str = f" bite=${bite:.0f}" if bite else ""

    if read_mode() == "live":
        _PENDING_ENTER[chat_id] = (req, time.time() + _CONFIRM_TTL)
        send(chat_id,
             f"⚠️ LIVE drip: {symbol} {short} ${notional:.0f} target, "
             f"min basis {min_basis:.0f}bps{bite_str}.\n"
             "This places REAL taker-taker orders each tick. Reply YES within 60s.")
        return
    _enqueue_manual(req)
    send(chat_id,
         f"📩 queued [PAPER] drip: {symbol} {short} ${notional:.0f} target, "
         f"min basis {min_basis:.0f}bps{bite_str}. /cancel {symbol} to stop.")


def cmd_import(chat_id: str, arg: str):
    """Import existing venue positions into the bot's DB for management.

    Usage: /import          — scan both venues, show offsetting pairs
           /import SYMBOL   — import a specific symbol's offsetting pair
    """
    symbol = arg.strip().upper() if arg.strip() else ""
    req = {"action": "import"}
    if symbol:
        req["symbol"] = symbol
    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    if active.strip() != "active":
        send(chat_id, f"⚠️ trader service is {active.strip()} — start it first (/start), "
                      "the monitor queries the venues.")
        return
    _enqueue_manual(req)
    if symbol:
        send(chat_id, f"📩 import requested for {symbol}. Scanning venues…")
    else:
        send(chat_id, "📩 scanning both venues for importable offsetting positions…")


def cmd_funding(chat_id: str, arg: str):
    """Rank funding-carry opportunities across the equity universe.

    Shells out to scripts/funding_scan.py (needs the venv + live API egress)
    so the always-on control bot stays stdlib-only and the trading loop is
    untouched. Optional arg = how many to show (default 12).
    """
    top = arg.strip() if arg.strip().isdigit() else "12"
    script = BASE_DIR / "scripts" / "funding_scan.py"
    send(chat_id, "⏳ scanning funding rates…")
    code, out = run([PYTHON, str(script), "--top", top], timeout=90)
    send(chat_id, out or f"(no output, exit {code})")


def cmd_book(chat_id: str, arg: str):
    """Top-5 order-book snapshot for one name on both venues.

    Shells out to scripts/book_snapshot.py (venv + live API egress).
    Usage: /book SYMBOL
    """
    symbol = arg.strip().split()[0].upper() if arg.strip() else ""
    if not symbol:
        send(chat_id, "Usage: /book SYMBOL  (e.g. /book NBIS)")
        return
    script = BASE_DIR / "scripts" / "book_snapshot.py"
    send(chat_id, f"⏳ fetching {symbol} books…")
    code, out = run([PYTHON, str(script), symbol], timeout=30)
    send(chat_id, f"<pre>{out}</pre>" if out else f"(no output, exit {code})",
         parse_mode="HTML")


def cmd_help(chat_id: str, _arg: str):
    send(chat_id,
         "Commands:\n"
         "/status — service state + spreads + positions\n"
         "/spreads — current spread vs threshold detail\n"
         "/book SYM — top-5 order book on both venues\n"
         "/funding [n] — top funding-carry opportunities\n"
         "/enter SYM DIR NOTIONAL [basis_bps] — open a funding hold; basis_bps waits for a fill level\n"
         "/close SYM [basis_bps] — close a position; basis_bps waits for a fill level\n"
         "/cancel SYM — cancel a pending basis-gated /enter or /close or /drip\n"
         "/drip SYM DIR NOTIONAL MIN_BPS [BITE] — taker-taker drip entry\n"
         "/import [SYM] — adopt existing venue positions into the bot for management\n"
         "/autoentry on|off — toggle auto basis-arb entry (exits unaffected)\n"
         "/positions — open positions detail\n"
         "/trades [n] — last n closed trades with P&L detail\n"
         "/pnl — realised P&L (today + all-time)\n"
         "/log [n] — last n journal lines\n"
         "/mode — show configured mode\n"
         "/paper — switch to paper mode (restarts)\n"
         "/live YES — switch to live mode (restarts)\n"
         "/start — start trader\n"
         "/stop YES — stop trader (positions left open!)\n"
         "/restart — git pull + restart\n"
         "/flatten YES — emergency close ALL positions")


HANDLERS = {
    "/status": cmd_status, "/positions": cmd_positions, "/pos": cmd_positions,
    "/pnl": cmd_pnl, "/trades": cmd_trades,
    "/book": cmd_book,
    "/funding": cmd_funding, "/carry": cmd_funding,
    "/enter": cmd_enter, "/close": cmd_close, "/cancel": cmd_cancel,
    "/drip": cmd_drip, "/import": cmd_import,
    "/autoentry": cmd_autoentry,
    "/log": cmd_log, "/logs": cmd_log,
    "/spreads": cmd_spreads, "/spread": cmd_spreads,
    "/mode": cmd_mode, "/paper": cmd_paper, "/live": cmd_live,
    "/start": cmd_start, "/stop": cmd_stop, "/restart": cmd_restart,
    "/flatten": cmd_flatten, "/help": cmd_help,
}


def handle_message(msg: dict):
    chat_id = str(msg.get("chat", {}).get("id", ""))
    text = (msg.get("text") or "").strip()
    if not chat_id or not text:
        return

    if chat_id not in ALLOWED:
        # Silent ignore + log — don't confirm the bot exists to strangers
        print(f"IGNORED unauthorized chat {chat_id}: {text!r}", flush=True)
        return

    # Strip @botname suffix that group chats add
    parts = text.split(maxsplit=1)
    cmd = parts[0].split("@")[0].lower()
    arg = parts[1] if len(parts) > 1 else ""

    # Resolve a pending confirmation if they reply with a bare YES
    if text.upper() == "YES":
        # Manual live entry (carries args) takes priority over action-name confirms.
        pend_enter = _PENDING_ENTER.pop(chat_id, None)
        if pend_enter and pend_enter[1] > time.time():
            req = pend_enter[0]
            _enqueue_manual(req)
            short = "L-HL/S-AST" if req["direction"] == "long_hl_short_aster" else "L-AST/S-HL"
            send(chat_id,
                 f"📩 queued LIVE entry: {req['symbol']} {short} ${req['notional']:.0f}. "
                 "You'll get an alert when it's placed.")
            return
        pend = _PENDING.pop(chat_id, None)
        if pend and pend[1] > time.time():
            action = pend[0]  # 'stop' | 'flatten'
            HANDLERS["/" + action](chat_id, "YES")
            return
        send(chat_id, "Nothing pending to confirm (or it expired).")
        return

    handler = HANDLERS.get(cmd)
    if handler:
        print(f"cmd from {chat_id}: {cmd} {arg!r}", flush=True)
        try:
            handler(chat_id, arg)
        except Exception as e:
            send(chat_id, f"Error running {cmd}: {e}")
    else:
        send(chat_id, f"Unknown command {cmd}. /help for the list.")


def main():
    if not TOKEN:
        raise SystemExit("ALERT_TELEGRAM_BOT_TOKEN not set")
    if not ALLOWED:
        raise SystemExit("No allowed chat IDs — set ALERT_TELEGRAM_CHAT_ID or CONTROL_TELEGRAM_CHAT_IDS")
    print(f"Control bot up. service={SERVICE} allowed={ALLOWED} python={PYTHON}", flush=True)

    try:
        _api("setMyCommands", {"commands": json.dumps([
            {"command": "status", "description": "Service state + spreads + positions"},
            {"command": "spreads", "description": "Current spread vs threshold"},
            {"command": "book", "description": "Top-5 order book on both venues: SYM"},
            {"command": "funding", "description": "Top funding-carry opportunities"},
            {"command": "positions", "description": "Open positions + pending basis gates"},
            {"command": "enter", "description": "Open a funding hold: SYM DIR NOTIONAL [basis_bps]"},
            {"command": "close", "description": "Close a position: SYM [basis_bps]"},
            {"command": "cancel", "description": "Cancel a pending basis-gated order: SYM"},
            {"command": "drip", "description": "Taker-taker drip entry: SYM DIR NOTIONAL MIN_BPS"},
            {"command": "import", "description": "Adopt existing venue positions: [SYM]"},
            {"command": "autoentry", "description": "Toggle auto basis-arb entry: on|off"},
            {"command": "trades", "description": "Last N closed trades with P&L"},
            {"command": "pnl", "description": "Realised P&L today + all-time"},
            {"command": "log", "description": "Last n journal lines"},
            {"command": "mode", "description": "Show configured mode"},
            {"command": "paper", "description": "Switch to paper mode"},
            {"command": "live", "description": "Switch to live mode (YES to confirm)"},
            {"command": "start", "description": "Start trader"},
            {"command": "stop", "description": "Stop trader (YES to confirm)"},
            {"command": "restart", "description": "Git pull + restart"},
            {"command": "flatten", "description": "Emergency close ALL positions"},
            {"command": "help", "description": "Command list"},
        ])}, timeout=10)
    except Exception as e:
        print(f"setMyCommands failed: {e}", flush=True)

    offset = 0
    while True:
        try:
            resp = _api("getUpdates", {"offset": offset, "timeout": 30}, timeout=40)
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if msg:
                    handle_message(msg)
        except Exception as e:
            print(f"poll error: {e}", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()

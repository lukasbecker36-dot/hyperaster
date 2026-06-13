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
_CONFIRM_TTL = 60  # seconds


# ── Telegram I/O ──

def _api(method: str, params: dict, timeout: int = 35) -> dict:
    url = f"{API}/{method}"
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def send(chat_id: str, text: str):
    # Telegram caps messages at 4096 chars
    for i in range(0, len(text), 3900):
        try:
            _api("sendMessage", {"chat_id": chat_id, "text": text[i:i + 3900]}, timeout=15)
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


def cmd_positions(chat_id: str, _arg: str):
    try:
        from config import ROUND_TRIP_FEE, EXIT_TARGET_NET_USD, EXIT_TARGET_NET_USD_BY_SYMBOL
        from position_manager import estimate_funding_pnl
    except Exception:
        send(chat_id, "Import error")
        return
    try:
        rows = query_db(
            "SELECT symbol, status, direction, entry_spread_bps, qty, entry_time, "
            "paper, notional_usd, hl_entry_price, aster_entry_price, "
            "hl_funding_rate, aster_funding_rate, "
            "COALESCE(entry_baseline_bps, 0) "
            "FROM positions WHERE status NOT IN ('closed','error') ORDER BY entry_time"
        )
    except Exception as e:
        send(chat_id, f"DB error: {e}")
        return
    if not rows:
        send(chat_id, "No open positions.")
        return
    now = time.time() * 1000
    live = _load_live_spreads()
    stale = ""
    if live.get("_ts") and time.time() - live["_ts"] > 30:
        stale = " ⚠️stale"
    lines = ["📊 Open positions:"]
    for (sym, status, direction, spread, qty, etime, paper, notional,
         hl_px, ast_px, hl_fr, ast_fr, entry_base) in rows:
        held_h = (now - (etime or now)) / 3_600_000
        tag = " [paper]" if paper else ""
        notional = notional or 1000
        fees = notional * ROUND_TRIP_FEE
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

        lines.append(
            f"• {sym}{tag} [{status}] {short_dir}\n"
            f"    excess: entry={spread:+.0f}bps  {excess_str}  exit≤0bps\n"
            f"    HL:{hl_px:.2f}  Ast:{ast_px:.2f}  qty={qty or 0}\n"
            f"    funding=${funding:+.2f}  fees=${fees:.2f}  held={held_h:.1f}h{est_net_str}\n"
            f"    target=${sym_target:.2f} net | need gross≥${sym_target - funding + fees:.2f}"
        )
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
             "🚨 EMERGENCY FLATTEN — this market-closes EVERY open position on "
             "BOTH venues (taker fees, immediate).\n\n"
             f"Send /flatten YES within {_CONFIRM_TTL}s to confirm.")
        return
    send(chat_id, "🚨 flattening — querying both venues and closing everything…")
    rc, out = run(
        [PYTHON, str(BASE_DIR / "flatten.py"), "--reconcile", "--yes"],
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


def cmd_help(chat_id: str, _arg: str):
    send(chat_id,
         "Commands:\n"
         "/status — service state + spreads + positions\n"
         "/spreads — current spread vs threshold detail\n"
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
            {"command": "positions", "description": "Open positions detail"},
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

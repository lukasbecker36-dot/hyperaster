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
  /restart     git pull + restart the trader, capture daemon AND control bot
  /flatten     emergency close ALL positions (requires: /flatten YES)
  /help        command list
"""

import html
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
AUTO_EXIT_FILE = BASE_DIR / "data" / "auto_exit"
AUTO_NOTIONAL_FILE = BASE_DIR / "data" / "auto_notional"
MANUAL_CMD_DIR = BASE_DIR / "data" / "manual_cmds"
BALANCES_FILE = BASE_DIR / "data" / "balances.json"

# Adaptive price precision, so sub-cent crypto (Notcoin ~$0.00036) doesn't render
# as "0.00" in /positions and /trades. Imported from config, but this bot is
# stdlib-only by design and tolerates config being unimportable (see db_path),
# so it degrades to the old fixed 2dp rather than failing to start.
try:
    from config import fmt_px
except Exception:
    def fmt_px(px) -> str:
        try:
            return f"{float(px):.2f}"
        except (TypeError, ValueError):
            return "?"

TOKEN = os.getenv("ALERT_TELEGRAM_BOT_TOKEN", "")
SERVICE = os.getenv("CONTROL_SERVICE_NAME", "hyperaster")
# The control bot's OWN systemd unit, so /restart can restart it too (to pick up
# control_bot.py changes). Defaults to "<trader>-control"; override via
# CONTROL_BOT_SERVICE_NAME if yours differs.
CONTROL_BOT_SERVICE = os.getenv("CONTROL_BOT_SERVICE_NAME", f"{SERVICE}-control")
# The order-book capture daemon's systemd unit, so /restart also refreshes it
# (it reads the gate-active backoff flag + runs the retention prune). Defaults to
# "<trader>-capture"; override via CAPTURE_SERVICE_NAME. Restarted only if the
# unit is actually loaded, so a wrong/absent name is a no-op, not an error.
CAPTURE_SERVICE = os.getenv("CAPTURE_SERVICE_NAME", f"{SERVICE}-capture")
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


def send_pre(chat_id: str, out: str, code: int = 0):
    """Send subprocess output as a monospaced <pre> block, HTML-escaping the
    body first. Without the escape, stray <, >, & in the text — e.g. the
    '⚠ = < -10' / 'sum > fees' legend in /opps — trip Telegram's HTML parser
    into a 400 Bad Request, which silently drops the reply and reads as a hung
    command (you see the '⏳' message and then nothing)."""
    if not out:
        send(chat_id, f"(no output, exit {code})")
        return
    # Split on the RAW text (by line, so a wide table row stays intact) and wrap
    # each chunk in its own <pre> — otherwise send()'s blind 3900-char chunking
    # would cut a long block's open/close tags apart and 400 again.
    chunk, lines = [], out.split("\n")
    n = 0
    for line in lines:
        if n + len(line) + 1 > 3500 and chunk:
            send(chat_id, f"<pre>{html.escape(chr(10).join(chunk))}</pre>",
                 parse_mode="HTML")
            chunk, n = [], 0
        chunk.append(line)
        n += len(line) + 1
    if chunk:
        send(chat_id, f"<pre>{html.escape(chr(10).join(chunk))}</pre>",
             parse_mode="HTML")


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
    notl, is_override = _current_auto_notional()
    notl_str = f"${notl:.0f}/leg" + (" (override)" if is_override else "")
    # Session health (cycles + auto flags) published by the trader.
    health_line = ""
    try:
        h = json.loads((BASE_DIR / "data" / "bot_health.json").read_text())
        ae = "on" if h.get("auto_entry", True) else "OFF"
        ax = "on" if h.get("auto_exit", True) else "OFF"
        health_line = (f"cycles: {h.get('cycles_ok', 0)} ok / "
                       f"{h.get('cycles_error', 0)} error  |  "
                       f"auto-entry {ae} · auto-exit {ax}\n")
    except Exception:
        pass
    send(chat_id,
         f"🤖 {SERVICE}: {active.upper()} ({uptime})\n"
         f"mode: {mode}\n"
         f"open positions: {open_live} live / {open_paper} paper\n"
         f"auto-entry size: {notl_str}\n"
         f"{health_line}\n"
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


def _load_position_costs() -> dict:
    """Load position_costs.json — actual accrued funding/fees per open position,
    reconciled from the venues by the trader (vs the entry-rate estimate)."""
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "position_costs.json")
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
    drip_exits = g.get("drip_exits", {}) or {}
    if not entries and not exits and not drips and not drip_exits:
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
        unhedged = d.get("unhedged_qty", 0)
        uh_str = f" unhedged={unhedged:.3f}sh" if unhedged > 0 else ""
        out.append(
            f"  • {sym} 💧DRIP {short} ${d.get('filled_notional',0):.0f}"
            f"/${d.get('target_notional',0):.0f} "
            f"({d.get('fills',0)} fills) bite={d.get('bite_qty',0)}sh "
            f"min≥{d.get('min_basis_bps',0):.0f}bps{uh_str}"
        )
    for sym, d in drip_exits.items():
        short = "L-HL/S-AST" if d.get("direction") == "long_hl_short_aster" else "L-AST/S-HL"
        out.append(
            f"  • {sym} 💧EXIT {short} {d.get('exited_qty',0):.3f}"
            f"/{d.get('total_qty',0):.3f}sh "
            f"({d.get('fills',0)} fills) bite={d.get('bite_qty',0)}sh "
            f"max≤{d.get('max_basis_bps',0):.0f}bps"
        )
    out.append("  (/cancel SYM to clear a gate)")
    return out


def cmd_positions(chat_id: str, _arg: str):
    try:
        from config import (
            ROUND_TRIP_FEE, CARRY_ROUND_TRIP_FEE, carry_round_trip_fee, round_trip_fee,
            EXIT_TARGET_NET_USD, EXIT_TARGET_NET_USD_BY_SYMBOL, NOTIONAL_PER_LEG,
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
            "COALESCE(entry_maker_venue, ''), COALESCE(aster_funding_window_h, 8), "
            "COALESCE(aster_hedged_qty, 0) "
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
    costs = _load_position_costs()   # actual funding/fees from the venues, per symbol
    stale = ""
    if live.get("_ts") and time.time() - live["_ts"] > 30:
        stale = " ⚠️stale"
    lines = ["📊 Open positions:"]
    for (sym, status, direction, spread, qty, etime, paper, notional,
         hl_px, ast_px, hl_fr, ast_fr, entry_base, hold_for_funding,
         entry_maker_venue, ast_win_h, hedged_qty) in rows:
        held_h = (now - (etime or now)) / 3_600_000
        tag = " [paper]" if paper else ""
        if hold_for_funding:
            tag += " 💰carry"
        elif entry_maker_venue == "hl":
            tag += " 🅼maker"
        notional = notional or 1000
        short_dir = "HL↑ Ast↓" if "long_hl" in (direction or "") else "HL↓ Ast↑"

        # In-progress states (maker resting / hedging on fill / closing) get a
        # compact "being worked" line — so you can see a trade is live and how
        # far along it is BEFORE it's fully on, the same way exits are visible.
        if status != "open":
            verb = {"entering": "🛠 ENTERING", "scaling": "🛠 SCALING",
                    "exiting": "🛠 EXITING"}.get(status, f"[{status}]")
            tgt = qty or 0
            prog = f"{hedged_qty:.3f}/{tgt:.3f}sh" if tgt else f"{hedged_qty:.3f}sh"
            pct = f" ({hedged_qty / tgt * 100:.0f}%)" if tgt else ""
            if status in ("entering", "scaling"):
                px = f" @ {fmt_px(hl_px)}" if hl_px else ""
                detail = f"HL maker resting{px}, hedged {prog}{pct}"
            elif status == "exiting":
                detail = f"closing {tgt:.3f}sh — HL maker resting, hedged {prog}{pct}"
            else:
                detail = f"qty={tgt}"
            lines.append(
                f"• {sym}{tag} {verb} {short_dir} ${notional:.0f}\n"
                f"    {detail} · held {held_h:.1f}h"
            )
            continue
        # Maker-first (carry + convergence) pay HL-maker/Aster-taker; legacy
        # taker convergence pays HL-taker/Aster-maker.
        fees = notional * (carry_round_trip_fee(sym) if entry_maker_venue == "hl"
                           else round_trip_fee(sym))
        # Funding: prefer the ACTUAL settled figure the trader reconciled from the
        # venues (position_costs.json, refreshed ~5min). The estimate extrapolates
        # a single entry-snapshot rate linearly and drifts badly on names whose
        # funding swings (SKHX: est -$4.43 vs actual ~flat). Fall back to the
        # estimate if there's no fresh actual yet.
        c = costs.get(sym) if isinstance(costs, dict) else None
        fund_src = ""
        if c and isinstance(c, dict) and time.time() - c.get("ts", 0) < 900:
            funding = c.get("funding", 0.0)
            fund_src = " (actual)"
            # entry commission so far is real too; keep round-trip fee estimate for
            # the target math but show the actual accrued fee if larger info wanted.
        else:
            funding = estimate_funding_pnl(
                direction or "long_hl_short_aster", held_h, notional,
                hl_fr or 0, ast_fr or 0, ast_win_h or 8.0,
            )
            fund_src = " (est)"
        # Profit target scales with the position's notional, matching the trader's
        # exit logic — a fixed $3 on a $20 test position would be ~1500bps.
        base_target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(sym, EXIT_TARGET_NET_USD)
        sym_target = base_target * ((notional / NOTIONAL_PER_LEG) if notional else 1.0)

        # Current MID excess vs the ENTRY baseline — an approximation of the
        # convergence proximity. The live exit also folds in the oracle
        # correction and the executable half-spread deviation (not carried in
        # latest_spreads.json); est_net below is the real executable gate.
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

        # Entry basis from the ACTUAL fills (matches /basis leg convention:
        # in-your-favour, maker-HL/taker-Aster). Gross round trip = entry_basis
        # + exit_basis, so you profit when
        #   exit_basis > fees_bps − entry_basis − funding_accrued_bps.
        # Funding keeps accruing, so the breakeven drifts down (carry trades)
        # or up (paying) over time — it's live, not fixed at entry.
        basis_str = ""
        if hl_px and ast_px and hl_px > 0 and ast_px > 0:
            entry_mid = (hl_px + ast_px) / 2
            pos_dir = direction or "long_hl_short_aster"
            entry_basis = ((ast_px - hl_px) if "long_hl" in pos_dir
                           else (hl_px - ast_px)) / entry_mid * 10000
            fees_bps = fees / notional * 10000 if notional else 0.0
            funding_bps = funding / notional * 10000 if notional else 0.0
            be_exit = fees_bps - entry_basis - funding_bps
            basis_str = (f"    basis: entry={entry_basis:+.1f}bps  "
                         f"breakeven exit≥{be_exit:+.1f}bps (fees+funding incl)\n")

        if hold_for_funding:
            lines.append(
                f"• {sym}{tag} [{status}] {short_dir}\n"
                f"{basis_str}"
                f"    HL:{fmt_px(hl_px)}  Ast:{fmt_px(ast_px)}  qty={qty_str}\n"
                f"    funding=${funding:+.2f}{fund_src}  fees=${fees:.2f}  "
                f"held={held_h:.1f}h{est_net_str}"
            )
        else:
            lines.append(
                f"• {sym}{tag} [{status}] {short_dir}\n"
                f"    excess: entry={spread:+.0f}bps  {excess_str}  exit≤0bps\n"
                f"{basis_str}"
                f"    HL:{fmt_px(hl_px)}  Ast:{fmt_px(ast_px)}  qty={qty_str}\n"
                f"    funding=${funding:+.2f}{fund_src}  fees=${fees:.2f}  "
                f"held={held_h:.1f}h{est_net_str}\n"
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


def _render_balances(snap: dict) -> str:
    """Format a cached balances snapshot (written by the monitor)."""
    hl = snap.get("hl") or {}
    aster = snap.get("aster") or {}
    ts = snap.get("_ts", 0)
    age = int(time.time() - ts) if ts else None
    age_str = ""
    if age is not None:
        age_str = f" (as of {age}s ago)" if age < 120 else f" (as of {age // 60}m ago)"
    lines = [f"💰 Balances{age_str}"]
    if hl:
        lines.append(
            f"HL (xyz): ${hl.get('account_value', 0):.2f} {hl.get('asset', 'USDC')} "
            f"(free ${hl.get('withdrawable', 0):.2f}, margin used ${hl.get('margin_used', 0):.2f})"
        )
    else:
        lines.append("HL (xyz): —")
    if aster:
        lines.append(
            f"Aster: ${aster.get('balance', 0):.2f} {aster.get('asset', 'USDT')} "
            f"(free ${aster.get('available', 0):.2f}, uPnL ${aster.get('unrealized_pnl', 0):+.2f})"
        )
    else:
        lines.append("Aster: —")
    if hl and aster:
        lines.append(f"Total ≈ ${hl.get('account_value', 0) + aster.get('balance', 0):.2f} (USDC+USDT)")
    return "\n".join(lines)


def cmd_balance(chat_id: str, _arg: str):
    """Show exchange balances. The control bot holds no API keys, so it asks the
    monitor (the single API-touching process) to fetch fresh balances — which it
    replies with over the alert channel — and shows the last cached snapshot now
    for instant feedback."""
    _, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    trader_up = active.strip() == "active"

    cached = None
    try:
        cached = json.loads(BALANCES_FILE.read_text())
    except Exception:
        cached = None

    if trader_up:
        _enqueue_manual({"action": "balance"})
        if cached:
            send(chat_id, _render_balances(cached) + "\n\n⏳ fetching fresh balances…")
        else:
            send(chat_id, "⏳ requesting balances from the trader… (reply arrives shortly)")
    else:
        if cached:
            send(chat_id, _render_balances(cached) +
                 "\n\n⚠️ trader not running — this is the last cached snapshot; "
                 "start it to refresh.")
        else:
            send(chat_id, "⚠️ trader not running and no cached balances — "
                          "start the trader (/start) to query live balances.")


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

    # 1. Restart the trader first — THIS process survives it, so we can confirm.
    rc, out = run(["systemctl", "restart", SERVICE], timeout=30)
    time.sleep(2)
    _, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    send(chat_id, f"trader restart: {'ok' if rc == 0 else 'FAILED'} — now {active}\n{out}".strip())

    # 1b. Restart the capture daemon too (separate unit) so it picks up the
    #     gate-active backoff flag + retention prune. Only if it's loaded, and
    #     not if it's the same unit as the trader (already restarted).
    if CAPTURE_SERVICE and CAPTURE_SERVICE not in (SERVICE, CONTROL_BOT_SERVICE):
        _, cap_load = run(
            ["systemctl", "show", CAPTURE_SERVICE, "--property=LoadState", "--value"],
            timeout=10,
        )
        if cap_load.strip() == "loaded":
            rc_c, out_c = run(["systemctl", "restart", CAPTURE_SERVICE], timeout=30)
            time.sleep(2)
            _, cap_active = run(["systemctl", "is-active", CAPTURE_SERVICE], timeout=10)
            send(chat_id, f"capture restart: {'ok' if rc_c == 0 else 'FAILED'} — "
                          f"now {cap_active}\n{out_c}".strip())
        else:
            send(chat_id,
                 f"ℹ️ capture service '{CAPTURE_SERVICE}' not found "
                 f"(LoadState={cap_load.strip() or '?'}) — skipped. If your capture "
                 f"daemon runs under a different unit or via nohup, restart it "
                 f"manually to pick up the HL backoff + prune. Set "
                 f"CAPTURE_SERVICE_NAME to silence this.")

    # 1c. The session-flip service (its own unit). It persists state after every
    #     fill and reconciles on boot, so a restart between flips is harmless.
    _, flip_load = run(["systemctl", "show", FLIP_SERVICE, "--property=LoadState", "--value"], timeout=10)
    if flip_load.strip() == "loaded":
        rc_f, out_f = run(["systemctl", "restart", FLIP_SERVICE], timeout=30)
        time.sleep(2)
        _, flip_active = run(["systemctl", "is-active", FLIP_SERVICE], timeout=10)
        send(chat_id, f"session-flip restart: {'ok' if rc_f == 0 else 'FAILED'} — now {flip_active}\n{out_f}".strip())

    # 2. Restart the control bot itself LAST — this tears down THIS process, so it
    #    must be the final step and can't confirm afterwards. Skip if it's the same
    #    unit as the trader (already restarted above). Verify the unit exists first
    #    so we don't kill ourselves for a wrong service name.
    if CONTROL_BOT_SERVICE == SERVICE:
        return
    _, load_state = run(
        ["systemctl", "show", CONTROL_BOT_SERVICE, "--property=LoadState", "--value"],
        timeout=10,
    )
    if load_state.strip() != "loaded":
        send(chat_id,
             f"⚠️ control-bot service '{CONTROL_BOT_SERVICE}' not found "
             f"(LoadState={load_state.strip() or '?'}). Trader is updated, but "
             f"control_bot.py changes need a manual bot restart. Set "
             f"CONTROL_BOT_SERVICE_NAME to your bot unit if the name differs.")
        return
    send(chat_id,
         f"🔁 restarting the control bot ('{CONTROL_BOT_SERVICE}') now — last message "
         f"from this instance. Give it a few seconds, then /status to confirm I'm back.")
    # --no-block: enqueue the restart in systemd (PID 1) and return immediately;
    # the job completes independently of this process being torn down by it.
    run(["systemctl", "restart", "--no-block", CONTROL_BOT_SERVICE], timeout=10)


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
            "entry_time, exit_time, paper, COALESCE(entry_baseline_bps,0) "
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
         qty, notional, gross, fees, funding, net, reason, etime, xtime, paper,
         entry_base) in rows:
        tag = " [paper]" if paper else ""
        held_h = ((xtime or 0) - (etime or 0)) / 3_600_000
        long_hl = "long_hl" in (direction or "")
        short_dir = "HL↑ Ast↓" if long_hl else "HL↓ Ast↑"
        # entry_spread_bps stored the ENTRY EXCESS (deviation vs baseline); the
        # exit stored the RAW mid spread. Convert the exit to the position's own
        # EXCESS too (raw − entry baseline, oriented) so both numbers are the
        # same metric — convergence reads as excess falling toward/below 0.
        raw_exit_excess = (exit_sp or 0) - (entry_base or 0)
        exit_excess = raw_exit_excess if long_hl else -raw_exit_excess
        lines.append(
            f"\n• {sym}{tag} {short_dir} — {reason}\n"
            f"  entry excess={entry_sp or 0:.1f}bps → exit excess={exit_excess:.1f}bps\n"
            f"  HL: {fmt_px(hl_in)}→{fmt_px(hl_out)}  Ast: {fmt_px(ast_in)}→{fmt_px(ast_out)}\n"
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


def cmd_autoexit(chat_id: str, arg: str):
    """Toggle the auto convergence/target/timeout exit. Manual /close and /drip_exit unaffected."""
    a = arg.strip().lower()
    if a not in ("on", "off"):
        cur = "off" if (AUTO_EXIT_FILE.exists()
                        and AUTO_EXIT_FILE.read_text().strip().lower() == "off") else "on"
        send(chat_id,
             f"auto-exit is currently {cur.upper()}.\n"
             "/autoexit off — stop auto-closing positions (convergence/target/timeout)\n"
             "/autoexit on — resume auto exit\n"
             "/drip_exit still works regardless of this setting.")
        return
    AUTO_EXIT_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTO_EXIT_FILE.write_text(a + "\n")
    if a == "off":
        send(chat_id, "🛑 auto-exit DISABLED — positions won't auto-close on convergence/target/timeout. "
                      "Manual /close and /drip_exit still work. (takes effect within ~1s, no restart)")
    else:
        send(chat_id, "✅ auto-exit ENABLED — positions will auto-close on convergence/target/timeout again.")


def _current_auto_notional() -> tuple[float, bool]:
    """Return (per-leg notional USD, is_override). Override from auto_notional
    file, else the config default."""
    from config import NOTIONAL_PER_LEG
    try:
        v = float(AUTO_NOTIONAL_FILE.read_text().strip())
        if v >= 1.0:
            return v, True
    except Exception:
        pass
    return float(NOTIONAL_PER_LEG), False


def cmd_notional(chat_id: str, arg: str):
    """Set the per-leg notional (USD) for AUTO convergence entries. Manual /enter
    and /drip size independently. Takes effect within ~1s, no restart."""
    from config import NOTIONAL_PER_LEG
    a = arg.strip().lower()
    cur, is_override = _current_auto_notional()
    if not a:
        src = "override" if is_override else "config default"
        send(chat_id,
             f"auto-entry notional: ${cur:.0f}/leg ({src})\n"
             f"/notional 250 — set auto entries to $250/leg\n"
             f"/notional reset — revert to config default (${NOTIONAL_PER_LEG:.0f})\n"
             "Affects NEW auto convergence entries only; open positions and "
             "manual /enter are unchanged. The profit target scales with size.")
        return
    if a in ("reset", "default", "off"):
        try:
            AUTO_NOTIONAL_FILE.unlink()
        except FileNotFoundError:
            pass
        send(chat_id, f"↩️ auto-entry notional reset to config default ${NOTIONAL_PER_LEG:.0f}/leg.")
        return
    try:
        v = float(a.replace("$", "").replace(",", ""))
    except ValueError:
        send(chat_id, f"Usage: /notional <USD> | reset  (got {arg!r})")
        return
    if v < 1.0:
        send(chat_id, "Notional must be ≥ $1.")
        return
    AUTO_NOTIONAL_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTO_NOTIONAL_FILE.write_text(f"{v}\n")
    warn = "  ⚠️ larger than the config default" if v > NOTIONAL_PER_LEG else ""
    send(chat_id,
         f"✅ auto-entry notional set to ${v:.0f}/leg.{warn}\n"
         "New auto convergence entries will size to this (takes effect within ~1s, "
         "no restart). Open positions unchanged; the profit target scales with size.")


def cmd_enter(chat_id: str, arg: str):
    """Manually open ONE delta-neutral funding-carry hold via the monitor.

    Usage: /enter SYMBOL DIRECTION NOTIONAL [ENTRY_BPS [EXIT_BPS]]
      DIRECTION: long_hl_short_aster | long_aster_short_hl
                 (aliases: buy_hl / buy_aster / L-HL/S-AST / L-AST/S-HL)
      NOTIONAL : USD per leg
      ENTRY_BPS (optional): only fill once the executable entry basis is at or
                 better than this (bps, in your favour). Omit, or 'now', =
                 enter immediately.
      EXIT_BPS (optional): arm a /close gate at this exit basis AS SOON AS the
                 entry fills. Set both and the whole round trip runs unattended —
                 the point being you're asleep while an Asian session prints the
                 exit level.

    Held for funding carry — the bot NEVER auto-closes it on convergence, a
    mark-to-market stop or a timeout. With EXIT_BPS it closes on that basis gate;
    otherwise only on a manual /close.
    Leverage 5x + isolated margin are applied automatically on entry.
    In live mode this places REAL orders and requires a typed YES.
    """
    toks = arg.split()
    if len(toks) not in (3, 4, 5):
        send(chat_id,
             "Usage: /enter SYMBOL DIRECTION NOTIONAL [ENTRY_BPS [EXIT_BPS]]\n"
             "e.g. /enter SMSN long_hl_short_aster 1000\n"
             "     /enter SMSN buy_hl 1000 -10        (wait until entry basis ≥ -10bps)\n"
             "     /enter SKHX hl 1000 100 -20        (enter ≥100bps, then auto-arm exit ≥-20bps)\n"
             "     /enter SKHX hl 1000 now -20        (enter now, arm exit ≥-20bps)\n"
             "DIRECTION aliases: buy_hl / buy_aster / L-HL/S-AST / L-AST/S-HL\n"
             "Check /basis SYM first — it shows the live level each gate compares against.")
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
    if len(toks) >= 4 and toks[3].lower() not in ("now", "-", "market"):
        try:
            target_bps = float(toks[3])
        except ValueError:
            send(chat_id, f"Bad entry basis target {toks[3]!r} — must be a number (bps), "
                          "or 'now' to enter immediately.")
            return
    exit_bps = None
    if len(toks) == 5:
        try:
            exit_bps = float(toks[4])
        except ValueError:
            send(chat_id, f"Bad exit basis target {toks[4]!r} — must be a number (bps).")
            return

    req = {"action": "enter", "symbol": symbol, "direction": direction, "notional": notional}
    if target_bps is not None:
        req["target_bps"] = target_bps
    if exit_bps is not None:
        req["exit_target_bps"] = exit_bps
    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    if active.strip() != "active":
        send(chat_id, f"⚠️ trader service is {active.strip()} — start it first (/start), "
                      "the monitor is what places the order.")
        return

    short = "L-HL/S-AST" if direction == "long_hl_short_aster" else "L-AST/S-HL"
    gate = f" once basis ≥ {target_bps:.0f}bps" if target_bps is not None else " NOW"

    # Show the implied round trip. entry + exit basis IS the gross round trip, so
    # a typo'd pair that can never cover fees is visible before you commit rather
    # than after the entry has filled and the exit sits unreachable all night.
    econ = ""
    if exit_bps is not None:
        try:
            from config import carry_round_trip_fee
            fee_bps = carry_round_trip_fee(symbol) * 10000
        except Exception:
            fee_bps = 4.8
        if target_bps is not None:
            rt = target_bps + exit_bps
            net = rt - fee_bps
            flag = "" if net > 0 else "  ⚠️ NEGATIVE — this pair cannot cover fees"
            econ = (f"\nimplied round trip: {target_bps:+.0f} + {exit_bps:+.0f} = "
                    f"{rt:+.0f}bps gross, {net:+.0f}bps net of {fee_bps:.1f}bps fees{flag}")
        else:
            econ = (f"\nexit gate at {exit_bps:+.0f}bps; entry is at market so the round "
                    f"trip depends on your fill (fees {fee_bps:.1f}bps)")
    exit_note = (f"\nOn fill it auto-arms a /close gate at exit basis ≥ {exit_bps:+.0f}bps."
                 if exit_bps is not None else "")

    if read_mode() == "live":
        _PENDING_ENTER[chat_id] = (req, time.time() + _CONFIRM_TTL)
        hold = ("Closes on that exit gate (or a manual /close)." if exit_bps is not None
                else "Held indefinitely — NO auto-stop or timeout, closes only on /close.")
        send(chat_id,
             f"⚠️ LIVE order: enter {symbol} {short} ${notional:.0f}/leg as a funding "
             f"hold{gate}.{exit_note}{econ}\n"
             f"5x isolated. {hold}\n"
             "This places REAL orders. Reply YES within 60s to confirm.")
        return
    _enqueue_manual(req)
    send(chat_id, f"📩 queued [PAPER] entry: {symbol} {short} ${notional:.0f}{gate}."
                  f"{exit_note}{econ}\nYou'll get an alert when it's placed.")


def cmd_close(chat_id: str, arg: str):
    """Manually close one open position via the monitor.

    Usage: /close SYMBOL [BPS] [hl|aster] [NOTIONAL]
      BPS: wait until exit basis ≥ this. Omit or 0 = close now.
      hl|aster: which side rests the limit order (full close only). Omit = auto.
      NOTIONAL: USD to close. Omit = close full position. Partial always taker-taker.
      Args after BPS can be in any order.
    """
    toks = arg.split()
    if not toks:
        send(chat_id, "Usage: /close SYMBOL [BPS] [hl|aster] [NOTIONAL]\n"
                      "e.g. /close SMSN              (close all now)\n"
                      "     /close SMSN 5             (close all, basis ≥ 5bps)\n"
                      "     /close SMSN 5 hl          (close all, maker on HL)\n"
                      "     /close SMSN 0 500         (close $500 now)\n"
                      "     /close SMSN 5 500         (close $500, basis ≥ 5bps)")
        return
    symbol = toks[0].upper()
    req = {"action": "close", "symbol": symbol}
    gate = ""
    if len(toks) >= 2:
        try:
            bps = float(toks[1])
        except ValueError:
            send(chat_id, f"Bad basis target {toks[1]!r} — must be a number (bps).")
            return
        # 0 = close now (documented above); a 0 gate would wait for basis ≥ 0,
        # which is NOT "now" on a name whose exit basis sits negative.
        if bps != 0:
            req["target_bps"] = bps
            gate = f" once exit basis ≥ {bps:.0f}bps"
    maker_venue = ""
    close_notional = 0.0
    for t in toks[2:]:
        if t.lower() in ("hl", "aster"):
            maker_venue = t.lower()
        else:
            try:
                close_notional = float(t)
                if close_notional <= 0:
                    raise ValueError
            except ValueError:
                send(chat_id, f"Bad argument {t!r} — expected 'hl', 'aster', or a positive number (USD).")
                return
    if maker_venue:
        req["maker_venue"] = maker_venue
    if close_notional:
        req["close_notional"] = close_notional
    venue_str = f" maker={maker_venue}" if maker_venue else ""
    notional_str = f" ${close_notional:.0f}" if close_notional else " (full)"
    _enqueue_manual(req)
    send(chat_id, f"📩 queued close{notional_str} for {symbol}{gate}{venue_str}.")


def cmd_cancel(chat_id: str, arg: str):
    """Cancel a pending gate/drip, abort an in-progress maker entry, or abort a
    stuck /close in progress (cancels resting exit orders, reverts to open)."""
    symbol = arg.strip().upper()
    if not symbol:
        send(chat_id, "Usage: /cancel SYMBOL")
        return
    _enqueue_manual({"action": "cancel", "symbol": symbol})
    send(chat_id, f"📩 cancel requested for any pending gate on {symbol}.")


def cmd_drip(chat_id: str, arg: str):
    """Taker-taker drip entry: small bites until target notional is reached.

    Usage: /drip SYMBOL DIRECTION NOTIONAL MIN_BASIS_BPS BITE_QTY
      DIRECTION: long_hl_short_aster | long_aster_short_hl (or aliases)
      NOTIONAL:  total target USD per leg
      MIN_BASIS_BPS: minimum executable basis (bps) to place a bite
      BITE_QTY: shares per bite (e.g. 0.6)

    Each tick, if the executable spread ≥ MIN_BASIS_BPS, places one small
    taker-taker order (HL IOC + Aster IOC). Accumulates into one position.
    Use /cancel SYMBOL to stop early.
    """
    toks = arg.split()
    if len(toks) != 5:
        send(chat_id,
             "Usage: /drip SYMBOL DIRECTION NOTIONAL MIN_BASIS_BPS BITE_QTY\n"
             "e.g. /drip ZHIPU buy_hl 2000 50 0.6\n"
             "BITE_QTY is in shares (e.g. 0.6 = 0.6 shares per bite)\n"
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
    try:
        bite_qty = float(toks[4])
        if bite_qty <= 0:
            raise ValueError
    except ValueError:
        send(chat_id, f"Bad bite qty {toks[4]!r} — must be a positive number (shares).")
        return

    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    if active.strip() != "active":
        send(chat_id, f"⚠️ trader service is {active.strip()} — start it first (/start).")
        return

    req = {"action": "drip", "symbol": symbol, "direction": direction,
           "notional": notional, "min_basis_bps": min_basis, "bite_qty": bite_qty}
    short = "L-HL/S-AST" if direction == "long_hl_short_aster" else "L-AST/S-HL"
    bite_str = f" bite={bite_qty}shares"

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


def cmd_drip_exit(chat_id: str, arg: str):
    """Taker-taker drip exit: unwind position in small bites when spread narrows.

    Usage: /drip_exit SYMBOL MAX_BASIS_BPS BITE_QTY [NOTIONAL]
      MAX_BASIS_BPS: exit when executable spread ≤ this (e.g. 0 = fully converged)
      BITE_QTY: shares per bite (e.g. 0.04)
      NOTIONAL (optional): USD to exit. Omit = exit full position.

    Reverses the position's entry direction. Use /cancel SYMBOL to stop.
    """
    toks = arg.split()
    if len(toks) not in (3, 4):
        send(chat_id,
             "Usage: /drip_exit SYMBOL MAX_BASIS_BPS BITE_QTY [NOTIONAL]\n"
             "e.g. /drip_exit ZHIPU 30 0.04          (exit all)\n"
             "     /drip_exit ZHIPU 30 0.04 500      (exit $500 worth)\n"
             "Exits when spread ≤ MAX_BASIS_BPS, in bites of BITE_QTY shares.")
        return
    symbol = toks[0].upper()
    try:
        max_basis = float(toks[1])
    except ValueError:
        send(chat_id, f"Bad max basis {toks[1]!r} — must be a number (bps).")
        return
    try:
        bite_qty = float(toks[2])
        if bite_qty <= 0:
            raise ValueError
    except ValueError:
        send(chat_id, f"Bad bite qty {toks[2]!r} — must be a positive number (shares).")
        return
    target_notional = 0.0
    if len(toks) == 4:
        try:
            target_notional = float(toks[3])
            if target_notional <= 0:
                raise ValueError
        except ValueError:
            send(chat_id, f"Bad notional {toks[3]!r} — must be a positive number.")
            return

    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    if active.strip() != "active":
        send(chat_id, f"⚠️ trader service is {active.strip()} — start it first (/start).")
        return

    req = {"action": "drip_exit", "symbol": symbol,
           "max_basis_bps": max_basis, "bite_qty": bite_qty}
    if target_notional:
        req["target_notional"] = target_notional
    bite_str = f" bite={bite_qty}shares"
    notional_str = f" ${target_notional:.0f}" if target_notional else " (full position)"

    if read_mode() == "live":
        _PENDING_ENTER[chat_id] = (req, time.time() + _CONFIRM_TTL)
        send(chat_id,
             f"⚠️ LIVE drip_exit: {symbol}{notional_str} exit when spread "
             f"≤{max_basis:.0f}bps{bite_str}.\n"
             "This places REAL taker-taker exit orders each tick. Reply YES within 60s.")
        return
    _enqueue_manual(req)
    send(chat_id,
         f"📩 queued [PAPER] drip_exit: {symbol}{notional_str} exit when spread "
         f"≤{max_basis:.0f}bps{bite_str}. /cancel {symbol} to stop.")


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


def cmd_forget(chat_id: str, arg: str):
    """Remove a tracked position from /positions WITHOUT placing any orders —
    for one you already closed on the venue by hand (DB out of sync). The
    monitor verifies both venues are flat first and refuses otherwise, so it
    can never orphan a real naked leg. Places NO orders (unlike /close)."""
    symbol = arg.strip().upper().split()[0] if arg.strip() else ""
    if not symbol:
        send(chat_id, "Usage: /forget SYMBOL — drop a position already closed on "
                      "the venue (verifies both venues are flat; places no orders).")
        return
    rc, active = run(["systemctl", "is-active", SERVICE], timeout=10)
    if active.strip() != "active":
        send(chat_id, f"⚠️ trader service is {active.strip()} — start it first (/start); "
                      "the monitor must query the venues to confirm it's flat.")
        return
    _enqueue_manual({"action": "forget", "symbol": symbol})
    send(chat_id, f"📩 /forget {symbol}: checking both venues are flat, then removing…")


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


def cmd_backtest(chat_id: str, arg: str):
    """Backtest the convergence strategy on recent 1m candles.

    Shells out to scripts/backtest_tg.py (venv + live API egress). Args are
    order-independent: a number = hours (default 48), a word = one symbol.
      /backtest            — whole universe, 48h
      /backtest 72         — whole universe, 72h
      /backtest SNDK       — just SNDK, 48h
      /backtest 168 QCOM   — QCOM, last week
    """
    hours, symbol, cost, sweep = "48", None, "taker", False
    for tok in arg.split():
        low = tok.lower()
        if tok.isdigit():
            hours = tok
        elif low in ("taker", "maker", "none"):
            cost = low
        elif low == "sweep":
            sweep = True
        elif tok.isalpha():
            symbol = tok.upper()
    if sweep and not symbol:
        send(chat_id, "Usage: /backtest SYM sweep [hours] — calibrate one name's threshold.")
        return
    cmd = [PYTHON, str(BASE_DIR / "scripts" / "backtest_tg.py"),
           "--hours", hours, "--cost", cost]
    if symbol:
        cmd += ["--symbol", symbol]
    if sweep:
        cmd += ["--sweep"]
    what = f"{symbol} threshold sweep" if sweep else ("the universe" if not symbol else symbol)
    send(chat_id, f"⏳ {what} over {hours}h ({cost} cost, ~30–90s)…")
    code, out = run(cmd, timeout=200)
    send_pre(chat_id, out, code)


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
    send_pre(chat_id, out, code)


def cmd_basis(chat_id: str, arg: str):
    """Live executable basis for one name — the exact numbers the /enter and
    /close basis gates check (maker-HL/taker-Aster, from live bid/ask), plus a
    24h average (exact from the capture DB when available, else ~candle mids).

    Shells out to scripts/basis_snapshot.py (venv + live API egress).
    Usage: /basis SYMBOL
    """
    symbol = arg.strip().split()[0].upper() if arg.strip() else ""
    if not symbol:
        send(chat_id, "Usage: /basis SYMBOL  (e.g. /basis SKHX)")
        return
    script = BASE_DIR / "scripts" / "basis_snapshot.py"
    send(chat_id, f"⏳ fetching {symbol} basis…")
    code, out = run([PYTHON, str(script), symbol], timeout=45)
    send_pre(chat_id, out, code)


def cmd_opps(chat_id: str, arg: str):
    """Rank the top basis-trade opportunities by 24h p90 round-trip.

    For each name: p90(buy-HL leg) + p90(buy-AST leg) — the round trip you'd
    capture catching each leg at its 10%-best spike. Reads the capture DB, no
    network. Usage: /opps [N]  (default 5)
    """
    n = arg.strip().split()[0] if arg.strip() else "5"
    if not n.isdigit():
        n = "5"
    script = BASE_DIR / "scripts" / "basis_opportunities.py"
    send(chat_id, "⏳ ranking basis opportunities…")
    # 180s not 45s: the first call on an older DB builds the covering index (a
    # one-time full scan); after that it's index-only and returns in seconds.
    code, out = run([PYTHON, str(script), n], timeout=180)
    if code == 124:
        out = (out + " — first run builds a fast index over the capture DB; "
               "re-run /opps and it'll be quick.")
    send_pre(chat_id, out, code)


def cmd_revert(chat_id: str, arg: str):
    """Rank names whose CURRENT basis has flipped sign vs its 24h average — the
    spread is stretched away from its mean, so a revert-to-mean round trip is on.

    Score = (|hl now|+|hl avg| + |ast now|+|ast avg|)/2. Reads the capture DB,
    no network. Usage: /revert [N]  (default 5)
    """
    n = arg.strip().split()[0] if arg.strip() else "5"
    if not n.isdigit():
        n = "5"
    script = BASE_DIR / "scripts" / "basis_reversion.py"
    send(chat_id, "⏳ scanning for sign-flip reversions…")
    code, out = run([PYTHON, str(script), n], timeout=180)
    if code == 124:
        out = (out + " — first run builds a fast index over the capture DB; "
               "re-run /revert and it'll be quick.")
    send_pre(chat_id, out, code)


# ── Session flip (session_flip.py, its own service) ──
# The strategy reads data/sessionflip.json every 10s, so config changes here take
# effect without a restart. One-shot actions (close, reset) go through a request dir.
SF_CONFIG = BASE_DIR / "data" / "sessionflip.json"
SF_STATE = BASE_DIR / "data" / "sessionflip_state.json"
SF_TRADES = BASE_DIR / "data" / "sessionflip_trades.csv"
SF_CMD_DIR = BASE_DIR / "data" / "sessionflip_cmd"
FLIP_SERVICE = os.getenv("FLIP_SERVICE_NAME", f"{SERVICE}-flip")
# chat_id -> (subcommand args, expires_at) for /sf actions that need a YES
_PENDING_SF: dict[str, tuple[str, float]] = {}


def _sf_cfg() -> dict:
    from session_flip import DEFAULT_CONFIG
    cfg = dict(DEFAULT_CONFIG)
    try:
        cfg.update(json.loads(SF_CONFIG.read_text()))
    except Exception:
        pass
    return cfg


def _sf_write(cfg: dict):
    tmp = SF_CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, indent=1))
    tmp.replace(SF_CONFIG)


def _sf_request(action: str, **kw):
    SF_CMD_DIR.mkdir(parents=True, exist_ok=True)
    (SF_CMD_DIR / f"{time.time_ns()}.json").write_text(json.dumps({"action": action, **kw}))


def _sf_live_open() -> list[str]:
    try:
        st = json.loads(SF_STATE.read_text())
    except Exception:
        return []
    return [n for n, p in st.get("pos", {}).items() if p.get("qty") and p.get("mode") == "live"]


def _hl_info(body: dict):
    req = urllib.request.Request("https://api.hyperliquid.xyz/info", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=15).read())


def _sf_pnl() -> str:
    """Lifetime + open P&L. LIVE comes from Hyperliquid itself (fills' closedPnl and
    fee, userFunding, open positions' unrealizedPnl) for the flip names since the first
    live fill in the trade log. PAPER is replayed from the trade log."""
    from datetime import datetime, timezone
    from session_flip import read_trades, replay_trades
    rows = read_trades()
    lines = ["Session flip P&L"]

    live = replay_trades(rows, "live")
    first = live["total"]["first"]
    if first:
        since = int(datetime.fromisoformat(first).timestamp() * 1000) - 60_000
        names = set(live["names"]) | set(_sf_cfg()["names"])
        addr = os.getenv("HL_ACCOUNT_ADDRESS") or os.getenv("HL_WALLET_ADDRESS", "")
        try:
            fills = _hl_info({"type": "userFillsByTime", "user": addr, "startTime": since})
            fund = _hl_info({"type": "userFunding", "user": addr, "startTime": since, "dex": "xyz"})
            state = _hl_info({"type": "clearinghouseState", "user": addr, "dex": "xyz"})
        except Exception as e:
            return f"couldn't read Hyperliquid: {e}"
        base = lambda coin: coin.split(":")[-1]
        per = {n: {"closed": 0.0, "fees": 0.0, "fund": 0.0, "unreal": 0.0} for n in names}
        for f in fills:
            n = base(f.get("coin", ""))
            if n in per:
                per[n]["closed"] += float(f.get("closedPnl") or 0)
                per[n]["fees"] += float(f.get("fee") or 0)
        for f in fund:
            d = f.get("delta", {})
            n = base(d.get("coin", ""))
            if n in per:
                per[n]["fund"] += float(d.get("usdc") or 0)        # + received, - paid
        open_lines = []
        for ap in state.get("assetPositions", []):
            p = ap.get("position", {})
            n = base(p.get("coin", ""))
            if n in per and float(p.get("szi") or 0):
                szi, entry = float(p["szi"]), float(p.get("entryPx") or 0)
                u = float(p.get("unrealizedPnl") or 0)
                per[n]["unreal"] = u
                mark = entry + u / szi if szi else entry
                open_lines.append(f"  {n:8s} {'LONG ' if szi > 0 else 'SHORT'} {abs(szi):.4g} "
                                  f"@ {fmt_px(entry)} → {fmt_px(mark)}  ${u:+.2f} "
                                  f"({u / (abs(szi) * entry) * 100 if entry else 0:+.1f}%)")
        closed = sum(v["closed"] for v in per.values())
        fees = sum(v["fees"] for v in per.values())
        fnd = sum(v["fund"] for v in per.values())
        unreal = sum(v["unreal"] for v in per.values())
        lt = live["total"]
        ms = state.get("marginSummary", {})
        lines += [f"\nLIVE since {first[5:16].replace('T', ' ')} UTC",
                  f"Closed  ${closed - fees + fnd:+.2f}  (trades {closed:+.2f}, fees -{fees:.2f}, "
                  f"funding {fnd:+.2f})",
                  f"        {lt['legs']} legs closed, {lt['wins']} winners"
                  + (f" ({lt['wins'] / lt['legs']:.0%})" if lt["legs"] else ""),
                  f"Open    ${unreal:+.2f} unrealised"] + open_lines + [
                  f"LIFETIME ${closed - fees + fnd + unreal:+.2f}",
                  f"\nper name (closed+open, net of fees/funding):"]
        for n in sorted(per):
            v = per[n]
            if any(v.values()):
                lines.append(f"  {n:8s} ${v['closed'] - v['fees'] + v['fund'] + v['unreal']:+.2f}")
        if ms:
            lines.append(f"\nHL equity dex: account ${float(ms.get('accountValue', 0)):,.2f}, "
                         f"positions ${float(ms.get('totalNtlPos', 0)):,.0f}, "
                         f"margin used ${float(ms.get('totalMarginUsed', 0)):,.2f}")
    else:
        lines.append("\nLIVE: no live trades yet")

    paper = replay_trades(rows, "paper")
    pt = paper["total"]
    if pt["first"]:
        lines.append(f"\nPAPER (from {pt['first'][5:10]}): ${pt['realised'] - pt['fees']:+.2f} closed, "
                     f"{pt['legs']} legs, {pt['wins']} winners (funding not included)")
    return "\n".join(lines)


def _sf_status() -> str:
    from datetime import datetime, timezone
    from session_flip import leverage_for, next_boundary, notional_for, target_side
    cfg = _sf_cfg()
    try:
        st = json.loads(SF_STATE.read_text())
    except Exception:
        st = {"pos": {}}
    try:
        req = urllib.request.Request("https://api.hyperliquid.xyz/info",
                                     data=json.dumps({"type": "allMids", "dex": "xyz"}).encode(),
                                     headers={"Content-Type": "application/json"})
        mids = {k.split(":")[-1]: float(v) for k, v in json.loads(urllib.request.urlopen(req, timeout=10).read()).items()}
    except Exception:
        mids = {}
    _, active = run(["systemctl", "is-active", FLIP_SERVICE], timeout=10)
    now = datetime.now(timezone.utc)
    state = ("OFF" if not cfg["enabled"] else "PAUSED" if cfg.get("paused") else "ON")
    lines = [f"Session flip — {state}, {cfg['mode'].upper()}, service {active.strip()}",
             ", ".join(f"{n} ${notional_for(cfg, n):.0f}@{leverage_for(cfg, n)}x" for n in cfg["names"])
             + f" · flip {cfg['lead_seconds']}s before each boundary",
             ""]
    tot = 0.0
    for n in sorted(set(cfg["names"]) | {k for k, p in st.get("pos", {}).items() if p.get("qty")}):
        p = st.get("pos", {}).get(n, {})
        q, avg = p.get("qty", 0.0), p.get("avg", 0.0)
        mark = mids.get(n, avg)
        unreal = q * (mark - avg) if q else 0.0
        tot += unreal
        side = "LONG" if q > 0 else "SHORT" if q < 0 else "flat"
        nxt = ""
        if n in cfg["names"]:
            b = next_boundary(n, now)
            nxt = f" · next {({1: 'LONG', -1: 'SHORT', 0: 'FLAT (holiday)'})[target_side(n, b)]} {b:%a %H:%M}"
        flag = "" if p.get("status", "ok") == "ok" else f" ⚠️ {p['status']}"
        lines.append(f"{n:8s} {side:5s} {abs(q):.4g} @ {fmt_px(avg) if q else '-'} → {fmt_px(mark) if q else '-'}"
                     f"  ${unreal:+.2f}{nxt}{flag}")
    lines.append(f"\nOpen P&L ${tot:+.2f} · lifetime and fees/funding: /sf_pnl")
    return "\n".join(lines)


def cmd_sf(chat_id: str, arg: str):
    parts = arg.split()
    sub = parts[0].lower() if parts else "status"
    rest = parts[1:]
    confirmed = bool(rest) and rest[-1].upper() == "YES"
    if confirmed:
        rest = rest[:-1]
    cfg = _sf_cfg()

    def need_yes(what: str):
        _PENDING_SF[chat_id] = (" ".join([sub] + rest), time.time() + _CONFIRM_TTL)
        send(chat_id, f"⚠️ {what}\nReply YES within {_CONFIRM_TTL}s to confirm.")

    if sub == "status":
        send_pre(chat_id, _sf_status())
    elif sub == "pnl":
        send_pre(chat_id, _sf_pnl())
    elif sub == "on":
        cfg.update(enabled=True, paused=False); _sf_write(cfg)
        send(chat_id, f"session flip ON ({cfg['mode']}). It joins the current side within ~10s.")
    elif sub == "off":
        live = _sf_live_open()
        if live and not confirmed:
            return need_yes(f"OFF closes the LIVE positions in {', '.join(live)} at market.")
        cfg["enabled"] = False; _sf_write(cfg)
        send(chat_id, "session flip OFF — open positions are being closed.")
    elif sub == "pause":
        cfg["paused"] = True; _sf_write(cfg)
        send(chat_id, "session flip PAUSED — positions held, no flips. /sf resume to continue.")
    elif sub == "resume":
        cfg["paused"] = False; _sf_write(cfg)
        send(chat_id, "session flip resumed.")
    elif sub == "paper":
        live = _sf_live_open()
        if live and not confirmed:
            return need_yes(f"Switching to paper closes the LIVE positions in {', '.join(live)} at market.")
        cfg["mode"] = "paper"; _sf_write(cfg)
        send(chat_id, "session flip → PAPER.")
    elif sub == "live":
        if not confirmed:
            from session_flip import leverage_for, notional_for
            return need_yes("Go LIVE: real IOC orders on Hyperliquid — " + ", ".join(
                f"{n} ${notional_for(cfg, n):.0f}@{leverage_for(cfg, n)}x" for n in cfg["names"])
                + ". Flips ~4x/day; paper positions are closed first; the arb bot stops "
                  "auto-entering these names.")
        cfg["mode"] = "live"; _sf_write(cfg)
        send(chat_id, "session flip → LIVE. Watch for the first flip message.")
    elif sub in ("notional", "lev") and rest:
        # /sf notional 200 (default for all) · /sf notional ZHIPU 150 (one name)
        key, by = ("notional", "notional_by_name") if sub == "notional" else ("leverage", "leverage_by_name")
        conv = (lambda v: max(10.0, float(v))) if sub == "notional" else (lambda v: max(1, min(10, int(v))))
        if len(rest) >= 2:
            cfg.setdefault(by, {})[rest[0].upper()] = conv(rest[1])
        else:
            cfg[key] = conv(rest[0])
        _sf_write(cfg)
        from session_flip import leverage_for, notional_for
        send(chat_id, "applied from the next flip: " + ", ".join(
            f"{n} ${notional_for(cfg, n):.0f}@{leverage_for(cfg, n)}x" for n in cfg["names"]))
    elif sub == "names" and rest:
        from session_flip import NAME_GROUP
        names = [n.upper() for n in " ".join(rest).replace(",", " ").split()]
        bad = [n for n in names if n not in NAME_GROUP]
        if bad:
            return send(chat_id, f"unknown: {', '.join(bad)}. Supported: {', '.join(sorted(NAME_GROUP))}")
        cfg["names"] = names; _sf_write(cfg)
        send(chat_id, f"names: {', '.join(names)} (anything removed is closed).")
    elif sub == "close":
        if _sf_live_open() and not confirmed:
            return need_yes("Close ALL session-flip positions now (LIVE ones at market)? Flipping resumes at "
                            "the next boundary unless you also /sf pause or /sf off.")
        _sf_request("flatten")
        send(chat_id, "close requested — confirmation follows from the flip service.")
    elif sub == "reset" and rest:
        _sf_request("reset", name=rest[0].upper())
        send(chat_id, f"reset requested for {rest[0].upper()}.")
    elif sub == "trades":
        n = int(rest[0]) if rest and rest[0].isdigit() else 15
        try:
            rows = SF_TRADES.read_text().strip().splitlines()[1:]
        except Exception:
            rows = []
        send_pre(chat_id, "\n".join(rows[-n:]) or "no trades yet")
    else:
        send(chat_id, "/sf [status] · on · off · pause · resume · paper · live · notional USD · lev N · "
                      "names SKHX,EWY,… · close · reset SYM · trades [n]")


def cmd_help(chat_id: str, _arg: str):
    send(chat_id,
         "SESSION FLIP (Korea/HK long overnight, short in session):\n"
         "/sf — positions, open P&L, next flips\n"
         "/sf_pnl — lifetime P&L (closed trades, fees, funding) + open P&L, live from Hyperliquid\n"
         "/sf_trades · /sf_pause · /sf_resume · /sf_on · /sf_off (closes all)\n"
         "/sf_live (asks YES) · /sf_paper\n"
         "/sf notional [SYM] USD · /sf lev [SYM] N · /sf names SKHX,EWY,MINIMAX,ZHIPU\n"
         "/sf close · /sf reset SYM\n"
         "/balance — balances on both venues · /restart — pull + restart everything\n"
         "\nARB (idle — not in the menu, still typeable):\n"
         "/status — service state + spreads + positions\n"
         "/spreads — current spread vs threshold detail\n"
         "/book SYM — top-5 order book on both venues\n"
         "/basis SYM — live entry/exit basis + 24h avg (gate reference)\n"
         "/opps [N] — top basis opportunities by 24h p90 round-trip\n"
         "/revert [N] — names whose basis flipped sign vs 24h avg (reversion)\n"
         "/funding [n] — top funding-carry opportunities\n"
         "/backtest [hours] [SYM] — backtest convergence on recent candles\n"
         "/enter SYM DIR NOTIONAL [entry_bps [exit_bps]] — funding hold; entry_bps waits for a fill level, exit_bps auto-arms the close on fill\n"
         "/close SYM [bps] [hl|aster] [USD] — close full or partial\n"
         "/cancel SYM — cancel a pending gate/drip, or abort an in-progress entry/close\n"
         "/drip SYM DIR NOTIONAL MIN_BPS BITE_QTY — taker-taker drip entry\n"
         "/drip_exit SYM MAX_BPS BITE_QTY [NOTIONAL] — drip exit (partial or full)\n"
         "/import [SYM] — adopt existing venue positions into the bot for management\n"
         "/forget SYM — drop a position already closed on the venue (no orders; verifies flat)\n"
         "/autoentry on|off — toggle auto basis-arb entry (exits unaffected)\n"
         "/autoexit on|off — toggle auto convergence/target exit (/drip_exit unaffected)\n"
         "/notional [USD|reset] — set auto-entry size per leg (live, no restart)\n"
         "/positions — open positions detail\n"
         "/trades [n] — last n closed trades with P&L detail\n"
         "/pnl — realised P&L (today + all-time)\n"
         "/balance — USDC/USDT balances on both venues\n"
         "/log [n] — last n journal lines\n"
         "/mode — show configured mode\n"
         "/paper — switch to paper mode (restarts)\n"
         "/live YES — switch to live mode (restarts)\n"
         "/start — start trader\n"
         "/stop YES — stop trader (positions left open!)\n"
         "/restart — git pull + restart trader, capture, session flip & control bot\n"
         "/flatten YES — emergency close ALL arb positions")


HANDLERS = {
    "/status": cmd_status, "/positions": cmd_positions, "/pos": cmd_positions,
    "/pnl": cmd_pnl, "/trades": cmd_trades,
    "/balance": cmd_balance, "/balances": cmd_balance,
    "/book": cmd_book, "/basis": cmd_basis, "/opps": cmd_opps,
    "/revert": cmd_revert, "/flip": cmd_revert,
    "/backtest": cmd_backtest, "/bt": cmd_backtest,
    "/funding": cmd_funding, "/carry": cmd_funding,
    "/enter": cmd_enter, "/close": cmd_close, "/cancel": cmd_cancel,
    "/drip": cmd_drip, "/drip_exit": cmd_drip_exit, "/import": cmd_import,
    "/forget": cmd_forget,
    "/autoentry": cmd_autoentry, "/autoexit": cmd_autoexit,
    "/notional": cmd_notional,
    "/log": cmd_log, "/logs": cmd_log,
    "/spreads": cmd_spreads, "/spread": cmd_spreads,
    "/mode": cmd_mode, "/paper": cmd_paper, "/live": cmd_live,
    "/start": cmd_start, "/stop": cmd_stop, "/restart": cmd_restart,
    "/flatten": cmd_flatten, "/help": cmd_help,
    "/sf": cmd_sf, "/sessionflip": cmd_sf,
}
# One-tap menu shortcuts: /sf_trades -> /sf trades, etc. Any arguments pass through.
for _sub in ("pnl", "trades", "pause", "resume", "off", "on", "live", "paper", "close", "status"):
    HANDLERS[f"/sf_{_sub}"] = (lambda s: lambda chat_id, arg: cmd_sf(chat_id, f"{s} {arg}".strip()))(_sub)


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
        pend_sf = _PENDING_SF.pop(chat_id, None)
        if pend_sf and pend_sf[1] > time.time():
            cmd_sf(chat_id, pend_sf[0] + " YES")
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
        # Menu = the session-flip workflow only. The arb is idle (auto-entry off), so its
        # commands are left out of the menu but still work when typed (see /help).
        _api("setMyCommands", {"commands": json.dumps([
            {"command": "sf", "description": "Session flip: positions, open P&L, next flips"},
            {"command": "sf_pnl", "description": "Lifetime + open P&L (live from Hyperliquid)"},
            {"command": "sf_trades", "description": "Last session-flip fills"},
            {"command": "sf_pause", "description": "Hold positions, stop flipping"},
            {"command": "sf_resume", "description": "Resume flipping"},
            {"command": "sf_off", "description": "Close all session-flip positions and stop"},
            {"command": "sf_on", "description": "Start session flip"},
            {"command": "sf_live", "description": "Switch session flip to LIVE (asks YES)"},
            {"command": "sf_paper", "description": "Switch session flip to paper"},
            {"command": "balance", "description": "USDC/USDT balances on both venues"},
            {"command": "restart", "description": "Git pull + restart all services"},
            {"command": "help", "description": "All commands"},
        ])}, timeout=10)
    except Exception as e:
        print(f"setMyCommands failed: {e}", flush=True)

    offset = 0
    # Skip the backlog on startup. Otherwise any command still queued at
    # Telegram gets replayed on every boot — and a /restart in the queue makes
    # the bot restart itself, re-read the same /restart, and restart again: an
    # endless loop (the self-restart tears us down before the update is
    # confirmed, so Telegram redelivers it). getUpdates(offset=-1) returns just
    # the newest update; starting the poll one past it discards everything
    # older. Commands sent while the bot was down are intentionally dropped
    # (safer than replaying a stale /restart or /flatten on boot).
    try:
        resp = _api("getUpdates", {"offset": -1, "timeout": 0}, timeout=10)
        results = resp.get("result", [])
        if results:
            offset = results[-1]["update_id"] + 1
            print(f"Skipped {len(results)} backlog update(s); starting at offset {offset}",
                  flush=True)
    except Exception as e:
        print(f"backlog skip failed ({e}) — starting at offset 0", flush=True)

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

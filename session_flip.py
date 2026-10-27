#!/usr/bin/env python3
"""
Session flip — a separate strategy that shares this repo's Hyperliquid client.

THE EDGE (scratchpad flip_pnl.py / asia_cal.py, 2026-10-27)
  Korean and Hong Kong names on HL drift UP while their home exchange is closed and
  give it back once it opens: perp traders bid them through the US session, and the
  open pulls the perp back to the cash price. 26 years of KOSPI / SK Hynix / Tencent
  cash data show the same split (overnight +, intraday -). Shanghai A-shares run the
  other way (T+1 settlement): up in the session, down overnight.

  Backtest on HL hourly data, $1,000 at 1x per name, funding and costs included:
    SKHX +$1,369 (142d, t=2.2)   EWY +$1,047 (t=2.5)   SMSN +$998   HYUNDAI +$843
    MINIMAX +$2,182 (93d, t=2.7) ZHIPU +$1,705          UNITREE +$695  CXMT +$114
  Every month positive across the book, but per-name t-stats are 1.6-2.7 on a few
  months, and the Korean names' edge has shrunk since July. Hence paper first.

THE RULE (UTC, Mon-Fri; weekends hold the overnight side from Friday to Monday)
  KR  (KRX 09:00-15:30 KST)   long 07:00 -> 00:00, short 00:00 -> 07:00
  HK  (HKEX 09:30-16:00 HKT)  long 08:00 -> 01:00, short 01:00 -> 08:00
  CN  (SSE 09:30-15:00 CST)   short 07:00 -> 01:00, long 01:00 -> 07:00   (reversed)
  Each flip trades the whole move at once (2x notional) with an IOC at the touch,
  `lead_seconds` before the boundary, so the overnight leg is out before the open
  prints. Same hours the backtest used.

CONTROL
  data/sessionflip.json          config, written by control_bot (/sf ...)
  data/sessionflip_cmd/*.json    one-shot requests (flatten)
  data/sessionflip_state.json    positions + P&L, owned by this process
  data/sessionflip_trades.csv    every fill (paper and live)

SAFETY
  - Paper by default; live only after `/sf live YES`.
  - Never trades a name where the arb bot holds a position (positions.db), and never
    touches a venue position it did not open: a mismatch between the venue and the
    state file parks that name and alerts.
  - While live, the arb bot's auto-entry skips these names (flip_reserve.py).
  - An ambiguous order (timeout / non-JSON) is reconciled against the venue position
    before anything else is sent.
"""

import asyncio
import csv
import json
import logging
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parent
DATA = BASE_DIR / "data"
CONFIG_FILE = DATA / "sessionflip.json"
STATE_FILE = DATA / "sessionflip_state.json"
TRADES_FILE = DATA / "sessionflip_trades.csv"
CMD_DIR = DATA / "sessionflip_cmd"

log = logging.getLogger("sessionflip")

# Session windows in UTC minutes-of-day, and which side is held OUTSIDE the session.
GROUPS = {
    "KR": {"open": 0, "close": 7 * 60, "off_side": +1},
    "HK": {"open": 1 * 60, "close": 8 * 60, "off_side": +1},
    "CN": {"open": 1 * 60, "close": 7 * 60, "off_side": -1},
}
NAME_GROUP = {
    "SKHX": "KR", "SMSN": "KR", "EWY": "KR", "HYUNDAI": "KR", "SKHY": "KR", "KR200": "KR",
    "MINIMAX": "HK", "ZHIPU": "HK",
    "UNITREE": "CN", "CXMT": "CN",
}
DEFAULT_CONFIG = {
    "enabled": True,
    "mode": "paper",                 # paper | live
    "notional": 250.0,               # USD per name, held long or short
    "leverage": 3,
    "notional_by_name": {},          # overrides, e.g. {"ZHIPU": 150}
    # Most of these names are isolated-only on HL, so a position can lose only its own
    # margin: liquidation sits ~1/lev - 5% away. Worst adverse move inside one hold
    # (Apr-Oct 2026): SKHX 25%, EWY 11%, MINIMAX 27.5%, ZHIPU 37.5% (3 of 183 holds
    # beyond 3x's ~28%). Hence 2x for the HK IPOs.
    "leverage_by_name": {"MINIMAX": 2, "ZHIPU": 2},
    "names": ["SKHX", "EWY", "MINIMAX", "ZHIPU"],
    "lead_seconds": 60,              # flip this long before each boundary
    "paused": False,                 # True: hold positions, no flips
}


def notional_for(cfg: dict, name: str) -> float:
    return float(cfg.get("notional_by_name", {}).get(name, cfg["notional"]))


def leverage_for(cfg: dict, name: str) -> int:
    return int(cfg.get("leverage_by_name", {}).get(name, cfg["leverage"]))
TAKER_FEE = 0.00009                  # HL growth-mode taker (~0.9bp) for paper fills
IOC_BUFFER_BPS = 15                  # cap on how far through the touch an IOC may fill
MAX_ATTEMPTS = 3


# ---------------------------------------------------------------- pure schedule logic
def target_side(name: str, now: datetime) -> int:
    """+1 long / -1 short that `name` should hold at UTC time `now`."""
    g = GROUPS[NAME_GROUP[name]]
    if now.weekday() >= 5:
        return g["off_side"]
    m = now.hour * 60 + now.minute
    in_session = g["open"] <= m < g["close"]
    return -g["off_side"] if in_session else g["off_side"]


def next_boundary(name: str, now: datetime) -> datetime:
    """The next time target_side(name) changes (weekend-aware)."""
    t = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    side = target_side(name, now)
    for _ in range(8 * 24 * 60):
        if target_side(name, t) != side:
            return t
        t += timedelta(minutes=1)
    return t


# ---------------------------------------------------------------- persistence helpers
def _atomic_write(path: Path, text: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    try:
        cfg.update(json.loads(CONFIG_FILE.read_text()))
    except FileNotFoundError:
        _atomic_write(CONFIG_FILE, json.dumps(cfg, indent=1))
    except Exception as e:
        log.error(f"bad {CONFIG_FILE.name} ({e}); using defaults")
    cfg["names"] = [n.upper() for n in cfg.get("names", []) if n.upper() in NAME_GROUP]
    return cfg


def load_state() -> dict:
    try:
        st = json.loads(STATE_FILE.read_text())
    except Exception:
        st = {}
    st.setdefault("pos", {})          # name -> {"mode", "qty", "avg", "realised", "fees", "funding", "status"}
    st.setdefault("last_funding_hour", 0)
    return st


def save_state(st: dict):
    _atomic_write(STATE_FILE, json.dumps(st, indent=1))


def record_trade(row: dict):
    new = not TRADES_FILE.exists()
    with TRADES_FILE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ts", "mode", "name", "side", "qty", "px", "fee", "reason"])
        if new:
            w.writeheader()
        w.writerow(row)


def apply_fill(p: dict, signed_qty: float, px: float, fee: float):
    """Average-cost accounting on a signed position. Realises P&L on the part of the
    fill that reduces the position; a flip through zero opens the rest at `px`."""
    q, avg = p.get("qty", 0.0), p.get("avg", 0.0)
    if q == 0 or (q > 0) == (signed_qty > 0):
        new_q = q + signed_qty
        p["avg"] = (avg * abs(q) + px * abs(signed_qty)) / abs(new_q) if new_q else 0.0
        p["qty"] = new_q
    else:
        closing = min(abs(signed_qty), abs(q))
        p["realised"] = p.get("realised", 0.0) + closing * (px - avg) * (1 if q > 0 else -1)
        rest = abs(signed_qty) - closing
        new_q = q + signed_qty
        if abs(new_q) < 1e-12:
            p["qty"], p["avg"] = 0.0, 0.0
        elif rest > 0:
            p["qty"], p["avg"] = new_q, px          # flipped through zero
        else:
            p["qty"] = new_q                         # partial reduce keeps avg
    p["fees"] = p.get("fees", 0.0) + fee


# ---------------------------------------------------------------- telegram
def tg(text: str):
    token = os.getenv("ALERT_TELEGRAM_BOT_TOKEN")
    chat = os.getenv("ALERT_TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": chat, "text": "[session-flip] " + text[:3900]}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data), timeout=10).read()
    except Exception as e:
        log.warning(f"telegram send failed: {e}")


def arb_holds(name: str) -> bool:
    """True if the arb bot is actively working a live position in this symbol.
    'error' rows are left out: they are failed exits the arb no longer tracks (its
    /forget can't clear them), and any HL leg one did leave behind is caught anyway
    by the venue check in move_to, which parks a name whose HL position we didn't open."""
    try:
        from config import DB_PATH
        db = DB_PATH
    except Exception:
        db = str(DATA / "positions.db")
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        n = con.execute("SELECT COUNT(*) FROM positions WHERE symbol=? AND paper=0 "
                        "AND status IN ('entering', 'open', 'exiting')", (name,)).fetchone()[0]
        con.close()
        return n > 0
    except Exception as e:
        log.warning(f"arb position check failed ({e}) — treating {name} as held")
        return True


# ---------------------------------------------------------------- the trader
class SessionFlip:
    def __init__(self, client):
        self.client = client
        self.st = load_state()
        self.loaded: set[str] = set()
        self.lev_set: dict[str, int] = {}          # name -> leverage confirmed by the venue
        self.only_isolated: dict[str, bool] = {}

    # -- venue helpers
    async def ensure(self, name: str) -> bool:
        if name in self.loaded:
            return True
        await self.client.ensure_session()
        await self.client._load_hl_xyz_specs([name])
        if name not in self.client._hl_asset_index:
            return False
        await self.client._infer_hl_tick_sizes([name])
        if not self.only_isolated:
            # One meta read covers every name; asking for cross on an isolated-only
            # market fails, which would leave HL's default (up to 10x) in place.
            async with self.client.session.post(
                    "https://api.hyperliquid.xyz/info", json={"type": "meta", "dex": "xyz"}) as r:
                meta = await r.json(content_type=None)
            self.only_isolated = {u["name"].split(":")[-1]: bool(u.get("onlyIsolated"))
                                  for u in meta.get("universe", [])}
        self.loaded.add(name)
        return True

    async def venue_qty(self, name: str) -> float:
        p = await self.client.get_hl_position(name)
        try:
            return float(p.get("szi", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    async def book(self, name: str):
        b = await self.client.get_orderbook("hl", name)
        if not b or b.bid <= 0 or b.ask <= 0:
            raise RuntimeError(f"{name}: empty HL book")
        return b

    # -- execution
    async def trade_live(self, name: str, delta: float) -> tuple[float, float]:
        """Move the venue position by `delta` coins with IOCs. Returns (filled, avg px)."""
        filled, cost = 0.0, 0.0
        for attempt in range(MAX_ATTEMPTS):
            want = delta - filled
            if abs(want) * (await self.book(name)).mid < 10:      # HL $10 minimum
                break
            b = await self.book(name)
            side = "buy" if want > 0 else "sell"
            ref = b.ask if want > 0 else b.bid
            before = await self.venue_qty(name)
            r = await self.client.place_hl_ioc(name, side, abs(want), ref, buffer_bps=IOC_BUFFER_BPS)
            if r.ambiguous:
                ok, moved, _ = await self.client.reconcile_hl_position_delta(name, before, want)
                if abs(moved) > 0:
                    filled += moved
                    cost += moved * ref
                log.warning(f"{name}: ambiguous IOC reconciled, moved {moved:+.4f}")
                continue
            if r.success and r.filled_qty > 0:
                q = r.filled_qty if want > 0 else -r.filled_qty
                filled += q
                cost += q * r.fill_price
            elif r.error and r.error != "ioc_not_filled":
                raise RuntimeError(f"{name} IOC rejected: {r.error}")
            await asyncio.sleep(1)
        return filled, (cost / filled if filled else 0.0)

    async def move_to(self, name: str, side: int, cfg: dict, reason: str) -> str:
        """Bring `name` to side*notional (side 0 = flat). Returns a one-line summary."""
        p = self.st["pos"].setdefault(name, {"qty": 0.0, "avg": 0.0, "realised": 0.0, "fees": 0.0,
                                             "funding": 0.0, "status": "ok", "mode": cfg["mode"]})
        if not p.get("qty"):
            p["mode"] = cfg["mode"]
        mode = p["mode"]            # an open position is always worked in the mode it was opened in
        b = await self.book(name)
        target = side * notional_for(cfg, name) / b.mid if side else 0.0
        target = float(self.client.format_hl_qty(name, abs(target))) * (1 if target >= 0 else -1)
        if mode == "live":
            if arb_holds(name):
                p["status"] = "arb holds this symbol"
                return f"{name}: SKIPPED — the arb bot has an open {name} position"
            venue = await self.venue_qty(name)
            if abs(venue - p["qty"]) > max(1e-9, 0.02 * abs(p["qty"]) + 1e-6):
                p["status"] = f"venue {venue:+.4f} != state {p['qty']:+.4f}"
                return f"{name}: PARKED — venue shows {venue:+.4f}, state {p['qty']:+.4f}. Fix by hand, then /sf reset {name}"
            lev = leverage_for(cfg, name)
            if side and self.lev_set.get(name) != lev:
                cross = not self.only_isolated.get(name, True)
                if not await self.client.set_hl_leverage(name, lev, cross=cross):
                    p["status"] = f"could not set {lev}x {'cross' if cross else 'isolated'}"
                    return f"{name}: NOT TRADED — HL refused {lev}x leverage; check margin, then /sf reset {name}"
                self.lev_set[name] = lev
        delta = target - p["qty"]
        if abs(delta) * b.mid < 10:
            return ""
        if mode == "paper":
            px = b.ask if delta > 0 else b.bid
            filled = delta
        else:
            filled, px = await self.trade_live(name, delta)
            p["status"] = "ok" if abs(filled) >= abs(delta) * 0.98 else f"partial {filled:+.4f}/{delta:+.4f}"
        fee = abs(filled) * px * TAKER_FEE
        if filled:
            apply_fill(p, filled, px, fee)
            record_trade({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "mode": mode,
                          "name": name, "side": "buy" if filled > 0 else "sell", "qty": f"{abs(filled):.6g}",
                          "px": f"{px:.6g}", "fee": f"{fee:.4f}", "reason": reason})
        save_state(self.st)
        tag = {1: "LONG", -1: "SHORT", 0: "FLAT"}[side]
        return f"{name} → {tag} {abs(p['qty']):.4g} @ {px:.5g}" + ("" if p["status"] == "ok" else f" ({p['status']})")

    # -- periodic jobs
    async def accrue_paper_funding(self, cfg: dict):
        """Paper positions accrue HL's live hourly funding once per hour (longs pay +)."""
        hour = int(time.time() // 3600)
        if self.st["last_funding_hour"] == hour:
            return
        self.st["last_funding_hour"] = hour
        names = [n for n, p in self.st["pos"].items() if p.get("mode") == "paper" and p.get("qty")]
        if not names:
            save_state(self.st)
            return
        await self.client.refresh_hl_oracles(names)
        for n in names:
            p = self.st["pos"][n]
            mark = self.client.get_hl_oracle(n) or p["avg"]
            p["funding"] = p.get("funding", 0.0) - p["qty"] * mark * self.client.get_hl_funding_rate(n)
        save_state(self.st)

    def requests(self) -> list[dict]:
        CMD_DIR.mkdir(parents=True, exist_ok=True)
        out = []
        for f in sorted(CMD_DIR.glob("*.json")):
            try:
                out.append(json.loads(f.read_text()))
            except Exception:
                pass
            f.unlink(missing_ok=True)
        return out

    async def step(self):
        cfg = load_config()
        for req in self.requests():
            if req.get("action") == "flatten":
                lines = [await self.move_to(n, 0, cfg, "manual close") for n, p in self.st["pos"].items() if p.get("qty")]
                tg("closed on request:\n" + ("\n".join(l for l in lines if l) or "nothing open"))
            elif req.get("action") == "reset" and req.get("name") in self.st["pos"]:
                self.st["pos"][req["name"]]["status"] = "ok"
                save_state(self.st)
                tg(f"{req['name']}: status reset")
        if not cfg["enabled"]:
            open_ = [n for n, p in self.st["pos"].items() if p.get("qty")]
            if open_:
                lines = [await self.move_to(n, 0, cfg, "disabled") for n in open_]
                tg("disabled — flattened:\n" + "\n".join(l for l in lines if l))
            return
        await self.accrue_paper_funding(cfg)
        # A mode switch closes the other mode's book first (paper at the touch, live for real).
        stale = [n for n, p in self.st["pos"].items() if p.get("qty") and p.get("mode") != cfg["mode"]]
        if stale:
            lines = [await self.move_to(n, 0, cfg, f"switched to {cfg['mode']}") for n in stale]
            tg(f"mode is now {cfg['mode']} — closed the old book:\n" + "\n".join(l for l in lines if l))
        if cfg["paused"]:
            return
        when = datetime.now(timezone.utc) + timedelta(seconds=cfg["lead_seconds"])
        lines = []
        # names dropped from the list are closed
        for n, p in list(self.st["pos"].items()):
            if p.get("qty") and n not in cfg["names"]:
                lines.append(await self.move_to(n, 0, cfg, "removed from list"))
        for n in cfg["names"]:
            p = self.st["pos"].get(n, {})
            if p.get("status", "ok") != "ok" and not p["status"].startswith("partial"):
                continue
            if not await self.ensure(n):
                lines.append(f"{n}: not an HL xyz market")
                continue
            want = target_side(n, when)
            have = 0 if not p.get("qty") else (1 if p["qty"] > 0 else -1)
            if want != have or p.get("status", "ok").startswith("partial"):
                try:
                    lines.append(await self.move_to(n, want, cfg, "flip"))
                except Exception as e:
                    log.exception(f"{n} flip failed")
                    self.st["pos"].setdefault(n, {})["status"] = f"error: {e}"
                    save_state(self.st)
                    lines.append(f"{n}: ERROR {e}")
        lines = [l for l in lines if l]
        if lines:
            tg(f"{cfg['mode'].upper()} {when:%a %H:%M} UTC\n" + "\n".join(lines))

    async def run(self):
        cfg = load_config()
        tg(f"up — {cfg['mode']}, {'enabled' if cfg['enabled'] else 'disabled'}: "
           + ", ".join(f"{n} ${notional_for(cfg, n):.0f}@{leverage_for(cfg, n)}x" for n in cfg["names"]))
        # Startup reconcile for live names: never adopt or fight an unknown venue position.
        for n, p in self.st["pos"].items():
            if p.get("mode") == "live" and await self.ensure(n):
                venue = await self.venue_qty(n)
                if abs(venue - p.get("qty", 0)) > max(1e-9, 0.02 * abs(p.get("qty", 0)) + 1e-6):
                    p["status"] = f"venue {venue:+.4f} != state {p.get('qty', 0):+.4f}"
                    tg(f"⚠️ {n}: venue {venue:+.4f} vs state {p.get('qty', 0):+.4f} — parked until /sf reset {n}")
        save_state(self.st)
        while True:
            try:
                await self.step()
            except Exception as e:
                log.exception("step failed")
                tg(f"step error: {e}")
            await asyncio.sleep(10)


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    DATA.mkdir(exist_ok=True)
    from auth import load_api_keys
    from exchange_client import ExchangeClient
    client = ExchangeClient(load_api_keys())
    await client.ensure_session()
    try:
        await SessionFlip(client).run()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

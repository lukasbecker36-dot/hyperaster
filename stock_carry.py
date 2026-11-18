#!/usr/bin/env python3
"""
Stock carry — a market-neutral funding book on Hyperliquid's xyz stock perps, sharing this repo's HL client.

THE EDGE (hyperdata repo, scratchpad xs_bt.py / sx7.py, Nov 2026; paper book stock_paper.py since 3 Nov)
  Shorts collect the funding longs pay. Rank the xyz perps by their mean funding over the last 24h, short the
  top 20%, buy the bottom 20%. Backtest Apr-Nov 2026 at 2.5bp/side: Sharpe 1.1 on the dev half, 3.9 on the
  holdout, ~+51%/yr on $1 a side, worst drawdown ~-10%; negative in most variants before August, so the regime
  matters. The funding leg (+15-30%/yr) is steady; the price leg is noisy. Paper 3-18 Nov: +7.1% on $1k a side.

THE RULE (daily at REBALANCE UTC)
  Universe: xyz perps, no FX / commodities, >= $200k 24h volume, >= 14 days of history, minus any name the
  session flip trades and any name the arb bot holds. Score = mean hourly funding over the last 24h.
  Today's cohort: long the lowest-funding 20%, short the highest 20%, `side_usd` a side, weights inverse to
  10-day hourly volatility, no name above `cap` (10%) of its side. The book is the average of the last three
  daily cohorts (each held 72h), traded to once a day; changes under $10 (HL's minimum) are skipped.
  4x isolated margin: most xyz perps are isolated-only, so a position stands on its own margin and is
  liquidated ~20% against its entry (25% margin less ~5% maintenance). 72h moves Apr-Nov: 4x liquidates
  ~1.4% of positions, 3x ~0.3%, 5x ~3.6%.

SAFETY
  - Paper by default; live only after `/sc live YES`.
  - Before anything that adds margin it checks the xyz account's withdrawable balance; short of it, only
    reductions are traded and Telegram says so.
  - Never trades a name the session flip is configured for or the arb bot holds; while live, the arb bot's
    auto-entry skips this book's names (flip_reserve.py).
  - A venue position that doesn't match the state file (a liquidation, a manual trade) parks that name and
    alerts; `/sc reset SYM` adopts what the venue shows.

CONTROL
  data/stockcarry.json          config, written by control_bot (/sc ...)
  data/stockcarry_cmd/*.json    one-shot requests (flatten, rebalance, reset)
  data/stockcarry_state.json    positions + cohorts, owned by this process
  data/stockcarry_trades.csv    every fill (paper and live)
"""

import asyncio
import csv
import json
import logging
import math
import os
import statistics
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except Exception:
    pass

from session_flip import apply_fill, arb_holds, replay_trades

BASE_DIR = Path(__file__).resolve().parent
DATA = BASE_DIR / "data"
CONFIG_FILE = DATA / "stockcarry.json"
STATE_FILE = DATA / "stockcarry_state.json"
TRADES_FILE = DATA / "stockcarry_trades.csv"
CMD_DIR = DATA / "stockcarry_cmd"
SF_CONFIG = DATA / "sessionflip.json"
HL_INFO = "https://api.hyperliquid.xyz/info"

log = logging.getLogger("stockcarry")

FX_COMM = {"GOLD", "SILVER", "CL", "COPPER", "NATGAS", "PLATINUM", "PALLADIUM", "BRENTOIL", "HO", "JPY", "EUR", "GBP"}
DEFAULT_CONFIG = {
    "enabled": True,
    "mode": "paper",                 # paper | live
    "side_usd": 1000.0,              # $ long and $ short
    "leverage": 4,
    "cap": 0.10,                     # max share of one side in one name
    "q": 0.20,                       # top / bottom fraction of the universe
    "look_h": 24,                    # funding lookback
    "hold_d": 3,                     # cohorts averaged
    "min_vol": 200_000.0,            # $ 24h volume
    "min_age_d": 14,
    "rebalance_utc": "00:05",
    "exclude": [],                   # never trade these (on top of the session-flip names)
    "paused": False,                 # True: hold positions, no rebalances
}
TAKER_FEE = 0.00009                  # HL growth-mode taker (~0.9bp) for paper fills
IOC_BUFFER_BPS = 15
MAX_ATTEMPTS = 3
MIN_ORDER_USD = 10.0                 # HL minimum order value
MAINT = 0.05                         # maintenance ~ half the initial margin at 10x max leverage
MARGIN_SPARE = 1.10                  # free margin wanted per $ of new margin before adding
PACE_S = 1.2                         # between per-coin /info reads at a rebalance


# ---------------------------------------------------------------- pure helpers
def capped_weights(raw: dict, cap) -> dict:
    """Normalise `raw` to sum 1 with no weight above `cap`; the excess goes to the rest pro rata. With fewer
    than 1/cap names the cap can't hold, so it falls back to equal weights."""
    if not raw:
        return {}
    if cap is None:
        tot = sum(raw.values())
        return {c: v / tot for c, v in raw.items()}
    if len(raw) * cap < 1:
        return {c: 1 / len(raw) for c in raw}
    fixed: dict = {}
    while True:
        free = {c: v for c, v in raw.items() if c not in fixed}
        left = 1 - sum(fixed.values())
        tot = sum(free.values())
        w = {**fixed, **{c: left * v / tot for c, v in free.items()}}
        over = [c for c in free if w[c] > cap + 1e-12]
        if not over:
            return w
        fixed.update({c: cap for c in over})


def long_short(signal: dict, vol: dict, q: float, side_usd: float, cap=None, min_n: int = 3) -> dict:
    """Long the top q of `signal`, short the bottom q, `side_usd` a side by capped inverse vol. {name: usd}."""
    s = sorted((v, c) for c, v in signal.items() if v is not None and vol.get(c))
    n = int(len(s) * q)
    if n < min_n:
        return {}
    out = {}
    for names, sign in (([c for _, c in s[-n:]], 1), ([c for _, c in s[:n]], -1)):
        for c, wt in capped_weights({c: 1 / vol[c] for c in names}, cap).items():
            out[c] = sign * side_usd * wt
    return out


def book_target(cohorts: list, hold_d: int) -> dict:
    """Average of the last `hold_d` cohorts ({name: usd})."""
    out: dict = {}
    for w in cohorts[-hold_d:]:
        for c, usd in w.items():
            out[c] = out.get(c, 0.0) + usd / hold_d
    return out


def hourly_vol(closes: list) -> float | None:
    r = [math.log(b / a) for a, b in zip(closes, closes[1:]) if a > 0 and b > 0]
    return statistics.pstdev(r) if len(r) >= 48 else None


def liq_price(qty: float, avg: float, lev: int) -> float:
    side = 1 if qty > 0 else -1
    return avg * (1 - side * (1 / lev - MAINT))


def _atomic_write(path: Path, text: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    try:
        cfg.update(json.loads(CONFIG_FILE.read_text()))
    except FileNotFoundError:
        DATA.mkdir(exist_ok=True)
        _atomic_write(CONFIG_FILE, json.dumps(cfg, indent=1))
    except Exception as e:
        log.error(f"bad {CONFIG_FILE.name} ({e}); using defaults")
    cfg["leverage"] = max(1, min(10, int(cfg["leverage"])))
    cfg["exclude"] = [n.upper() for n in cfg.get("exclude", [])]
    return cfg


def sf_names() -> set:
    """Names the session flip is configured for (any mode): HL nets positions per coin, so never share one."""
    try:
        cfg = json.loads(SF_CONFIG.read_text())
        return {n.upper() for n in cfg.get("names", [])} if cfg.get("enabled", True) else set()
    except Exception:
        return set()


def load_state() -> dict:
    try:
        st = json.loads(STATE_FILE.read_text())
    except Exception:
        st = {}
    st.setdefault("pos", {})          # name -> {"mode", "qty", "avg", "realised", "fees", "funding", "status"}
    st.setdefault("cohorts", [])
    st.setdefault("last_rebalance", "")
    st.setdefault("last_funding_hour", 0)
    st.setdefault("liqs", [])         # paper liquidations: {"ts", "name", "loss"}
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


def read_trades() -> list[dict]:
    try:
        with TRADES_FILE.open() as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def tg(text: str):
    token = os.getenv("ALERT_TELEGRAM_BOT_TOKEN")
    chat = os.getenv("ALERT_TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": chat, "text": "[stock-carry] " + text[:3900]}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data), timeout=10).read()
    except Exception as e:
        log.warning(f"telegram send failed: {e}")


def due(cfg: dict, st: dict, now: datetime) -> bool:
    hh, mm = (int(x) for x in cfg["rebalance_utc"].split(":"))
    return (now.hour, now.minute) >= (hh, mm) and st["last_rebalance"] != now.strftime("%Y-%m-%d")


# ---------------------------------------------------------------- the trader
class StockCarry:
    def __init__(self, client):
        self.client = client
        self.st = load_state()
        self.loaded: set[str] = set()
        self.lev_set: dict[str, int] = {}
        self.only_isolated: dict[str, bool] = {}
        self._last_info = 0.0

    # -- market data (public /info through the client's session)
    async def info(self, body: dict, pace: bool = False):
        for k in range(6):
            if pace:
                wait = self._last_info + PACE_S - time.time()
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_info = time.time()
            async with self.client.session.post(HL_INFO, json=body) as r:
                if r.status == 429:
                    await asyncio.sleep(10 * (k + 1))
                    continue
                return await r.json(content_type=None)
        raise RuntimeError("HL /info rate-limited")

    async def snapshot(self) -> dict:
        meta, ctxs = await self.info({"type": "metaAndAssetCtxs", "dex": "xyz"})
        out = {}
        for u, c in zip(meta["universe"], ctxs):
            if u.get("isDelisted"):
                continue
            name = u["name"].split(":")[-1]
            self.only_isolated[name] = bool(u.get("onlyIsolated")) or u.get("marginMode") in ("strictIsolated", "noCross")
            try:
                mid = float(c.get("midPx") or c.get("markPx") or 0)
            except (TypeError, ValueError):
                mid = 0.0
            if mid > 0:
                out[name] = {"mid": mid, "vol": float(c.get("dayNtlVlm") or 0), "funding": float(c.get("funding") or 0)}
        return out

    async def build_cohort(self, cfg: dict, snap: dict, now_s: float) -> tuple[dict, dict]:
        """Today's cohort {name: usd} and each candidate's 24h mean funding (annualised)."""
        skip = sf_names() | set(cfg["exclude"])
        names = [n for n, s in snap.items() if s["vol"] >= cfg["min_vol"] and n not in FX_COMM and n not in skip]
        sig, vol = {}, {}
        age_s = cfg["min_age_d"] * 86400
        for n in names:
            try:
                cl = await self.info({"type": "candleSnapshot", "req": {
                    "coin": f"xyz:{n}", "interval": "1h", "startTime": int((now_s - age_s - 3600) * 1000),
                    "endTime": int(now_s * 1000)}}, pace=True)
                if not cl or int(cl[0]["t"]) / 1000 > now_s - age_s + 6 * 3600:
                    continue                                 # listed < min_age_d
                vol[n] = hourly_vol([float(x["c"]) for x in cl[-241:]])
                fh = await self.info({"type": "fundingHistory", "coin": f"xyz:{n}",
                                      "startTime": int((now_s - cfg["look_h"] * 3600) * 1000)}, pace=True)
                rates = [float(x["fundingRate"]) for x in fh or []]
                if rates:
                    sig[n] = -statistics.mean(rates)         # high funding -> short
            except Exception as e:
                log.warning(f"{n} skipped: {e}")
        return (long_short(sig, vol, cfg["q"], cfg["side_usd"], cfg["cap"]),
                {n: -v * 8760 for n, v in sig.items()})

    # -- venue helpers
    async def ensure(self, name: str) -> bool:
        if name in self.loaded:
            return True
        await self.client.ensure_session()
        await self.client._load_hl_xyz_specs([name])
        if name not in self.client._hl_asset_index:
            return False
        await self.client._infer_hl_tick_sizes([name])
        self.loaded.add(name)
        return True

    async def venue_qty(self, name: str) -> float:
        p = await self.client.get_hl_position(name)
        try:
            return float(p.get("szi", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    async def trade_live(self, name: str, delta: float) -> tuple[float, float]:
        """Move the venue position by `delta` coins with IOCs. Returns (filled, avg px)."""
        filled, cost = 0.0, 0.0
        for _ in range(MAX_ATTEMPTS):
            want = delta - filled
            b = await self.client.get_orderbook("hl", name)
            if not b or b.bid <= 0 or b.ask <= 0:
                raise RuntimeError(f"{name}: empty HL book")
            if abs(want) * b.mid < MIN_ORDER_USD:
                break
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

    async def move_to(self, name: str, usd: float, cfg: dict, mid: float, reason: str) -> tuple[float, str]:
        """Bring `name` to `usd` notional (signed). Returns ($ traded, a short status if not plainly done)."""
        p = self.st["pos"].setdefault(name, {"qty": 0.0, "avg": 0.0, "realised": 0.0, "fees": 0.0,
                                             "funding": 0.0, "status": "ok", "mode": cfg["mode"]})
        if not p.get("qty"):
            if p.get("mode") != cfg["mode"]:
                p.update(realised=0.0, fees=0.0, funding=0.0)
            p["mode"] = cfg["mode"]
        mode = p["mode"]
        target = usd / mid if usd else 0.0
        target = float(self.client.format_hl_qty(name, abs(target))) * (1 if target >= 0 else -1)
        delta = target - p["qty"]
        if abs(delta) * mid < MIN_ORDER_USD:
            return 0.0, ""
        if mode == "live":
            if arb_holds(name):
                p["status"] = "arb holds this symbol"
                return 0.0, "arb holds it"
            venue = await self.venue_qty(name)
            if abs(venue - p["qty"]) > max(1e-9, 0.02 * abs(p["qty"]) + 1e-6):
                p["status"] = f"venue {venue:+.4g} != state {p['qty']:+.4g}"
                tg(f"⚠️ {name}: venue {venue:+.4g} vs state {p['qty']:+.4g} — parked (liquidated or traded by hand?). "
                   f"/sc reset {name} adopts the venue position.")
                return 0.0, "parked"
            if target and self.lev_set.get(name) != cfg["leverage"]:
                cross = not self.only_isolated.get(name, True)
                if not await self.client.set_hl_leverage(name, cfg["leverage"], cross=cross):
                    p["status"] = f"could not set {cfg['leverage']}x"
                    return 0.0, "leverage refused"
                self.lev_set[name] = cfg["leverage"]
            filled, px = await self.trade_live(name, delta)
            p["status"] = "ok" if abs(filled) >= abs(delta) * 0.98 else f"partial {filled:+.4g}/{delta:+.4g}"
        else:
            b = await self.client.get_orderbook("hl", name)
            filled, px = delta, (b.ask if delta > 0 else b.bid)
        fee = abs(filled) * px * TAKER_FEE
        if filled:
            apply_fill(p, filled, px, fee)
            record_trade({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "mode": mode, "name": name,
                          "side": "buy" if filled > 0 else "sell", "qty": f"{abs(filled):.6g}", "px": f"{px:.6g}",
                          "fee": f"{fee:.4f}", "reason": reason})
        save_state(self.st)
        return abs(filled) * px, ("" if p["status"] == "ok" else p["status"])

    def margin_needed(self, target: dict, snap: dict, cfg: dict) -> float:
        """Extra margin the rebalance adds: growth in each name's |notional|, at the leverage."""
        add = 0.0
        for n, usd in target.items():
            cur = self.st["pos"].get(n, {}).get("qty", 0.0) * snap.get(n, {}).get("mid", 0.0)
            if usd * cur < 0:
                add += abs(usd)
            else:
                add += max(0.0, abs(usd) - abs(cur))
        return add / cfg["leverage"]

    async def rebalance(self, cfg: dict, reason: str = "daily rebalance") -> str:
        snap = await self.snapshot()
        cohort, fund = await self.build_cohort(cfg, snap, time.time())
        if not cohort:
            return "no cohort today (too few eligible names) — positions unchanged"
        self.st["cohorts"] = (self.st["cohorts"] + [cohort])[-cfg["hold_d"]:]
        target = book_target(self.st["cohorts"], cfg["hold_d"])
        live = cfg["mode"] == "live"
        adds_ok = True
        if live:
            need = self.margin_needed(target, snap, cfg)
            bal = await self.client.get_hl_balance("xyz")
            free = bal.get("withdrawable", 0.0) if bal else 0.0
            if need * MARGIN_SPARE > free:
                adds_ok = False
                tg(f"⚠️ needs ${need:,.0f} more margin, xyz withdrawable ${free:,.0f} — only reductions traded today")
        traded, n_tr, notes = 0.0, 0, []
        names = set(target) | {n for n, p in self.st["pos"].items() if p.get("qty")}
        # reductions first, so the margin they free is there for the adds
        order = sorted(names, key=lambda n: abs(target.get(n, 0.0)) - abs(
            self.st["pos"].get(n, {}).get("qty", 0.0) * snap.get(n, {}).get("mid", 0.0)))
        for n in order:
            s = snap.get(n)
            if not s:
                notes.append(f"{n}: no quote")
                continue
            want = target.get(n, 0.0)
            cur_usd = self.st["pos"].get(n, {}).get("qty", 0.0) * s["mid"]
            if not adds_ok and abs(want) > abs(cur_usd) and want * cur_usd >= 0:
                continue
            st = self.st["pos"].get(n, {}).get("status", "ok")
            if st != "ok" and not st.startswith("partial"):
                notes.append(f"{n}: {st}")
                continue
            try:
                if not await self.ensure(n):
                    notes.append(f"{n}: not an HL xyz market")
                    continue
                usd, note = await self.move_to(n, want, cfg, s["mid"], reason)
                traded += usd
                n_tr += usd > 0
                if note:
                    notes.append(f"{n}: {note}")
            except Exception as e:
                log.exception(f"{n} trade failed")
                self.st["pos"].setdefault(n, {})["status"] = f"error: {e}"
                save_state(self.st)
                notes.append(f"{n}: ERROR {e}")
        save_state(self.st)
        longs = sorted((c for c in cohort if cohort[c] > 0), key=lambda c: fund.get(c, 0))
        shorts = sorted((c for c in cohort if cohort[c] < 0), key=lambda c: -fund.get(c, 0))
        gross = sum(abs(p["qty"]) * snap.get(n, {}).get("mid", p["avg"]) for n, p in self.st["pos"].items() if p.get("qty"))
        return (f"{cfg['mode'].upper()} {reason}: {n_tr} trades, ${traded:,.0f} traded · book ${gross:,.0f} gross, "
                f"margin ~${gross / cfg['leverage']:,.0f} at {cfg['leverage']}x\n"
                f"cohort long {', '.join(longs)}\ncohort short {', '.join(shorts)}"
                + ("\n" + "\n".join(notes) if notes else ""))

    async def flatten(self, cfg: dict, reason: str) -> str:
        snap = await self.snapshot()
        out = []
        for n, p in list(self.st["pos"].items()):
            if not p.get("qty"):
                continue
            mid = snap.get(n, {}).get("mid") or p["avg"]
            if await self.ensure(n):
                usd, note = await self.move_to(n, 0.0, cfg, mid, reason)
                out.append(f"{n} closed ${usd:,.0f}" + (f" ({note})" if note else ""))
        self.st["cohorts"] = []
        save_state(self.st)
        return "\n".join(out) or "nothing open"

    # -- hourly paper bookkeeping (live funding and liquidations happen on the venue)
    async def paper_hourly(self, cfg: dict):
        hour = int(time.time() // 3600)
        if self.st["last_funding_hour"] == hour:
            return
        self.st["last_funding_hour"] = hour
        open_ = {n: p for n, p in self.st["pos"].items() if p.get("mode") == "paper" and p.get("qty")}
        if not open_:
            save_state(self.st)
            return
        snap = await self.snapshot()
        for n, p in open_.items():
            s = snap.get(n)
            if not s:
                continue
            p["funding"] = p.get("funding", 0.0) - p["qty"] * s["mid"] * s["funding"]
            side = 1 if p["qty"] > 0 else -1
            if side * (s["mid"] - liq_price(p["qty"], p["avg"], cfg["leverage"])) <= 0:
                loss = -abs(p["qty"]) * p["avg"] / cfg["leverage"]           # the position's margin
                p["realised"] = p.get("realised", 0.0) + loss
                record_trade({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "mode": "paper",
                              "name": n, "side": "sell" if side > 0 else "buy", "qty": f"{abs(p['qty']):.6g}",
                              "px": f"{p['avg'] * (1 + loss / (p['qty'] * p['avg'])):.6g}", "fee": "0",
                              "reason": f"liquidated {cfg['leverage']}x"})
                self.st["liqs"].append({"ts": int(time.time()), "name": n, "loss": round(loss, 2)})
                p["qty"], p["avg"] = 0.0, 0.0
                tg(f"PAPER {n} liquidated at {cfg['leverage']}x: ${loss:+,.2f}")
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
            a = req.get("action")
            if a == "flatten":
                tg("closed on request:\n" + await self.flatten(cfg, "manual close"))
            elif a == "rebalance":
                tg(await self.rebalance(cfg, "manual rebalance"))
                self.st["last_rebalance"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                save_state(self.st)
            elif a == "reset" and req.get("name"):
                n = req["name"]
                p = self.st["pos"].setdefault(n, {"qty": 0.0, "avg": 0.0, "realised": 0.0, "fees": 0.0,
                                                  "funding": 0.0, "mode": cfg["mode"]})
                if p.get("mode") == "live" and await self.ensure(n):
                    vp = await self.client.get_hl_position(n)
                    p["qty"] = float(vp.get("szi", 0) or 0)
                    p["avg"] = float(vp.get("entryPx", 0) or 0) if p["qty"] else 0.0
                p["status"] = "ok"
                save_state(self.st)
                tg(f"{n}: reset — state now {p['qty']:+.4g} @ {p['avg']:.6g}")
        if not cfg["enabled"]:
            if any(p.get("qty") for p in self.st["pos"].values()):
                tg("disabled — flattened:\n" + await self.flatten(cfg, "disabled"))
            return
        stale = [n for n, p in self.st["pos"].items() if p.get("qty") and p.get("mode") != cfg["mode"]]
        if stale:
            tg(f"mode is now {cfg['mode']} — closed the old book:\n" + await self.flatten(
                dict(cfg, mode=self.st["pos"][stale[0]]["mode"]), f"switched to {cfg['mode']}"))
        await self.paper_hourly(cfg)
        if cfg["paused"]:
            return
        now = datetime.now(timezone.utc)
        if due(cfg, self.st, now):
            self.st["last_rebalance"] = now.strftime("%Y-%m-%d")
            save_state(self.st)
            tg(await self.rebalance(cfg))

    async def run(self):
        cfg = load_config()
        tg(f"up — {cfg['mode']}, {'enabled' if cfg['enabled'] else 'disabled'}: ${cfg['side_usd']:,.0f} a side at "
           f"{cfg['leverage']}x, max {cfg['cap']:.0%} a name, rebalance {cfg['rebalance_utc']} UTC")
        for n, p in self.st["pos"].items():
            if p.get("mode") == "live" and p.get("qty") and await self.ensure(n):
                venue = await self.venue_qty(n)
                if abs(venue - p["qty"]) > max(1e-9, 0.02 * abs(p["qty"]) + 1e-6):
                    p["status"] = f"venue {venue:+.4g} != state {p['qty']:+.4g}"
                    tg(f"⚠️ {n}: venue {venue:+.4g} vs state {p['qty']:+.4g} — parked until /sc reset {n}")
        save_state(self.st)
        while True:
            try:
                await self.step()
            except Exception as e:
                log.exception("step failed")
                tg(f"step error: {e}")
            await asyncio.sleep(20)


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    DATA.mkdir(exist_ok=True)
    from auth import load_api_keys
    from exchange_client import ExchangeClient
    client = ExchangeClient(load_api_keys())
    await client.ensure_session()
    try:
        await StockCarry(client).run()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

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
  Isolated margin, LEVERAGE PER NAME: the highest leverage up to `leverage` (4x) that keeps the liquidation
  (1/lev less ~5% maintenance) beyond a `lev_sigmas` (3) sigma move over the 3-day hold, from the name's 10-day
  hourly vol. Blue chips stay at 4x (~20% to liquidation); 7-8%/day names (NBIS, ZHIPU, KORU, SHAZ) get 2x or less.
  A flat 4x liquidated those four on 23 Nov after ~20% one-day squeezes: a routine 3-day move for them.
  NO LONG THROUGH AN EX-DIVIDEND DATE: xyz oracles are the raw price, so a perp drops by the dividend at the
  ex-date open, and funding pays longs back only ~16% of it beforehand (hyperdata scratchpad divs.py /
  divgap.py, 24 Nov 2026: 16 US ex-dates, perp-vs-stock gap slope 0.15 on the dividend). Low pre-ex funding
  would otherwise pull those names into the long side. exdiv.json lists windows for dividends >= ~0.3%: a new
  cohort doesn't buy a name with an ex-date inside its hold (the next-lowest funding name takes the slot), and
  a long from an older cohort is closed at the rebalance before its date. Shorts hold through (they gain it).
  After each live rebalance any position whose margin is below its notional / its leverage is topped up
  (updateIsolatedMargin), so a lower leverage also reaches positions opened at the old one.

SAFETY
  - Paper by default; live only after `/sc live YES`.
  - Before anything that adds margin it checks the free collateral (the xyz dex's withdrawable, or for a
    unified account the spot USDC less margin in use); short of it, only reductions are traded and Telegram
    says so.
  - Never trades a name the session flip is configured for or the arb bot holds; while live, the arb bot's
    auto-entry skips this book's names (flip_reserve.py).
  - Every hour (live) HL's liquidation fills are booked into the state (realised loss, trade row, Telegram)
    so the name stays tradable; any OTHER venue/state mismatch (a manual trade) parks that name and alerts;
    `/sc reset SYM` adopts what the venue shows.
  - A day's cohort only counts once its rebalance actually trades: a run that can't read the venue, or that
    scored under 90% of the names (HL rate limits), trades nothing and retries in 15 min (3 tries a day). A
    second rebalance the same day replaces that day's cohort instead of adding one.
  - Short of margin, every add is scaled down by the same factor (reductions in full) so the book stays
    balanced, instead of skipping all adds.
  - Kill switch: if the book's P&L falls `kill_dd` (10%) of the gross book below its peak, rebalancing pauses
    and Telegram alerts (positions are held; /sc_off closes them, /sc_resume carries on from there).

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
EXDIV_FILE = BASE_DIR / "exdiv.json"
HL_INFO = "https://api.hyperliquid.xyz/info"

log = logging.getLogger("stockcarry")

FX_COMM = {"GOLD", "SILVER", "CL", "COPPER", "NATGAS", "PLATINUM", "PALLADIUM", "BRENTOIL", "HO", "JPY", "EUR", "GBP"}
DEFAULT_CONFIG = {
    "enabled": True,
    "mode": "paper",                 # paper | live
    "side_usd": 1000.0,              # $ long and $ short
    "leverage": 4,                   # the MOST any name gets; each name's is set from its vol (lev_for)
    "lev_sigmas": 3.0,               # liquidation kept beyond this many sigmas of a hold_d-day move
    "cap": 0.10,                     # max share of one side in one name
    "q": 0.20,                       # top / bottom fraction of the universe
    "look_h": 24,                    # funding lookback
    "hold_d": 3,                     # cohorts averaged
    "min_vol": 200_000.0,            # $ 24h volume
    "min_age_d": 14,
    "rebalance_utc": "00:05",
    "exclude": [],                   # never trade these (on top of the session-flip names)
    "paused": False,                 # True: hold positions, no rebalances
    "kill_dd": 0.10,                 # pause when P&L falls this share of the gross book (2 x side_usd) off its peak
    "max_skip": 0.10,                # don't trade a cohort scored from fewer than 90% of the candidate names
    "exdiv_min_pct": 0.3,            # no long held through an ex-dividend date this big (exdiv.json)
}
TAKER_FEE = 0.00009                  # HL growth-mode taker (~0.9bp) for paper fills
IOC_BUFFER_BPS = 15
MAX_ATTEMPTS = 3
MIN_ORDER_USD = 10.0                 # HL minimum order value
MAINT = 0.05                         # maintenance ~ half the initial margin at 10x max leverage
MARGIN_SPARE = 1.10                  # free margin wanted per $ of new margin before adding
PACE_S = 1.2                         # between per-coin /info reads at a rebalance
RETRY_S = 900                        # a failed daily rebalance retries after this long...
MAX_RETRIES = 3                      # ...this many times a day


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


def long_short(signal: dict, vol: dict, q: float, side_usd: float, cap=None, min_n: int = 3,
               no_long=frozenset()) -> dict:
    """Long the top q of `signal`, short the bottom q, `side_usd` a side by capped inverse vol. {name: usd}.
    Names in `no_long` are passed over for the long side (the next best fill in); they can still be shorted."""
    s = sorted((v, c) for c, v in signal.items() if v is not None and vol.get(c))
    n = int(len(s) * q)
    if n < min_n:
        return {}
    out = {}
    shorts = [c for _, c in s[:n]]
    longs = [c for _, c in reversed(s) if c not in no_long and c not in shorts][:n]
    for names, sign in ((longs, 1), (shorts, -1)):
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


def load_exdiv(path: Path | None = None) -> dict:
    """The hand-kept ex-dividend calendar: {"valid_until": "YYYY-MM-DD", "names": {name: [{from, to, pct}]}}.
    A missing or broken file gives an empty calendar (and a warning at the rebalance)."""
    try:
        d = json.loads((path or EXDIV_FILE).read_text())
        return {"valid_until": d.get("valid_until", ""), "names": {k.upper(): v for k, v in d.get("names", {}).items()}}
    except Exception as e:
        log.warning(f"ex-dividend calendar unreadable: {e}")
        return {"valid_until": "", "names": {}}


def exdiv_hits(cal: dict, start: str, end: str, min_pct: float) -> dict:
    """Names with an ex-dividend window (>= min_pct) overlapping [start, end] (ISO dates): {name: entry}."""
    out = {}
    for n, ents in cal.get("names", {}).items():
        for e in ents:
            if float(e.get("pct", 0)) >= min_pct and e["from"] <= end and e["to"] >= start:
                out[n] = e
                break
    return out


def _day(now_s: float, plus_d: int = 0) -> str:
    return datetime.fromtimestamp(now_s + plus_d * 86400, timezone.utc).strftime("%Y-%m-%d")


def hourly_vol(closes: list) -> float | None:
    r = [math.log(b / a) for a, b in zip(closes, closes[1:]) if a > 0 and b > 0]
    return statistics.pstdev(r) if len(r) >= 48 else None


def lev_for(hvol, hold_d: int, max_lev: int, sigmas: float) -> int:
    """Highest leverage <= max_lev whose liquidation (1/lev - MAINT against entry) sits beyond `sigmas` x the
    name's hold_d-day vol (hourly vol x sqrt(24 x days): xyz perps trade around the clock). No vol: 2x at most."""
    if not hvol:
        return min(2, max_lev)
    move = sigmas * hvol * math.sqrt(24 * hold_d)
    return max(1, min(max_lev, math.floor(1 / (move + MAINT))))


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
    days = st.get("cohort_days")
    st["cohort_days"] = days if isinstance(days, list) and len(days) == len(st["cohorts"]) else [""] * len(st["cohorts"])
    st.setdefault("retry_after", 0)
    st.setdefault("retries", {})      # day -> failed attempts
    st.setdefault("pnl_peak", None)
    st.setdefault("killed", False)
    st.setdefault("last_rebalance", "")
    st.setdefault("last_funding_hour", 0)
    st.setdefault("liqs", [])         # liquidations: {"ts", "name", "loss", "mode"}
    st.setdefault("vol", {})          # name -> 10-day hourly vol at the last scoring (sets its leverage)
    st.setdefault("liq_seen_ms", int((time.time() - 86400) * 1000))   # HL fills booked up to here
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
    return ((now.hour, now.minute) >= (hh, mm) and st["last_rebalance"] != now.strftime("%Y-%m-%d")
            and now.timestamp() >= st.get("retry_after", 0))


def with_cohort(cohorts: list, days: list, cohort: dict, day: str, hold_d: int) -> tuple[list, list]:
    """The cohort list after adding today's: a cohort already dated today is replaced (a second rebalance the
    same day), and only the last hold_d are kept."""
    keep = [(c, d) for c, d in zip(cohorts, days) if d != day]
    keep = (keep + [(cohort, day)])[-hold_d:]
    return [c for c, _ in keep], [d for _, d in keep]


def scale_add(want: float, cur: float, k: float) -> float:
    """Shrink the ADDING part of a move to `want` (from `cur`, $) by k; reductions are kept whole."""
    if k >= 1:
        return want
    if want * cur > 0 and abs(want) > abs(cur):
        return cur + (want - cur) * k          # growing a position: only the growth is scaled
    if want * cur < 0 or cur == 0:
        return want * k                        # opening or flipping: the new side is scaled
    return want                                # reducing


# ---------------------------------------------------------------- the trader
class StockCarry:
    def __init__(self, client):
        self.client = client
        self.st = load_state()
        self.loaded: set[str] = set()
        self.lev_set: dict[str, int] = {}
        self.only_isolated: dict[str, bool] = {}
        self._last_info = 0.0
        self._venue: dict | None = None             # one venue read, valid for the rebalance/flatten in progress
        self.last_ok = True                         # did the last rebalance run (not abort)?
        self.skipped: list = []
        self.scored_of = 0
        self.exdiv_passed: dict = {}                # names the cohort didn't buy for an ex-dividend date

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
        self.skipped = []                                    # names HL didn't answer for (not: too young)
        for n in names:
            try:
                cl = await self.info({"type": "candleSnapshot", "req": {
                    "coin": f"xyz:{n}", "interval": "1h", "startTime": int((now_s - age_s - 3600) * 1000),
                    "endTime": int(now_s * 1000)}}, pace=True)
                if not isinstance(cl, list):
                    raise RuntimeError(f"bad candles reply {str(cl)[:60]}")
                if not cl or int(cl[0]["t"]) / 1000 > now_s - age_s + 6 * 3600:
                    continue                                 # listed < min_age_d
                vol[n] = hourly_vol([float(x["c"]) for x in cl[-241:]])
                if vol[n]:
                    self.st["vol"][n] = vol[n]
                fh = await self.info({"type": "fundingHistory", "coin": f"xyz:{n}",
                                      "startTime": int((now_s - cfg["look_h"] * 3600) * 1000)}, pace=True)
                if not isinstance(fh, list):
                    raise RuntimeError(f"bad funding reply {str(fh)[:60]}")
                rates = [float(x["fundingRate"]) for x in fh]
                if rates:
                    sig[n] = -statistics.mean(rates)         # high funding -> short
            except Exception as e:
                log.warning(f"{n} skipped: {e}")
                self.skipped.append(n)
        self.scored_of = len(names)
        # a cohort is held until the rebalance hold_d days on, which comes after that day's Asian open
        no_long = exdiv_hits(load_exdiv(), _day(now_s), _day(now_s, cfg["hold_d"]), cfg["exdiv_min_pct"])
        free = long_short(sig, vol, cfg["q"], cfg["side_usd"], cfg["cap"])
        cohort = long_short(sig, vol, cfg["q"], cfg["side_usd"], cfg["cap"], no_long=set(no_long))
        self.exdiv_passed = {n: no_long[n] for n in free if free[n] > 0 and n not in cohort}
        return cohort, {n: -v * 8760 for n, v in sig.items()}

    def lev(self, name: str, cfg: dict) -> int:
        return lev_for(self.st["vol"].get(name), cfg["hold_d"], cfg["leverage"], cfg.get("lev_sigmas", 3.0))

    # -- venue helpers
    async def ensure(self, name: str) -> bool:
        if name in self.loaded:
            return True
        await self.prepare([name])
        return name in self.loaded

    async def prepare(self, names) -> None:
        """Load HL specs for every name not loaded yet in ONE market-list read, then each one's tick size
        (one book read each, paced). Loading them one by one re-read the whole xyz market list per name:
        ~40 heavy reads in a burst, which HL rate-limited on 21 Nov, so valid markets (TLT, KSTR...) came
        back as 'not an HL xyz market'. A name still missing after this is retried at the next rebalance."""
        todo = [n for n in dict.fromkeys(names) if n not in self.loaded]
        if not todo:
            return
        await self.client.ensure_session()
        for attempt in range(2):
            missing = [n for n in todo if n not in self.client._hl_asset_index]
            if not missing:
                break
            if attempt:
                await asyncio.sleep(5)          # one retry after a pause if HL refused the read
            await self.client._load_hl_xyz_specs(missing)
        for n in todo:
            if n in self.client._hl_asset_index:
                await self.client._infer_hl_tick_sizes([n])
                self.loaded.add(n)
                await asyncio.sleep(0.25)

    async def venue_qty(self, name: str) -> float:
        p = await self.client.get_hl_position(name)
        try:
            return float(p.get("szi", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    async def venue_positions(self) -> dict:
        """Every xyz position in ONE request ({name: szi}), and an exception if HL didn't answer properly.
        The client's per-name get_hl_position() returns {} on any error, which reads as flat: on 20 Nov a
        rate-limited startup check (41 calls at once) parked AAPL as 'venue 0' while HL held exactly ours."""
        addr = (getattr(self.client, "api_keys", None) or {}).get("hl_account_address")
        d = await self.info({"type": "clearinghouseState", "user": addr, "dex": "xyz"})
        if not isinstance(d, dict) or not isinstance(d.get("assetPositions"), list):
            raise RuntimeError(f"bad clearinghouseState reply: {str(d)[:120]}")
        return {ap["position"]["coin"].split(":")[-1]: float(ap["position"]["szi"]) for ap in d["assetPositions"]}

    @staticmethod
    def mismatch(venue: float, qty: float) -> bool:
        return abs(venue - qty) > max(1e-9, 0.02 * abs(qty) + 1e-6)

    def reconcile(self, venue: dict) -> list[str]:
        """Park live names whose venue position differs from state; un-park ones that match again."""
        msgs = []
        for n, p in self.st["pos"].items():
            if p.get("mode") != "live":
                continue
            v, q = venue.get(n, 0.0), p.get("qty", 0.0)
            parked = p.get("status", "ok").startswith("venue")
            if q and self.mismatch(v, q) and not parked:
                p["status"] = f"venue {v:+.4g} != state {q:+.4g}"
                msgs.append(f"⚠️ {n}: venue {v:+.4g} vs state {q:+.4g} — parked (liquidated or traded by hand?). "
                            f"/sc reset {n} adopts the venue position.")
            elif parked and not self.mismatch(v, q):
                p["status"] = "ok"
                msgs.append(f"✅ {n}: venue matches state again ({q:+.4g}) — un-parked")
        return msgs

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
            # one venue read per rebalance/flatten (raises if HL didn't answer: never read as flat)
            if self._venue is None:
                self._venue = await self.venue_positions()
            venue = self._venue.get(name, 0.0)
            if self.mismatch(venue, p["qty"]):
                p["status"] = f"venue {venue:+.4g} != state {p['qty']:+.4g}"
                tg(f"⚠️ {name}: venue {venue:+.4g} vs state {p['qty']:+.4g} — parked (liquidated or traded by hand?). "
                   f"/sc reset {name} adopts the venue position.")
                return 0.0, "parked"
            lev = self.lev(name, cfg)
            if target and self.lev_set.get(name) != lev:
                cross = not self.only_isolated.get(name, True)
                ok = await self.client.set_hl_leverage(name, lev, cross=cross)
                if not ok:                      # usually a rate-limited request: one retry after a pause
                    await asyncio.sleep(3)
                    ok = await self.client.set_hl_leverage(name, lev, cross=cross)
                if ok:
                    self.lev_set[name] = lev
                elif not p["qty"]:
                    # not traded this time, but not parked either: the next rebalance tries again
                    return 0.0, f"HL didn't confirm {lev}x leverage (busy?) — not traded, retried next rebalance"
                # an open position keeps trading at its old leverage; the margin top-up after the rebalance
                # brings its margin up to notional / lev
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
            if mode == "live" and self._venue is not None:
                self._venue[name] = self._venue.get(name, 0.0) + filled
        save_state(self.st)
        return abs(filled) * px, ("" if p["status"] == "ok" else p["status"])

    async def free_margin(self) -> float:
        """Collateral free to back new positions. A plain account keeps xyz margin on the xyz dex ('withdrawable');
        a unified account (HL userAbstraction 'unifiedAccount') backs every dex from spot USDC instead, so there
        the xyz dex shows $0 and the free amount is spot USDC less the margin in use. Spot's 'hold' already IS
        that margin (19 Nov: hold $178.12 = xyz margin used $178.12), so it's taken once: the larger of the
        hold and the perp dexes' margin used. Subtracting both had made $302 free look like $124."""
        xyz = await self.client.get_hl_balance("xyz") or {}
        free = xyz.get("withdrawable", 0.0)
        addr = (getattr(self.client, "api_keys", None) or {}).get("hl_account_address")
        if addr:
            try:
                spot = await self.info({"type": "spotClearinghouseState", "user": addr})
                usdc = [b for b in spot.get("balances", []) if b.get("coin") == "USDC"]
                total = sum(float(b.get("total") or 0) for b in usdc)
                hold = sum(float(b.get("hold") or 0) for b in usdc)
                main = await self.client.get_hl_balance("") or {}
                used = xyz.get("margin_used", 0.0) + main.get("margin_used", 0.0)
                free = max(free, total - max(hold, used))
            except Exception as e:
                log.warning(f"spot balance read failed ({e}); using the xyz dex balance only")
        return free

    def margin_needed(self, target: dict, snap: dict, cfg: dict) -> float:
        """Extra margin the rebalance adds: growth in each name's |notional|, at the leverage."""
        add = 0.0
        for n, usd in target.items():
            cur = self.st["pos"].get(n, {}).get("qty", 0.0) * snap.get(n, {}).get("mid", 0.0)
            grow = abs(usd) if usd * cur < 0 else max(0.0, abs(usd) - abs(cur))
            add += grow / self.lev(n, cfg)
        return add

    async def rebalance(self, cfg: dict, reason: str = "daily rebalance") -> str:
        """Run one rebalance; sets self.last_ok False if it aborted before trading (nothing committed)."""
        self._venue = None
        self.last_ok = False
        try:
            return await self._rebalance(cfg, reason)
        finally:
            self._venue = None

    async def _rebalance(self, cfg: dict, reason: str) -> str:
        snap = await self.snapshot()
        cohort, fund = await self.build_cohort(cfg, snap, time.time())
        if self.scored_of and len(self.skipped) > cfg["max_skip"] * self.scored_of:
            return (f"⚠️ {reason}: only scored {self.scored_of - len(self.skipped)} of {self.scored_of} names "
                    f"(HL busy?) — nothing traded, cohort not counted; retrying")
        if not cohort:
            self.last_ok = True
            return "no cohort today (too few eligible names) — positions unchanged"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cohorts, days = with_cohort(self.st["cohorts"], self.st["cohort_days"], cohort, today, cfg["hold_d"])
        target = book_target(cohorts, cfg["hold_d"])
        # no long into an ex-date before the next rebalance (tomorrow's Asian open comes first); older cohorts
        # may hold one bought before its date was in the calendar
        cal, now_s = load_exdiv(), time.time()
        exd = exdiv_hits(cal, _day(now_s), _day(now_s, 1), cfg["exdiv_min_pct"])
        cut = [n for n in exd if target.get(n, 0.0) > 0]
        for n in cut:
            target[n] = 0.0
        div_notes = [f"ex-dividend: not buying {n} (ex {e['from']}{'' if e['to'] == e['from'] else '..' + e['to']}, "
                     f"~{e['pct']}%)" for n, e in sorted(self.exdiv_passed.items())]
        div_notes += [f"ex-dividend: {n} long closed (ex {exd[n]['from']}, ~{exd[n]['pct']}%)" for n in sorted(cut)]
        if cal["valid_until"] < _day(now_s, cfg["hold_d"] + 7):
            div_notes.append(f"⚠️ ex-dividend calendar {'missing' if not cal['valid_until'] else 'ends ' + cal['valid_until']}"
                             f" — rebuild exdiv.json (scripts/exdiv_build.py)")
        live = cfg["mode"] == "live"
        k = 1.0                                  # share of each add that fits the free margin
        if live:
            try:
                self._venue = await self.venue_positions()
            except Exception as e:
                save_state(self.st)
                return f"⚠️ couldn't read the venue positions ({e}) — nothing traded; next rebalance or /sc rebalance retries"
            for m in self.reconcile(self._venue):
                tg(m)
            need = self.margin_needed(target, snap, cfg)
            free = await self.free_margin()
            if need * MARGIN_SPARE > free:
                k = max(0.0, free / (need * MARGIN_SPARE))
                tg(f"⚠️ adds need ${need:,.0f} more margin, free collateral ${free:,.0f} — every add scaled to "
                   f"{k:.0%} so the book stays balanced (reductions in full)")
        # committed now: this cohort counts towards the 3-day book
        self.st["cohorts"], self.st["cohort_days"] = cohorts, days
        self.last_ok = True
        traded, n_tr, notes = 0.0, 0, div_notes
        names = set(target) | {n for n, p in self.st["pos"].items() if p.get("qty")}
        for p in self.st["pos"].values():       # the old leverage refusal used to park: un-park those
            if p.get("status", "ok").startswith("could not set"):
                p["status"] = "ok"
        await self.prepare(sorted(names))
        # reductions first, so the margin they free is there for the adds
        order = sorted(names, key=lambda n: abs(target.get(n, 0.0)) - abs(
            self.st["pos"].get(n, {}).get("qty", 0.0) * snap.get(n, {}).get("mid", 0.0)))
        for n in order:
            s = snap.get(n)
            if not s:
                notes.append(f"{n}: no quote")
                continue
            cur_usd = self.st["pos"].get(n, {}).get("qty", 0.0) * s["mid"]
            want = scale_add(target.get(n, 0.0), cur_usd, k)
            st = self.st["pos"].get(n, {}).get("status", "ok")
            if st != "ok" and not st.startswith("partial"):
                notes.append(f"{n}: {st}")
                continue
            try:
                if not await self.ensure(n):
                    notes.append(f"{n}: couldn't load its HL market details (HL busy?) — retried next rebalance")
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
        if live:
            notes += await self.top_up_margin(cfg)
        longs = sorted((c for c in cohort if cohort[c] > 0), key=lambda c: fund.get(c, 0))
        shorts = sorted((c for c in cohort if cohort[c] < 0), key=lambda c: -fund.get(c, 0))
        gross = sum(abs(p["qty"]) * snap.get(n, {}).get("mid", p["avg"]) for n, p in self.st["pos"].items() if p.get("qty"))
        levs: dict = {}
        margin = 0.0
        for n, p in self.st["pos"].items():
            if p.get("qty"):
                lv = self.lev(n, cfg)
                levs[lv] = levs.get(lv, 0) + 1
                margin += abs(p["qty"]) * snap.get(n, {}).get("mid", p["avg"]) / lv
        mix = ", ".join(f"{lv}x {k}" for lv, k in sorted(levs.items(), reverse=True))
        return (f"{cfg['mode'].upper()} {reason}: {n_tr} trades, ${traded:,.0f} traded · book ${gross:,.0f} gross, "
                f"margin ~${margin:,.0f} (names by leverage: {mix or 'none'})\n"
                f"cohort long {', '.join(longs)}\ncohort short {', '.join(shorts)}"
                + ("\n" + "\n".join(notes) if notes else ""))

    async def flatten(self, cfg: dict, reason: str) -> str:
        self._venue = None
        try:
            return await self._flatten(cfg, reason)
        finally:
            self._venue = None

    async def _flatten(self, cfg: dict, reason: str) -> str:
        snap = await self.snapshot()
        out = []
        await self.prepare(sorted(n for n, p in self.st["pos"].items() if p.get("qty")))
        for n, p in list(self.st["pos"].items()):
            if not p.get("qty"):
                continue
            mid = snap.get(n, {}).get("mid") or p["avg"]
            if await self.ensure(n):
                usd, note = await self.move_to(n, 0.0, cfg, mid, reason)
                out.append(f"{n} closed ${usd:,.0f}" + (f" ({note})" if note else ""))
        self.st["cohorts"], self.st["cohort_days"] = [], []
        save_state(self.st)
        return "\n".join(out) or "nothing open"

    async def clearinghouse(self) -> dict:
        """{name: HL position dict} on the xyz dex, in one checked read."""
        addr = (getattr(self.client, "api_keys", None) or {}).get("hl_account_address")
        d = await self.info({"type": "clearinghouseState", "user": addr, "dex": "xyz"})
        if not isinstance(d, dict) or not isinstance(d.get("assetPositions"), list):
            raise RuntimeError(f"bad clearinghouseState reply: {str(d)[:120]}")
        return {ap["position"]["coin"].split(":")[-1]: ap["position"] for ap in d["assetPositions"]}

    async def top_up_margin(self, cfg: dict) -> list[str]:
        """Bring each live isolated position's margin up to notional / its leverage (never takes margin out).
        A position opened at a higher leverage, or one that has run against us, gets its liquidation pushed back
        out. Scaled down together if the free collateral can't cover it all."""
        try:
            ch = await self.clearinghouse()
            free = await self.free_margin()
        except Exception as e:
            return [f"margin top-up skipped: couldn't read the venue ({e})"]
        want = {}
        for n, p in self.st["pos"].items():
            v = ch.get(n)
            if p.get("mode") != "live" or not p.get("qty") or not v:
                continue
            if (v.get("leverage") or {}).get("type") != "isolated":
                continue
            need = float(v["positionValue"]) / self.lev(n, cfg) - float(v["marginUsed"])
            if need >= 1.0:                       # cents aren't worth a signed action
                want[n] = need
        if not want:
            return []
        k = min(1.0, free / (sum(want.values()) * MARGIN_SPARE)) if free > 0 else 0.0
        done, total = [], 0.0
        for n, need in sorted(want.items(), key=lambda kv: -kv[1]):
            usd = round(need * k, 2)
            if usd < 1.0 or not await self.ensure(n):
                continue
            if await self.client.add_hl_isolated_margin(n, usd):
                done.append(f"{n} +${usd:.2f}")
                total += usd
            await asyncio.sleep(0.5)
        if not done:
            return [f"margin top-up wanted ${sum(want.values()):,.0f} but nothing was added (free ${free:,.0f})"]
        return [f"margin topped up ${total:,.2f} to each name's leverage: {', '.join(done)}"
                + ("" if k >= 1 else f" (scaled to {k:.0%}: free collateral ${free:,.0f})")]

    async def book_liquidations(self, cfg: dict) -> list[str]:
        """Book HL liquidation fills on live names into the state (so the name isn't parked as a mismatch)."""
        addr = (getattr(self.client, "api_keys", None) or {}).get("hl_account_address")
        since = int(self.st.get("liq_seen_ms") or 0)
        fills = await self.info({"type": "userFillsByTime", "user": addr, "startTime": since + 1,
                                 "aggregateByTime": True})
        if not isinstance(fills, list):
            raise RuntimeError(f"bad userFillsByTime reply: {str(fills)[:120]}")
        msgs = []
        for f in sorted(fills, key=lambda f: f["time"]):
            self.st["liq_seen_ms"] = max(int(self.st.get("liq_seen_ms") or 0), int(f["time"]))
            if not f.get("liquidation") or not str(f.get("coin", "")).startswith("xyz:"):
                continue
            n = f["coin"].split(":")[-1]
            p = self.st["pos"].get(n)
            if not p or p.get("mode") != "live" or not p.get("qty"):
                continue
            sz = float(f["sz"]) * (1 if f["side"] == "B" else -1)
            if (p["qty"] > 0) == (sz > 0):
                continue                           # not a reduction of our position
            sz = max(-abs(p["qty"]), min(abs(p["qty"]), sz))
            px, fee = float(f["px"]), float(f.get("fee") or 0)
            before, avg = p.get("realised", 0.0), p["avg"]
            apply_fill(p, sz, px, fee)
            loss = p["realised"] - before - fee
            record_trade({"ts": datetime.fromtimestamp(f["time"] / 1000, timezone.utc).isoformat(timespec="seconds"),
                          "mode": "live", "name": n, "side": "buy" if sz > 0 else "sell", "qty": f"{abs(sz):.6g}",
                          "px": f"{px:.6g}", "fee": f"{fee:.4f}", "reason": "liquidated on HL"})
            self.st["liqs"].append({"ts": int(f["time"] / 1000), "name": n, "loss": round(loss, 2), "mode": "live"})
            p["status"] = "ok"
            msgs.append(f"💥 LIVE {n} liquidated by HL: {'short' if sz > 0 else 'long'} {abs(sz):.4g} @ {px:.6g} "
                        f"(entry {avg:.6g}, {px / avg - 1:+.1%}) — ${loss:+,.2f}. Booked; {n} stays tradable, "
                        f"next rebalance at {self.lev(n, cfg)}x.")
        return msgs

    async def live_hourly(self, cfg: dict):
        """Live: book HL liquidations, then park any other venue/state mismatch (alerts once per name)."""
        if not any(p.get("mode") == "live" and p.get("qty") for p in self.st["pos"].values()):
            return
        try:
            msgs = await self.book_liquidations(cfg)
            msgs += self.reconcile(await self.venue_positions())
        except Exception as e:
            log.warning(f"hourly venue check skipped: {e}")
            return
        save_state(self.st)
        for m in msgs:
            tg(m)

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
            lev = self.lev(n, cfg)
            if side * (s["mid"] - liq_price(p["qty"], p["avg"], lev)) <= 0:
                loss = -abs(p["qty"]) * p["avg"] / lev                       # the position's margin
                p["realised"] = p.get("realised", 0.0) + loss
                record_trade({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "mode": "paper",
                              "name": n, "side": "sell" if side > 0 else "buy", "qty": f"{abs(p['qty']):.6g}",
                              "px": f"{p['avg'] * (1 + loss / (p['qty'] * p['avg'])):.6g}", "fee": "0",
                              "reason": f"liquidated {lev}x"})
                self.st["liqs"].append({"ts": int(time.time()), "name": n, "loss": round(loss, 2)})
                p["qty"], p["avg"] = 0.0, 0.0
                tg(f"PAPER {n} liquidated at {lev}x: ${loss:+,.2f}")
        save_state(self.st)

    def book_pnl(self, cfg: dict, snap: dict) -> float:
        """The current mode's P&L from the state: realised - fees + open (marked to `snap`), + paper funding.
        Live funding is paid on the venue and isn't in the state (small next to price; see /sc_pnl)."""
        tot = 0.0
        for n, p in self.st["pos"].items():
            if p.get("mode") != cfg["mode"]:
                continue
            tot += p.get("realised", 0.0) - p.get("fees", 0.0) + (p.get("funding", 0.0) if cfg["mode"] == "paper" else 0.0)
            if p.get("qty"):
                tot += p["qty"] * (snap.get(n, {}).get("mid", p["avg"]) - p["avg"])
        return tot

    def kill_check(self, cfg: dict, pnl: float) -> str | None:
        """Track the P&L peak; past `kill_dd` of the gross book below it, pause (once) and say so."""
        if self.st["killed"] and not cfg["paused"]:          # resumed by hand: start a fresh peak
            self.st["killed"], self.st["pnl_peak"] = False, pnl
        peak = pnl if self.st["pnl_peak"] is None else max(self.st["pnl_peak"], pnl)
        self.st["pnl_peak"] = peak
        limit = cfg["kill_dd"] * 2 * cfg["side_usd"]
        if not self.st["killed"] and peak - pnl >= limit > 0:
            self.st["killed"] = True
            try:
                raw = json.loads(CONFIG_FILE.read_text())
            except Exception:
                raw = {}
            raw["paused"] = True
            _atomic_write(CONFIG_FILE, json.dumps(raw, indent=1))
            return (f"🛑 kill switch: P&L ${pnl:+,.2f} is ${peak - pnl:,.2f} below its peak ${peak:+,.2f} "
                    f"(limit {cfg['kill_dd']:.0%} of the ${2 * cfg['side_usd']:,.0f} book) — rebalancing PAUSED, "
                    f"positions held. /sc_off closes everything; /sc_resume carries on.")
        return None

    async def hourly(self, cfg: dict):
        hour = int(time.time() // 3600)
        if self.st.get("kill_hour") == hour:
            return
        self.st["kill_hour"] = hour
        if not any(p.get("qty") for p in self.st["pos"].values()):
            save_state(self.st)
            return
        if cfg["mode"] == "live":
            await self.live_hourly(cfg)
        msg = self.kill_check(cfg, self.book_pnl(cfg, await self.snapshot()))
        save_state(self.st)
        if msg:
            tg(msg)

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
                if self.last_ok:
                    self.st["last_rebalance"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                save_state(self.st)
            elif a == "reset" and req.get("name"):
                n = req["name"]
                p = self.st["pos"].setdefault(n, {"qty": 0.0, "avg": 0.0, "realised": 0.0, "fees": 0.0,
                                                  "funding": 0.0, "mode": cfg["mode"]})
                if p.get("mode") == "live":
                    try:                # one checked read: a failed per-name query would adopt 'flat'
                        addr = (getattr(self.client, "api_keys", None) or {}).get("hl_account_address")
                        d = await self.info({"type": "clearinghouseState", "user": addr, "dex": "xyz"})
                        vp = next((ap["position"] for ap in d["assetPositions"]
                                   if ap["position"]["coin"].split(":")[-1] == n), {})
                    except Exception as e:
                        tg(f"{n}: reset NOT done — couldn't read the venue ({e}); try again")
                        continue
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
        await self.hourly(cfg)
        if cfg["paused"] or self.st["killed"]:
            return
        now = datetime.now(timezone.utc)
        if due(cfg, self.st, now):
            day, prev = now.strftime("%Y-%m-%d"), self.st["last_rebalance"]
            self.st["last_rebalance"] = day           # set first: a crash mid-run doesn't re-trade today
            save_state(self.st)
            msg = await self.rebalance(cfg)
            if not self.last_ok:
                n = self.st["retries"].get(day, 0) + 1
                self.st["retries"] = {day: n}
                if n < MAX_RETRIES:
                    self.st["last_rebalance"] = prev
                    self.st["retry_after"] = now.timestamp() + RETRY_S
                    msg += f" (try {n} of {MAX_RETRIES}, next in {RETRY_S // 60} min)"
                else:
                    msg += f" (gave up after {MAX_RETRIES} tries; positions unchanged until tomorrow or /sc rebalance)"
                save_state(self.st)
            tg(msg)

    async def run(self):
        cfg = load_config()
        tg(f"up — {cfg['mode']}, {'enabled' if cfg['enabled'] else 'disabled'}: ${cfg['side_usd']:,.0f} a side at "
           f"up to {cfg['leverage']}x (each name's set by its vol), max {cfg['cap']:.0%} a name, rebalance {cfg['rebalance_utc']} UTC")
        if any(p.get("mode") == "live" and p.get("qty") for p in self.st["pos"].values()):
            try:                        # liquidations first, so a liquidated name is booked, not parked
                for m in await self.book_liquidations(cfg) + self.reconcile(await self.venue_positions()):
                    tg(m)
            except Exception as e:      # don't park anything on a failed read; the next rebalance checks again
                log.warning(f"startup venue check skipped: {e}")
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

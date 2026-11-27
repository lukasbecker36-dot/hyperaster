"""
Dividend capture (TEST, $100 a side): long the stock's perp on Aster, short it on HL's xyz dex, over an
ex-dividend date. Run inside stock_carry.py's loop; every trade needs a Telegram "Yes".

WHY (hyperdata scratchpad div_venues.py / divgap.py, 24-27 Nov 2026)
  Both venues' prices drop by the dividend at the ex-date open. Aster pays its LONGS the dividend as a
  one-off "special dividend funding" from shorts just before that open (docs.asterdex.com, "Dividend
  introduction for Equity Perpetual Futures": 20:00 ET the day before for US stocks, 08:00 KST / 09:00 HKT
  on the ex-date; reduce-only for the 30 min before it). HL pays nothing; on US names its perp didn't
  discount the drop beforehand (funding paid longs back ~16% of it over 5 days). So long Aster / short HL
  collects ~the dividend: IBM and BX traded level on both venues through their ex-dates. Whether Aster's
  special funding really arrives, and how much, is what this test is for: the close report shows Aster's
  income by type.

FLOW
  - exdiv.json events with an exact date (from == to) in names on both venues, with no stock carry position.
  - From the session before the ex-date until 45 min before Aster's special funding: every 5 min, expected
    edge = dividend - (Aster ask / HL bid - 1) - ~0.12% fees. At >= MIN_EDGE: one Telegram offer with Yes/No.
  - Yes: Aster market buy (IOC), then HL short of the same size, both isolated 2x. A leg that can't be
    matched is unwound.
  - Closed 1h after the ex-date open (or on the offer's Close button): both legs at market, then a report.
  - While a trade is on, stock carry leaves the name alone and the HL leg is netted out of its venue checks.
"""
import json
import logging
import time
import urllib.parse
import urllib.request
import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

log = logging.getLogger("divcap")

KR = {"SKHX", "SMSN", "HYUNDAI"}             # Aster's KR schedule (KST)
HK: set = set()                              # Aster's HK schedule (HKT); none with dividends listed yet
FEES = 0.0012                                # 4 taker fills (Aster ~0.04%, HL ~0.01%) + a little slippage
MIN_EDGE = 0.0025
SIZE_USD = 100.0
LEV = 2
SCAN_S = 300
RESERVE_D = 4                                # stock carry steps out of the name this long before
IOC_BPS = 30


def prev_weekday(d: date) -> date:
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def event_times(name: str, ex: str) -> tuple[float, float, float]:
    """(entry_from, entry_until, close_at) as epoch seconds for an ex-date `ex` (ISO) on `name`."""
    d = date.fromisoformat(ex)
    p = prev_weekday(d)
    if name in KR or name in HK:
        tz = ZoneInfo("Asia/Seoul" if name in KR else "Asia/Hong_Kong")
        special = datetime(d.year, d.month, d.day, 8 if name in KR else 9, 0, tzinfo=tz)
        opened = datetime(d.year, d.month, d.day, 9, 0 if name in KR else 30, tzinfo=tz)
        start = datetime(p.year, p.month, p.day, 9, 45, tzinfo=tz)
    else:
        tz = ZoneInfo("America/New_York")
        special = datetime(p.year, p.month, p.day, 20, 0, tzinfo=tz)       # "ex_date - 1", 20:00 ET
        opened = datetime(d.year, d.month, d.day, 9, 30, tzinfo=tz)
        start = datetime(p.year, p.month, p.day, 9, 45, tzinfo=tz)
    return start.timestamp(), (special - timedelta(minutes=45)).timestamp(), (opened + timedelta(hours=1)).timestamp()


def expected_edge(div: float, aster_ask: float, hl_bid: float) -> float:
    """Long Aster at its ask, short HL at its bid: collect `div` (Aster's special funding), give back what Aster
    already trades above HL, less fees."""
    return div - (aster_ask / hl_bid - 1) - FEES


def tg_buttons(text: str, buttons: list[list[tuple[str, str]]]):
    token, chat = os.getenv("ALERT_TELEGRAM_BOT_TOKEN"), os.getenv("ALERT_TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    kb = {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row] for row in buttons]}
    try:
        data = urllib.parse.urlencode({"chat_id": chat, "text": "[div-capture] " + text[:3800],
                                       "reply_markup": json.dumps(kb)}).encode()
        urllib.request.urlopen(urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data),
                               timeout=10).read()
    except Exception as e:
        log.warning(f"telegram send failed: {e}")


class DivCapture:
    def __init__(self, bot, tg, load_exdiv, sf_names, arb_holds):
        self.bot, self.tg, self.load_exdiv = bot, tg, load_exdiv
        self.sf_names, self.arb_holds = sf_names, arb_holds
        self.st = bot.st.setdefault("divcap", {})
        for k in ("offers", "open"):
            self.st.setdefault(k, {})
        self.st.setdefault("done", [])
        self.last_scan = 0.0
        self.aster_ok: dict = {}

    # -- what stock carry needs to know
    def hl_qty(self, name: str) -> float:
        return sum(t["hl_qty"] for t in self.st["open"].values() if t["name"] == name)

    def names(self, now: float | None = None) -> set:
        """Names stock carry must leave alone: a trade open or opening, or an exact-date event whose offer
        window opens within RESERVE_D days (so stock carry is flat by then and an offer can be made)."""
        now = now or time.time()
        out = {t["name"] for t in self.st["open"].values()} | {
            o["name"] for o in self.st["offers"].values() if o.get("status") == "opening"}
        for name, ents in self.load_exdiv().get("names", {}).items():
            for e in ents:
                if e["from"] == e["to"] and float(e.get("pct", 0)) / 100 >= MIN_EDGE + FEES:
                    start, until, _ = event_times(name, e["from"])
                    if start - RESERVE_D * 86400 <= now < until:
                        out.add(name)
        return out

    # -- the loop
    async def tick(self, now: float | None = None):
        now = now or time.time()
        for eid, t in list(self.st["open"].items()):
            if now >= t["close_at"]:
                await self.close(eid, "1h after the ex-date open")
        if now - self.last_scan >= SCAN_S:
            self.last_scan = now
            await self.scan(now)

    async def scan(self, now: float):
        cal = self.load_exdiv()
        busy = self.sf_names() | {t["name"] for t in self.st["open"].values()}
        for name, ents in cal.get("names", {}).items():
            for e in ents:
                if e["from"] != e["to"]:
                    continue                                   # estimated window: no exact date to time it
                eid = f"{name}-{e['from']}"
                if eid in self.st["offers"] or eid in self.st["open"] or name in busy:
                    continue
                start, until, _ = event_times(name, e["from"])
                if not start <= now < until:
                    continue
                if (self.bot.st["pos"].get(name, {}).get("qty") or self.arb_holds(name)):
                    continue
                books = await self.books(name)
                if not books:
                    continue
                (ab, aa), (hb, ha) = books
                edge = expected_edge(float(e["pct"]) / 100, aa, hb)
                if edge < MIN_EDGE:
                    continue
                self.st["offers"][eid] = {"name": name, "ex": e["from"], "div": float(e["pct"]) / 100,
                                          "edge": edge, "sent": now, "until": until, "status": "offered"}
                self.bot.save()
                fmt = lambda ts: datetime.fromtimestamp(ts, timezone.utc).strftime("%a %d %b %H:%M UTC")
                tg_buttons(
                    f"💸 {name} ex-dividend {e['from']} (~{e['pct']}%, {e.get('src', '')})\n"
                    f"Long Aster @ {aa:.6g} / short HL @ {hb:.6g} (Aster {aa / hb - 1:+.2%} vs HL)\n"
                    f"Expected edge {edge:+.2%} after fees ≈ ${edge * SIZE_USD:+.2f} on ${SIZE_USD:.0f} a side "
                    f"(Aster pays its longs the dividend as special funding; HL's short gains the drop at the open)\n"
                    f"Offer good until {fmt(until)}; closes {fmt(event_times(name, e['from'])[2])}.",
                    [[("✅ Yes, put it on", f"dc:yes:{eid}"), ("❌ No", f"dc:no:{eid}")]])

    async def books(self, name: str):
        try:
            if name not in self.aster_ok:
                await self.bot.client._load_aster_specs([name])
                self.aster_ok[name] = name in getattr(self.bot.client, "aster_specs", {})
            if not self.aster_ok[name] or not await self.bot.ensure(name):
                return None
            a = await self.bot.client.get_orderbook("aster", name)
            h = await self.bot.client.get_orderbook("hl", name)
            if min(a.bid, a.ask, h.bid, h.ask) <= 0:
                return None
            return (a.bid, a.ask), (h.bid, h.ask)
        except Exception as e:
            log.warning(f"{name} books: {e}")
            return None

    # -- requests from the Telegram buttons
    async def request(self, op: str, eid: str):
        o = self.st["offers"].get(eid)
        if op == "close":
            if eid in self.st["open"]:
                await self.close(eid, "closed on request")
            else:
                self.tg(f"div capture {eid}: nothing open")
            return
        if not o:
            self.tg(f"div capture {eid}: no such offer")
            return
        if op == "no":
            o["status"] = "declined"
            self.bot.save()
            self.tg(f"div capture {eid}: skipped")
            return
        if o["status"] != "offered":
            self.tg(f"div capture {eid}: already {o['status']}")
            return
        if time.time() >= o["until"]:
            o["status"] = "expired"
            self.bot.save()
            self.tg(f"div capture {eid}: too late — Aster goes reduce-only before its dividend funding")
            return
        await self.open(eid, o)

    async def open(self, eid: str, o: dict):
        name, c = o["name"], self.bot.client
        books = await self.books(name)
        if not books:
            self.tg(f"div capture {eid}: couldn't read both books — nothing traded")
            return
        (ab, aa), (hb, ha) = books
        edge = expected_edge(o["div"], aa, hb)
        if edge < 0:
            o["status"] = "edge gone"
            self.bot.save()
            self.tg(f"div capture {eid}: edge now {edge:+.2%} (Aster {aa / hb - 1:+.2%} vs HL) — not traded")
            return
        if self.bot.st["pos"].get(name, {}).get("qty"):
            self.tg(f"div capture {eid}: stock carry holds {name} now — not traded")
            return
        o["status"] = "opening"
        self.bot.save()
        qty = float(c.format_hl_qty(name, float(c.format_aster_qty(name, SIZE_USD / aa))))
        if qty * aa < 10:
            o["status"] = "too small"
            self.bot.save()
            self.tg(f"div capture {eid}: ${SIZE_USD:.0f} rounds below the venues' minimums — not traded")
            return
        await c.set_aster_margin_type(name, "ISOLATED")
        await c.set_aster_leverage(name, LEV)
        ra = await c.place_aster_ioc(name, "buy", qty, aa)
        aq = float(ra.filled_qty or 0) if ra.success else 0.0
        if aq <= 0:
            o["status"] = "Aster didn't fill"
            self.bot.save()
            self.tg(f"div capture {eid}: Aster buy didn't fill ({ra.error or 'no fill'}) — nothing open")
            return
        await c.set_hl_leverage(name, LEV, cross=not self.bot.only_isolated.get(name, True))
        hq, hcost = 0.0, 0.0
        for _ in range(3):
            want = float(c.format_hl_qty(name, aq - hq))
            if want <= 0:
                break
            hbk = await c.get_orderbook("hl", name)
            r = await c.place_hl_ioc(name, "sell", want, hbk.bid, buffer_bps=IOC_BPS)
            if r.success and r.filled_qty > 0:
                hq += r.filled_qty
                hcost += r.filled_qty * r.fill_price
        if hq < aq * 0.98:                                     # unmatched: take the Aster excess back off
            extra = float(c.format_aster_qty(name, aq - hq))
            if extra > 0:
                abk = await c.get_orderbook("aster", name)
                await c.place_aster_ioc(name, "sell", extra, abk.bid, reduce_only=True)
                aq -= extra
        if hq <= 0:
            o["status"] = "HL didn't fill (Aster leg unwound)"
            self.bot.save()
            self.tg(f"div capture {eid}: HL short didn't fill — Aster leg sold back. Check /sc and Aster.")
            return
        _, _, close_at = event_times(name, o["ex"])
        self.st["open"][eid] = {"name": name, "ex": o["ex"], "div": o["div"], "opened": time.time(),
                                "close_at": close_at, "aster_qty": aq, "aster_px": ra.fill_price,
                                "hl_qty": -hq, "hl_px": hcost / hq}
        o["status"] = "open"
        self.bot.save()
        t = self.st["open"][eid]
        self.tg(f"💸 div capture {name} OPEN: long {aq:g} on Aster @ {t['aster_px']:.6g}, short {hq:g} on HL @ "
                f"{t['hl_px']:.6g} (Aster {t['aster_px'] / t['hl_px'] - 1:+.2%}). Closes "
                f"{datetime.fromtimestamp(close_at, timezone.utc).strftime('%a %d %b %H:%M UTC')}.")
        tg_buttons(f"{name}: close it early?", [[("Close now", f"dc:close:{eid}")]])

    async def close(self, eid: str, why: str):
        t, c = self.st["open"][eid], self.bot.client
        name = t["name"]
        await self.books(name)                                # loads both venues' specs (e.g. after a restart)
        a_left, h_left = t["aster_qty"], -t["hl_qty"]
        a_px = h_px = 0.0
        a_done = h_done = 0.0
        for _ in range(3):
            if h_left - h_done > 0:
                b = await c.get_orderbook("hl", name)
                want = float(c.format_hl_qty(name, h_left - h_done))
                if want > 0:
                    r = await c.place_hl_ioc(name, "buy", want, b.ask, buffer_bps=IOC_BPS)
                    if r.success and r.filled_qty > 0:
                        h_px = (h_px * h_done + r.fill_price * r.filled_qty) / (h_done + r.filled_qty)
                        h_done += r.filled_qty
            if a_left - a_done > 0:
                b = await c.get_orderbook("aster", name)
                want = float(c.format_aster_qty(name, a_left - a_done))
                if want > 0:
                    r = await c.place_aster_ioc(name, "sell", want, b.bid, reduce_only=True)
                    if r.success and r.filled_qty > 0:
                        a_px = (a_px * a_done + r.fill_price * r.filled_qty) / (a_done + r.filled_qty)
                        a_done += r.filled_qty
        if a_done < a_left * 0.98 or h_done < h_left * 0.98:
            t["aster_qty"], t["hl_qty"] = a_left - a_done, -(h_left - h_done)
            self.bot.save()
            self.tg(f"⚠️ div capture {name}: close incomplete (Aster {a_done:g}/{a_left:g}, HL {h_done:g}/{h_left:g}) "
                    f"— retrying every pass")
            t["close_at"] = time.time()                       # keep trying
            return
        start_ms, end_ms = int(t["opened"] * 1000) - 60_000, int(time.time() * 1000) + 60_000
        inc = await self.aster_income(name, start_ms, end_ms)
        hl_fund = await self.hl_funding(name, start_ms, end_ms)
        a_price = a_done * (a_px - t["aster_px"])
        h_price = h_done * (t["hl_px"] - h_px)
        h_fees = -(h_done * (t["hl_px"] + h_px)) * 0.00009
        a_fund, a_fees = inc.pop("FUNDING_FEE", 0.0), inc.pop("COMMISSION", 0.0)
        other = sum(inc.values())
        net = a_price + h_price + a_fund + a_fees + other + hl_fund + h_fees
        notional = t["aster_qty"] * t["aster_px"]
        row = dict(t, closed=time.time(), why=why, aster_exit=a_px, hl_exit=h_px, aster_price=a_price,
                   hl_price=h_price, aster_funding=a_fund, aster_fees=a_fees, aster_other=inc, hl_funding=hl_fund,
                   hl_fees=h_fees, net=net)
        self.st["done"].append(row)
        del self.st["open"][eid]
        if eid in self.st["offers"]:
            self.st["offers"][eid]["status"] = "closed"
        self.bot.save()
        self.tg(f"🏁 div capture {name} closed ({why}): ${net:+.2f} on ${notional:.0f} a side\n"
                f"Aster: price {a_price:+.2f}, funding {a_fund:+.2f} (expected dividend ≈ "
                f"{t['div'] * notional:+.2f}), fees {a_fees:+.2f}"
                + (f", other {other:+.2f} {inc}" if inc else "")
                + f"\nHL: price {h_price:+.2f}, funding {hl_fund:+.2f}, fees ~{h_fees:+.2f}")

    async def aster_income(self, name: str, start_ms: int, end_ms: int) -> dict:
        """Every income type Aster booked on this symbol (the special dividend funding may show as its own type)."""
        from config import aster_symbol_for
        c = self.bot.client
        sym = aster_symbol_for(name)
        signed = c._sign_aster({"symbol": sym, "startTime": start_ms, "endTime": end_ms, "limit": 1000})
        try:
            async with c.session.get("https://fapi.asterdex.com/fapi/v1/income", params=signed, timeout=c.timeout) as r:
                data = await r.json(content_type=None)
        except Exception as e:
            log.warning(f"aster income: {e}")
            return {}
        out: dict = {}
        for row in data if isinstance(data, list) else []:
            if row.get("symbol") == sym:
                out[row.get("incomeType")] = out.get(row.get("incomeType"), 0.0) + float(row.get("income") or 0)
        return out

    async def hl_funding(self, name: str, start_ms: int, end_ms: int) -> float:
        addr = (getattr(self.bot.client, "api_keys", None) or {}).get("hl_account_address")
        try:
            r = await self.bot.info({"type": "userFunding", "user": addr, "startTime": start_ms, "endTime": end_ms})
            return sum(float(x["delta"]["usdc"]) for x in r if x["delta"]["coin"].split(":")[-1] == name)
        except Exception as e:
            log.warning(f"hl funding: {e}")
            return 0.0

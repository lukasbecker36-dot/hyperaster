"""Offline tests for div_capture.py (inside stock_carry), fake venues only."""
import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import div_capture as dc
import stock_carry as sc
from test_stock_carry import FakeClient, cfg_with, make_bot, sandbox  # noqa: F401  (fixture)


def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


def test_us_times_follow_asters_schedule():
    start, until, close = dc.event_times("QCOM", "2026-12-03")         # Thursday, ET = UTC-5
    assert utc(start) == "2026-12-02 14:45" and utc(until) == "2026-12-03 00:15" and utc(close) == "2026-12-03 15:30"
    start, until, close = dc.event_times("IBM", "2026-11-09")          # Monday: the session before is Friday
    assert utc(start) == "2026-11-06 14:45" and utc(until) == "2026-11-07 00:15"


def test_korean_times_follow_asters_schedule():
    start, until, close = dc.event_times("SKHX", "2026-11-27")         # KST = UTC+9
    assert utc(until) == "2026-11-26 22:15" and utc(close) == "2026-11-27 01:00"


def test_expected_edge():
    assert dc.expected_edge(0.01, 100.0, 100.0) == pytest.approx(0.01 - dc.FEES)
    assert dc.expected_edge(0.019, 102.0, 100.0) == pytest.approx(-0.001 - dc.FEES)


class Venues(FakeClient):
    """FakeClient (HL) plus an Aster book and positions."""

    def __init__(self, hl_mid, aster_mid):
        super().__init__({"QCOM": hl_mid})
        self.aster_mid, self.aster_pos, self.aster_specs = aster_mid, 0.0, {"QCOM": object()}
        self.aster_orders = []

    async def _load_aster_specs(self, names): pass
    async def get_orderbook(self, ex, n):
        m = self.aster_mid if ex == "aster" else self.mids[n]
        return SimpleNamespace(bid=m * 0.9999, ask=m * 1.0001, mid=m)
    def format_aster_qty(self, n, q): return f"{q:.2f}"
    def format_hl_qty(self, n, q): return f"{q:.2f}"
    async def set_aster_margin_type(self, n, t): return True
    async def set_aster_leverage(self, n, lev): return True
    async def place_aster_ioc(self, n, side, qty, price, reduce_only=False):
        self.aster_pos += qty if side == "buy" else -qty
        self.aster_orders.append((side, qty, reduce_only))
        return SimpleNamespace(success=True, filled_qty=qty, fill_price=price, error="")


def setup_bot(tmp, hl_mid=100.0, aster_mid=100.0, pct=0.56):
    (tmp / "exdiv.json").write_text(json.dumps({"valid_until": "2099-01-01", "names": {
        "QCOM": [{"from": "2026-12-03", "to": "2026-12-03", "pct": pct, "src": "announced"}]}}))
    c = Venues(hl_mid, aster_mid)
    bot = make_bot(c, {})
    sent = []
    bot.divcap.tg = sent.append
    return bot, c, sent


IN_WINDOW = datetime(2026, 12, 2, 18, 0, tzinfo=timezone.utc).timestamp()


def test_offer_sent_once_with_buttons(sandbox, monkeypatch):
    tmp, _ = sandbox
    bot, c, sent = setup_bot(tmp)
    offers = []
    monkeypatch.setattr(dc, "tg_buttons", lambda text, b: offers.append((text, b)))
    asyncio.run(bot.divcap.scan(IN_WINDOW))
    asyncio.run(bot.divcap.scan(IN_WINDOW + 600))
    assert len(offers) == 1 and "QCOM ex-dividend 2026-12-03" in offers[0][0]
    assert offers[0][1][0][0][1] == "dc:yes:QCOM-2026-12-03"


def test_no_offer_when_aster_already_prices_the_dividend(sandbox, monkeypatch):
    tmp, _ = sandbox
    bot, c, sent = setup_bot(tmp, aster_mid=100.5)                       # Aster +0.5% vs a 0.56% dividend
    offers = []
    monkeypatch.setattr(dc, "tg_buttons", lambda text, b: offers.append(text))
    asyncio.run(bot.divcap.scan(IN_WINDOW))
    assert offers == []


def test_no_offer_outside_the_window_or_when_carry_holds_it(sandbox, monkeypatch):
    tmp, _ = sandbox
    bot, c, sent = setup_bot(tmp)
    offers = []
    monkeypatch.setattr(dc, "tg_buttons", lambda text, b: offers.append(text))
    asyncio.run(bot.divcap.scan(datetime(2026, 12, 3, 1, 0, tzinfo=timezone.utc).timestamp()))   # reduce-only by then
    bot.st["pos"]["QCOM"] = {"qty": 1.0, "mode": "live"}
    asyncio.run(bot.divcap.scan(IN_WINDOW))
    assert offers == []


def test_yes_opens_both_legs_and_carry_nets_it_out(sandbox, monkeypatch):
    tmp, _ = sandbox
    bot, c, sent = setup_bot(tmp)
    monkeypatch.setattr(dc, "tg_buttons", lambda text, b: None)
    asyncio.run(bot.divcap.scan(IN_WINDOW))
    monkeypatch.setattr(dc.time, "time", lambda: IN_WINDOW + 60)
    asyncio.run(bot.divcap.request("yes", "QCOM-2026-12-03"))
    t = bot.st["divcap"]["open"]["QCOM-2026-12-03"]
    assert c.aster_pos == pytest.approx(1.0) and c.venue["QCOM"] == pytest.approx(-1.0) and t["hl_qty"] == -1.0
    assert any("OPEN" in m for m in sent)
    assert "QCOM" in bot.divcap.names()

    async def ch():
        return {"assetPositions": [{"position": {"coin": "xyz:QCOM", "szi": "-1.0"}}]}
    bot.info = lambda body, pace=False: ch()
    bot.client.api_keys = {"hl_account_address": "0x1"}
    assert asyncio.run(sc.StockCarry.venue_positions(bot)) == {"QCOM": 0.0}   # not stock carry's


def test_declined_and_expired_offers_trade_nothing(sandbox, monkeypatch):
    tmp, _ = sandbox
    bot, c, sent = setup_bot(tmp)
    monkeypatch.setattr(dc, "tg_buttons", lambda text, b: None)
    asyncio.run(bot.divcap.scan(IN_WINDOW))
    asyncio.run(bot.divcap.request("no", "QCOM-2026-12-03"))
    asyncio.run(bot.divcap.request("yes", "QCOM-2026-12-03"))
    assert c.aster_orders == [] and any("already declined" in m for m in sent)


def test_close_reports_aster_income_by_type(sandbox, monkeypatch):
    tmp, _ = sandbox
    bot, c, sent = setup_bot(tmp)
    monkeypatch.setattr(dc, "tg_buttons", lambda text, b: None)
    asyncio.run(bot.divcap.scan(IN_WINDOW))
    monkeypatch.setattr(dc.time, "time", lambda: IN_WINDOW + 60)
    asyncio.run(bot.divcap.request("yes", "QCOM-2026-12-03"))
    c.mids["QCOM"], c.aster_mid = 99.44, 99.44                           # both dropped the dividend

    async def income(name, a, b):
        return {"FUNDING_FEE": 0.56, "COMMISSION": -0.08}
    async def hl_funding(name, a, b):
        return 0.0
    bot.divcap.aster_income, bot.divcap.hl_funding = income, hl_funding
    asyncio.run(bot.divcap.tick(datetime(2026, 12, 3, 15, 31, tzinfo=timezone.utc).timestamp()))
    assert bot.st["divcap"]["open"] == {} and c.aster_pos == pytest.approx(0.0) and c.venue["QCOM"] == pytest.approx(0.0)
    done = bot.st["divcap"]["done"][0]
    assert done["net"] == pytest.approx(0.56 - 0.08 + done["aster_price"] + done["hl_price"] + done["hl_fees"])
    assert any("closed" in m and "funding +0.56" in m for m in sent)


def test_stock_carry_steps_out_ahead_of_an_event(sandbox):
    tmp, _ = sandbox
    bot, c, sent = setup_bot(tmp)
    start = dc.event_times("QCOM", "2026-12-03")[0]
    assert "QCOM" in bot.divcap.names(start - 3 * 86400)
    assert "QCOM" not in bot.divcap.names(start - 5 * 86400)
    assert "QCOM" not in bot.divcap.names(dc.event_times("QCOM", "2026-12-03")[1] + 60)
    small = setup_bot(tmp, pct=0.2)[0]                                       # too small to ever offer
    assert "QCOM" not in small.divcap.names(start)

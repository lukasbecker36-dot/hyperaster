"""Offline tests for stock_carry.py — a fake HL client, no network."""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import stock_carry as sc


# ---- pure helpers
def test_cap_spreads_the_excess_and_falls_back_to_equal_weights():
    w = sc.capped_weights({"A": 10.0, **{c: 1.0 for c in "BCDEFGHIJKLM"}}, 0.10)
    assert w["A"] == pytest.approx(0.10) and sum(w.values()) == pytest.approx(1.0)
    assert w["B"] == pytest.approx(0.90 / 12)
    assert sc.capped_weights({"A": 5.0, "B": 1.0}, 0.10) == {"A": 0.5, "B": 0.5}


def test_long_short_shorts_high_funding_and_caps():
    sig = {c: i for i, c in enumerate("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn")}     # higher = long
    vol = {c: 0.01 for c in sig} | {"n": 0.0001}
    w = sc.long_short(sig, vol, 0.3, 1000.0, cap=0.10)
    assert w["n"] == pytest.approx(100.0) and sum(v for v in w.values() if v > 0) == pytest.approx(1000.0)
    assert all(w[c] < 0 for c in "ABCDEFGHIJKL") and "M" not in w


def test_book_target_averages_last_cohorts():
    t = sc.book_target([{"A": 300.0}, {"A": 300.0, "B": -300.0}, {"B": -300.0}, {"C": 300.0}], 3)
    assert t == pytest.approx({"A": 100.0, "B": -200.0, "C": 100.0})


def test_liq_price_at_4x():
    assert sc.liq_price(1.0, 100.0, 4) == pytest.approx(80.0)
    assert sc.liq_price(-1.0, 100.0, 4) == pytest.approx(120.0)


def test_rebalance_is_due_once_a_day_after_the_time():
    cfg = dict(sc.DEFAULT_CONFIG)
    st = {"last_rebalance": ""}
    assert not sc.due(cfg, st, datetime(2026, 11, 19, 0, 4, tzinfo=timezone.utc))
    assert sc.due(cfg, st, datetime(2026, 11, 19, 0, 5, tzinfo=timezone.utc))
    st["last_rebalance"] = "2026-11-19"
    assert not sc.due(cfg, st, datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc))


# ---- the trader against a fake venue
class FakeClient:
    def __init__(self, mids, venue=None, withdrawable=10_000.0):
        self.mids, self.venue = dict(mids), dict(venue or {})
        self._hl_asset_index, self.orders, self.lev = {}, [], []
        self.withdrawable = withdrawable

    async def ensure_session(self): pass
    async def _load_hl_xyz_specs(self, names):
        for n in names:
            self._hl_asset_index[n] = 110000
    async def _infer_hl_tick_sizes(self, names): pass
    async def get_orderbook(self, ex, n):
        m = self.mids[n]
        return SimpleNamespace(bid=m * 0.9999, ask=m * 1.0001, mid=m)
    def format_hl_qty(self, n, q): return f"{q:.4f}"
    async def get_hl_position(self, n): return {"szi": str(self.venue.get(n, 0.0)), "entryPx": str(self.mids[n])}
    async def set_hl_leverage(self, n, lev, cross=True): self.lev.append((n, lev, cross)); return True
    async def place_hl_ioc(self, n, side, qty, ref, buffer_bps=None):
        self.venue[n] = self.venue.get(n, 0.0) + (qty if side == "buy" else -qty)
        self.orders.append((n, side, qty))
        return SimpleNamespace(success=True, filled_qty=qty, fill_price=ref, ambiguous=False, error="")
    async def get_hl_balance(self, dex="xyz"): return {"withdrawable": self.withdrawable}
    async def add_hl_isolated_margin(self, n, usd):
        self.margin_adds = getattr(self, "margin_adds", []) + [(n, usd)]
        return True


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    for k, f in (("CONFIG_FILE", "cfg.json"), ("STATE_FILE", "state.json"), ("TRADES_FILE", "trades.csv"),
                 ("CMD_DIR", "cmd"), ("SF_CONFIG", "sf.json"), ("EXDIV_FILE", "exdiv.json")):
        monkeypatch.setattr(sc, k, tmp_path / f)
    sent = []
    monkeypatch.setattr(sc, "tg", lambda text: sent.append(text))
    monkeypatch.setattr(sc, "arb_holds", lambda n: False)
    return tmp_path, sent


def make_bot(client, cohort):
    bot = sc.StockCarry(client)
    snap = {n: {"mid": m, "vol": 1e6, "funding": 0.00001} for n, m in client.mids.items()}

    async def snapshot():
        return snap

    async def build_cohort(cfg, s, now):
        return dict(cohort), {n: 0.1 for n in cohort}

    async def venue_positions():
        if getattr(client, "venue_fail", False):
            raise RuntimeError("bad clearinghouseState reply: None")      # what a 429 looked like
        return {n: q for n, q in client.venue.items() if q}

    bot.snapshot, bot.build_cohort, bot.venue_positions = snapshot, build_cohort, venue_positions
    bot.only_isolated = {n: True for n in client.mids}
    bot.st["vol"] = {n: 0.002 for n in client.mids}       # blue-chip vol: the full 4x

    async def clearinghouse():
        return getattr(client, "ch", {})
    bot.clearinghouse = clearinghouse
    return bot


def cfg_with(tmp, **kw):
    cfg = dict(sc.DEFAULT_CONFIG, **kw)
    (tmp / "cfg.json").write_text(json.dumps(cfg))
    return sc.load_config()


def test_paper_rebalance_builds_a_third_of_the_book(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp)
    c = FakeClient({"A": 10.0, "B": 20.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    msg = asyncio.run(bot.rebalance(cfg))
    assert bot.st["pos"]["A"]["qty"] == pytest.approx(10.0) and bot.st["pos"]["B"]["qty"] == pytest.approx(-5.0)
    assert c.orders == [] and "PAPER daily rebalance: 2 trades" in msg      # paper never touches the venue


def test_live_sets_isolated_leverage_and_trades(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    asyncio.run(bot.rebalance(cfg))
    assert sorted(c.lev) == [("A", 4, False), ("B", 4, False)]
    assert c.venue == pytest.approx({"A": 10.0, "B": -5.0})
    assert bot.st["pos"]["A"]["mode"] == "live"


def test_live_short_of_margin_scales_adds_and_reduces_in_full(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, venue={"B": -15.0}, withdrawable=10.0)
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["pos"]["B"] = {"qty": -15.0, "avg": 20.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "ok", "mode": "live"}
    asyncio.run(bot.rebalance(cfg))
    # A's $100 add needs $25 margin (x1.1 spare) against $10 free: scaled to 10/27.5 = 36%
    assert c.venue["A"] == pytest.approx(100 * 10 / 27.5 / 10, rel=0.01)
    assert c.venue["B"] == pytest.approx(-5.0)                  # the reduction went through in full
    assert any("scaled to 36%" in s for s in sent)


def test_scale_add_shrinks_only_the_adding_part():
    assert sc.scale_add(100.0, 40.0, 0.5) == pytest.approx(70.0)     # growth 60 -> 30
    assert sc.scale_add(-100.0, 0.0, 0.5) == pytest.approx(-50.0)    # new position
    assert sc.scale_add(-100.0, 40.0, 0.5) == pytest.approx(-50.0)   # flip: the new side scaled
    assert sc.scale_add(20.0, 40.0, 0.5) == 20.0                     # reduction untouched
    assert sc.scale_add(100.0, 40.0, 1.0) == 100.0


def test_cohort_is_committed_only_when_the_rebalance_trades(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0})
    c.venue_fail = True
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    asyncio.run(bot.rebalance(cfg))
    assert not bot.last_ok and bot.st["cohorts"] == [] and bot.st["cohort_days"] == []
    c.venue_fail = False
    asyncio.run(bot.rebalance(cfg))
    assert bot.last_ok and len(bot.st["cohorts"]) == 1


def test_second_rebalance_same_day_replaces_that_days_cohort(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp)
    c = FakeClient({"A": 10.0, "B": 20.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["cohorts"], bot.st["cohort_days"] = [{"A": 30.0}, {"A": 30.0}], ["2026-11-19", "2026-11-20"]
    asyncio.run(bot.rebalance(cfg))
    asyncio.run(bot.rebalance(cfg))
    assert len(bot.st["cohorts"]) == 3 and bot.st["cohort_days"][:2] == ["2026-11-19", "2026-11-20"]
    assert sc.with_cohort([{"x": 1}, {"y": 1}], ["d1", "d2"], {"z": 1}, "d2", 3) == ([{"x": 1}, {"z": 1}], ["d1", "d2"])


def test_cohort_scored_from_too_few_names_is_not_traded(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    real = bot.build_cohort

    async def flaky(cfg_, snap, now):
        out = await real(cfg_, snap, now)
        bot.skipped, bot.scored_of = ["X", "Y"], 10                 # HL refused 2 of 10 names
        return out

    bot.build_cohort = flaky
    msg = asyncio.run(bot.rebalance(cfg))
    assert "only scored 8 of 10" in msg and not bot.last_ok and c.orders == [] and bot.st["cohorts"] == []


def test_failed_daily_rebalance_retries_three_times(sandbox, monkeypatch):
    tmp, sent = sandbox
    cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0})
    c.venue_fail = True
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    now = [datetime(2026, 11, 23, 0, 6, tzinfo=timezone.utc)]

    class FD(datetime):
        @classmethod
        def now(cls, tz=None):
            return now[0]

    monkeypatch.setattr(sc, "datetime", FD)
    for i in range(4):
        asyncio.run(bot.step())
        now[0] = now[0] + timedelta(minutes=16)
    tries = [s for s in sent if "couldn't read the venue" in s]
    assert len(tries) == 3 and "try 1 of 3" in tries[0] and "gave up" in tries[2]
    assert bot.st["last_rebalance"] == "2026-11-23"


def test_kill_switch_pauses_once_and_rearms_after_resume(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, side_usd=500.0)                         # limit: 10% of the $1,000 book = $100
    bot = sc.StockCarry(FakeClient({"A": 10.0}))
    assert bot.kill_check(cfg, 20.0) is None and bot.st["pnl_peak"] == 20.0
    assert bot.kill_check(cfg, -70.0) is None                   # $90 off the peak
    msg = bot.kill_check(cfg, -81.0)
    assert msg and "PAUSED" in msg and json.loads((tmp / "cfg.json").read_text())["paused"] is True
    assert bot.kill_check(cfg, -200.0) is None                  # alerts once
    cfg = cfg_with(tmp, side_usd=500.0, paused=False)           # /sc resume
    assert bot.kill_check(cfg, -200.0) is None and bot.st["pnl_peak"] == -200.0 and not bot.st["killed"]


def test_unified_account_counts_spot_usdc(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, withdrawable=0.0)          # unified: the xyz dex shows $0
    c.api_keys = {"hl_account_address": "0xabc"}
    bot = make_bot(c, {"A": 300.0, "B": -300.0})

    async def info(body, pace=False):
        assert body == {"type": "spotClearinghouseState", "user": "0xabc"}
        return {"balances": [{"coin": "USDC", "total": "480.98", "hold": "0.0"}, {"coin": "HYPE", "total": "1"}]}

    bot.info = info
    assert asyncio.run(bot.free_margin()) == pytest.approx(480.98)
    asyncio.run(bot.rebalance(cfg))
    assert c.venue == pytest.approx({"A": 10.0, "B": -5.0}) and not any("only reductions" in s for s in sent)


def test_unified_margin_in_use_is_counted_once(sandbox):
    """19 Nov live: spot USDC 480.67 with 178.12 on hold, and the xyz dex reporting the same 178.12 as margin
    used. Free is 302.55, not 480.67 - 178.12 - 178.12."""
    tmp, sent = sandbox
    c = FakeClient({"A": 10.0}, withdrawable=0.0)
    c.api_keys = {"hl_account_address": "0xabc"}

    async def bal(dex="xyz"):
        return {"withdrawable": 0.0, "margin_used": 178.12 if dex == "xyz" else 0.0}

    c.get_hl_balance = bal
    bot = make_bot(c, {})

    async def info(body, pace=False):
        return {"balances": [{"coin": "USDC", "total": "480.67", "hold": "178.12"}]}

    bot.info = info
    assert asyncio.run(bot.free_margin()) == pytest.approx(302.55)


def test_venue_mismatch_parks_the_name(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, venue={"A": 0.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["pos"]["A"] = {"qty": 20.0, "avg": 10.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "ok", "mode": "live"}     # we think we're long; the venue is flat (liquidated?)
    asyncio.run(bot.rebalance(cfg))
    assert bot.st["pos"]["A"]["status"].startswith("venue") and not any(o[0] == "A" for o in c.orders)
    assert any("parked" in s for s in sent)


def test_failed_venue_read_trades_nothing_and_parks_nothing(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, venue={"A": 20.0})
    c.venue_fail = True
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["pos"]["A"] = {"qty": 20.0, "avg": 10.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "ok", "mode": "live"}
    msg = asyncio.run(bot.rebalance(cfg))
    assert "couldn't read the venue" in msg and c.orders == [] and bot.st["pos"]["A"]["status"] == "ok"


def test_parked_name_unparks_when_the_venue_matches_again(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, venue={"A": 0.047})
    bot = make_bot(c, {"A": 0.47, "B": -300.0})                # A's target ~= what it holds
    bot.st["pos"]["A"] = {"qty": 0.047, "avg": 10.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "venue +0 != state +0.047", "mode": "live"}   # the false 20 Nov park
    asyncio.run(bot.rebalance(cfg))
    assert bot.st["pos"]["A"]["status"] == "ok" and any("un-parked" in s for s in sent)


def test_rebalance_loads_every_name_in_one_market_list_read(sandbox, monkeypatch):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0, "C": 30.0})
    loads = []
    orig = c._load_hl_xyz_specs

    async def counted(names):
        loads.append(list(names))
        await orig(names)

    c._load_hl_xyz_specs = counted
    monkeypatch.setattr(sc.asyncio, "sleep", lambda s, _real=asyncio.sleep: _real(0))
    bot = make_bot(c, {"A": 300.0, "B": -300.0, "C": 300.0})
    asyncio.run(bot.rebalance(cfg))
    assert loads == [["A", "B", "C"]] and len(c.orders) == 3


def test_leverage_refusal_is_retried_and_never_parks(sandbox, monkeypatch):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0})
    monkeypatch.setattr(sc.asyncio, "sleep", lambda s, _real=asyncio.sleep: _real(0))
    calls = []

    async def refuse_a(n, lev, cross=True):
        calls.append(n)
        return n != "A"

    c.set_hl_leverage = refuse_a
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["pos"]["B"] = {"qty": 0.0, "avg": 0.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "could not set 4x", "mode": "live"}           # parked by the old code
    msg = asyncio.run(bot.rebalance(cfg))
    assert calls.count("A") == 2 and "A" not in c.venue                         # retried once, not traded
    assert bot.st["pos"]["A"]["status"] == "ok" and "retried next rebalance" in msg
    assert c.venue["B"] == pytest.approx(-5.0)                                  # un-parked and traded
    async def accept(n, lev, cross=True):
        return True

    c.set_hl_leverage = accept
    bot.st["last_rebalance"] = ""
    asyncio.run(bot.rebalance(cfg))
    assert c.venue["A"] > 0                                                     # traded the next time


def test_session_flip_names_are_never_traded(sandbox):
    tmp, sent = sandbox
    (tmp / "sf.json").write_text(json.dumps({"enabled": True, "names": ["MINIMAX", "zhipu"]}))
    assert sc.sf_names() == {"MINIMAX", "ZHIPU"}


def test_paper_liquidation_loses_the_margin(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp)
    c = FakeClient({"A": 10.0, "B": 20.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    asyncio.run(bot.rebalance(cfg))
    c.mids["A"] = 7.9                                           # -21%: through 4x's -20%
    bot.snapshot = (lambda s: (lambda: asyncio.sleep(0, result=s)))(
        {n: {"mid": m, "vol": 1e6, "funding": 0.0} for n, m in c.mids.items()})
    asyncio.run(bot.paper_hourly(cfg))
    assert bot.st["pos"]["A"]["qty"] == 0.0 and bot.st["liqs"][0]["name"] == "A"
    assert bot.st["liqs"][0]["loss"] == pytest.approx(-10.0 * 10.0001 / 4, rel=1e-3)   # 1/4 of $100 at entry
    rows = sc.read_trades()
    assert rows[-1]["reason"] == "liquidated 4x"
    rep = sc.replay_trades(rows, "paper")
    assert rep["names"]["A"]["realised"] == pytest.approx(bot.st["liqs"][0]["loss"], rel=1e-3)


def test_flatten_request_closes_everything(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp)
    c = FakeClient({"A": 10.0, "B": 20.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    asyncio.run(bot.rebalance(cfg))
    sc.CMD_DIR.mkdir(parents=True, exist_ok=True)
    (sc.CMD_DIR / "1.json").write_text(json.dumps({"action": "flatten"}))
    bot.st["last_rebalance"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    asyncio.run(bot.step())
    assert all(not p["qty"] for p in bot.st["pos"].values()) and bot.st["cohorts"] == []


# ---- leverage per name, margin top-up, live liquidations (23 Nov: NBIS/ZHIPU/KORU/SHAZ liquidated at a flat 4x)
def test_lev_for_scales_leverage_down_with_vol():
    assert sc.lev_for(0.002, 3, 4, 3.0) == 4                 # AAPL-like: 4x
    assert sc.lev_for(0.07 / 24 ** 0.5, 3, 4, 3.0) == 2      # NBIS-like 7%/day: 2x
    assert sc.lev_for(0.03, 3, 4, 3.0) == 1                  # wilder: 1x
    assert sc.lev_for(None, 3, 4, 3.0) == 2                  # unknown vol: cautious
    assert sc.lev_for(0.0001, 3, 3, 3.0) == 3                # never above the configured max


def test_live_rebalance_sets_each_names_leverage_and_sizes_margin_by_it(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["vol"]["B"] = 0.07 / 24 ** 0.5
    assert bot.margin_needed({"A": 100.0, "B": -100.0}, {"A": {"mid": 10.0}, "B": {"mid": 20.0}}, cfg) \
        == pytest.approx(100 / 4 + 100 / 2)
    msg = asyncio.run(bot.rebalance(cfg))
    assert sorted(c.lev) == [("A", 4, False), ("B", 2, False)]
    assert "4x 1, 2x 1" in msg


def test_open_position_whose_leverage_wont_change_still_trades(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, venue={"B": -2.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["pos"]["B"] = {"qty": -2.0, "avg": 20.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "ok", "mode": "live"}

    async def refuse(n, lev, cross=True):
        return n != "B"
    c.set_hl_leverage = refuse
    asyncio.run(bot.rebalance(cfg))
    assert c.venue["B"] == pytest.approx(-5.0)


def test_top_up_brings_margin_to_notional_over_leverage(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, venue={"B": -5.0})
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["vol"]["B"] = 0.07 / 24 ** 0.5                     # 2x
    bot.st["pos"]["B"] = {"qty": -5.0, "avg": 20.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "ok", "mode": "live"}
    c.ch = {"B": {"positionValue": "100", "marginUsed": "25", "leverage": {"type": "isolated", "value": 4}}}
    notes = asyncio.run(bot.top_up_margin(cfg))
    assert c.margin_adds == [("B", 25.0)] and "B +$25.00" in notes[0]
    c.ch["B"]["marginUsed"] = "50"
    assert asyncio.run(bot.top_up_margin(cfg)) == []           # already there: nothing sent


def test_hourly_books_a_live_liquidation_and_does_not_park(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 24.0}, venue={})          # HL closed B
    bot = make_bot(c, {})
    bot.st["pos"]["B"] = {"qty": -5.0, "avg": 20.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "ok", "mode": "live"}
    fills = [{"coin": "xyz:B", "side": "B", "sz": "5", "px": "24", "fee": "0.01", "time": 1795000000000,
              "closedPnl": "-20", "liquidation": {"markPx": "24", "method": "market"}},
             {"coin": "xyz:A", "side": "B", "sz": "1", "px": "10", "fee": "0", "time": 1795000001000}]

    async def info(body, pace=False):
        assert body["type"] == "userFillsByTime"
        return fills
    bot.info = info
    bot.st["liq_seen_ms"] = 1794999999000
    asyncio.run(bot.live_hourly(cfg))
    p = bot.st["pos"]["B"]
    assert p["qty"] == 0.0 and p["realised"] == pytest.approx(-20.0) and p["status"] == "ok"
    assert bot.st["liqs"][-1] == {"ts": 1795000000, "name": "B", "loss": -20.01, "mode": "live"}
    assert any("liquidated by HL" in m for m in sent) and not any("parked" in m for m in sent)
    assert sc.read_trades()[-1]["reason"] == "liquidated on HL"
    assert bot.st["liq_seen_ms"] == 1795000001000
    sent.clear()
    asyncio.run(bot.live_hourly(cfg))                         # nothing open any more: no second booking
    assert sent == []


# ---- ex-dividend dates
def write_cal(tmp, names, valid_until="2099-01-01"):
    (tmp / "exdiv.json").write_text(json.dumps({"valid_until": valid_until, "names": names}))


def test_long_short_passes_over_no_long_names_but_still_shorts_them():
    sig = {c: i for i, c in enumerate("ABCDEFGHIJ")}                 # J highest -> long
    vol = {c: 0.01 for c in sig}
    w = sc.long_short(sig, vol, 0.3, 300.0, no_long={"J", "A"})
    assert "J" not in w and {c for c in w if w[c] > 0} == {"I", "H", "G"}
    assert w["A"] < 0                                                # a short through an ex-date gains the dividend


def test_exdiv_hits_overlap_and_threshold():
    cal = {"names": {"X": [{"from": "2026-12-12", "to": "2026-12-20", "pct": 3.5}],
                     "S": [{"from": "2026-12-13", "to": "2026-12-13", "pct": 0.1}]}}
    assert set(sc.exdiv_hits(cal, "2026-12-09", "2026-12-12", 0.3)) == {"X"}
    assert sc.exdiv_hits(cal, "2026-12-21", "2026-12-24", 0.3) == {}
    assert sc.exdiv_hits(cal, "2026-12-13", "2026-12-13", 0.3).keys() == {"X"}      # S is too small


def test_older_cohorts_long_is_closed_before_its_ex_date(sandbox):
    tmp, sent = sandbox
    today = datetime.now(timezone.utc)
    write_cal(tmp, {"A": [{"from": (today + timedelta(days=1)).strftime("%Y-%m-%d"),
                           "to": (today + timedelta(days=1)).strftime("%Y-%m-%d"), "pct": 2.0}]})
    cfg = cfg_with(tmp)
    c = FakeClient({"A": 10.0, "B": 20.0, "C": 5.0})
    bot = make_bot(c, {"C": 300.0, "B": -300.0})
    bot.st["cohorts"] = [{"A": 300.0, "B": -300.0}]
    bot.st["cohort_days"] = ["2000-01-01"]
    bot.st["pos"]["A"] = {"mode": "paper", "qty": 10.0, "avg": 10.0, "realised": 0.0, "fees": 0.0, "funding": 0.0, "status": "ok"}
    msg = asyncio.run(bot.rebalance(cfg))
    assert bot.st["pos"]["A"]["qty"] == pytest.approx(0.0)
    assert "A long closed" in msg and bot.st["pos"]["C"]["qty"] > 0


def test_short_is_kept_through_an_ex_date(sandbox):
    tmp, sent = sandbox
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    write_cal(tmp, {"B": [{"from": today, "to": today, "pct": 2.0}]})
    cfg = cfg_with(tmp)
    bot = make_bot(FakeClient({"A": 10.0, "B": 20.0}), {"A": 300.0, "B": -300.0})
    asyncio.run(bot.rebalance(cfg))
    assert bot.st["pos"]["B"]["qty"] == pytest.approx(-5.0)


def test_cohort_skips_a_long_with_an_ex_date_inside_its_hold(sandbox):
    tmp, sent = sandbox
    now = datetime(2026, 12, 10, 0, 5, tzinfo=timezone.utc)
    write_cal(tmp, {"N": [{"from": "2026-12-13", "to": "2026-12-13", "pct": 3.0}],     # day 3: inside the hold
                    "M": [{"from": "2026-12-14", "to": "2026-12-14", "pct": 3.0}]})    # day 4: after it
    cfg = cfg_with(tmp, q=0.3, min_age_d=1)
    names = list("ABCDEFGHIJ") + ["M", "N"]
    rank = {n: i for i, n in enumerate(names)}               # N lowest funding -> first long pick, then M
    bot = sc.StockCarry(FakeClient({n: 10.0 for n in names}))

    async def info(body, pace=False):
        if body["type"] == "candleSnapshot":
            return [{"t": int(now.timestamp() * 1000) - 2 * 86400_000 + h * 3600_000, "c": 10 + (h % 2) * 0.01} for h in range(60)]
        return [{"fundingRate": str(-rank[body["coin"].split(":")[1]] * 1e-5)}]
    bot.info = info
    snap = {n: {"mid": 10.0, "vol": 1e6, "funding": 0.0} for n in names}
    cohort, _ = asyncio.run(bot.build_cohort(cfg, snap, now.timestamp()))
    longs = {n for n, v in cohort.items() if v > 0}
    assert "N" not in longs and "M" in longs and len(longs) == 3
    assert set(bot.exdiv_passed) == {"N"}


def test_shipped_calendar_parses():
    cal = sc.load_exdiv(sc.BASE_DIR / "exdiv.json")
    assert cal["valid_until"] >= "2027-01-01" and cal["names"]
    for n, ents in cal["names"].items():
        for e in ents:
            assert e["from"] <= e["to"] and float(e["pct"]) > 0, n

"""Offline tests for stock_carry.py — a fake HL client, no network."""
import asyncio
import json
from datetime import datetime, timezone
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


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    for k, f in (("CONFIG_FILE", "cfg.json"), ("STATE_FILE", "state.json"), ("TRADES_FILE", "trades.csv"),
                 ("CMD_DIR", "cmd"), ("SF_CONFIG", "sf.json")):
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


def test_live_short_of_margin_only_reduces(sandbox):
    tmp, sent = sandbox
    cfg = cfg_with(tmp, mode="live")
    c = FakeClient({"A": 10.0, "B": 20.0}, venue={"B": -15.0}, withdrawable=10.0)
    bot = make_bot(c, {"A": 300.0, "B": -300.0})
    bot.st["pos"]["B"] = {"qty": -15.0, "avg": 20.0, "realised": 0.0, "fees": 0.0, "funding": 0.0,
                          "status": "ok", "mode": "live"}
    asyncio.run(bot.rebalance(cfg))
    assert "A" not in c.venue                                   # the add was skipped
    assert c.venue["B"] == pytest.approx(-5.0)                  # the reduction went through
    assert any("only reductions" in s for s in sent)


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

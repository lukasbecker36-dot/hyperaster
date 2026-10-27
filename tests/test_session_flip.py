"""Offline tests for session_flip.py — a fake HL client, no network."""
import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import session_flip as sf


def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


# ---- schedule
def test_korea_long_overnight_short_in_session():
    assert sf.target_side("SKHX", utc(2026, 10, 27, 3, 0)) == -1      # Tue 03:00 in session
    assert sf.target_side("SKHX", utc(2026, 10, 27, 7, 0)) == +1      # close at 07:00
    assert sf.target_side("SKHX", utc(2026, 10, 27, 23, 59)) == +1


def test_weekend_holds_overnight_side():
    assert sf.target_side("SKHX", utc(2026, 10, 31, 3, 0)) == +1      # Saturday
    assert sf.target_side("ZHIPU", utc(2026, 11, 1, 2, 0)) == +1      # Sunday


def test_shanghai_is_reversed_and_hk_hours():
    assert sf.target_side("UNITREE", utc(2026, 10, 27, 2, 0)) == +1
    assert sf.target_side("UNITREE", utc(2026, 10, 27, 7, 30)) == -1
    assert sf.target_side("ZHIPU", utc(2026, 10, 27, 0, 30)) == +1     # HK not open yet
    assert sf.target_side("ZHIPU", utc(2026, 10, 27, 7, 30)) == -1


def test_next_boundary_skips_weekend():
    b = sf.next_boundary("SKHX", utc(2026, 10, 30, 8, 0))              # Fri after close
    assert b == utc(2026, 11, 2, 0, 0)                                 # Mon open


# ---- accounting
def test_apply_fill_flip_through_zero():
    p = {}
    sf.apply_fill(p, +2, 100.0, 0.0)
    sf.apply_fill(p, -4, 110.0, 0.0)                                   # sell 4: close 2 (+20), open short 2
    assert p["realised"] == pytest.approx(20.0)
    assert p["qty"] == -2 and p["avg"] == 110.0
    sf.apply_fill(p, +2, 105.0, 0.0)                                   # cover: +10
    assert p["realised"] == pytest.approx(30.0) and p["qty"] == 0


# ---- fake venue
class FakeClient:
    def __init__(self, mid=100.0, venue=None, fill_frac=1.0):
        self.mid, self.venue, self.fill_frac = mid, dict(venue or {}), fill_frac
        self._hl_asset_index = {}
        self.orders, self.lev = [], []

    async def ensure_session(self): pass
    async def _load_hl_xyz_specs(self, names):
        for n in names:
            self._hl_asset_index[n] = 110000
    async def _infer_hl_tick_sizes(self, names): pass
    async def get_orderbook(self, ex, n):
        return SimpleNamespace(bid=self.mid - 0.05, ask=self.mid + 0.05, mid=self.mid)
    def format_hl_qty(self, n, q): return f"{q:.3f}"
    async def get_hl_position(self, n): return {"szi": str(self.venue.get(n, 0.0))}
    lev_ok = True
    async def set_hl_leverage(self, n, lev, cross=True): self.lev.append((n, lev, cross)); return self.lev_ok
    async def place_hl_ioc(self, n, side, qty, ref, buffer_bps=None):
        q = qty * self.fill_frac
        self.venue[n] = self.venue.get(n, 0.0) + (q if side == "buy" else -q)
        self.orders.append((n, side, qty))
        return SimpleNamespace(success=True, filled_qty=q, fill_price=ref, ambiguous=False, error="")
    async def refresh_hl_oracles(self, names): pass
    def get_hl_oracle(self, n): return self.mid
    def get_hl_funding_rate(self, n): return 0.0001


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(sf, "CONFIG_FILE", tmp_path / "cfg.json")
    monkeypatch.setattr(sf, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(sf, "TRADES_FILE", tmp_path / "trades.csv")
    monkeypatch.setattr(sf, "CMD_DIR", tmp_path / "cmd")
    monkeypatch.setattr(sf, "tg", lambda text: sent.append(text))
    monkeypatch.setattr(sf, "arb_holds", lambda n: False)
    sent = []
    return tmp_path, sent


def make_bot(c, isolated=("EWY", "MINIMAX", "ZHIPU")):
    bot = sf.SessionFlip(c)
    bot.only_isolated = {n: n in isolated for n in sf.NAME_GROUP}   # skip the meta read
    return bot


def write_cfg(tmp, **kw):
    cfg = dict(sf.DEFAULT_CONFIG, **kw)
    (tmp / "cfg.json").write_text(json.dumps(cfg))


def freeze(monkeypatch, when):
    class FD(datetime):
        @classmethod
        def now(cls, tz=None): return when
    monkeypatch.setattr(sf, "datetime", FD)


def test_paper_joins_side_and_flips(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["SKHX"], notional=1000)
    c = FakeClient()
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))                      # in session -> short
    asyncio.run(bot.step())
    assert bot.st["pos"]["SKHX"]["qty"] == pytest.approx(-10.0)
    assert c.orders == []                                              # paper never touches the venue
    freeze(monkeypatch, utc(2026, 10, 27, 6, 59, 30))                  # 60s lead crosses 07:00 -> long
    asyncio.run(bot.step())
    assert bot.st["pos"]["SKHX"]["qty"] == pytest.approx(10.0)
    assert "LONG" in sent[-1]


def test_live_flip_trades_double_and_sets_leverage(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["SKHX"], notional=1000, mode="live", leverage=3)
    c = FakeClient()
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())
    freeze(monkeypatch, utc(2026, 10, 27, 7, 0))
    asyncio.run(bot.step())
    assert c.orders == [("SKHX", "sell", pytest.approx(10.0)), ("SKHX", "buy", pytest.approx(20.0))]
    assert c.venue["SKHX"] == pytest.approx(10.0) and c.lev == [("SKHX", 3, True)]   # SKHX allows cross


def test_live_parks_on_foreign_position(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["SKHX"], mode="live")
    c = FakeClient(venue={"SKHX": 3.0})                                 # someone else's position
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())
    assert c.orders == [] and "PARKED" in sent[-1]
    asyncio.run(bot.step())                                            # stays parked, no orders
    assert c.orders == []


def test_live_skips_when_arb_holds(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["SKHX"], mode="live")
    monkeypatch.setattr(sf, "arb_holds", lambda n: True)
    c = FakeClient()
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())
    assert c.orders == [] and "arb bot" in sent[-1]


def test_switch_to_live_closes_paper_book_first(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["SKHX"], notional=1000)
    c = FakeClient()
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())                                            # paper short
    write_cfg(tmp, names=["SKHX"], notional=1000, mode="live")
    asyncio.run(bot.step())
    p = bot.st["pos"]["SKHX"]
    assert p["mode"] == "live" and p["qty"] == pytest.approx(-10.0)
    assert c.orders == [("SKHX", "sell", pytest.approx(10.0))]         # only the live entry hit the venue


def test_disable_flattens_live(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["SKHX"], notional=1000, mode="live")
    c = FakeClient()
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())
    write_cfg(tmp, names=["SKHX"], notional=1000, mode="live", enabled=False)
    asyncio.run(bot.step())
    assert c.venue["SKHX"] == pytest.approx(0.0) and bot.st["pos"]["SKHX"]["qty"] == 0


def test_partial_fill_is_retried_next_step(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["SKHX"], notional=1000, mode="live")
    c = FakeClient(fill_frac=0.5)
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())                                            # 3 IOCs at 50%: 87.5% filled
    assert bot.st["pos"]["SKHX"]["status"].startswith("partial")
    c.fill_frac = 1.0
    asyncio.run(bot.step())
    assert bot.st["pos"]["SKHX"]["qty"] == pytest.approx(-10.0, rel=1e-3)
    assert bot.st["pos"]["SKHX"]["status"] == "ok"


def test_arb_holds_reads_live_rows_only(tmp_path, monkeypatch):
    db = tmp_path / "p.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE positions (symbol TEXT, status TEXT, paper INT)")
    con.executemany("INSERT INTO positions VALUES (?,?,?)",
                    [("SKHX", "open", 1), ("EWY", "closed", 0), ("SMSN", "error", 0), ("ZHIPU", "exiting", 0)])
    con.commit(); con.close()
    import config
    monkeypatch.setattr(config, "DB_PATH", str(db))
    assert not sf.arb_holds("SKHX") and not sf.arb_holds("EWY")      # paper / closed
    assert not sf.arb_holds("SMSN") and sf.arb_holds("ZHIPU")         # stale error row / live exit


def test_flip_reserve_only_when_live(tmp_path, monkeypatch):
    import flip_reserve as fr
    f = tmp_path / "sessionflip.json"
    monkeypatch.setattr(fr, "_FILE", f)
    f.write_text(json.dumps({"enabled": True, "mode": "paper", "names": ["SKHX"]}))
    monkeypatch.setattr(fr, "_cache", (0.0, frozenset()))
    assert fr.reserved_symbols() == frozenset()
    f.write_text(json.dumps({"enabled": True, "mode": "live", "names": ["skhx"]}))
    monkeypatch.setattr(fr, "_cache", (0.0, frozenset()))
    assert fr.reserved_symbols() == {"SKHX"}


def test_isolated_names_get_isolated_and_per_name_size(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["ZHIPU"], mode="live", notional=1000, notional_by_name={"ZHIPU": 500})
    c = FakeClient()
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())
    assert c.lev == [("ZHIPU", 2, False)]                              # default 2x override, isolated
    assert c.venue["ZHIPU"] == pytest.approx(-5.0)                     # $500 / 100


def test_refused_leverage_blocks_the_trade(sandbox, monkeypatch):
    tmp, sent = sandbox
    write_cfg(tmp, names=["EWY"], mode="live")
    c = FakeClient()
    c.lev_ok = False
    bot = make_bot(c)
    freeze(monkeypatch, utc(2026, 10, 27, 3, 0))
    asyncio.run(bot.step())
    assert c.orders == [] and "NOT TRADED" in sent[-1]
    asyncio.run(bot.step())                                            # parked until /sf reset
    assert c.orders == []


def test_replay_trades_separates_modes_and_counts_legs():
    rows = [
        {"ts": "2026-10-27T01:22:00+00:00", "mode": "paper", "name": "SKHX", "side": "sell", "qty": "1", "px": "100", "fee": "0.01"},
        {"ts": "2026-10-27T01:38:00+00:00", "mode": "paper", "name": "SKHX", "side": "buy", "qty": "1", "px": "101", "fee": "0.01"},
        {"ts": "2026-10-27T01:42:00+00:00", "mode": "live", "name": "SKHX", "side": "sell", "qty": "1", "px": "105", "fee": "0.02"},
        {"ts": "2026-10-27T06:59:00+00:00", "mode": "live", "name": "SKHX", "side": "buy", "qty": "2", "px": "100", "fee": "0.04"},
        {"ts": "2026-10-27T07:59:00+00:00", "mode": "live", "name": "EWY", "side": "buy", "qty": "1", "px": "50", "fee": "0.01"},
    ]
    live = sf.replay_trades(rows, "live")
    assert live["names"]["SKHX"]["realised"] == pytest.approx(5.0)       # short 105 -> covered 100
    assert live["names"]["SKHX"]["qty"] == pytest.approx(1.0)            # flipped long 1
    assert live["total"]["legs"] == 1 and live["total"]["wins"] == 1
    assert live["total"]["fees"] == pytest.approx(0.07)
    assert live["total"]["first"].startswith("2026-10-27T01:42")
    paper = sf.replay_trades(rows, "paper")
    assert paper["total"]["realised"] == pytest.approx(-1.0) and paper["total"]["wins"] == 0


def test_retimed_boundaries():
    assert sf.target_side("SKHX", utc(2026, 10, 27, 5, 59)) == -1     # still short
    assert sf.target_side("SKHX", utc(2026, 10, 27, 6, 0)) == +1      # long from 06:00
    assert sf.target_side("ZHIPU", utc(2026, 10, 27, 1, 14)) == +1    # still long
    assert sf.target_side("ZHIPU", utc(2026, 10, 27, 1, 15)) == -1    # short from 01:15
    assert sf.next_boundary("EWY", utc(2026, 10, 27, 3, 0)) == utc(2026, 10, 27, 6, 0)
    assert sf.next_boundary("MINIMAX", utc(2026, 10, 27, 9, 0)) == utc(2026, 10, 28, 1, 15)

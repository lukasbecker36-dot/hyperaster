"""control_bot's HL P&L helpers: paging across a shared timestamp, the incremental cache, 429 retries."""
import io
import urllib.error

import pytest

import control_bot as cb


def funding_rows(hours, coins):
    """HL-style userFunding rows: every coin's funding for an hour shares one timestamp."""
    return [{"time": 1000 + h * 3600_000, "hash": "0x0", "delta": {"coin": f"xyz:{c}", "usdc": "0.01"}}
            for h in range(hours) for c in coins]


@pytest.fixture
def fake_hl(tmp_path, monkeypatch):
    monkeypatch.setattr(cb, "PNL_CACHE", tmp_path / "cache.json")
    monkeypatch.setattr(cb, "_HL_PAGE", {"userFunding": 7, "userFillsByTime": 7})   # small pages
    store, calls = {"rows": []}, []

    def info(body, retries=3):
        calls.append(body["startTime"])
        rows = sorted((r for r in store["rows"] if r["time"] >= body["startTime"]), key=lambda r: r["time"])
        return rows[:7]

    monkeypatch.setattr(cb, "_hl_info", info)
    return store, calls


def test_pages_that_split_an_hour_count_every_row_once(fake_hl):
    store, calls = fake_hl
    store["rows"] = funding_rows(4, ["A", "B", "C"])          # 12 rows, pages of 7 end mid-hour
    sums = cb._hl_totals("userFunding", "0xu", 0, dex="xyz")
    assert sums == {"xyz:A": {"usdc": pytest.approx(0.04)}, "xyz:B": {"usdc": pytest.approx(0.04)},
                    "xyz:C": {"usdc": pytest.approx(0.04)}}


def test_cache_fetches_only_new_rows(fake_hl):
    store, calls = fake_hl
    store["rows"] = funding_rows(2, ["A", "B"])
    assert cb._hl_totals("userFunding", "0xu", 0)["xyz:A"]["usdc"] == pytest.approx(0.02)
    calls.clear()
    store["rows"] = funding_rows(3, ["A", "B"])               # one more hour arrives
    sums = cb._hl_totals("userFunding", "0xu", 0)
    assert sums["xyz:A"]["usdc"] == pytest.approx(0.03) and sums["xyz:B"]["usdc"] == pytest.approx(0.03)
    assert calls == [1000 + 3600_000]                         # resumed at the last hour seen, one request


def test_fills_sum_closed_and_fees_by_coin(fake_hl):
    store, calls = fake_hl
    store["rows"] = [{"time": 5 + i // 3, "tid": i, "coin": "xyz:A", "closedPnl": "1.5", "fee": "0.1"}
                     for i in range(9)]                       # 3 fills per ms; the 7-row page ends mid-group
    s = cb._hl_totals("userFillsByTime", "0xu", 0)
    assert s["xyz:A"]["closed"] == pytest.approx(13.5) and s["xyz:A"]["fee"] == pytest.approx(0.9)


def test_429_waits_and_retries(monkeypatch):
    n = {"calls": 0}

    def urlopen(req, timeout=15):
        n["calls"] += 1
        if n["calls"] < 3:
            raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", {}, None)
        return io.BytesIO(b'{"ok": 1}')

    monkeypatch.setattr(cb.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(cb.time, "sleep", lambda s: None)
    assert cb._hl_info({"type": "x"}) == {"ok": 1} and n["calls"] == 3

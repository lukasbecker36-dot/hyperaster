#!/usr/bin/env python3
"""ZHIPU spread & convergence analysis — last 48 hours.

No dependencies beyond stdlib. Run on server:
    python3 /opt/hyperaster/scripts/zhipu_backtest.py
"""
import json
import math
import statistics
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"
ASTER_BASE = "https://fapi.asterdex.com"


def post_json(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data,
                                headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def get_json(url, params=None):
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def fetch_hl_candles(coin, interval, lookback_hours):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_hours * 3600 * 1000
    return post_json(HYPERLIQUID_API, {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": interval,
                "startTime": start_ms, "endTime": end_ms},
    })


def fetch_aster_candles(symbol, interval, lookback_hours):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_hours * 3600 * 1000
    all_candles = []
    cur = start_ms
    while cur < end_ms:
        data = get_json(f"{ASTER_BASE}/fapi/v1/klines", {
            "symbol": symbol, "interval": interval,
            "startTime": cur, "endTime": end_ms, "limit": 1500,
        })
        if not data:
            break
        all_candles.extend(data)
        cur = data[-1][0] + 60000
        if len(data) < 1500:
            break
    return all_candles


def fetch_hl_funding(coin, lookback_hours):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_hours * 3600 * 1000
    return post_json(HYPERLIQUID_API, {
        "type": "fundingHistory", "coin": coin,
        "startTime": start_ms, "endTime": end_ms,
    })


def fetch_aster_funding(symbol, lookback_hours):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_hours * 3600 * 1000
    return get_json(f"{ASTER_BASE}/fapi/v1/fundingRate", {
        "symbol": symbol, "startTime": start_ms,
        "endTime": end_ms, "limit": 1000,
    })


def main():
    hours = 48
    print("Fetching data...")

    hl_candles = fetch_hl_candles("xyz:ZHIPU", "1m", hours)
    aster_candles = fetch_aster_candles("ZHIPUUSDT", "1m", hours)
    hl_funding = fetch_hl_funding("xyz:ZHIPU", hours)
    aster_funding = fetch_aster_funding("ZHIPUUSDT", hours)

    print(f"Fetched: {len(hl_candles)} HL candles, {len(aster_candles)} Aster candles")

    # Build minute-keyed dicts
    hl_by_min = {}
    for c in hl_candles:
        t = c.get("t", 0)
        minute = t // 60000 * 60000
        hl_by_min[minute] = {
            "o": float(c["o"]), "h": float(c["h"]),
            "l": float(c["l"]), "c": float(c["c"]),
        }

    aster_by_min = {}
    for c in aster_candles:
        minute = c[0] // 60000 * 60000
        aster_by_min[minute] = {
            "o": float(c[1]), "h": float(c[2]),
            "l": float(c[3]), "c": float(c[4]),
        }

    common = sorted(set(hl_by_min) & set(aster_by_min))
    print(f"Overlapping minutes: {len(common)}\n")
    if not common:
        print("No overlapping data — are both venues listing ZHIPU?")
        return

    # Spread series  (positive = Aster premium over HL)
    spreads = []
    for t in common:
        hl = hl_by_min[t]
        ast = aster_by_min[t]
        hl_mid = (hl["o"] + hl["c"]) / 2
        ast_mid = (ast["o"] + ast["c"]) / 2
        mid = (hl_mid + ast_mid) / 2
        if mid > 0:
            bps = (ast_mid - hl_mid) / mid * 10000
            spreads.append((t, bps, hl_mid, ast_mid))

    if not spreads:
        print("No valid spread data")
        return

    vals = [s[1] for s in spreads]
    t0 = datetime.fromtimestamp(spreads[0][0] / 1000, tz=timezone.utc)
    t1 = datetime.fromtimestamp(spreads[-1][0] / 1000, tz=timezone.utc)

    W = 70
    print("=" * W)
    print("ZHIPU  SPREAD ANALYSIS  (Aster - HL, bps)")
    print(f"Period : {t0:%Y-%m-%d %H:%M} -> {t1:%Y-%m-%d %H:%M} UTC")
    print(f"Points : {len(vals)} minutes")
    print("=" * W)

    # -- Basic stats --
    avg = statistics.mean(vals)
    med = statistics.median(vals)
    std = statistics.stdev(vals) if len(vals) > 1 else 0
    mn, mx = min(vals), max(vals)
    pcts = statistics.quantiles(vals, n=100)

    print(f"\n{'Statistic':<14} {'Value':>10}")
    print(f"  Mean         {avg:+.1f} bps")
    print(f"  Median       {med:+.1f} bps")
    print(f"  Std dev      {std:.1f} bps")
    print(f"  Min          {mn:+.1f} bps")
    print(f"  Max          {mx:+.1f} bps")
    print(f"  5th pctl     {pcts[4]:+.1f} bps")
    print(f"  25th pctl    {pcts[24]:+.1f} bps")
    print(f"  75th pctl    {pcts[74]:+.1f} bps")
    print(f"  95th pctl    {pcts[94]:+.1f} bps")

    # -- Distribution buckets --
    print(f"\nSpread distribution:")
    buckets = [
        ("< -200", lambda x: x < -200),
        ("-200 to -100", lambda x: -200 <= x < -100),
        ("-100 to -50", lambda x: -100 <= x < -50),
        ("-50 to 0", lambda x: -50 <= x < 0),
        ("0 to +50", lambda x: 0 <= x < 50),
        ("+50 to +100", lambda x: 50 <= x < 100),
        ("+100 to +200", lambda x: 100 <= x < 200),
        ("+200 to +300", lambda x: 200 <= x < 300),
        (">= +300", lambda x: x >= 300),
    ]
    for label, fn in buckets:
        c = sum(1 for v in vals if fn(v))
        bar = "#" * int(c / len(vals) * 40)
        print(f"  {label:>14}: {c:4d} ({c / len(vals) * 100:5.1f}%) {bar}")

    # -- Direction bias --
    pos = sum(1 for v in vals if v > 0)
    neg = sum(1 for v in vals if v < 0)
    print(f"\nDirection bias:")
    print(f"  Aster > HL : {pos} min ({pos / len(vals) * 100:.1f}%)")
    print(f"  HL > Aster : {neg} min ({neg / len(vals) * 100:.1f}%)")

    # -- Time in regimes --
    print(f"\nTime in spread regimes:")
    regimes = [
        ("Tight  (+/-30 bps)", lambda x: abs(x) <= 30),
        ("Moderate (30-100)", lambda x: 30 < abs(x) <= 100),
        ("Wide   (100-200)", lambda x: 100 < abs(x) <= 200),
        ("V.wide (>200)", lambda x: abs(x) > 200),
    ]
    for name, fn in regimes:
        c = sum(1 for v in vals if fn(v))
        print(f"  {name}: {c:4d} min ({c / len(vals) * 100:5.1f}%)")

    # -- Convergence episodes --
    WIDE = 100
    CONV = 30

    print(f"\n{'=' * W}")
    print(f"CONVERGENCE EPISODES  (wide >= {WIDE}bps -> converged <= {CONV}bps)")
    print("=" * W)

    episodes = []
    in_wide = False
    ws = 0
    peak = 0.0

    for i, (t, bps, _, _) in enumerate(spreads):
        if not in_wide and abs(bps) >= WIDE:
            in_wide = True
            ws = i
            peak = bps
        elif in_wide:
            if abs(bps) > abs(peak):
                peak = bps
            if abs(bps) <= CONV:
                dur = i - ws
                episodes.append(dict(
                    start=spreads[ws][0], end=t,
                    peak=peak, end_bps=bps, dur=dur, conv=True))
                in_wide = False
            elif i == len(spreads) - 1:
                dur = i - ws
                episodes.append(dict(
                    start=spreads[ws][0], end=t,
                    peak=peak, end_bps=bps, dur=dur, conv=False))

    if episodes:
        for ep in episodes:
            s = datetime.fromtimestamp(ep["start"] / 1000, tz=timezone.utc)
            e = datetime.fromtimestamp(ep["end"] / 1000, tz=timezone.utc)
            tag = "CONVERGED" if ep["conv"] else "STILL WIDE"
            print(f"  {s:%m-%d %H:%M} -> {e:%m-%d %H:%M}  "
                  f"peak {ep['peak']:+.0f} -> {ep['end_bps']:+.0f} bps  "
                  f"({ep['dur']} min)  [{tag}]")
        conv = [e for e in episodes if e["conv"]]
        print(f"\n  Total episodes : {len(episodes)}")
        print(f"  Converged      : {len(conv)}")
        if conv:
            print(f"  Avg conv time  : {statistics.mean(e['dur'] for e in conv):.0f} min")
            print(f"  Min conv time  : {min(e['dur'] for e in conv)} min")
            print(f"  Max conv time  : {max(e['dur'] for e in conv)} min")
    else:
        print("  No episodes where spread exceeded +/-100 bps")

    # -- Mean-reversion half-life (autocorrelation) --
    print(f"\n{'=' * W}")
    print("MEAN-REVERSION SPEED")
    print("=" * W)

    demeaned = [v - avg for v in vals]
    n = len(demeaned)
    var = sum(x * x for x in demeaned) / n
    if var > 0:
        for lag in [1, 5, 15, 30, 60]:
            if lag >= n:
                break
            cov = sum(demeaned[i] * demeaned[i + lag] for i in range(n - lag)) / (n - lag)
            acf = cov / var
            print(f"  Autocorrelation (lag {lag:3d} min): {acf:.3f}")
        acf1_cov = sum(demeaned[i] * demeaned[i + 1] for i in range(n - 1)) / (n - 1)
        acf1 = acf1_cov / var
        if 0 < acf1 < 1:
            half_life = -1 / math.log(acf1)
            print(f"\n  Estimated half-life: {half_life:.1f} min")
            if half_life < 60:
                print(f"  -> Spread mean-reverts fairly quickly ({half_life:.0f} min)")
            elif half_life < 240:
                print(f"  -> Moderate mean-reversion ({half_life / 60:.1f} hrs)")
            else:
                print(f"  -> Slow mean-reversion ({half_life / 60:.1f} hrs) — convergence is unreliable")
        else:
            print(f"  ACF(1) = {acf1:.3f} — spread is not mean-reverting")

    # -- Funding --
    print(f"\n{'=' * W}")
    print("FUNDING RATES")
    print("=" * W)

    if hl_funding:
        rates = [float(f.get("fundingRate", 0)) for f in hl_funding]
        if rates:
            print(f"\n  HL (hourly, {len(rates)} settlements):")
            print(f"    Mean : {statistics.mean(rates) * 10000:+.2f} bps/hr")
            print(f"    Last : {rates[-1] * 10000:+.2f} bps/hr")
            print(f"    Min  : {min(rates) * 10000:+.2f} bps/hr")
            print(f"    Max  : {max(rates) * 10000:+.2f} bps/hr")

    if aster_funding:
        rates = [float(f.get("fundingRate", 0)) for f in aster_funding]
        if rates:
            avg_r = statistics.mean(rates)
            print(f"\n  Aster (8-hourly, {len(rates)} settlements):")
            print(f"    Mean : {avg_r * 10000:+.2f} bps/8hr  ({avg_r * 10000 / 8:+.2f} bps/hr)")
            print(f"    Last : {rates[-1] * 10000:+.2f} bps/8hr")
            print(f"    Min  : {min(rates) * 10000:+.2f} bps/8hr")
            print(f"    Max  : {max(rates) * 10000:+.2f} bps/8hr")

    # -- Hourly summary --
    print(f"\n{'=' * W}")
    print("HOURLY SPREAD SUMMARY")
    print("=" * W)
    print(f"  {'Hour (UTC)':<16} {'Mean':>8} {'Min':>8} {'Max':>8} {'Std':>8}")

    hourly = {}
    for t, bps, _, _ in spreads:
        h = datetime.fromtimestamp(t / 1000, tz=timezone.utc).strftime("%m-%d %H:00")
        hourly.setdefault(h, []).append(bps)

    for h in sorted(hourly):
        v = hourly[h]
        h_avg = statistics.mean(v)
        h_std = statistics.stdev(v) if len(v) > 1 else 0
        print(f"  {h:<16} {h_avg:+8.1f} {min(v):+8.1f} {max(v):+8.1f} {h_std:8.1f}")


if __name__ == "__main__":
    main()

"""
Fetch historical data from Hyperliquid and Aster DEX for equity perps.
Outputs: CSV files with hourly candles and funding rates for each overlapping symbol.
"""

import requests
import pandas as pd
import time
import json
import urllib3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import (
    HYPERLIQUID_API, ASTER_API, CANDLE_INTERVAL, HISTORY_DAYS
)

# Python 3.14 strict SSL rejects some valid certs missing key usage extensions.
# These are public read-only APIs, so disable verification.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SESSION = requests.Session()
SESSION.verify = False

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

ASTER_INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "1d": "1d",
}

# Known equity perp tickers — we'll validate these against live exchange data
EQUITY_KEYWORDS = [
    "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA",
    "AMD", "NFLX", "COIN", "PLTR", "HOOD", "CRCL", "SBET",
]


def get_hyperliquid_all_mids():
    """Get all available markets on Hyperliquid with mid prices."""
    resp = SESSION.post(HYPERLIQUID_API, json={"type": "allMids"})
    resp.raise_for_status()
    return resp.json()


def get_hyperliquid_meta():
    """Get metadata for all perp markets on Hyperliquid."""
    resp = SESSION.post(HYPERLIQUID_API, json={"type": "meta"})
    resp.raise_for_status()
    return resp.json()


def get_aster_exchange_info():
    """Get all available symbols on Aster."""
    resp = SESSION.get(f"{ASTER_API}/fapi/v1/exchangeInfo")
    resp.raise_for_status()
    return resp.json()


def get_hyperliquid_xyz_assets():
    """Get all XYZ perp DEX assets (equity/commodity perps) from Hyperliquid."""
    resp = SESSION.post(HYPERLIQUID_API, json={"type": "perpDexs"})
    resp.raise_for_status()
    dexs = resp.json()
    # XYZ DEX is typically at index 1 (index 0 is null/main)
    xyz_assets = {}
    for dex in dexs:
        if dex is None:
            continue
        if dex.get("name", "").lower() == "xyz":
            for asset_pair in dex.get("assetToStreamingOiCap", []):
                full_name = asset_pair[0]  # e.g. "xyz:AAPL"
                base = full_name.split(":")[-1]
                xyz_assets[base] = full_name
            break
    return xyz_assets


def find_overlapping_equity_perps():
    """Find equity perps available on both exchanges."""
    print("Fetching Hyperliquid XYZ perp assets...")
    hl_xyz = get_hyperliquid_xyz_assets()
    print(f"  Found {len(hl_xyz)} XYZ assets on Hyperliquid")

    print("Fetching Aster markets...")
    aster_info = get_aster_exchange_info()
    aster_symbols_raw = [s["symbol"] for s in aster_info.get("symbols", [])]
    aster_base_map = {}
    for sym in aster_symbols_raw:
        for suffix in ["USDT", "USDC", "USD"]:
            if sym.endswith(suffix):
                base = sym[: -len(suffix)]
                aster_base_map[base] = sym
                break

    print(f"  Found {len(aster_symbols_raw)} total symbols on Aster")

    # Filter to equity-like assets (exclude commodities, FX, indices)
    commodities_fx = {
        "ALUMINIUM", "BRENTOIL", "CL", "COPPER", "CORN", "DXY", "EUR", "GBP",
        "GOLD", "JPY", "KRW", "NATGAS", "PALLADIUM", "PLATINUM", "SILVER",
        "TTF", "URANIUM", "WHEAT", "VIX", "VOL",
    }
    indices = {"SP500", "JP225", "NIFTY", "IBOV", "KR200", "XYZ100", "SPCX"}
    etfs = {"EWJ", "EWT", "EWY", "EWZ", "URNM", "XLE", "USAR"}

    hl_equity = {
        base: full for base, full in hl_xyz.items()
        if base not in commodities_fx and base not in indices and base not in etfs
    }
    print(f"\nHyperliquid equity perps: {sorted(hl_equity.keys())}")

    aster_equity = {base: sym for base, sym in aster_base_map.items() if base in hl_equity}
    print(f"Aster matching equity perps: {sorted(aster_equity.keys())}")

    overlap = sorted(set(hl_equity.keys()) & set(aster_equity.keys()))
    print(f"\nOverlapping equity perps ({len(overlap)}): {overlap}")

    # Return mapping: base_ticker -> (hl_coin_name, aster_symbol)
    overlap_map = {}
    for base in overlap:
        overlap_map[base] = {
            "hl_coin": hl_equity[base],  # e.g. "xyz:AAPL"
            "aster_symbol": aster_equity[base],  # e.g. "AAPLUSDT"
        }
    return overlap_map


def fetch_hyperliquid_candles(coin, interval, start_ms, end_ms):
    """Fetch candle data from Hyperliquid. Max 5000 candles per request."""
    resp = SESSION.post(HYPERLIQUID_API, json={
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
        }
    })
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["t"] = pd.to_datetime(df["t"].astype(int), unit="ms", utc=True)
    for col in ["o", "h", "l", "c", "v"]:
        if col in df.columns:
            df[col] = df[col].astype(float)
    df = df.rename(columns={"t": "timestamp", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    return df


def fetch_hyperliquid_funding(coin, start_ms, end_ms):
    """Fetch funding rate history from Hyperliquid."""
    all_data = []
    current_start = start_ms

    while current_start < end_ms:
        chunk_end = min(current_start + 7 * 24 * 3600 * 1000, end_ms)
        resp = SESSION.post(HYPERLIQUID_API, json={
            "type": "fundingHistory",
            "coin": coin,
            "startTime": current_start,
            "endTime": chunk_end,
        })
        resp.raise_for_status()
        data = resp.json()
        if data:
            all_data.extend(data)
        current_start = chunk_end
        time.sleep(0.2)

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df["time"] = pd.to_datetime(df["time"].astype(int), unit="ms", utc=True)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df = df.rename(columns={"time": "timestamp"})
    return df[["timestamp", "fundingRate", "premium"]] if "premium" in df.columns else df[["timestamp", "fundingRate"]]


def fetch_aster_candles(symbol, interval, start_ms, end_ms):
    """Fetch kline data from Aster. Max 1500 per request."""
    all_data = []
    current_start = start_ms

    while current_start < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": 1500,
        }
        resp = SESSION.get(f"{ASTER_API}/fapi/v1/klines", params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_data.extend(data)
        current_start = int(data[-1][0]) + 1
        if len(data) < 1500:
            break
        time.sleep(0.2)

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    df["timestamp"] = pd.to_datetime(df["open_time"].astype(int), unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["timestamp", "open", "high", "low", "close", "volume"]]


def fetch_aster_funding(symbol, start_ms, end_ms):
    """Fetch funding rate history from Aster."""
    all_data = []
    current_start = start_ms

    while current_start < end_ms:
        params = {
            "symbol": symbol,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": 1000,
        }
        resp = SESSION.get(f"{ASTER_API}/fapi/v1/fundingRate", params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_data.extend(data)
        last_time = int(data[-1].get("fundingTime", data[-1].get("time", current_start)))
        current_start = last_time + 1
        if len(data) < 1000:
            break
        time.sleep(0.2)

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    time_col = "fundingTime" if "fundingTime" in df.columns else "time"
    df["timestamp"] = pd.to_datetime(df[time_col].astype(int), unit="ms", utc=True)
    df["fundingRate"] = df["fundingRate"].astype(float)
    return df[["timestamp", "fundingRate"]]


def fetch_hyperliquid_l2(coin):
    """Fetch current L2 order book from Hyperliquid."""
    resp = SESSION.post(HYPERLIQUID_API, json={
        "type": "l2Book",
        "coin": coin,
    })
    resp.raise_for_status()
    return resp.json()


def fetch_aster_depth(symbol, limit=20):
    """Fetch current order book from Aster."""
    resp = SESSION.get(f"{ASTER_API}/fapi/v1/depth", params={"symbol": symbol, "limit": limit})
    resp.raise_for_status()
    return resp.json()


def main():
    overlap = find_overlapping_equity_perps()

    if not overlap:
        print("\nNo overlapping equity perps found. Check if tickers have changed.")
        # Save what we found for debugging
        hl_mids = get_hyperliquid_all_mids()
        pd.Series(sorted(hl_mids.keys())).to_csv(OUTPUT_DIR / "hl_all_coins.csv", index=False, header=["coin"])

        aster_info = get_aster_exchange_info()
        syms = [s["symbol"] for s in aster_info.get("symbols", [])]
        pd.Series(sorted(syms)).to_csv(OUTPUT_DIR / "aster_all_symbols.csv", index=False, header=["symbol"])
        print("Saved full symbol lists to output/ for manual inspection.")
        return

    now = datetime.now(timezone.utc)
    end_ms = int(now.timestamp() * 1000)
    start_ms = int((now - timedelta(days=HISTORY_DAYS)).timestamp() * 1000)

    print(f"\nFetching {HISTORY_DAYS} days of data: {now - timedelta(days=HISTORY_DAYS):%Y-%m-%d} to {now:%Y-%m-%d}")

    for base_ticker, info in overlap.items():
        hl_coin = info["hl_coin"]       # e.g. "xyz:AAPL"
        aster_symbol = info["aster_symbol"]  # e.g. "AAPLUSDT"

        print(f"\n{'='*60}")
        print(f"Processing {base_ticker}: {hl_coin} (HL) / {aster_symbol} (Aster)")
        print(f"{'='*60}")

        # Fetch candles
        print(f"  Fetching Hyperliquid candles...")
        hl_candles = fetch_hyperliquid_candles(hl_coin, CANDLE_INTERVAL, start_ms, end_ms)
        print(f"  Got {len(hl_candles)} Hyperliquid candles")

        print(f"  Fetching Aster candles...")
        aster_candles = fetch_aster_candles(aster_symbol, CANDLE_INTERVAL, start_ms, end_ms)
        print(f"  Got {len(aster_candles)} Aster candles")

        # Fetch funding
        print(f"  Fetching Hyperliquid funding rates...")
        hl_funding = fetch_hyperliquid_funding(hl_coin, start_ms, end_ms)
        print(f"  Got {len(hl_funding)} Hyperliquid funding records")

        print(f"  Fetching Aster funding rates...")
        aster_funding = fetch_aster_funding(aster_symbol, start_ms, end_ms)
        print(f"  Got {len(aster_funding)} Aster funding records")

        # Fetch current order books for spread estimation
        print(f"  Fetching current order books...")
        try:
            hl_book = fetch_hyperliquid_l2(hl_coin)
            levels = hl_book.get("levels", [[], []])
            if levels and len(levels) >= 2 and levels[0] and levels[1]:
                best_ask = float(levels[0][0]["px"]) if levels[0] else None
                best_bid = float(levels[1][0]["px"]) if levels[1] else None
                hl_spread_bps = ((best_ask - best_bid) / ((best_ask + best_bid) / 2)) * 10000 if best_ask and best_bid else None
                print(f"  HL spread: {hl_spread_bps:.1f} bps" if hl_spread_bps else "  HL spread: N/A")
            else:
                hl_spread_bps = None
        except Exception as e:
            print(f"  HL order book error: {e}")
            hl_spread_bps = None

        try:
            aster_book = fetch_aster_depth(aster_symbol)
            if aster_book.get("asks") and aster_book.get("bids"):
                best_ask = float(aster_book["asks"][0][0])
                best_bid = float(aster_book["bids"][0][0])
                aster_spread_bps = ((best_ask - best_bid) / ((best_ask + best_bid) / 2)) * 10000
                print(f"  Aster spread: {aster_spread_bps:.1f} bps")
            else:
                aster_spread_bps = None
        except Exception as e:
            print(f"  Aster order book error: {e}")
            aster_spread_bps = None

        # Save candles (use base_ticker for clean filenames)
        if not hl_candles.empty:
            hl_candles.to_csv(OUTPUT_DIR / f"{base_ticker}_hl_candles.csv", index=False)
        if not aster_candles.empty:
            aster_candles.to_csv(OUTPUT_DIR / f"{base_ticker}_aster_candles.csv", index=False)

        # Save funding
        if not hl_funding.empty:
            hl_funding.to_csv(OUTPUT_DIR / f"{base_ticker}_hl_funding.csv", index=False)
        if not aster_funding.empty:
            aster_funding.to_csv(OUTPUT_DIR / f"{base_ticker}_aster_funding.csv", index=False)

        # Save spread snapshot
        spread_info = {
            "coin": base_ticker,
            "hl_coin": hl_coin,
            "aster_symbol": aster_symbol,
            "hl_spread_bps": hl_spread_bps,
            "aster_spread_bps": aster_spread_bps,
            "snapshot_time": now.isoformat(),
        }
        with open(OUTPUT_DIR / f"{base_ticker}_spreads.json", "w") as f:
            json.dump(spread_info, f, indent=2)

        time.sleep(0.5)

    # Save overlap summary
    pd.DataFrame([
        {"coin": k, "hl_coin": v["hl_coin"], "aster_symbol": v["aster_symbol"]}
        for k, v in overlap.items()
    ]).to_csv(OUTPUT_DIR / "overlap_symbols.csv", index=False)

    print(f"\nDone. Data saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()

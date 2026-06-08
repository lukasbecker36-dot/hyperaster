"""
Basis arb backtest with oracle-adjusted entry signals and per-symbol thresholds.

Key differences from analyze_arb.py:
- oracle_delta_bps = median(spread_bps) over the data window — proxy for the
  structural index price difference between exchanges that won't converge
- excess_bps = spread_bps - oracle_delta_bps — the convergeable part
- Entry when abs(excess_bps) >= per-symbol threshold (from ENTRY_THRESHOLD_BPS_BY_SYMBOL)
- Exit logic unchanged (best exit within MAX_HOLD_HOURS)
- Convergence P&L unchanged (oracle delta cancels in entry - exit)
"""

import numpy as np
import pandas as pd
from pathlib import Path

from config import (
    HYPERLIQUID_MAKER_FEE, HYPERLIQUID_TAKER_FEE,
    ASTER_MAKER_FEE, ASTER_TAKER_FEE,
    NOTIONAL_PER_LEG, MAX_HOLD_HOURS,
    ENTRY_THRESHOLD_BPS_BY_SYMBOL, ENTRY_THRESHOLD_BPS,
    EXIT_THRESHOLD_BPS,
)

OUTPUT = Path("output")
TS_PARSE = {"format": "ISO8601", "utc": True}

ROUND_TRIP_COST_BPS = (2 * ASTER_MAKER_FEE + 2 * HYPERLIQUID_TAKER_FEE) * 10000


def simulate_trades(coin: str) -> pd.DataFrame:
    merged_path = OUTPUT / f"{coin}_merged_prices.csv"
    if not merged_path.exists():
        return pd.DataFrame()

    prices = pd.read_csv(merged_path)
    prices["timestamp"] = pd.to_datetime(prices["timestamp"], **TS_PARSE)
    prices = prices.sort_values("timestamp").reset_index(drop=True)

    if "spread_bps" not in prices.columns or len(prices) < 20:
        return pd.DataFrame()

    spread_bps = prices["spread_bps"].values
    timestamps = prices["timestamp"].values

    # Oracle delta proxy: median spread over the full data window
    oracle_delta = float(np.median(spread_bps))
    excess_bps = spread_bps - oracle_delta

    threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(coin, ENTRY_THRESHOLD_BPS)

    # Funding
    funding_path = OUTPUT / f"{coin}_funding_merged.csv"
    cum_funding_hl = np.zeros(len(prices))
    cum_funding_aster = np.zeros(len(prices))
    if funding_path.exists():
        fund = pd.read_csv(funding_path)
        fund["timestamp"] = pd.to_datetime(fund["timestamp"], **TS_PARSE)
        for _, row in fund.iterrows():
            idx = prices["timestamp"].searchsorted(row["timestamp"])
            if idx < len(prices):
                cum_funding_hl[idx] = row.get("hl_funding", 0) or 0
                cum_funding_aster[idx] = row.get("aster_funding", 0) or 0
    cum_funding_hl = np.cumsum(cum_funding_hl)
    cum_funding_aster = np.cumsum(cum_funding_aster)

    trades = []
    i = 0
    max_look = int(MAX_HOLD_HOURS)

    while i < len(prices) - 1:
        exc = excess_bps[i]

        if abs(exc) < threshold:
            i += 1
            continue

        long_hl = exc < 0  # HL cheap in excess terms → long HL, short Aster
        window_end = min(i + max_look, len(prices))

        best_j, best_net = None, -999.0

        for j in range(i + 1, window_end):
            # Convergence: oracle delta cancels, so same as raw spread difference
            if long_hl:
                conv_bps = spread_bps[j] - spread_bps[i]  # want spread to rise toward 0
                fund_pnl = (
                    -(cum_funding_hl[j] - cum_funding_hl[i])
                    + (cum_funding_aster[j] - cum_funding_aster[i])
                )
            else:
                conv_bps = spread_bps[i] - spread_bps[j]
                fund_pnl = (
                    (cum_funding_hl[j] - cum_funding_hl[i])
                    - (cum_funding_aster[j] - cum_funding_aster[i])
                )

            net = conv_bps + fund_pnl * 10000 - ROUND_TRIP_COST_BPS

            if net > best_net:
                best_net = net
                best_j = j

        if best_j is not None and best_net > 0:
            entry_time = pd.Timestamp(timestamps[i])
            exit_time = pd.Timestamp(timestamps[best_j])
            holding_hours = (exit_time - entry_time).total_seconds() / 3600

            if long_hl:
                conv_bps = spread_bps[best_j] - spread_bps[i]
                fund_pnl = (
                    -(cum_funding_hl[best_j] - cum_funding_hl[i])
                    + (cum_funding_aster[best_j] - cum_funding_aster[i])
                )
            else:
                conv_bps = spread_bps[i] - spread_bps[best_j]
                fund_pnl = (
                    (cum_funding_hl[best_j] - cum_funding_hl[i])
                    - (cum_funding_aster[best_j] - cum_funding_aster[i])
                )

            trades.append({
                "entry_time": str(entry_time),
                "exit_time": str(exit_time),
                "holding_hours": round(holding_hours, 1),
                "entry_spread_bps": round(spread_bps[i], 2),
                "exit_spread_bps": round(spread_bps[best_j], 2),
                "oracle_delta_bps": round(oracle_delta, 2),
                "entry_excess_bps": round(exc, 2),
                "exit_excess_bps": round(excess_bps[best_j], 2),
                "long_exchange": "HL" if long_hl else "Aster",
                "short_exchange": "Aster" if long_hl else "HL",
                "fee_scenario": "taker_HL_maker_Aster",
                "convergence_pnl_bps": round(conv_bps, 2),
                "funding_pnl_bps": round(fund_pnl * 10000, 2),
                "trading_cost_bps": round(ROUND_TRIP_COST_BPS, 2),
                "net_pnl_bps": round(best_net, 2),
                "net_pnl_usd": round(best_net / 10000 * NOTIONAL_PER_LEG, 2),
            })
            i = best_j + 1
        else:
            i += 1

    return pd.DataFrame(trades)


def build_merged_prices():
    """Generate *_merged_prices.csv for every symbol that has candle data."""
    TS_PARSE_LOC = {"format": "ISO8601", "utc": True}
    for hl_f in sorted(OUTPUT.glob("*_hl_candles.csv")):
        coin = hl_f.stem.replace("_hl_candles", "")
        out = OUTPUT / f"{coin}_merged_prices.csv"
        aster_f = OUTPUT / f"{coin}_aster_candles.csv"
        if not aster_f.exists():
            continue
        hl = pd.read_csv(hl_f)
        hl["timestamp"] = pd.to_datetime(hl["timestamp"], **TS_PARSE_LOC)
        ast = pd.read_csv(aster_f)
        ast["timestamp"] = pd.to_datetime(ast["timestamp"], **TS_PARSE_LOC)
        hl = hl[["timestamp", "close"]].rename(columns={"close": "hl_close"})
        ast = ast[["timestamp", "close"]].rename(columns={"close": "aster_close"})
        m = pd.merge(hl, ast, on="timestamp", how="inner")
        if len(m) < 20:
            continue
        m["mid_price"] = (m["hl_close"] + m["aster_close"]) / 2
        m["spread"] = m["hl_close"] - m["aster_close"]
        m["spread_bps"] = m["spread"] / m["mid_price"] * 10000
        m.to_csv(out, index=False)


def main():
    build_merged_prices()
    all_trades = []

    coins = sorted(set(
        f.stem.replace("_merged_prices", "")
        for f in OUTPUT.glob("*_merged_prices.csv")
    ))

    print(f"\nRunning oracle-adjusted backtest for {len(coins)} symbols...")
    print(f"Round-trip cost: {ROUND_TRIP_COST_BPS:.1f}bps  (Aster maker 0bps + HL taker 4.5bps x2)\n")

    summary_rows = []
    for coin in coins:
        merged_path = OUTPUT / f"{coin}_merged_prices.csv"
        df = pd.read_csv(merged_path)
        if "spread_bps" not in df.columns or len(df) < 20:
            continue

        oracle_delta = float(df["spread_bps"].median())
        excess = df["spread_bps"] - oracle_delta
        threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(coin, ENTRY_THRESHOLD_BPS)

        trades = simulate_trades(coin)
        if not trades.empty:
            trades["coin"] = coin
            all_trades.append(trades)

        summary_rows.append({
            "coin": coin,
            "threshold_bps": threshold,
            "oracle_delta_bps": round(oracle_delta, 1),
            "data_hours": len(df),
            "entry_hours": int((excess.abs() >= threshold).sum()),
            "n_trades": len(trades),
            "total_pnl_usd": round(trades["net_pnl_usd"].sum(), 2) if not trades.empty else 0,
            "avg_pnl_bps": round(trades["net_pnl_bps"].mean(), 1) if not trades.empty else 0,
            "avg_hold_h": round(trades["holding_hours"].mean(), 1) if not trades.empty else 0,
            "win_rate_pct": round((trades["net_pnl_bps"] > 0).mean() * 100, 1) if not trades.empty else 0,
        })

    summary = pd.DataFrame(summary_rows)
    summary["entry_freq_pct"] = (summary["entry_hours"] / summary["data_hours"] * 100).round(1)
    summary["trades_per_day"] = (summary["n_trades"] / (summary["data_hours"] / 24)).round(2)
    summary = summary.sort_values("total_pnl_usd", ascending=False).reset_index(drop=True)
    summary.index += 1

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 180)
    pd.set_option("display.float_format", "{:.1f}".format)

    print("=" * 180)
    print("ORACLE-ADJUSTED BACKTEST — per-symbol thresholds on excess spread")
    print("=" * 180)

    cols = [
        "coin", "threshold_bps", "oracle_delta_bps",
        "n_trades", "win_rate_pct", "total_pnl_usd",
        "avg_pnl_bps", "avg_hold_h", "entry_freq_pct", "trades_per_day",
    ]
    print(summary[cols].to_string())

    if all_trades:
        all_df = pd.concat(all_trades, ignore_index=True)
        all_df.to_csv(OUTPUT / "all_basis_trades_v2.csv", index=False)
        summary.to_csv(OUTPUT / "symbol_analysis_v2.csv", index=False)

        total_usd = all_df["net_pnl_usd"].sum()
        n = len(all_df)
        avg_hold = all_df["holding_hours"].mean()
        print(f"\nTOTAL: {n} trades | ${total_usd:.2f} P&L | avg hold {avg_hold:.1f}h")
        print(f"Saved to output/all_basis_trades_v2.csv and output/symbol_analysis_v2.csv")

        # Compare symbols with large oracle deltas vs small — are they still worth trading?
        print("\n" + "=" * 80)
        print("ORACLE DELTA IMPACT — symbols where delta > 20bps")
        print("=" * 80)
        big_delta = summary[summary["oracle_delta_bps"].abs() > 20].sort_values("oracle_delta_bps", key=abs, ascending=False)
        print(big_delta[["coin", "oracle_delta_bps", "threshold_bps", "n_trades", "total_pnl_usd", "avg_pnl_bps"]].to_string())


if __name__ == "__main__":
    main()

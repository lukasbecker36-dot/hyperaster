"""
Analyze arbitrage opportunities between Hyperliquid and Aster equity perps.

Two strategies:
1. Cross-exchange basis arb: long cheap / short expensive, wait for convergence
2. Funding rate arb: harvest funding differential while delta-neutral

P&L components:
- Convergence profit (price spread mean-reversion)
- Bid-offer spread cost (entry + exit)
- Trading fees (maker one side, taker other)
- Funding rate gain/loss over holding period
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path

from config import (
    HYPERLIQUID_MAKER_FEE, HYPERLIQUID_TAKER_FEE,
    ASTER_MAKER_FEE, ASTER_TAKER_FEE,
    NOTIONAL_PER_LEG, MAX_HOLDING_HOURS,
)

OUTPUT_DIR = Path("output")

TS_PARSE = {"format": "ISO8601", "utc": True}


def load_candles(coin):
    hl_path = OUTPUT_DIR / f"{coin}_hl_candles.csv"
    aster_path = OUTPUT_DIR / f"{coin}_aster_candles.csv"
    if not hl_path.exists() or not aster_path.exists():
        return None, None
    hl = pd.read_csv(hl_path)
    hl["timestamp"] = pd.to_datetime(hl["timestamp"], **TS_PARSE)
    aster = pd.read_csv(aster_path)
    aster["timestamp"] = pd.to_datetime(aster["timestamp"], **TS_PARSE)
    return hl, aster


def load_funding(coin):
    hl_path = OUTPUT_DIR / f"{coin}_hl_funding.csv"
    aster_path = OUTPUT_DIR / f"{coin}_aster_funding.csv"
    frames = []
    for path in [hl_path, aster_path]:
        if path.exists():
            df = pd.read_csv(path)
            df["timestamp"] = pd.to_datetime(df["timestamp"], **TS_PARSE)
            frames.append(df)
        else:
            frames.append(pd.DataFrame())
    return frames[0], frames[1]


def load_spreads(coin):
    path = OUTPUT_DIR / f"{coin}_spreads.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def merge_price_data(hl_candles, aster_candles):
    hl = hl_candles[["timestamp", "close"]].rename(columns={"close": "hl_close"})
    aster = aster_candles[["timestamp", "close"]].rename(columns={"close": "aster_close"})
    merged = pd.merge(hl, aster, on="timestamp", how="inner")
    if merged.empty:
        return merged
    merged["mid_price"] = (merged["hl_close"] + merged["aster_close"]) / 2
    merged["spread"] = merged["hl_close"] - merged["aster_close"]
    merged["spread_bps"] = merged["spread"] / merged["mid_price"] * 10000
    return merged


def merge_funding_data(hl_funding, aster_funding):
    if hl_funding.empty or aster_funding.empty:
        return pd.DataFrame()
    hl = hl_funding[["timestamp", "fundingRate"]].rename(columns={"fundingRate": "hl_funding"}).copy()
    aster = aster_funding[["timestamp", "fundingRate"]].rename(columns={"fundingRate": "aster_funding"}).copy()
    hl = hl.sort_values("timestamp")
    aster = aster.sort_values("timestamp")
    merged = pd.merge_asof(hl, aster, on="timestamp", tolerance=pd.Timedelta("1h"), direction="nearest")
    merged = merged.dropna(subset=["aster_funding"])
    merged["funding_diff"] = merged["hl_funding"] - merged["aster_funding"]
    return merged


def compute_trading_costs(spread_info):
    """Compute round-trip trading costs in decimal (per dollar of notional)."""
    hl_spread_bps = spread_info.get("hl_spread_bps") if spread_info else None
    aster_spread_bps = spread_info.get("aster_spread_bps") if spread_info else None

    # Use absolute values; negative HL spreads are likely bid/ask ordering artifacts
    hl_half_spread = abs(hl_spread_bps or 10.0) / 2 / 10000
    aster_half_spread = abs(aster_spread_bps or 10.0) / 2 / 10000

    # Two scenarios: maker on one exchange, taker on the other
    scenarios = {
        "maker_HL_taker_Aster": {
            "fee_cost": 2 * HYPERLIQUID_MAKER_FEE + 2 * ASTER_TAKER_FEE,
        },
        "taker_HL_maker_Aster": {
            "fee_cost": 2 * HYPERLIQUID_TAKER_FEE + 2 * ASTER_MAKER_FEE,
        },
    }
    spread_cost = 2 * hl_half_spread + 2 * aster_half_spread  # entry + exit both sides
    for s in scenarios.values():
        s["total_cost"] = s["fee_cost"] + spread_cost
        s["total_cost_bps"] = s["total_cost"] * 10000
    return scenarios


def analyze_basis_arb(merged_prices, spread_info, funding_merged):
    """
    Vectorized basis arb analysis.

    For each hour, compute the best exit within MAX_HOLDING_HOURS using
    rolling spread statistics rather than brute-force search.
    """
    if merged_prices.empty or len(merged_prices) < 10:
        return pd.DataFrame()

    scenarios = compute_trading_costs(spread_info)
    best_scenario = min(scenarios.items(), key=lambda x: x[1]["total_cost"])
    scenario_name, costs = best_scenario
    min_cost = costs["total_cost"]
    min_cost_bps = costs["total_cost_bps"]

    prices = merged_prices.sort_values("timestamp").reset_index(drop=True)
    spread_bps = prices["spread_bps"].values
    timestamps = prices["timestamp"].values

    # Pre-compute cumulative funding for the holding period
    cum_funding_hl = np.zeros(len(prices))
    cum_funding_aster = np.zeros(len(prices))
    if not funding_merged.empty:
        for _, row in funding_merged.iterrows():
            idx = prices["timestamp"].searchsorted(row["timestamp"])
            if idx < len(prices):
                cum_funding_hl[idx] = row["hl_funding"]
                cum_funding_aster[idx] = row["aster_funding"]
    cum_funding_hl = np.cumsum(cum_funding_hl)
    cum_funding_aster = np.cumsum(cum_funding_aster)

    trades = []
    max_look = MAX_HOLDING_HOURS  # each candle is 1h

    i = 0
    while i < len(prices) - 1:
        entry_spread_bps = spread_bps[i]
        entry_abs = abs(entry_spread_bps)

        if entry_abs <= min_cost_bps:
            i += 1
            continue

        long_hl = entry_spread_bps < 0  # HL cheaper
        window_end = min(i + max_look, len(prices))

        # Find best exit in window
        best_j = None
        best_net = -999

        for j in range(i + 1, window_end):
            if long_hl:
                convergence_bps = spread_bps[j] - entry_spread_bps
            else:
                convergence_bps = entry_spread_bps - spread_bps[j]

            # Funding P&L over holding period
            if long_hl:
                funding_pnl = -(cum_funding_hl[j] - cum_funding_hl[i]) + (cum_funding_aster[j] - cum_funding_aster[i])
            else:
                funding_pnl = (cum_funding_hl[j] - cum_funding_hl[i]) - (cum_funding_aster[j] - cum_funding_aster[i])

            net_bps = convergence_bps + funding_pnl * 10000 - min_cost_bps

            if net_bps > best_net:
                best_net = net_bps
                best_j = j

        if best_j is not None and best_net > 0:
            entry_time = pd.Timestamp(timestamps[i])
            exit_time = pd.Timestamp(timestamps[best_j])
            holding_hours = (exit_time - entry_time).total_seconds() / 3600

            # Recompute components for the chosen exit
            if long_hl:
                conv_bps = spread_bps[best_j] - entry_spread_bps
                fund_pnl = -(cum_funding_hl[best_j] - cum_funding_hl[i]) + (cum_funding_aster[best_j] - cum_funding_aster[i])
            else:
                conv_bps = entry_spread_bps - spread_bps[best_j]
                fund_pnl = (cum_funding_hl[best_j] - cum_funding_hl[i]) - (cum_funding_aster[best_j] - cum_funding_aster[i])
            fund_bps = fund_pnl * 10000

            trades.append({
                "entry_time": str(entry_time),
                "exit_time": str(exit_time),
                "holding_hours": round(holding_hours, 1),
                "entry_spread_bps": round(entry_spread_bps, 2),
                "exit_spread_bps": round(spread_bps[best_j], 2),
                "long_exchange": "HL" if long_hl else "Aster",
                "short_exchange": "Aster" if long_hl else "HL",
                "fee_scenario": scenario_name,
                "convergence_pnl_bps": round(conv_bps, 2),
                "funding_pnl_bps": round(fund_bps, 2),
                "trading_cost_bps": round(min_cost_bps, 2),
                "net_pnl_bps": round(best_net, 2),
                "net_pnl_pct": round(best_net / 10000 * 100, 4),
                "net_pnl_usd": round(best_net / 10000 * NOTIONAL_PER_LEG, 2),
            })
            # Skip ahead past exit to avoid overlapping trades
            i = best_j + 1
        else:
            i += 1

    return pd.DataFrame(trades)


def analyze_funding_arb(funding_merged, merged_prices):
    """Analyze funding rate differential arb opportunity."""
    if funding_merged.empty or merged_prices.empty:
        return pd.DataFrame()

    funding = funding_merged.sort_values("timestamp").reset_index(drop=True)
    avg_mid = merged_prices["mid_price"].mean()

    # Estimate entry/exit cost (one-time)
    spread_cost = 10 / 10000  # 10 bps conservative total spread cost
    fee_cost = 2 * HYPERLIQUID_MAKER_FEE + 2 * ASTER_TAKER_FEE
    total_cost = spread_cost + fee_cost

    results = []
    for window_hours in [8, 24, 48]:
        n_periods = max(1, window_hours // 8)
        cum_diff = funding["funding_diff"].rolling(n_periods, min_periods=1).sum()

        profitable_mask = cum_diff.abs() > total_cost
        n_profitable = profitable_mask.sum()
        n_total = len(cum_diff)

        if n_profitable > 0:
            avg_gross = cum_diff[profitable_mask].abs().mean()
            avg_net = avg_gross - total_cost

            results.append({
                "holding_period_hours": window_hours,
                "total_periods": n_total,
                "profitable_periods": int(n_profitable),
                "hit_rate_pct": round(n_profitable / n_total * 100, 1),
                "avg_gross_funding_diff_bps": round(avg_gross * 10000, 2),
                "entry_exit_cost_bps": round(total_cost * 10000, 2),
                "avg_net_pnl_bps": round(avg_net * 10000, 2),
                "avg_net_pnl_usd": round(avg_net * NOTIONAL_PER_LEG, 2),
                "est_monthly_trades": round(n_profitable / (len(funding) * 8 / 720) / n_periods, 1),
            })

    return pd.DataFrame(results)


def main():
    overlap_path = OUTPUT_DIR / "overlap_symbols.csv"
    if not overlap_path.exists():
        print("No overlap_symbols.csv found. Run fetch_data.py first.")
        return

    overlap = pd.read_csv(overlap_path)
    all_basis_trades = []
    all_funding_results = []
    summary_rows = []

    for _, row in overlap.iterrows():
        coin = row["coin"]
        print(f"\n{'='*60}")
        print(f"Analyzing {coin}")
        print(f"{'='*60}")

        hl_candles, aster_candles = load_candles(coin)
        if hl_candles is None or aster_candles is None:
            print(f"  Missing candle data, skipping.")
            continue

        merged_prices = merge_price_data(hl_candles, aster_candles)
        if merged_prices.empty:
            print(f"  No overlapping timestamps, skipping.")
            continue

        hl_funding, aster_funding = load_funding(coin)
        funding_merged = merge_funding_data(hl_funding, aster_funding)
        spread_info = load_spreads(coin)

        # Price spread stats
        print(f"  Data points: {len(merged_prices)} overlapping hours")
        print(f"  Mean spread: {merged_prices['spread_bps'].mean():.1f} bps")
        print(f"  Std spread:  {merged_prices['spread_bps'].std():.1f} bps")
        print(f"  Mean |spread|: {merged_prices['spread_bps'].abs().mean():.1f} bps")
        print(f"  Max |spread|: {merged_prices['spread_bps'].abs().max():.1f} bps")

        if not funding_merged.empty:
            print(f"  Funding periods: {len(funding_merged)}")
            print(f"  Mean HL funding: {funding_merged['hl_funding'].mean() * 10000:.2f} bps/period")
            print(f"  Mean Aster funding: {funding_merged['aster_funding'].mean() * 10000:.2f} bps/period")
            print(f"  Mean diff: {funding_merged['funding_diff'].mean() * 10000:.2f} bps/period")

        # Basis arb
        basis_trades = analyze_basis_arb(merged_prices, spread_info, funding_merged)
        if not basis_trades.empty:
            basis_trades["coin"] = coin
            all_basis_trades.append(basis_trades)
            avg_conv = basis_trades["convergence_pnl_bps"].mean()
            avg_fund = basis_trades["funding_pnl_bps"].mean()
            avg_cost = basis_trades["trading_cost_bps"].mean()
            avg_net = basis_trades["net_pnl_bps"].mean()
            print(f"  Basis arb: {len(basis_trades)} trades, avg hold {basis_trades['holding_hours'].mean():.0f}h")
            print(f"    Avg convergence: {avg_conv:+.1f}bps, funding: {avg_fund:+.1f}bps, "
                  f"costs: -{avg_cost:.1f}bps => net: {avg_net:+.1f}bps (${basis_trades['net_pnl_usd'].mean():.2f})")
            print(f"    Total P&L: ${basis_trades['net_pnl_usd'].sum():.2f}")
        else:
            print(f"  Basis arb: no profitable trades")

        # Funding arb
        funding_results = analyze_funding_arb(funding_merged, merged_prices)
        if not funding_results.empty:
            funding_results["coin"] = coin
            all_funding_results.append(funding_results)
            for _, fr in funding_results.iterrows():
                print(f"  Funding arb {int(fr['holding_period_hours'])}h: "
                      f"hit {fr['hit_rate_pct']}%, "
                      f"avg net {fr['avg_net_pnl_bps']:.1f}bps (${fr['avg_net_pnl_usd']:.2f})")
        else:
            print(f"  Funding arb: insufficient data")

        # Summary
        summary_rows.append({
            "coin": coin,
            "data_hours": len(merged_prices),
            "mean_spread_bps": round(merged_prices["spread_bps"].mean(), 1),
            "std_spread_bps": round(merged_prices["spread_bps"].std(), 1),
            "mean_abs_spread_bps": round(merged_prices["spread_bps"].abs().mean(), 1),
            "max_abs_spread_bps": round(merged_prices["spread_bps"].abs().max(), 1),
            "basis_trades": len(basis_trades) if not basis_trades.empty else 0,
            "basis_total_pnl": round(basis_trades["net_pnl_usd"].sum(), 2) if not basis_trades.empty else 0,
            "basis_avg_pnl": round(basis_trades["net_pnl_usd"].mean(), 2) if not basis_trades.empty else 0,
            "funding_periods": len(funding_merged),
            "mean_funding_diff_bps": round(funding_merged["funding_diff"].mean() * 10000, 2) if not funding_merged.empty else 0,
            "hl_spread_bps": abs(spread_info.get("hl_spread_bps") or 0) if spread_info else None,
            "aster_spread_bps": spread_info.get("aster_spread_bps") if spread_info else None,
        })

        merged_prices.to_csv(OUTPUT_DIR / f"{coin}_merged_prices.csv", index=False)
        if not funding_merged.empty:
            funding_merged.to_csv(OUTPUT_DIR / f"{coin}_funding_merged.csv", index=False)

    # Save aggregated results
    if all_basis_trades:
        all_basis_df = pd.concat(all_basis_trades, ignore_index=True)
        all_basis_df.to_csv(OUTPUT_DIR / "all_basis_trades.csv", index=False)
        print(f"\n\nSaved {len(all_basis_df)} basis trades to all_basis_trades.csv")

    if all_funding_results:
        all_funding_df = pd.concat(all_funding_results, ignore_index=True)
        all_funding_df.to_csv(OUTPUT_DIR / "all_funding_arb.csv", index=False)
        print(f"Saved funding arb results to all_funding_arb.csv")

    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        summary_df = summary_df.sort_values("basis_total_pnl", ascending=False)
        summary_df.to_csv(OUTPUT_DIR / "arb_summary.csv", index=False)

        print(f"\n{'='*80}")
        print(f"OVERALL SUMMARY (sorted by basis P&L)")
        print(f"{'='*80}")
        print(summary_df.to_string(index=False))

        print(f"\n\nTop basis arb opportunities:")
        top = summary_df[summary_df["basis_trades"] > 0].head(10)
        for _, r in top.iterrows():
            print(f"  {r['coin']:8s}  {r['basis_trades']:3.0f} trades  "
                  f"total ${r['basis_total_pnl']:8.2f}  "
                  f"avg ${r['basis_avg_pnl']:6.2f}  "
                  f"spread {r['mean_abs_spread_bps']:5.1f}bps mean, {r['max_abs_spread_bps']:6.1f}bps max")


if __name__ == "__main__":
    main()

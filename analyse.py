"""
Analyse historical basis trades and spread data to rank symbols by P&L and entry frequency.
Run:  python analyse.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

OUTPUT = Path("output")

# ── Load trade log ────────────────────────────────────────────────────────────

trades = pd.read_csv(OUTPUT / "all_basis_trades.csv", parse_dates=["entry_time", "exit_time"])
trades = trades[trades["fee_scenario"] == "maker_HL_taker_Aster"].copy()

# ── Per-symbol trade stats ────────────────────────────────────────────────────

def trade_stats(g):
    wins = g[g["net_pnl_bps"] > 0]
    return pd.Series({
        "n_trades":          len(g),
        "win_rate_pct":      round(len(wins) / len(g) * 100, 1),
        "total_pnl_bps":     round(g["net_pnl_bps"].sum(), 1),
        "total_pnl_usd":     round(g["net_pnl_usd"].sum(), 2),
        "avg_pnl_bps":       round(g["net_pnl_bps"].mean(), 1),
        "median_pnl_bps":    round(g["net_pnl_bps"].median(), 1),
        "best_trade_bps":    round(g["net_pnl_bps"].max(), 1),
        "worst_trade_bps":   round(g["net_pnl_bps"].min(), 1),
        "avg_hold_h":        round(g["holding_hours"].mean(), 1),
        "median_hold_h":     round(g["holding_hours"].median(), 1),
        "avg_entry_bps":     round(g["entry_spread_bps"].abs().mean(), 1),
        "avg_conv_pnl_bps":  round(g["convergence_pnl_bps"].mean(), 1),
        "avg_fund_pnl_bps":  round(g["funding_pnl_bps"].mean(), 1),
    })

by_coin = trades.groupby("coin").apply(trade_stats).reset_index()
by_coin = by_coin.sort_values("total_pnl_bps", ascending=False)

# ── Entry frequency from merged price files ───────────────────────────────────
# Count hours where |spread| >= 30 bps (entry threshold)

ENTRY_THRESH = 30.0
freq_rows = []
for f in sorted(OUTPUT.glob("*_merged_prices.csv")):
    coin = f.stem.replace("_merged_prices", "")
    df = pd.read_csv(f, parse_dates=["timestamp"])
    if "spread_bps" not in df.columns:
        continue
    total_h = len(df)
    entry_h = (df["spread_bps"].abs() >= ENTRY_THRESH).sum()
    freq_rows.append({
        "coin":          coin,
        "data_hours":    total_h,
        "entry_hours":   int(entry_h),
        "entry_freq_pct": round(entry_h / total_h * 100, 1) if total_h else 0,
        "p95_spread_bps": round(df["spread_bps"].abs().quantile(0.95), 1),
        "p99_spread_bps": round(df["spread_bps"].abs().quantile(0.99), 1),
        "max_spread_bps": round(df["spread_bps"].abs().max(), 1),
    })

freq = pd.DataFrame(freq_rows)

# ── Merge ─────────────────────────────────────────────────────────────────────

summary = by_coin.merge(freq, on="coin", how="left")

# Trades per day normalised to data window
summary["trades_per_day"] = (summary["n_trades"] / (summary["data_hours"] / 24)).round(2)

summary = summary.sort_values("total_pnl_bps", ascending=False).reset_index(drop=True)
summary.index += 1

# ── Print ─────────────────────────────────────────────────────────────────────

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)
pd.set_option("display.float_format", "{:.1f}".format)

print("\n" + "=" * 160)
print("SYMBOL RANKING — total P&L (basis arb, maker-HL / taker-Aster fee scenario)")
print("=" * 160)

cols_main = [
    "coin", "n_trades", "win_rate_pct", "total_pnl_bps", "total_pnl_usd",
    "avg_pnl_bps", "best_trade_bps", "worst_trade_bps",
    "avg_hold_h", "avg_entry_bps",
]
print(summary[cols_main].to_string())

print("\n" + "=" * 160)
print("ENTRY FREQUENCY & SPREAD DISTRIBUTION  (entry threshold = 30 bps)")
print("=" * 160)

cols_freq = [
    "coin", "n_trades", "trades_per_day", "entry_freq_pct",
    "p95_spread_bps", "p99_spread_bps", "max_spread_bps", "data_hours",
]
freq_view = summary[cols_freq].dropna(subset=["entry_freq_pct"]).sort_values("trades_per_day", ascending=False).reset_index(drop=True)
freq_view.index += 1
print(freq_view.to_string())

# ── P&L breakdown: convergence vs funding ────────────────────────────────────

print("\n" + "=" * 160)
print("P&L SOURCE BREAKDOWN  (avg bps per trade)")
print("=" * 160)

cols_src = ["coin", "n_trades", "avg_pnl_bps", "avg_conv_pnl_bps", "avg_fund_pnl_bps", "avg_hold_h", "median_hold_h"]
print(summary[cols_src].to_string())

# ── Save ─────────────────────────────────────────────────────────────────────

summary.to_csv(OUTPUT / "symbol_analysis.csv", index=False)
print(f"\nFull table saved to output/symbol_analysis.csv")

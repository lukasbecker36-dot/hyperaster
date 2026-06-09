import pandas as pd
import numpy as np
from pathlib import Path

OUTPUT = Path("output")
GLOBAL_DEFAULT = 30.0

rows = []
for hl_f in sorted(OUTPUT.glob("*_hl_candles.csv")):
    coin = hl_f.stem.replace("_hl_candles", "")
    if coin == "BB":
        continue
    ast_f = OUTPUT / f"{coin}_aster_candles.csv"
    if not ast_f.exists():
        continue
    hl = pd.read_csv(hl_f, parse_dates=["timestamp"])
    ast = pd.read_csv(ast_f, parse_dates=["timestamp"])
    if hl.empty or ast.empty:
        continue
    m = pd.merge(hl[["timestamp","close"]], ast[["timestamp","close"]], on="timestamp", suffixes=("_hl","_ast"))
    if len(m) < 20:
        continue
    mid = (m.close_hl + m.close_ast) / 2
    raw_spread_bps = (m.close_hl - m.close_ast) / mid * 10000  # signed
    oracle_delta = float(raw_spread_bps.median())              # structural gap
    excess_bps = (raw_spread_bps - oracle_delta).abs()         # convergeable part
    p75 = excess_bps.quantile(0.75)
    threshold = max(GLOBAL_DEFAULT, round(p75 / 5) * 5)
    rows.append((coin, round(oracle_delta, 1), round(p75, 1), threshold))

rows.sort(key=lambda x: x[3], reverse=True)
print("ENTRY_THRESHOLD_BPS_BY_SYMBOL = {")
for coin, delta, p75, thresh in rows:
    marker = "  # (default)" if thresh == GLOBAL_DEFAULT else f"  # excess p75={p75:.0f}bps  oracle_delta={delta:+.0f}bps"
    print(f'    "{coin}": {thresh:.0f},{marker}')
print("}")

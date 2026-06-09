# Hyperaster — Equity Perp Arb Monitor
### AsterDEX vs Hyperliquid XYZ

---

## What This Does

Cross-exchange basis arbitrage on equity perpetuals. Both AsterDEX and Hyperliquid XYZ list the same equity perps (AAPL, NVDA, TSLA, etc.) with different liquidity and different oracle price feeds. When the mid prices diverge beyond the round-trip fee, the position is:

- **Long the cheaper exchange** (buy AAPL perp)
- **Short the more expensive exchange** (sell AAPL perp)
- **Close both when spread converges** to near zero

Round-trip cost is ~9–14bps (0bps Aster maker + 4.5bps HL taker, each side). Thresholds are set at the historical p75 of excess spread per symbol, so every entry has a reasonable base rate of convergence.

---

## Key Concept: Oracle Delta Adjustment

The raw cross-exchange spread (`aster_mid - hl_mid`) includes a **structural component** that will never converge because the two exchanges use different index/oracle price feeds. This is the `oracle_delta`:

```
oracle_delta_bps = (aster_index_price - hl_oracle_price) / mid * 10000
excess_bps       = cross_bps - oracle_delta_bps
```

Only `excess_bps` is tradeable. We ignore raw spread entirely; all thresholds and entry/exit logic operate on `excess_bps`. For BIRD, for example, the oracle delta is a persistent ~123bps that looks like opportunity but never closes — the excess threshold is therefore much higher than for AAPL where the delta is near zero.

---

## Architecture

```
live_monitor.py     — main loop, two-speed polling, heartbeat
  └─ executor.py    — entry/exit logic, oracle-adjusted spread check
  └─ exchange_client.py  — orderbook fetch, order placement, signing
  └─ position_manager.py — in-memory + SQLite position state
  └─ database.py    — SQLite schema (positions, fills)

config.py           — all tunable parameters
auth.py             — EIP-712 signing helpers, key loading from .env

fetch_data.py       — historical OHLCV + funding download
compute_thresholds.py — computes per-symbol thresholds from history
analyze_arb_v2.py   — oracle-adjusted backtest (503 trades, ~$10k PnL)
test_live_orders.py — end-to-end signing test (place + cancel/expire)
```

### Two-Speed Polling

- **Slow scan** (every 5 min): fetches orderbooks for all ~34 symbols, refreshes HL oracle prices, ranks symbols by `excess / threshold` ratio, selects top 3 as fast-tick candidates.
- **Fast tick** (every 1s): polls only the top-3 candidates + any open positions. Acts on entry/exit triggers immediately.

---

## Exchange Integration Notes

### AsterDEX

- Orders: `POST /fapi/v3/order` with `timeInForce=GTX` (post-only maker)
- Auth: EIP-712 structured-data signing with an **agent wallet** (separate from main wallet). `ASTER_API_KEY` = agent wallet address, `ASTER_API_SECRET` = agent wallet private key, `ASTER_WALLET_ADDRESS` = main wallet.
- Prices must be snapped to tick size: `round(round(price / tick) * tick, precision)` — Aster rejects orders that aren't on a tick boundary.
- Entry: place a GTX limit order at best bid/ask. Poll every 10s for fill. If unfilled after `ENTRY_TIMEOUT_MINUTES`, cancel Aster leg and close HL position.

### Hyperliquid XYZ

- Orders: `POST https://api.hyperliquid.xyz/exchange` — same endpoint as mainnet HL.
- Auth: **phantom-agent signing scheme** — msgpack-serialise the action, append 8-byte nonce and vault address flag, keccak256 the result to get `connectionId`, then EIP-712 sign a `{source: "a", connectionId: bytes32}` struct under domain `{name: "Exchange", chainId: 1337}`.
- **Asset indices**: XYZ is a builder-deployed perp dex at offset **110000**. Asset index = `110000 + universe_position`. AAPL is at universe position 9 → asset index **110009**. This is NOT the same as the `perpDexs.assetToStreamingOiCap` ordering. We verify the offset at startup by querying `perpDexs` and finding XYZ's builder index.
- Do **not** include `"dex": "xyz"` in the action dict — HL's Rust deserialiser rejects unknown fields. The `dex` param is only for info-endpoint reads.
- `"expiresAfter": null` must be present in the payload (not absent).
- Entry: IOC limit order with a small buffer above ask (buys) or below bid (sells). Expected result is either fill or `"Order could not immediately match"` (treated as `ioc_not_filled` success).

---

## Setup

### 1. Install dependencies

```bash
pip install aiohttp eth-account python-dotenv pandas numpy msgpack hyperliquid-python-sdk
```

### 2. Create `.env`

```
ASTER_API_KEY=0x...          # agent wallet address
ASTER_API_SECRET=0x...       # agent wallet private key
ASTER_WALLET_ADDRESS=0x...   # main wallet address

HL_PRIVATE_KEY=0x...         # Hyperliquid wallet private key
HL_WALLET_ADDRESS=0x...      # Hyperliquid wallet address (must match key)
```

The Aster agent wallet is set up at asterdex.com under Account → API Management. The HL wallet needs a balance on Hyperliquid XYZ (deposit USDC at hyperliquid.xyz, select XYZ chain).

### 3. Verify signing works

```bash
python test_live_orders.py
```

Expected output:
```
[PASS] Aster GTX placed: order_id=...
[PASS] Aster cancel successful
[PASS] HL IOC submitted and expired unfilled (expected)
ALL TESTS PASSED — signing works on both exchanges
```

### 4. Discover symbols (optional — output/ already present)

```bash
python fetch_data.py
```

Downloads 30 days of OHLCV + funding for all overlapping equity perps, writes CSVs to `output/`, and produces `output/overlap_symbols.csv`.

### 5. Run in paper mode (default)

```bash
python live_monitor.py --paper
```

### 6. Run live

Start small: set `MAX_CONCURRENT_POSITIONS = 1` and `NOTIONAL_PER_LEG = 200` in `config.py` until you've verified one full entry→hold→exit cycle.

```bash
python live_monitor.py --live
```

---

## Key Parameters (`config.py`)

| Parameter | Value | Notes |
|---|---|---|
| `ENTRY_THRESHOLD_BPS` | 30 | Default threshold for symbols not in per-symbol dict |
| `EXIT_THRESHOLD_BPS` | 8 | Close when excess spread falls below this |
| `MAX_HOLD_HOURS` | 48 | Force-close after this regardless of spread |
| `NOTIONAL_PER_LEG` | 1000 | USD per leg (2000 total per position) |
| `MAX_CONCURRENT_POSITIONS` | 3 | Max simultaneous open positions |
| `POLL_INTERVAL_SECONDS` | 1 | Fast-tick interval |
| `SLOW_SCAN_INTERVAL_SECONDS` | 300 | Full re-rank interval (in live_monitor.py) |
| `PAPER_MODE` | True | Switch to False in config or use --live flag |

Per-symbol thresholds are in `ENTRY_THRESHOLD_BPS_BY_SYMBOL`. Each is set at the historical p75 of `excess_bps`, rounded to nearest 5bps. The comment on each line shows the oracle delta for reference.

---

## Backtest Results (analyze_arb_v2.py)

- **Period**: ~30 days of 1-hour OHLCV, 34 symbols
- **Method**: oracle-adjusted entry (median spread as oracle delta proxy), per-symbol thresholds
- **Trades**: 503
- **Gross PnL**: ~$10,400 (before fees)
- **Fee cost**: ~9bps round-trip per trade

The backtest uses hourly candles as a proxy — live execution on 1-minute or faster data will see more entries and tighter fills. The real constraint is Aster maker fill latency (post-only orders may take minutes to fill), which the backtest doesn't model.

---

## Known Issues / Next Steps

1. **Aster maker fill latency**: GTX orders can sit for minutes before filling, during which HL price may move. If the spread closes before fill, the entry is abandoned but with a small HL IOC cost already incurred. Consider a tighter `ENTRY_TIMEOUT_MINUTES` for volatile symbols.

2. **Oracle delta drift**: `oracle_delta_bps` is computed live using cached prices (Aster index refreshed each mark-price poll ~60s; HL oracle refreshed every 5-min slow scan). If the delta shifts between cache refresh and entry, the `excess_bps` check may be stale. Reduce `_mark_cache_ttl_ms` or add per-tick oracle refresh for the fast candidates if this becomes an issue.

3. **Per-symbol thresholds need periodic re-derivation**: Run `compute_thresholds.py` after refreshing `fetch_data.py` output whenever the symbols or market structure changes materially.

4. **HL IOC price buffer (`HL_IOC_BUFFER_BPS = 5`)**: Set conservatively. For illiquid symbols like BIRD, you may want to increase this to 10–20bps to improve fill rate.

5. **BB is blocked** (`BLOCKED_SYMBOLS`): Aster BBUSDT tracks a different underlying than HL XYZ's xyz:BB — their mids run at a persistent ~50% divergence. Any other symbols showing `MAX_PRICE_RATIO_DIVERGENCE > 0.20` at startup will also be skipped.

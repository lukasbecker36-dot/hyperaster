# Perp-Spot Basis Trade: Aster DEX (Perps) vs MEXC (Spot)

## Project Overview

Build a Python (asyncio) system to identify and automatically capture **basis trades** (cash-and-carry arbitrage) between **Aster DEX perpetual futures** and **MEXC spot markets**.

The core trade: when a perp trades at a premium to spot, **short the perp on Aster** and **buy spot on MEXC**. Earn the basis (premium) as it converges, plus collect funding if it's favourable. Reverse for discounts: long the perp, short-sell spot (if borrowable on MEXC).

This is a sibling project to [hyperaster](https://github.com/lukasbecker36-dot/hyperaster), which runs perp-perp arbs between Hyperliquid and Aster. Reuse its architectural patterns, but the exchange clients, signal logic, and position management are new.

-----

## What Makes This Different from Perp-Perp

| | Perp-Perp (hyperaster) | Perp-Spot (this project) |
|---|---|---|
| Venues | Aster perp ↔ Hyperliquid perp | Aster perp ↔ MEXC spot |
| Convergence | Both legs are perps — spreads mean-revert but can diverge again | Perp converges to spot by construction (funding mechanism) |
| Funding | Net of two perp funding rates | Only one funding rate (Aster perp); spot has no funding |
| Carry | Can be headwind or tailwind | Funding IS the carry — short perp at premium = paid to hold |
| Spot leg | N/A | Must handle spot buy/sell, no leverage on spot side |
| Margin | Both legs margined (USDC + USDT) | Perp margined (USDT), spot fully funded (USDT) |
| Capital efficiency | ~2× leverage across both legs | ~1.5× (perp margined, spot fully funded) |
| Borrow risk | None | Short-sell spot requires borrow availability on MEXC |

-----

## Phase 1: Analysis

### What We're Looking For

A basis trade profits from the **premium/discount** of a perpetual future vs its spot price. The analysis must produce:

1. **Basis (premium/discount)** — `(perp_mark - spot_mid) / spot_mid` in bps. Positive = perp premium (short perp, buy spot). Negative = perp discount (long perp, sell spot).
2. **Funding carry** — Aster perps pay/charge funding every **8 hours**. When shorting a perp at premium, funding is typically received (positive carry). Decompose: is the basis mostly a one-off convergence or a persistent funding stream?
3. **Trading costs** — Aster perp fees (currently 0% maker / 0.09% taker during RWA Sprint) + MEXC spot fees (0.1% maker / 0.1% taker, reducible with MX token). Round-trip = entry + exit on both legs.
4. **Spot borrow cost** — For discount trades (long perp, short spot), MEXC charges borrow interest. Fetch hourly borrow rates per asset.
5. **Orderbook depth** — Both venues. Basis trades typically need larger notional than perp-perp arbs because the edge per dollar is smaller but more reliable.

### Fee Structure (verify via APIs)

| | Aster DEX (perps) | MEXC (spot) |
|---|---|---|
| Maker | 0% (RWA Sprint promo, may change) | 0% (current promo, verify) |
| Taker | 0.09 bps (0.0009%) | 0.05% (with MX deduction) |
| Funding | Every 8 hours | N/A (spot) |
| Margin asset | USDT | USDT |
| Borrow | N/A | Hourly rate, varies by asset |

### Key Questions

1. Which assets have persistent basis (perp premium/discount) on Aster?
2. How does the basis correlate with funding rate direction?
3. Are there time-of-day patterns (US market hours, Asian hours, weekends)?
4. What notional can the trade support given Aster perp depth and MEXC spot depth?
5. Is the basis large enough to cover round-trip fees on both legs?
6. For discount trades: is borrow available on MEXC? At what rate? Does the rate eat the edge?
7. How quickly does the basis converge — is this a minutes/hours trade or a days/weeks carry trade?

### Deliverables

- Historical basis analysis per asset (mean, median, std, percentiles, time series)
- Funding rate decomposition (how much of the edge is funding vs convergence)
- Fee-adjusted P&L projections at various holding periods
- Depth analysis — max notional per asset before slippage dominates

-----

## Phase 2: Automated Execution

### Architecture

Reuse the proven patterns from hyperaster:

```
┌─────────────────────────────────────────────┐
│              Basis Trade Engine              │
│                                              │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │ Basis    │→ │ Signal   │→ │ Execution │ │
│  │ Monitor  │  │ Generator│  │ Manager   │ │
│  └──────────┘  └──────────┘  └───────────┘ │
│       ↑              ↑             ↓         │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │ Aster WS │  │ Risk     │  │ Position  │ │
│  │ MEXC WS  │  │ Manager  │  │ Tracker   │ │
│  └──────────┘  └──────────┘  └───────────┘ │
└─────────────────────────────────────────────┘
```

### Execution Flow (modelled on hyperaster)

**Entry (premium trade: short Aster perp, buy MEXC spot):**

1. Signal fires: basis exceeds threshold for N consecutive ticks
2. Place MEXC spot buy (taker IOC — spot is the "safe" leg, fills are final)
3. If MEXC fills: place Aster perp short (GTX maker at best ask, or IOC taker if urgent)
4. If MEXC doesn't fill: no exposure, abort cleanly
5. If MEXC fills but Aster fails within timeout: sell MEXC spot to unwind

**Entry (discount trade: long Aster perp, short-sell MEXC spot):**

1. Check MEXC borrow availability first — abort if none
2. Place Aster perp long (GTX maker at best bid)
3. If Aster fills: short-sell on MEXC spot
4. If Aster doesn't fill within timeout: cancel, no exposure

**Exit:**

1. Close Aster perp (opposite side, GTX maker with IOC taker fallback)
2. Close MEXC spot (sell if long, buy-to-cover if short)
3. Paper mode: instant-fill at reference prices

### Leg Risk

Same principle as hyperaster — if one leg fills and the other doesn't:
- Filled leg must be unwound within a hard timeout
- Log every instance for post-trade analysis
- Prefer filling the more liquid leg first (usually MEXC spot)

### Key Differences from hyperaster Execution

1. **Spot leg has no leverage** — capital requirement is ~2× vs perp-perp (full notional on spot side)
2. **No oracle delta adjustment** — spot IS the reference price; basis = perp - spot directly
3. **Funding is one-sided** — only Aster perp has funding; this simplifies carry math
4. **Borrow management** — discount trades require monitoring MEXC borrow availability and auto-repaying on exit
5. **No "convergence" exit in the perp-perp sense** — basis trades exit when: basis narrows to target, funding collected exceeds costs, or max hold time reached

-----

## Technical Details

### Aster DEX API

Reuse the Aster client patterns from hyperaster (`exchange_client.py`, `auth.py`).

- **Base URL:** `https://fapi.asterdex.com`
- **API style:** Binance-compatible REST (V3 endpoints for orders)
- **Auth:** EIP-712 agent-signed scheme (see hyperaster `auth.py` for implementation)
- **Key endpoints:**
  - `GET /fapi/v1/exchangeInfo` — listed contracts, tick sizes, lot sizes
  - `GET /fapi/v1/ticker/bookTicker` — best bid/offer all symbols
  - `GET /fapi/v1/premiumIndex` — mark price, index price, funding rate
  - `GET /fapi/v1/fundingRate?symbol=X` — funding rate history
  - `GET /fapi/v1/depth?symbol=X&limit=20` — orderbook
  - `POST /fapi/v3/order` — place order (signed)
  - `GET /fapi/v3/openOrders` — query resting orders
  - `GET /fapi/v3/positionRisk` — current positions
- **Order types used:**
  - GTX (post-only maker) for passive entry/exit
  - IOC (taker) for urgent fills and force-closes
- **WebSocket:** `wss://fstream.asterdex.com/stream?streams=<symbol>@bookTicker`

### MEXC API

- **Docs:** https://mexcdevelop.github.io/apidocs/spot_v3_en/
- **Base URL:** `https://api.mexc.com`
- **API style:** Binance-compatible REST
- **Auth:** API key + secret, HMAC-SHA256 signed requests (standard CEX pattern)
- **Key endpoints:**
  - `GET /api/v3/exchangeInfo` — listed spot pairs, filters, lot sizes
  - `GET /api/v3/ticker/bookTicker` — best bid/offer
  - `GET /api/v3/depth?symbol=X&limit=20` — orderbook
  - `GET /api/v3/ticker/price` — last trade price
  - `POST /api/v3/order` — place order (signed)
  - `GET /api/v3/openOrders` — query resting orders
  - `GET /api/v3/account` — balances
  - `GET /api/v3/myTrades` — recent fills
- **Margin/borrow endpoints (for short-selling spot):**
  - `POST /api/v3/margin/order` — margin order
  - `POST /api/v3/margin/loan` — borrow
  - `POST /api/v3/margin/repay` — repay
  - `GET /api/v3/margin/isolated/account` — margin account info
  - `GET /api/v3/margin/interestRate` — borrow rates
- **WebSocket:** `wss://wbs.mexc.com/ws` — subscribe to `<symbol>@bookTicker`
- **Rate limits:** 20 requests/second for order endpoints, 100/s for market data. Respect `X-MBX-USED-WEIGHT` headers.
- **No official Python SDK** — write a thin async wrapper using `aiohttp`. The Binance-compatible API means patterns from the Aster client transfer directly.

### Account Setup (manual)

**Aster DEX:**
1. Same setup as hyperaster — deposit USDT, generate API wallet
2. Can reuse the same Aster account if capital allows

**MEXC:**
1. Create account on https://www.mexc.com
2. Complete KYC (required for API trading)
3. Enable spot trading + margin trading (for short-selling)
4. Generate API key + secret in account settings (enable spot + margin permissions)
5. Deposit USDT
6. Save API key and secret

-----

## Infrastructure & Deployment

### Server Setup (Hetzner, same as hyperaster)

Deploy on the existing Hetzner server alongside hyperaster, or on a separate VPS.

```
/opt/basis-trade/
├── .venv/                     # Python virtual environment
├── .env                       # API keys (gitignored)
├── data/
│   ├── positions.db           # SQLite position tracking
│   └── mode.env               # paper|live mode flag
├── output/                    # Historical data, candles, analysis
└── ... (source files)
```

### Systemd Services (same pattern as hyperaster)

Two services:

**basis-trade.service** (main monitor):
```ini
[Unit]
Description=Basis Trade Monitor (Aster perp vs MEXC spot)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/basis-trade
EnvironmentFile=-/opt/basis-trade/.env
EnvironmentFile=-/opt/basis-trade/data/mode.env
ExecStart=/opt/basis-trade/.venv/bin/python live_monitor.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**basis-trade-control.service** (Telegram bot):
```ini
[Unit]
Description=Basis Trade Telegram Control Bot
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/basis-trade
EnvironmentFile=-/opt/basis-trade/.env
ExecStart=/opt/basis-trade/.venv/bin/python control_bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Telegram Control Bot

Reuse the hyperaster control bot architecture. Same command set, adapted labels:

- `/status` — service state, uptime, open positions, current basis readings
- `/spreads` — current basis vs threshold for top candidates
- `/positions` — open basis positions with entry basis, current basis, P&L
- `/trades [n]` — last N closed trades with full P&L breakdown
- `/pnl` — realised P&L (today + all-time, live + paper)
- `/log [n]` — last N journal lines
- `/mode` — show paper/live
- `/paper` / `/live YES` — switch modes
- `/start` / `/stop YES` / `/restart` — service control
- `/flatten YES` — emergency close all positions

**Auth:** allowlist of Telegram chat IDs (`CONTROL_TELEGRAM_CHAT_IDS`).
**Destructive commands** (`/stop`, `/flatten`, `/live`) require typing `YES` within 60s.
**Mode switching:** write to `data/mode.env`, restart service.

### Environment Variables (.env)

```bash
# Aster DEX (reuse from hyperaster if same account)
ASTER_API_KEY=0x...          # Agent wallet address
ASTER_API_SECRET=0x...       # Agent private key
ASTER_WALLET_ADDRESS=0x...   # Main wallet

# MEXC
MEXC_API_KEY=mx0v...
MEXC_API_SECRET=...

# Telegram
ALERT_TELEGRAM_BOT_TOKEN=...
ALERT_TELEGRAM_CHAT_ID=...
CONTROL_TELEGRAM_CHAT_IDS=...

# Optional
ALERT_DISCORD_WEBHOOK_URL=...
```

-----

## Code Reuse from hyperaster

The following modules can be adapted directly (copy + modify, or import if published as a shared package):

| hyperaster module | What to reuse | What changes |
|---|---|---|
| `auth.py` | Aster EIP-712 signing, `now_ms()`, `load_api_keys()` | Add MEXC HMAC-SHA256 signing |
| `control_bot.py` | Full Telegram bot framework, command routing, YES-confirm pattern | Rename service, update display labels |
| `position_manager.py` | Position dataclass, state machine, DB schema, P&L calculation | Replace HL fields with MEXC fields, add borrow tracking |
| `database.py` | `init_db()`, `get_connection()`, migrations | Same schema adapted for basis trades |
| `intents.py` | Crash-safe intent-before-order pattern | Replace HL intents with MEXC intents |
| `recovery.py` | Startup reconciliation of incomplete intents | Adapt for MEXC spot + Aster perp |
| `notify.py` | Multi-channel alerting (Telegram, Discord, webhook) | Copy as-is |
| `executor.py` | Paper/live mode, GTX repricing loop, timeout escalation, ambiguous order handling | Replace HL leg with MEXC spot leg, remove oracle delta logic |
| `live_monitor.py` | Slow scan / fast tick / heartbeat loop structure, candidate ranking | Replace spread logic with basis logic, remove oracle delta |
| `config.py` | Config pattern, fee constants, threshold dict, blocked symbols | New fee schedule, basis thresholds |

### What NOT to reuse

- **Oracle delta smoothing** — not needed. Spot IS the reference price; there's no structural oracle feed gap.
- **Two-perp funding normalisation** — only one funding rate (Aster). Spot has none.
- **HL-specific signing** — replaced by MEXC HMAC signing.
- **Cross-venue ticker aliasing** (`_ASTER_TO_CANON`)  — MEXC spot tickers may need their own alias map.

-----

## Project Structure

```
basis-trade/
├── CLAUDE.md                  # This file
├── pyproject.toml
├── config.py                  # All tunables: fees, thresholds, sizing
├── auth.py                    # API key loading, Aster EIP-712, MEXC HMAC
├── live_monitor.py            # Main async loop (slow scan, fast tick, heartbeat)
├── executor.py                # Entry/exit execution, paper/live, leg risk
├── exchange_client.py         # Aster perp + MEXC spot REST wrappers
├── position_manager.py        # Position state machine, DB, P&L
├── database.py                # SQLite init + migrations
├── intents.py                 # Crash-safe intent logging
├── recovery.py                # Startup reconciliation
├── control_bot.py             # Telegram control bot
├── notify.py                  # Multi-channel alerting
├── data/                      # Runtime data (DB, mode file)
├── output/                    # Historical data, analysis outputs
├── scripts/
│   ├── fetch_data.py          # Historical basis data collection
│   ├── backtest_basis.py      # Historical basis trade backtester
│   └── probe_mexc.py          # MEXC API discovery/testing
└── .env                       # API keys (gitignored)
```

-----

## Configuration

```python
# ── Exchange endpoints ──
ASTER_BASE = "https://fapi.asterdex.com"
MEXC_BASE = "https://api.mexc.com"

# ── Fee schedule ──
ASTER_MAKER_FEE = 0.0         # RWA Sprint promo (verify)
ASTER_TAKER_FEE = 0.00009     # 0.9 bps
MEXC_MAKER_FEE = 0.0          # Current promo (verify)
MEXC_TAKER_FEE = 0.0005       # 5 bps (with MX deduction)

ROUND_TRIP_FEE = 2 * ASTER_MAKER_FEE + 2 * MEXC_TAKER_FEE  # both legs, entry + exit

# ── Strategy parameters ──
ENTRY_BASIS_BPS = 30.0          # Basis threshold to enter (adjustable per symbol)
EXIT_BASIS_BPS = 5.0            # Exit when basis narrows to this
ADVERSE_STOP_BPS = 20.0         # Hard stop if basis inverts past this
ENTRY_CONFIRM_TICKS = 3         # Consecutive qualifying ticks before entry
MAX_HOLD_HOURS = 168            # 1 week max hold (basis trades can be longer)
MIN_RAW_BASIS_BPS = 5.0         # Minimum raw basis (spot vs perp mark)

# ── Position sizing ──
NOTIONAL_PER_LEG = 1000         # USD per leg
MAX_CONCURRENT_POSITIONS = 3

# ── Execution ──
POLL_INTERVAL_SECONDS = 1
ASTER_FILL_POLL_SECONDS = 10
ENTRY_TIMEOUT_MINUTES = 60
EXIT_TIMEOUT_MINUTES = 30

# ── Paper mode ──
PAPER_MODE = True
```

-----

## Signal Logic

### Basis Calculation

```
basis_bps = (aster_perp_mid - mexc_spot_mid) / mexc_spot_mid * 10000
```

- **Positive basis** (perp premium): short Aster perp + buy MEXC spot
- **Negative basis** (perp discount): long Aster perp + short-sell MEXC spot (requires borrow)

Unlike perp-perp arbs, there is **no oracle delta adjustment**. The spot price IS the fundamental reference. The basis is the raw, tradeable edge.

### Entry Conditions

1. `abs(basis_bps) >= threshold` (per-symbol or global)
2. Basis direction consistent for `ENTRY_CONFIRM_TICKS` consecutive ticks
3. Basis exceeds dynamic cost floor (round-trip fees + bid-ask spreads + margin)
4. For discount trades: MEXC borrow is available and borrow rate doesn't eat the edge
5. Orderbook depth sufficient for target notional on both venues

### Exit Conditions

1. **Converged:** `abs(basis_bps) <= EXIT_BASIS_BPS` AND estimated gross P&L >= 0
2. **Adverse stop:** basis inverted past `ADVERSE_STOP_BPS` against position
3. **Timeout:** held longer than `MAX_HOLD_HOURS`
4. **Funding flip:** funding rate flipped and now costs more than basis provides (for carry trades)

-----

## Risk Considerations

- **Spot leg is fully funded** — no liquidation risk on MEXC spot, but capital is tied up
- **Perp leg has liquidation risk** — must monitor margin and add collateral if basis widens against us
- **Borrow recall** — MEXC can recall borrowed assets, forcing a buy-to-cover at any time
- **MEXC counterparty risk** — centralised exchange, deposits are at custodial risk
- **Aster smart contract risk** — same as hyperaster
- **Stablecoin risk** — both legs are USDT-denominated so no cross-stablecoin basis (simpler than hyperaster)
- **Market hours** — equity perp basis may behave differently during/outside US market hours
- **Funding rate volatility** — funding can swing, turning a carry trade into a cost

-----

## Code Style & Conventions

- Python 3.11+, fully typed with type hints
- `asyncio` throughout — no blocking calls in the event loop
- `aiohttp` for HTTP
- All monetary values as `Decimal`, never `float`
- Structured logging via stdlib `logging` (same as hyperaster)
- SQLite for position tracking (same schema pattern)
- Tests via `pytest` + `pytest-asyncio`

-----

## Development Sequence

1. **MEXC spot client** — build and test: fetch exchange info, orderbook, account balances, place/cancel spot orders, margin borrow/repay
2. **Aster client** — copy from hyperaster, verify it works standalone
3. **Data collection** — fetch historical basis (perp mark vs spot price) per asset, store as CSV/parquet
4. **Analysis** — basis distribution, funding decomposition, fee-adjusted P&L projections
5. **Execution engine** — live monitor + executor, paper mode first
6. **Control bot** — Telegram interface
7. **Paper trading** — run with real data, simulated fills
8. **Live (small size)** — deploy with minimal notional, monitor closely

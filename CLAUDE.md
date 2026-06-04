# Equity Perps Arbitrage: Hyperliquid ↔ Aster DEX

## Project Overview

Build a Python (asyncio) system to identify and automatically capture arbitrage opportunities on **equity perpetual futures** listed on both **Hyperliquid** (HIP-3 via trade.xyz) and **Aster DEX**.

The project has two phases:

1. **Analysis** — scan all overlapping equity perps, quantify arb P&L after all costs
1. **Execution** — fully automated bot that monitors and captures arbs in production

-----

## Phase 1: Arb Analysis

### What We’re Looking For

A “convergence arb” on equity perps means: buy the cheap perp on venue A, sell the expensive perp on venue B, and hold until the price gap narrows (or earn carry via funding differential while waiting). The analysis must produce a clear P&L picture per equity name that decomposes into:

1. **Convergence profit** — the mid-to-mid price difference between venues, minus the cost of crossing each venue’s bid-offer spread (you pay the spread on entry AND exit on both legs)
1. **Funding carry** — net funding received or paid while the position is held. Hyperliquid settles funding every **1 hour**; Aster every **8 hours**. The analysis must normalise these to a common period and compute the net. Funding can be a profit centre (if you’re long the venue that pays you and short the venue that charges you) or a cost
1. **Trading fees** — round-trip fees on both legs. These differ materially between venues

### Fee Structure (verify these are still current via the APIs)

|            |Hyperliquid (HIP-3 equity perps)|Aster DEX (stock perps)|
|------------|--------------------------------|-----------------------|
|Maker       |~0.05% (2× base 0.025%)         |0%                     |
|Taker       |~0.10% (2× base 0.05%)          |0%                     |
|Funding     |Every 1 hour                    |Every 8 hours          |
|Margin asset|USDC                            |USDT                   |

**Important:** Aster announced 0% maker/taker on stock perps (Dec 2025). Verify this is still live — it’s a promotional policy and may change. Hyperliquid HIP-3 fees may have volume-tier discounts; the deployer (trade.xyz) may also be in “growth mode” which reduces fees.

### Margin Currency Basis

Hyperliquid is USDC-margined; Aster is USDT-margined. Any arb P&L calculation must account for USDC/USDT basis risk. If the stablecoins depeg, your P&L can shift. For the analysis, fetch the live USDC/USDT rate and include the spread cost of converting between them.

### Known Equity Perps (discover the full current list via APIs)

**Hyperliquid (HIP-3, trade.xyz namespace):**

- `xyz:AAPL`, `xyz:NVDA`, `xyz:TSLA`, `xyz:GOOGL`, `xyz:AMZN`, `xyz:META`, `xyz:MSFT`, `xyz:COIN`, `xyz:PLTR`, `xyz:AMD`, `xyz:NFLX`, `xyz:HOOD`
- Also indices: `xyz:XYZ100` (Nasdaq-like), commodities: `xyz:GOLD`, `xyz:SILVER`

**Aster DEX (stock perps):**

- AAPLUSDT was the first listing; more have been added since
- Use `/fapi/v1/exchangeInfo` to discover the current full list and filter for stock-type contracts

The analysis should automatically find the **intersection** of equity names listed on both venues.

### Deliverables for Phase 1

- `src/data/` — modules to pull orderbook snapshots, funding rates, and fee schedules from both venues
- `src/analysis/` — arb scanner that computes per-equity:
  - Current mid-price spread (bps)
  - Bid-offer adjusted entry cost (bps) — assumes taker entry on both legs
  - Maker entry cost variant (bps) — assumes resting limit orders
  - Predicted hourly funding differential (bps/hr)
  - Net arb P&L at various holding periods (1hr, 4hr, 8hr, 24hr, 1wk)
  - Historical spread distribution (mean, median, std, percentiles)
- `notebooks/` — Jupyter notebook that presents the analysis cleanly with charts
- `reports/` — markdown or HTML summary of findings per equity name

### Key Questions the Analysis Must Answer

1. Which equity names have persistent price dislocations between venues?
1. Are the spreads wide enough to cover round-trip fees + bid-offer on both sides?
1. Is the funding differential a tailwind or headwind? Can you earn carry while waiting for convergence?
1. What’s the optimal holding period per name?
1. What notional size can the arb support given current orderbook depth?
1. Are there time-of-day patterns (US market hours vs overnight vs weekends)?

-----

## Phase 2: Automated Execution Bot

### Architecture

```
┌─────────────────────────────────────────────┐
│                 Arb Engine                   │
│                                              │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │ Spread   │→ │ Signal   │→ │ Execution │ │
│  │ Monitor  │  │ Generator│  │ Manager   │ │
│  └──────────┘  └──────────┘  └───────────┘ │
│       ↑              ↑             ↓         │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │ HL WS    │  │ Risk     │  │ Position  │ │
│  │ Aster WS │  │ Manager  │  │ Tracker   │ │
│  └──────────┘  └──────────┘  └───────────┘ │
└─────────────────────────────────────────────┘
```

- **Spread Monitor** — concurrent websocket feeds from both venues, maintains real-time best bid/offer per equity perp
- **Signal Generator** — fires when spread exceeds threshold (from Phase 1 calibration), net of fees + estimated slippage
- **Risk Manager** — enforces position limits, max notional, max loss, correlation limits, margin utilisation caps
- **Execution Manager** — places simultaneous orders on both venues (limit or IOC depending on urgency), handles partial fills, retries, and the “leg risk” problem
- **Position Tracker** — tracks open arb positions, monitors P&L, triggers unwinds on convergence or stop-loss

### Leg Risk

This is the critical risk. If you get filled on one venue but not the other, you have naked directional exposure to a stock. The bot must:

- Prefer IOC (immediate-or-cancel) or FOK (fill-or-kill) order types for simultaneous entry
- If one leg fills and the other doesn’t within N seconds, aggressively cross the spread to complete
- Have a hard timeout after which the filled leg is unwound at market
- Log every instance of leg risk for post-trade analysis

### Configuration

All thresholds, limits, and parameters must be configurable via a YAML/TOML config file, not hardcoded:

- Entry spread threshold per equity (bps)
- Exit/convergence target (bps)
- Max position size per equity (notional USD)
- Max total portfolio notional
- Max drawdown before kill switch
- Funding-aware mode (enter only when funding is favourable)
- Order type preferences (maker vs taker)
- Retry/timeout parameters

-----

## Technical Details

### Hyperliquid API

- **Docs:** <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api>
- **Python SDK:** `pip install hyperliquid-python-sdk` (official, sync) — for async consider wrapping with `asyncio.to_thread` or use the ccxt-based `pip install hyperliquid` which has native async
- **Mainnet API:** `https://api.hyperliquid.xyz`
- **Testnet API:** `https://api.hyperliquid-testnet.xyz`
- **Info endpoint:** POST to `/info` with JSON body for market data, orderbooks, funding rates, user state
- **Exchange endpoint:** POST to `/exchange` with signed JSON for order placement
- **WebSocket:** `wss://api.hyperliquid.xyz/ws` — subscribe to `l2Book`, `trades`, `userFills`, `activeAssetData`
- **Auth:** Ethereum-style signing with private key (EIP-712). API wallets can be generated at <https://app.hyperliquid.xyz/API> with limited permissions (trade but no withdraw)
- **HIP-3 equity perps** use the `xyz:` namespace (deployer is trade.xyz). They appear in `allMids`, orderbook, and funding endpoints under their full name (e.g. `xyz:NVDA`). Margin is isolated only on HIP-3 markets
- **Funding history:** POST `/info` with `{"type": "fundingHistory", "coin": "xyz:NVDA", "startTime": ...}`
- **Predicted funding:** POST `/info` with `{"type": "metaAndAssetCtxs"}` returns predicted rates

### Aster DEX API

- **Docs:** <https://docs.asterdex.com/product/aster-perpetuals/api/api-documentation>
- **Base URL:** `https://fapi.asterdex.com`
- **API style:** Binance-compatible REST (same endpoint patterns as Binance Futures)
- **Auth:** API key + secret + optional passphrase. HMAC-SHA256 signed requests
- **Key endpoints:**
  - `GET /fapi/v1/exchangeInfo` — all listed contracts, tick sizes, lot sizes
  - `GET /fapi/v1/depth?symbol=AAPLUSDT&limit=20` — orderbook
  - `GET /fapi/v1/premiumIndex?symbol=AAPLUSDT` — mark price, index price, funding rate
  - `GET /fapi/v1/fundingRate?symbol=AAPLUSDT` — funding rate history
  - `GET /fapi/v1/ticker/bookTicker?symbol=AAPLUSDT` — best bid/offer
  - `POST /fapi/v1/order` — place order (signed)
- **WebSocket:** `wss://fstream.asterdex.com/ws/<listenKey>` (Binance-style user data stream), market streams at `wss://fstream.asterdex.com/stream?streams=aaplusdt@bookTicker`
- **No official Python SDK** — write a thin async wrapper using `aiohttp` or `httpx`. The Binance-compatible API means `python-binance` or `ccxt` may work with base URL override, but verify thoroughly before trusting
- **Stock perps** appear as regular symbols in exchangeInfo (e.g. `AAPLUSDT`, `NVDAUSDT`). Filter by `underlyingType` or naming convention to identify equity vs crypto perps

### Account Setup (user must do this manually)

**Hyperliquid:**

1. Create wallet on <https://app.hyperliquid.xyz>
1. Deposit USDC (bridged from Arbitrum)
1. Generate API wallet at <https://app.hyperliquid.xyz/API>
1. Save the API private key and your main wallet public address

**Aster DEX:**

1. Create account on <https://www.asterdex.com>
1. Deposit USDT
1. Generate API key + secret in account settings
1. Save API key, secret, and passphrase if applicable

-----

## Project Structure

```
equity-perps-arb/
├── CLAUDE.md                  # This file
├── pyproject.toml             # Poetry or pip project config
├── config/
│   ├── default.toml           # Default configuration
│   └── secrets.toml           # API keys (gitignored)
├── src/
│   ├── __init__.py
│   ├── exchanges/
│   │   ├── __init__.py
│   │   ├── base.py            # Abstract exchange interface
│   │   ├── hyperliquid.py     # HL client (REST + WS)
│   │   └── aster.py           # Aster client (REST + WS)
│   ├── data/
│   │   ├── __init__.py
│   │   ├── orderbook.py       # Orderbook snapshots & spread calc
│   │   ├── funding.py         # Funding rate fetching & normalisation
│   │   └── fees.py            # Fee schedule management
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── scanner.py         # Cross-venue arb scanner
│   │   ├── pnl.py             # P&L decomposition (convergence, funding, fees)
│   │   ├── depth.py           # Orderbook depth / capacity analysis
│   │   └── historical.py      # Historical spread analysis
│   ├── engine/
│   │   ├── __init__.py
│   │   ├── monitor.py         # Real-time spread monitor
│   │   ├── signal.py          # Signal generation
│   │   ├── executor.py        # Order execution with leg-risk management
│   │   ├── risk.py            # Risk manager
│   │   └── position.py        # Position tracker & P&L
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── logging.py         # Structured logging setup
│   │   └── types.py           # Shared dataclasses / types
│   └── main.py                # Bot entry point
├── notebooks/
│   └── arb_analysis.ipynb     # Phase 1 analysis notebook
├── tests/
│   ├── test_exchanges.py
│   ├── test_pnl.py
│   └── test_executor.py
├── scripts/
│   ├── fetch_snapshot.py      # One-shot data fetch for analysis
│   └── scan_arbs.py           # CLI arb scanner
└── .gitignore
```

-----

## Code Style & Conventions

- Python 3.11+, fully typed with type hints
- `asyncio` throughout — no blocking calls in the event loop
- Use `aiohttp` for HTTP, native websocket libraries for WS feeds
- Dataclasses or Pydantic models for all data structures
- Structured logging via `structlog` — every trade, signal, and error must be logged with context
- All monetary values as `Decimal`, never `float`
- Config via `tomli` / `tomllib`
- Tests via `pytest` + `pytest-asyncio`

-----

## Risk Warnings & Constraints

- **Regulatory:** equity perps on DEXs are in a grey area. The user is responsible for their own regulatory compliance. The bot should never be described as financial advice
- **Liquidity:** equity perps on both venues are significantly less liquid than crypto majors. Orderbook depth can be thin — always check depth before sizing
- **Oracle risk:** both platforms use oracle price feeds (Pyth, Chainstack, etc.) which can lag or diverge from real equity prices, especially outside US market hours and on weekends
- **Smart contract risk:** both venues carry smart contract / platform risk
- **USDC/USDT basis:** a depeg of either stablecoin creates P&L risk on the cross-venue position
- **Funding rate changes:** funding rates can swing quickly and turn a carry trade into a cost
- **Weekend/after-hours:** oracle prices may be stale outside US equity hours (9:30-16:00 ET). The analysis should flag whether arbs that appear overnight are real or artefacts of stale pricing

-----

## Development Sequence

### Step 1: Exchange Clients

Build and test the `exchanges/` layer first. Verify you can:

- Fetch all listed equity perps from both venues
- Pull orderbook snapshots with bid/offer
- Pull current and historical funding rates
- Pull fee schedules

### Step 2: Data Collection

Write scripts to collect snapshots over time. Store as parquet or SQLite for analysis.

### Step 3: Analysis

Build the P&L decomposition logic and run it across all equity names. Produce the notebook and report.

### Step 4: Execution Engine

Only after Phase 1 shows viable arbs. Build the real-time monitor, signal generator, and executor. Test extensively on testnets / paper trading before going live.

### Step 5: Paper Trading

Run the full bot with real market data but simulated fills. Validate P&L matches Phase 1 expectations.

### Step 6: Live (small size)

Deploy with minimal position sizes. Monitor closely. Scale up only after confirming edge.

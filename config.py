"""Configuration for the Hyperliquid XYZ vs Aster equity perp arb live monitor."""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "positions.db")

# ── Exchange endpoints ──
HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"
HL_EXCHANGE_URL = "https://api.hyperliquid.xyz/exchange"
ASTER_BASE = "https://fapi.asterdex.com"
ASTER_ORDER_URL = f"{ASTER_BASE}/fapi/v3/order"
ASTER_OPEN_ORDERS_URL = f"{ASTER_BASE}/fapi/v3/openOrders"
ASTER_POSITION_URL = f"{ASTER_BASE}/fapi/v3/positionRisk"
ASTER_EXCHANGE_INFO_URL = f"{ASTER_BASE}/fapi/v3/exchangeInfo"

# ── Fee schedule ──
# Hyperliquid XYZ: base taker 4.5bps / maker 1.5bps
# (adjust to 0.45 / 0.15 if confirmed on HIP-3 growth mode)
HL_TAKER_FEE = 0.00045
HL_MAKER_FEE = 0.00015

# Aster: RWA Sprint Season 1 (May-Jun 2026) - 0bps maker, 0.9bps taker
ASTER_MAKER_FEE = 0.0
ASTER_TAKER_FEE = 0.00009

# Round-trip: aster maker both legs + HL taker both legs
ROUND_TRIP_FEE = 2 * ASTER_MAKER_FEE + 2 * HL_TAKER_FEE  # ~0.09% = 9bps

# Minimum net executable premium (bps) after subtracting smoothed oracle delta.
# Belt-and-suspenders floor: prevents entries where oracle delta noise inflates the
# apparent excess during the first few ticks before the rolling median stabilises.
MIN_EXECUTABLE_PREMIUM_BPS = 15.0

# Minimum RAW book mid-spread (abs) in bps before an entry is considered.
# A weak sanity filter: there must be at least *some* gap between the venues,
# not just a baseline-relative wiggle. At ROUND_TRIP_FEE ~9bps a 5bps gap can
# never cover costs, so 12bps is a safe floor.
MIN_RAW_PREMIUM_BPS = 12.0

# ── Rolling book-spread baseline ──
# The live entry signal trades deviations of the book mid-spread from its own
# rolling median (NOT the venue oracle feeds — we trade the books, so we
# baseline against the books). excess = current_mid_spread - rolling_median.
# Window validated via scripts/backtest_1m.py --window-sweep: profitable across
# 30m-24h; 8h chosen for stability and resistance to the "dislocation slowly
# becomes the new baseline" failure mode of short windows.
BASELINE_WINDOW_MINUTES = 480          # 8h rolling median
BASELINE_MIN_SAMPLES = 30              # need this many before a baseline is usable
BASELINE_SAMPLE_INTERVAL_SECONDS = 55  # dedupe live samples to ~1/min (matches 1m candles)

# ── Strategy parameters ──
# Spread in bps above which we enter.  Round-trip cost ~9-14bps so 30bps = ~2x cushion.
ENTRY_THRESHOLD_BPS = 30.0
# Exit when spread falls below this (in bps, absolute cross-exchange mid spread)
EXIT_THRESHOLD_BPS = 8.0
# Profit-target exit: close when estimated net P&L (gross - fees + funding) reaches this.
EXIT_TARGET_NET_USD = 3.0

# Per-symbol exit targets. Names with high median peak reversion warrant a higher
# target to capture blowouts. Derived from scripts/backtest_1m.py --peak-analysis.
EXIT_TARGET_NET_USD_BY_SYMBOL: dict = {
}

# Number of consecutive qualifying scans the entry signal must persist before we
# commit capital. A genuine dislocation holds across ticks; a stale-feed/oracle-lag
# phantom (e.g. CBRS entering at +41bps then inverting to -25bps within minutes)
# evaporates the moment the feeds catch up. Filtering these out is the single biggest
# lever against fee-bleeding churn (every round trip costs ~9bps in HL taker fees).
ENTRY_CONFIRM_TICKS = 3

# Hard stop: bail a held position immediately if its OWN-direction executable excess
# inverts past this (negative) level. Protects against a phantom entry whose edge
# reverses hard before the normal converge-exit at EXIT_THRESHOLD_BPS would fire.
ADVERSE_STOP_BPS = 20.0

# Dynamic cost floor on entry. The per-symbol p75 thresholds capture "is the spread
# statistically wide?" but not "is it wide enough to profit after costs?". On exit we
# pay round-trip fees PLUS cross the bid-ask spread on both venues again. So require:
#   effective_threshold = max(p75_threshold, fee_bps + (aster_spread + hl_spread) + margin)
# This auto-lifts thin-book names where the p75 sits below breakeven, and is
# self-maintaining as liquidity changes — no per-ticker recalibration needed.
ENTRY_COST_MARGIN_BPS = 5.0

# Abandon entry (close HL leg) if Aster maker hasn't filled within this many minutes
ENTRY_TIMEOUT_MINUTES = 60
# Force-close Aster exit leg with a taker IOC if maker hasn't filled within this many minutes
# (pays ~0.9bps Aster taker once, but escapes being stuck one-legged while spread runs away)
EXIT_TIMEOUT_MINUTES = 30
# Force-close position after this many hours regardless of spread
MAX_HOLD_HOURS = 12
# Bail early if cumulative funding cost exceeds this USD amount — prevents
# slow bleed on positions where carry eats the entire potential profit.
MAX_FUNDING_DRAG_USD = 2.0

# ── Position sizing ──
NOTIONAL_PER_LEG = 1000          # USD per leg
MAX_CONCURRENT_POSITIONS = 8     # max simultaneous positions across all symbols

# ── Execution ──
POLL_INTERVAL_SECONDS = 1        # main loop interval
ASTER_FILL_POLL_SECONDS = 10     # how often to poll pending Aster maker orders for fills
HL_IOC_BUFFER_BPS = 5            # bps above/below current price for HL IOC limit
ASTER_IOC_BUFFER_BPS = 5         # bps past best for Aster IOC (force-close) limit
ORDER_TIMEOUT_SECONDS = 8        # HTTP request timeout

# ── Paper mode ──
PAPER_MODE = True                # set False for live execution

# ── Alerting ──
HEARTBEAT_INTERVAL_MINUTES = 60

# ── Per-symbol entry thresholds (bps) ──
# Derived from p75 of historical cross-exchange spread, rounded to nearest 5bps.
# Symbols not listed fall back to ENTRY_THRESHOLD_BPS.
ENTRY_THRESHOLD_BPS_BY_SYMBOL: dict = {
    "NBIS":    195,  # excess p75=193bps  oracle_delta=-58bps
    "ARM":     105,  # excess p75=103bps  oracle_delta=-23bps
    "RKLB":     95,  # excess p75=97bps   oracle_delta=-4bps
    "PLTR":     85,  # excess p75=87bps   oracle_delta=+3bps
    "SKHX":     85,  # excess p75=87bps   oracle_delta=-53bps  (Korean: SK Hynix)
    "SMSN":     85,  # excess p75=85bps   oracle_delta=-43bps  (Korean: Samsung)
    "DRAM":     80,  # excess p75=78bps   oracle_delta=-18bps
    "DELL":     75,  # excess p75=77bps   oracle_delta=-8bps
    "AMD":      70,  # excess p75=69bps   oracle_delta=-4bps
    "ORCL":     70,  # excess p75=71bps   oracle_delta=+1bps
    "MRVL":     45,  # excess p75=47bps   oracle_delta=-5bps
    "CBRS":     40,  # excess p75=41bps   oracle_delta=-16bps
    "INTC":     40,  # excess p75=38bps   oracle_delta=-0bps
    "MSTR":     40,  # excess p75=40bps   oracle_delta=-4bps
    "SNDK":     35,  # excess p75=37bps   oracle_delta=+4bps
    "AAPL":     30,  # (default)
    "AMZN":     30,  # (default)
    "CRCL":     30,  # (default)
    "GOOGL":    30,  # (default)
    "MU":       30,  # (default)
    "NVDA":     30,  # (default)
    "TSLA":     30,  # (default)
}

# ── Cross-venue ticker aliases ──
# The canonical base symbol equals HL's xyz coin suffix (e.g. "SMSN" from
# "xyz:SMSN"). For most names Aster uses the same base + "USDT". A few names
# differ: HL uses a short/GDR ticker while Aster's *tradeable* book uses the
# long name, and Aster's name-matching contract is a dead listing that 400s.
#   - Samsung:  HL xyz:SMSN  <-> Aster SAMSUNGUSDT  (SMSNUSDT is dead)
#   - SK Hynix: HL xyz:SKHX  <-> Aster SKHYNIXUSDT  (SKHXUSDT is dead)
# Map: canonical base -> Aster's tradeable base symbol.
ASTER_BASE_ALIAS: dict = {
    "SMSN": "SAMSUNG",
    "SKHX": "SKHYNIX",
}
# Reverse map for discovery / spec loading: Aster base -> canonical base.
ASTER_BASE_TO_CANON: dict = {v: k for k, v in ASTER_BASE_ALIAS.items()}


def aster_symbol_for(base: str) -> str:
    """Aster API symbol for a canonical base, honouring cross-venue aliases."""
    return f"{ASTER_BASE_ALIAS.get(base, base)}USDT"


# ── Non-equity exclusions ──
# Commodities, FX, indices, ETFs, and tokens that appear in the XYZ/Aster
# overlap but are not single-stock equity perps.
NON_EQUITY_SYMBOLS: set = {
    # Commodities & FX
    "ALUMINIUM", "BRENTOIL", "CL", "COPPER", "CORN", "DXY", "EUR", "GBP",
    "GOLD", "JPY", "KRW", "NATGAS", "PALLADIUM", "PLATINUM", "SILVER",
    "TTF", "URANIUM", "WHEAT", "VIX", "VOL",
    # Indices
    "SP500", "JP225", "NIFTY", "IBOV", "KR200", "XYZ100",
    # ETFs
    "EWJ", "EWT", "EWY", "EWZ", "URNM", "XLE", "USAR",
}

# ── Sanity / blocklist ──
# Reject entry if the two exchange mids differ by more than this fraction
# (catches mismatched instruments where Aster and HL track different underlyings)
MAX_PRICE_RATIO_DIVERGENCE = 0.20   # 20%
# Symbols permanently excluded from the scanner and monitor
# BB: mismatched instruments vs HL XYZ
# Peak-analysis blocklist: names with negative median peak reversion consistently
# lose money after fees — the spread widens further instead of reverting.
# Derived from scripts/backtest_1m.py --peak-analysis (48h, 480m baseline).
BLOCKED_SYMBOLS: set = {
    "BB", "BIRD",
    "META", "COIN", "LLY", "IBM", "MSFT", "URNM", "BABA", "AVGO",
    "CRWV", "EWT", "BX", "USAR", "WDC",
    # 2026-06-15 peak-analysis blocklist refresh (48h, 480m baseline):
    # Negative median peak reversion — spread widens instead of reverting
    "NFLX", "RIVN", "LITE", "HYUNDAI", "NOW", "TSM", "HOOD", "HIMS",
    # New listings with no data, unusable spreads, or no liquidity
    "GME", "EBAY", "COST", "BE", "NOK", "MINIMAX", "SPCX",
}

# ── Backward-compat aliases (fetch_data.py / live_scan.py) ──
ASTER_API = ASTER_BASE
CANDLE_INTERVAL = "1h"
HISTORY_DAYS = 30
HYPERLIQUID_MAKER_FEE = HL_MAKER_FEE
HYPERLIQUID_TAKER_FEE = HL_TAKER_FEE

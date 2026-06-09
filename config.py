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

# ── Strategy parameters ──
# Spread in bps above which we enter.  Round-trip cost ~9-14bps so 30bps = ~2x cushion.
ENTRY_THRESHOLD_BPS = 30.0
# Exit when spread falls below this (in bps, absolute cross-exchange mid spread)
EXIT_THRESHOLD_BPS = 8.0
# Abandon entry (close HL leg) if Aster maker hasn't filled within this many minutes
ENTRY_TIMEOUT_MINUTES = 60
# Force-close position after this many hours regardless of spread
MAX_HOLD_HOURS = 48

# ── Position sizing ──
NOTIONAL_PER_LEG = 1000          # USD per leg
MAX_CONCURRENT_POSITIONS = 3     # max simultaneous positions across all symbols

# ── Execution ──
POLL_INTERVAL_SECONDS = 1        # main loop interval
ASTER_FILL_POLL_SECONDS = 10     # how often to poll pending Aster maker orders for fills
HL_IOC_BUFFER_BPS = 5            # bps above/below current price for HL IOC limit
ORDER_TIMEOUT_SECONDS = 8        # HTTP request timeout

# ── Paper mode ──
PAPER_MODE = True                # set False for live execution

# ── Alerting ──
HEARTBEAT_INTERVAL_MINUTES = 60

# ── Per-symbol entry thresholds (bps) ──
# Derived from p75 of historical cross-exchange spread, rounded to nearest 5bps.
# Symbols not listed fall back to ENTRY_THRESHOLD_BPS.
ENTRY_THRESHOLD_BPS_BY_SYMBOL: dict = {
    "BIRD":    260,  # excess p75=260bps  oracle_delta=-123bps
    "HIMS":    175,  # excess p75=177bps  oracle_delta=+19bps
    "CRWV":    140,  # excess p75=141bps  oracle_delta=-6bps
    "NOW":     115,  # excess p75=113bps  oracle_delta=+12bps
    "ARM":     100,  # excess p75=102bps  oracle_delta=-24bps
    "HYUNDAI": 100,  # excess p75=102bps  oracle_delta=-23bps
    "RKLB":    100,  # excess p75=98bps   oracle_delta=-4bps
    "DRAM":     95,  # excess p75=93bps   oracle_delta=-20bps
    "PLTR":     85,  # excess p75=85bps   oracle_delta=+1bps
    "COIN":     80,  # excess p75=81bps   oracle_delta=-13bps
    "LITE":     75,  # excess p75=76bps   oracle_delta=-17bps
    "ORCL":     70,  # excess p75=69bps   oracle_delta=-0bps
    "AMD":      65,  # excess p75=64bps   oracle_delta=-3bps
    "DELL":     60,  # excess p75=58bps   oracle_delta=-8bps
    "HOOD":     60,  # excess p75=62bps   oracle_delta=+1bps
    "BABA":     55,  # excess p75=55bps   oracle_delta=-16bps
    "AVGO":     50,  # excess p75=52bps   oracle_delta=+2bps
    "IBM":      50,  # excess p75=52bps   oracle_delta=-12bps
    "MRVL":     45,  # excess p75=46bps   oracle_delta=-7bps
    "CBRS":     40,  # excess p75=40bps   oracle_delta=-16bps
    "INTC":     40,  # excess p75=40bps   oracle_delta=+0bps
    "META":     40,  # excess p75=41bps   oracle_delta=-2bps
    "MSTR":     40,  # excess p75=42bps   oracle_delta=-4bps
    "SNDK":     40,  # excess p75=38bps   oracle_delta=+4bps
    "MSFT":     35,  # excess p75=33bps   oracle_delta=-4bps
    "AAPL":     30,  # (default)
    "AMZN":     30,  # (default)
    "CRCL":     30,  # (default)
    "GOOGL":    30,  # (default)
    "MU":       30,  # (default)
    "NVDA":     30,  # (default)
    "TSLA":     30,  # (default)
    "TSM":      30,  # (default)
}

# ── Sanity / blocklist ──
# Reject entry if the two exchange mids differ by more than this fraction
# (catches mismatched instruments where Aster and HL track different underlyings)
MAX_PRICE_RATIO_DIVERGENCE = 0.20   # 20%
# Symbols permanently excluded from the scanner and monitor
BLOCKED_SYMBOLS: set = {"BB"}  # mismatched instruments vs HL XYZ

# ── Backward-compat aliases (fetch_data.py / live_scan.py) ──
ASTER_API = ASTER_BASE
CANDLE_INTERVAL = "1h"
HISTORY_DAYS = 30
HYPERLIQUID_MAKER_FEE = HL_MAKER_FEE
HYPERLIQUID_TAKER_FEE = HL_TAKER_FEE

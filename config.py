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
ASTER_BALANCE_URL = f"{ASTER_BASE}/fapi/v3/balance"
ASTER_EXCHANGE_INFO_URL = f"{ASTER_BASE}/fapi/v3/exchangeInfo"
ASTER_LEVERAGE_URL = f"{ASTER_BASE}/fapi/v1/leverage"
ASTER_MARGIN_TYPE_URL = f"{ASTER_BASE}/fapi/v1/marginType"

# ── Leverage & margin ──
# Applied per symbol on the first live entry (HL via updateLeverage isolated,
# Aster via /leverage + /marginType). HL HIP-3 markets are isolated-only.
LEVERAGE = 5
ASTER_MARGIN_TYPE = "ISOLATED"        # ISOLATED | CROSSED

# ── Fee schedule ──
# Hyperliquid XYZ: base taker 4.5bps / maker 1.5bps
# (adjust to 0.45 / 0.15 if confirmed on HIP-3 growth mode)
HL_TAKER_FEE = 0.00045
HL_MAKER_FEE = 0.00015

# Aster: RWA Sprint Season 1 (May-Jun 2026) - 0bps maker, 0.9bps taker
ASTER_MAKER_FEE = 0.0
ASTER_TAKER_FEE = 0.00009

# Round-trip: convergence arb pays HL taker + Aster maker on both legs
ROUND_TRIP_FEE = 2 * ASTER_MAKER_FEE + 2 * HL_TAKER_FEE  # ~0.09% = 9bps
# Carry trades execute HL maker + Aster taker on both legs
CARRY_ROUND_TRIP_FEE = 2 * HL_MAKER_FEE + 2 * ASTER_TAKER_FEE  # ~0.048% ≈ 4.8bps

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

# ── Oracle fair-value correction ──
# The entry/exit signal baselines the book spread against its own 8h median.
# That median is purely historical, so a genuine shift in FAIR VALUE (an oracle
# methodology change, a real re-rate of the underlying) is misread as tradeable
# "excess" for up to BASELINE_WINDOW hours until the median catches up — and you
# enter against a move that won't revert. The HL-oracle-minus-Aster-index delta
# is a zero-lag fair-value anchor: correcting the book baseline by how far the
# oracle delta has drifted from its own 8h norm distinguishes a convergeable
# book dislocation (trade it) from a fair-value shift (don't).
#   fair_spread = median_book_spread_8h + (oracle_delta_now − median_oracle_delta_8h)
#   excess      = book_spread_now − fair_spread
# When oracle history is insufficient (or this is off), the correction is 0 and
# the signal degrades to the pure book baseline (prior behaviour).
ORACLE_CORRECTION_ENABLED = True
ORACLE_BASELINE_MIN_SAMPLES = 20       # oracle-delta samples in-window before the correction is trusted

# ── Oracle staleness / market-hours guard ──
# Equity oracles stop ticking outside US market hours and on weekends. A book
# "dislocation" with a frozen oracle has no arbitrageable anchor and often can't
# converge until the oracle re-anchors (meanwhile mark-price margining can bleed
# the position). Skip AUTO entries when the HL oracle price hasn't moved in this
# many minutes. Only applies once oracle tracking is established for a symbol —
# a name with no oracle history yet is not blocked (other guards still apply).
ORACLE_STALENESS_GUARD_ENABLED = True
ORACLE_STALE_MINUTES = 15

# ── Executable (bid/offer) entry signal ──
# The raw signal measures the book MID spread and adds a bid-ask cost floor to
# the threshold. But you don't trade mids — you rest the HL leg as a maker and
# cross Aster as a taker, so the real entry edge is the executable HL-maker/
# Aster-taker basis (see _carry_basis_bps). That basis decomposes exactly as
#   exec_basis_dir = ±mid_spread + half_diff,   half_diff = (hl_spread − aster_spread)/2
# so the mid part keeps its candle-warmed 8h baseline while the half-spread
# differential gets its own live baseline. The entry excess then becomes the
# deviation of the EXECUTABLE basis from its structural norm — which neutralises
# mid-spread phantoms caused by transient one-sided books (a book widening on one
# side moves the mid but not the executable price). half_diff can't be warmed from
# candles, so until its baseline fills the correction is 0 and the signal is the
# prior mid-spread deviation (no regression at cold start).
EXECUTABLE_SIGNAL_ENABLED = True

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

# ── Manual funding-carry holds ──
# Positions opened via the manual /enter command (hold_for_funding=1) are held
# for funding carry, NOT basis convergence: the target/converge exits are
# disabled so they don't close the moment the basis reverts. Only safety exits
# apply — a longer max-hold timeout and a hard mark-to-market stop.
FUNDING_MAX_HOLD_HOURS = 168          # 1 week safety timeout for a funding hold
FUNDING_ADVERSE_STOP_USD = 25.0       # bail a funding hold if executable loss exceeds this

# Carry trades execute maker-first: the HL leg rests as a post-only maker and
# the Aster leg crosses (IOC) to hedge each HL fill. If the HL maker hasn't
# fully filled within this long, cancel the resting remainder (any filled
# portion is kept, already hedged on Aster).
MAKER_ENTRY_TIMEOUT_SEC = 300         # 5 min to fill the resting HL maker, else give up the rest
MAKER_REPRICE_TICK_FRAC = 0.5         # reprice the HL maker if it drifts > this×tick from the touch
# Carry exits also run maker-first (HL post-only sell/buy, Aster IOC taker hedge).
# If the resting HL exit maker hasn't fully filled within this long, cross the
# unfilled remainder as a taker to complete the exit (we asked to get out).
MAKER_EXIT_TIMEOUT_SEC = 300          # 5 min to fill the resting HL exit maker, else taker-complete

# A manual /enter or /close can carry a basis target (bps) — the trade only
# executes once the executable basis (from bid/ask, in the position's favour) is
# at or better than the target, so you don't cross at a bad level. A gated entry
# that never reaches its target expires after this long; gated exits never expire
# (a safety stop closes the position if the basis stays bad).
MANUAL_ENTRY_GATE_TIMEOUT_MIN = 120

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

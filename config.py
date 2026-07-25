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
ASTER_INCOME_URL = f"{ASTER_BASE}/fapi/v1/income"

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

# Aster: RWA Sprint Season 1 (May-Jun 2026) - 0bps maker, 0.9bps taker.
# NOTE: the 0/0.9bp schedule is the STOCK-perp promo. Aster CRYPTO perps charge
# the standard schedule — set ASTER_CRYPTO_*_FEE below — so crypto round trips
# cost more than equity ones. Per-symbol fees come from the *_fee(symbol) helpers.
ASTER_MAKER_FEE = 0.0
ASTER_TAKER_FEE = 0.00009
# Aster standard (crypto) schedule — confirmed on-account: 0bp maker, 4bp taker.
ASTER_CRYPTO_MAKER_FEE = 0.0       # 0 bps
ASTER_CRYPTO_TAKER_FEE = 0.0004    # 4.0 bps

# Round-trip: convergence arb pays HL taker + Aster maker on both legs
ROUND_TRIP_FEE = 2 * ASTER_MAKER_FEE + 2 * HL_TAKER_FEE  # ~0.09% = 9bps
# Carry trades execute HL maker + Aster taker on both legs
CARRY_ROUND_TRIP_FEE = 2 * HL_MAKER_FEE + 2 * ASTER_TAKER_FEE  # ~0.048% ≈ 4.8bps
# Crypto variants (HL main-dex + Aster standard schedule)
CRYPTO_ROUND_TRIP_FEE = 2 * ASTER_CRYPTO_MAKER_FEE + 2 * HL_TAKER_FEE
CRYPTO_CARRY_ROUND_TRIP_FEE = 2 * HL_MAKER_FEE + 2 * ASTER_CRYPTO_TAKER_FEE

# Master switch for LIVE HL crypto (main-dex) order placement. Reads/analysis of
# crypto are always on; this gates actually PLACING crypto orders. Enabled by
# default now that the dex router + leg-risk safety nets route by dex; set
# CRYPTO_TRADING_ENABLED=false in the env to disable without a code change.
CRYPTO_TRADING_ENABLED = os.getenv("CRYPTO_TRADING_ENABLED", "true").lower() in ("1", "true", "yes")


def _is_crypto_symbol(symbol: str) -> bool:
    """Best-effort crypto classification for fee selection in contexts without a
    live client (scripts, P&L). Sources, in order: the crypto set the live bot
    persists on discovery (data/crypto_symbols.json), else False (equity).
    The live trader passes authoritative per-symbol info where it can."""
    return symbol in _load_crypto_symbols()


_CRYPTO_SET_CACHE: dict = {"ts": 0.0, "set": set()}


def _load_crypto_symbols() -> set:
    """Cached read of data/crypto_symbols.json (written by the trader/capture on
    discovery). Refreshed every 60s. Empty set if the file is absent."""
    import json
    import time as _t
    now = _t.time()
    if now - _CRYPTO_SET_CACHE["ts"] < 60 and _CRYPTO_SET_CACHE["set"]:
        return _CRYPTO_SET_CACHE["set"]
    path = os.path.join(DATA_DIR, "crypto_symbols.json")
    try:
        with open(path) as fh:
            syms = set(json.load(fh))
    except Exception:
        syms = set()
    _CRYPTO_SET_CACHE.update(ts=now, set=syms)
    return syms


def save_crypto_symbols(syms) -> None:
    """Persist the discovered main-dex crypto set so fee helpers in client-less
    contexts (scripts, P&L) classify correctly. Atomic write; best-effort."""
    import json
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        path = os.path.join(DATA_DIR, "crypto_symbols.json")
        tmp = path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(sorted(syms), fh)
        os.replace(tmp, path)
        _CRYPTO_SET_CACHE.update(ts=0.0, set=set(syms))  # invalidate cache
    except Exception:
        pass


def carry_round_trip_fee(symbol: str) -> float:
    """Per-symbol maker-HL/taker-Aster round-trip fee (fraction). Crypto pays the
    Aster standard taker; equities pay the stock-perp promo."""
    return CRYPTO_CARRY_ROUND_TRIP_FEE if _is_crypto_symbol(symbol) else CARRY_ROUND_TRIP_FEE


def round_trip_fee(symbol: str) -> float:
    """Per-symbol taker-taker round-trip fee (fraction), crypto-aware."""
    return CRYPTO_ROUND_TRIP_FEE if _is_crypto_symbol(symbol) else ROUND_TRIP_FEE

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

# ── Thin-book liquidity guard ──
# Skip AUTO entries on names whose books are too thin/unstable to trade — the
# class of junk (e.g. ZHIPU) that produces wild tick-to-tick prices, big
# divergence losses, and unexecutable paper P&L. Two checks, both venues:
#   - top-of-book notional (size×price on the thinner side) must clear the floor
#   - each venue's own bid-ask spread must be under the cap
# 0 disables a check. Manual /enter and /drip are NOT gated (deliberate).
LIQUIDITY_GUARD_ENABLED = True
MIN_TOB_NOTIONAL_USD = 20.0      # min top-of-book depth on the thinner side, each venue
MAX_VENUE_SPREAD_BPS = 150.0     # reject if either venue's own spread exceeds this

# After a symbol exits on a stop (stop_loss/adverse/timeout/funding_drag), block
# re-entry on it for this long. Stops the fee-bleeding churn of re-entering the
# same non-reverting dislocation over and over (observed: 3× QCOM stop_loss in a
# row, held 0.0h each). 0 disables.
STOP_COOLDOWN_MINUTES = 30

# Only AUTO-enter names that have a calibrated per-symbol threshold (i.e. a key
# in ENTRY_THRESHOLD_BPS_BY_SYMBOL). Auto-discovered names are still watched and
# shown, but don't trade real money until you've vetted them (run /backtest, and
# if it holds up under taker-taker cost, add the name to the threshold table).
# This is why the auto-added names (QCOM 15% win, STRC 5%) were bleeding: they
# traded at the default 30bps before anyone checked whether they mean-revert.
AUTO_TRADE_ONLY_CALIBRATED = True

# ── Convergence stop-loss ──
# Basis (non-funding-hold) positions had no mark-to-market stop: the adverse
# stop only fires if the excess INVERTS, so a position that just diverges or
# fails to converge bled to the 12h timeout (observed RKLB −$15.68, STRC
# −$12.62). Close a basis position when its executable mark-to-market loss
# (est_net) reaches this. Scales with the position's notional (like the profit
# target), so small test sizes stop proportionally. 0 disables.
#
# This is effectively a bps stop: value / NOTIONAL_PER_LEG = the stop distance
# (20/1000 = 200bps). est_net marks at EXECUTABLE exit prices, so on a thin book
# a position opens already underwater by the bid-ask it must cross (~50-150bps on
# equity perps) — a tight stop then trips on book width before convergence gets a
# chance. 200bps gives room for the crossing cost plus real pre-convergence drift.
BASIS_ADVERSE_STOP_USD = 20.0

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
# disabled so they don't close the moment the basis reverts. There are NO
# automatic exits at all — no timeout and no mark-to-market safety stop. The
# operator opened it deliberately and owns the exit; it closes only on a manual
# /close. The two values below are retained for reference but are NOT enforced.
FUNDING_MAX_HOLD_HOURS = 0            # disabled (no timeout on a funding hold)
FUNDING_ADVERSE_STOP_USD = 0.0       # disabled (no mark-to-market safety stop)

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

# Maker→taker ENTRY escalation: when a convergence edge is so strong it clears
# the (wider) taker gate, cross the unfilled HL remainder as a taker instead of
# waiting on the resting maker. Disabled by default: it crosses additional HL
# sized from a possibly-stale fill count, and a maker fill racing the cancel
# double-filled HL in live (ARM: 0.06 intended → 0.12 executed, naked leg). The
# resting maker + reprice still fills strong edges; this only trades a little
# fill-rate for safety. If re-enabled, the cross now re-reads the true HL
# position after cancelling so it can't double-fill.
ENTRY_TAKER_ESCALATION_ENABLED = False

# A manual /enter or /close can carry a basis target (bps) — the trade only
# executes once the executable basis (from bid/ask, in the position's favour) is
# at or better than the target, so you don't cross at a bad level. A gated entry
# that never reaches its target expires after this long; gated exits never expire
# (a safety stop closes the position if the basis stays bad).
# 0 = no expiry: a gated entry waits indefinitely (e.g. to catch an overnight
# spike). /cancel SYM clears it manually.
MANUAL_ENTRY_GATE_TIMEOUT_MIN = 0

# ── Position sizing ──
NOTIONAL_PER_LEG = 1000          # USD per leg
MAX_CONCURRENT_POSITIONS = 8     # max simultaneous positions across all symbols

# ── Execution ──
POLL_INTERVAL_SECONDS = 1        # main loop interval
ASTER_FILL_POLL_SECONDS = 10     # how often to poll pending Aster maker orders for fills
HL_IOC_BUFFER_BPS = 5            # bps above/below current price for HL IOC limit
ASTER_IOC_BUFFER_BPS = 5         # bps past best for Aster IOC (force-close) limit
# Wider buffer for EXIT / emergency HL crosses (getting OUT dominates price). An
# IOC limit is only a CAP — it still fills at the resting book price, so a wide
# buffer never worsens the fill; it just guarantees the order is marketable even
# if the book moved between the snapshot and order arrival. A 5bps buffer let a
# liquid-but-jumpy name (SKHX) move past the limit → "could not immediately
# match" → the whole exit aborted. 40bps reliably crosses without changing fills.
HL_EXIT_IOC_BUFFER_BPS = 40
ORDER_TIMEOUT_SECONDS = 8        # HTTP request timeout

# ── Paper mode ──
PAPER_MODE = True                # set False for live execution

# Send reduceOnly on Aster closing orders. Default OFF: it was added as a
# naked-flip backstop but Aster appears to REJECT reduce-only orders (opens with
# plain IOC fill; closes with reduceOnly fail → naked legs / stuck exits). The
# real over-close protection is the qty accounting + live-position reconcile, so
# closes go out as plain IOCs. Only enable if Aster is confirmed to accept it.
ASTER_REDUCE_ONLY = False

# ── Alerting ──
HEARTBEAT_INTERVAL_MINUTES = 60
# Send a Telegram message when a LIVE position opens or closes. Paper trades
# never alert (they'd be far too noisy). Set False to silence.
TRADE_ALERTS_ENABLED = True

# ── Per-symbol entry thresholds (bps) ──
# Derived from p75 of historical cross-exchange spread, rounded to nearest 5bps.
# Symbols not listed fall back to ENTRY_THRESHOLD_BPS.
ENTRY_THRESHOLD_BPS_BY_SYMBOL: dict = {
    "STRC":    120,  # backtest-calibrated 2026-07-09
    "ARM":     105,  # excess p75=103bps  oracle_delta=-23bps
    "RKLB":     95,  # excess p75=97bps   oracle_delta=-4bps
    "PLTR":     85,  # excess p75=87bps   oracle_delta=+3bps
    "SKHX":     85,  # excess p75=87bps   oracle_delta=-53bps  (Korean: SK Hynix)
    "SMSN":     85,  # excess p75=85bps   oracle_delta=-43bps  (Korean: Samsung)
    "DRAM":     80,  # excess p75=78bps   oracle_delta=-18bps
    "AMD":      70,  # excess p75=69bps   oracle_delta=-4bps
    "ORCL":     70,  # excess p75=71bps   oracle_delta=+1bps
    "MRVL":     45,  # excess p75=47bps   oracle_delta=-5bps
    "CBRS":     40,  # excess p75=41bps   oracle_delta=-16bps
    "INTC":     40,  # excess p75=38bps   oracle_delta=-0bps
    "MSTR":     40,  # excess p75=40bps   oracle_delta=-4bps
    "SNDK":     35,  # excess p75=37bps   oracle_delta=+4bps
    "ASML":     30,  # backtest-calibrated 2026-07-09
    "DELL":     30,  # backtest-calibrated 2026-07-09 (lowered from 75)
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
#   - Samsung:   HL xyz:SMSN  <-> Aster SAMSUNGUSDT  (SMSNUSDT is dead)
#   - SK Hynix:  HL xyz:SKHX  <-> Aster SKHYNIXUSDT  (SKHXUSDT is dead)
#   - BlackBerry: HL xyz:BB   <-> Aster BBXUSDT      (BBUSDT is a crypto, wrong)
# Map: canonical base -> Aster's tradeable base symbol.
ASTER_BASE_ALIAS: dict = {
    "SMSN": "SAMSUNG",
    "SKHX": "SKHYNIX",
    "BB": "BBX",
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
# NOTE: BB (BlackBerry) was blocked for "mismatched instruments" — that was the
# ASTER_BASE_ALIAS bug (BB→BBUSDT is a crypto; the real book is BBXUSDT). With
# the BB->BBX alias added it now points at the right instrument, so it's
# un-blocked. It won't auto-trade until added to ENTRY_THRESHOLD_BPS_BY_SYMBOL
# (AUTO_TRADE_ONLY_CALIBRATED), and the 20% price-ratio guard still catches any
# residual mismatch.
# Peak-analysis blocklist: names with negative median peak reversion consistently
# lose money after fees — the spread widens further instead of reverting.
# Derived from scripts/backtest_1m.py --peak-analysis (48h, 480m baseline).
BLOCKED_SYMBOLS: set = {
    "BIRD",
    "META", "COIN", "LLY", "IBM", "MSFT", "URNM", "BABA", "AVGO",
    "CRWV", "EWT", "BX", "USAR", "WDC",
    # 2026-06-15 peak-analysis blocklist refresh (48h, 480m baseline):
    # Negative median peak reversion — spread widens instead of reverting
    "NFLX", "RIVN", "LITE", "HYUNDAI", "NOW", "TSM", "HOOD", "HIMS",
    # New listings with no data, unusable spreads, or no liquidity
    "GME", "EBAY", "COST", "BE", "NOK", "MINIMAX", "SPCX",
    # Thin/unstable book: wild tick-to-tick prices, big divergence losses,
    # extreme volatile funding (observed −$13.84 funding_drag). See liquidity guard.
    "ZHIPU",
    # 2026-07-09 backtest-negative blocklist: net-negative under taker-taker cost
    "NBIS", "AMAT", "ZM", "DKNG", "QCOM",
}

# ── Backward-compat aliases (fetch_data.py / live_scan.py) ──
ASTER_API = ASTER_BASE
CANDLE_INTERVAL = "1h"
HISTORY_DAYS = 30
HYPERLIQUID_MAKER_FEE = HL_MAKER_FEE
HYPERLIQUID_TAKER_FEE = HL_TAKER_FEE

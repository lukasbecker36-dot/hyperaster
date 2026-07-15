"""SQLite persistence for equity perp arb positions."""

import os
import sqlite3
from config import DB_PATH, DATA_DIR


def get_connection() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    # timeout: the trader and the control bot both write this DB — without a
    # busy timeout a concurrent write raises "database is locked" mid-trade.
    # WAL: lets readers proceed during writes (persistent once set, but cheap
    # to re-issue per connection).
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
    except sqlite3.OperationalError:
        pass
    return conn


def init_db():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS positions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol              TEXT NOT NULL,          -- e.g. 'AAPL'
            hl_coin             TEXT NOT NULL,          -- e.g. 'xyz:AAPL'
            aster_symbol        TEXT NOT NULL,          -- e.g. 'AAPLUSDT'
            direction           TEXT NOT NULL,          -- 'long_hl_short_aster' or 'long_aster_short_hl'
            status              TEXT NOT NULL DEFAULT 'entering',
            -- entering: HL filled, Aster maker pending
            -- open: both sides filled
            -- exiting: exit orders placed
            -- closed: complete
            -- error: manual intervention needed

            -- Entry
            entry_time          INTEGER,
            entry_spread_bps    REAL,
            hl_entry_price      REAL,
            aster_entry_price   REAL,
            hl_entry_order_id   TEXT,
            aster_entry_order_id TEXT,
            qty                 REAL,                  -- base token qty (same both sides)
            notional_usd        REAL,

            -- Exit
            exit_time           INTEGER,
            exit_spread_bps     REAL,
            hl_exit_order_id    TEXT,
            aster_exit_order_id TEXT,
            hl_exit_price       REAL,
            aster_exit_price    REAL,

            -- P&L
            gross_pnl           REAL,
            fee_cost            REAL,
            net_pnl             REAL,                  -- gross - fees + funding
            exit_reason         TEXT,                  -- 'converged', 'timeout', 'error'
            paper               INTEGER NOT NULL DEFAULT 0, -- 1 if paper-mode simulation

            -- Funding (snapshotted at entry, accrued over hold)
            hl_funding_rate     REAL DEFAULT 0,        -- HL hourly funding rate at entry
            aster_funding_rate  REAL DEFAULT 0,        -- Aster 8h funding rate at entry
            funding_pnl         REAL DEFAULT 0,        -- estimated net carry over the hold
            entry_baseline_bps  REAL DEFAULT 0,         -- rolling baseline at entry (for convergence exit)
            hold_for_funding    INTEGER DEFAULT 0,      -- 1 = manual funding-carry hold (skip converge/target exits)
            entry_maker_venue   TEXT DEFAULT '',        -- 'hl' = maker-first carry execution
            hl_baseline_szi     REAL DEFAULT 0,         -- HL signed size before the resting maker order
            aster_hedged_qty    REAL DEFAULT 0          -- Aster qty already hedged against HL fills
        );

        -- Migration: add entry_baseline_bps if missing (existing DBs)
    """)
    try:
        conn.execute("ALTER TABLE positions ADD COLUMN entry_baseline_bps REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # column already exists
    try:
        conn.execute("ALTER TABLE positions ADD COLUMN hold_for_funding INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # column already exists
    for _col, _type in (
        ("entry_maker_venue", "TEXT DEFAULT ''"),
        ("hl_baseline_szi", "REAL DEFAULT 0"),
        ("aster_hedged_qty", "REAL DEFAULT 0"),
        ("aster_baseline_amt", "REAL DEFAULT 0"),
        ("scale_pre_qty", "REAL DEFAULT 0"),
        ("scale_pre_hl_px", "REAL DEFAULT 0"),
        ("scale_pre_aster_px", "REAL DEFAULT 0"),
        ("scale_pre_notional", "REAL DEFAULT 0"),
        # Cumulative fills of the current resting Aster GTX across reprices.
        # Without this, a partial fill before a reprice was lost and the full
        # size was reposted — over-filling the leg (naked exposure).
        ("aster_gtx_filled", "REAL DEFAULT 0"),
        # Order id whose (terminal) fill was last banked into aster_gtx_filled.
        # Makes banking idempotent: the same dead order seen twice (repost
        # failure, restart) must not be counted twice.
        ("aster_gtx_banked_oid", "TEXT DEFAULT ''"),
        # Aster funding settlement window (hours) detected at entry — varies by
        # name (most 8h, some 4h e.g. SKHX). Funding P&L normalises by this.
        ("aster_funding_window_h", "REAL DEFAULT 8"),
    ):
        try:
            conn.execute(f"ALTER TABLE positions ADD COLUMN {_col} {_type}")
        except sqlite3.OperationalError:
            pass
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS trade_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER REFERENCES positions(id),
            timestamp   INTEGER NOT NULL,
            exchange    TEXT NOT NULL,
            side        TEXT NOT NULL,
            order_type  TEXT NOT NULL,
            order_id    TEXT,
            qty         REAL,
            fill_price  REAL,
            fee         REAL,
            status      TEXT,
            notes       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_pos_status ON positions(status);
        CREATE INDEX IF NOT EXISTS idx_pos_symbol ON positions(symbol);

        -- Intent log for position-changing order calls (HL IOC entry/exit,
        -- Aster IOC force-close). Written BEFORE the API call; updated to
        -- completed AFTER. On startup, any row with completed_at IS NULL is
        -- a crash window that needs reconciliation against venue state.
        CREATE TABLE IF NOT EXISTS order_intents (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id   INTEGER,                -- NULL for entry intents
            symbol        TEXT    NOT NULL,
            venue         TEXT    NOT NULL,       -- 'hl' | 'aster'
            action        TEXT    NOT NULL,       -- 'entry_ioc' | 'exit_ioc' | 'force_exit_ioc'
            direction     TEXT,                   -- direction of overall arb position
            side          TEXT    NOT NULL,       -- 'buy' | 'sell' on the specified venue
            qty           REAL    NOT NULL,
            ref_price     REAL,
            baseline_szi  REAL,                   -- HL position size BEFORE the call (HL only)
            created_at    INTEGER NOT NULL,
            completed_at  INTEGER,
            outcome       TEXT,                   -- 'filled' | 'rejected' | 'no_fill' | 'reconciled_filled' | 'ambiguous' | ...
            notes         TEXT,
            paper         INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_intent_open
            ON order_intents(symbol, completed_at);
    """)
    # Migrate existing DBs that predate the exit order id columns
    for col in ("hl_exit_order_id", "aster_exit_order_id"):
        try:
            conn.execute(f"ALTER TABLE positions ADD COLUMN {col} TEXT")
        except Exception:
            pass  # column already exists
    # Migrate existing DBs that predate the paper column
    try:
        conn.execute("ALTER TABLE positions ADD COLUMN paper INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    # Migrate existing DBs that predate funding accounting. Funding rates are
    # snapshotted at entry (HL hourly, Aster 8h); funding_pnl is the estimated
    # net carry over the hold, folded into net_pnl.
    for col in ("hl_funding_rate", "aster_funding_rate", "funding_pnl"):
        try:
            conn.execute(f"ALTER TABLE positions ADD COLUMN {col} REAL DEFAULT 0")
        except Exception:
            pass
    conn.commit()
    conn.close()

"""SQLite persistence for equity perp arb positions."""

import os
import sqlite3
from config import DB_PATH, DATA_DIR


def get_connection() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    return sqlite3.connect(DB_PATH)


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
            net_pnl             REAL,
            exit_reason         TEXT,                  -- 'converged', 'timeout', 'error'
            paper               INTEGER NOT NULL DEFAULT 0  -- 1 if paper-mode simulation
        );

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
    conn.commit()
    conn.close()

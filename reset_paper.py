#!/usr/bin/env python3
"""
Reset paper-trading history for a clean baseline.

Backs up the SQLite DB, then deletes ONLY paper rows (paper=1) from the
positions and order_intents tables. Live rows (paper=0) are never touched.

Run with the trader STOPPED (so in-memory paper positions don't get rewritten
on the next close):

    sudo systemctl stop hyperaster      # or /stop YES from Telegram
    .venv/bin/python reset_paper.py --yes
    sudo systemctl start hyperaster     # or /start from Telegram

Without --yes it does a dry run and just reports what would be deleted.
"""

import shutil
import sqlite3
import sys
import time
from pathlib import Path

from config import DB_PATH


def _count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE paper=1").fetchone()[0]
    except sqlite3.OperationalError:
        return 0  # table or column may not exist yet


def main():
    confirm = "--yes" in sys.argv
    db = Path(DB_PATH)
    if not db.exists():
        print(f"No DB at {db} — nothing to reset.")
        return

    conn = sqlite3.connect(str(db))
    pos_n = _count(conn, "positions")
    intent_n = _count(conn, "order_intents")

    print(f"DB: {db}")
    print(f"  paper positions:     {pos_n}")
    print(f"  paper order_intents: {intent_n}")

    if not confirm:
        print("\nDry run — pass --yes to back up and delete the paper rows above.")
        conn.close()
        return

    # Back up first
    backup = db.with_name(f"{db.stem}.{time.strftime('%Y%m%d-%H%M%S')}.bak{db.suffix}")
    shutil.copy2(db, backup)
    print(f"\nBacked up to {backup}")

    conn.execute("DELETE FROM positions WHERE paper=1")
    try:
        conn.execute("DELETE FROM order_intents WHERE paper=1")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()
    print(f"Deleted {pos_n} paper position(s) and {intent_n} paper intent(s).")
    print("Paper P&L is now clean. Start the trader to begin fresh.")


if __name__ == "__main__":
    main()

"""
Intent log helpers for crash-safe order placement.

Pattern:
    intent_id = record_intent(...)        # BEFORE API call
    result    = await place_order(...)
    complete_intent(intent_id, outcome)   # AFTER, regardless of outcome

If we crash between record and complete, recovery.reconcile_incomplete_intents()
on next startup finds the row and queries the venue to figure out what happened.
"""

import logging
from typing import Optional

from auth import now_ms
from database import get_connection

log = logging.getLogger(__name__)


def record_intent(
    symbol: str,
    venue: str,
    action: str,
    side: str,
    qty: float,
    *,
    position_id: Optional[int] = None,
    direction: Optional[str] = None,
    ref_price: float = 0.0,
    baseline_szi: float = 0.0,
    notes: str = "",
    paper: bool = False,
) -> int:
    """Insert an intent row and return its id."""
    conn = get_connection()
    cur = conn.execute(
        "INSERT INTO order_intents "
        "(position_id, symbol, venue, action, direction, side, qty, ref_price, "
        " baseline_szi, created_at, notes, paper) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (position_id, symbol, venue, action, direction, side, qty, ref_price,
         baseline_szi, now_ms(), notes, 1 if paper else 0),
    )
    conn.commit()
    intent_id = cur.lastrowid
    conn.close()
    return intent_id


def complete_intent(intent_id: int, outcome: str, *, notes: Optional[str] = None,
                    position_id: Optional[int] = None):
    """Mark an intent as resolved with the given outcome."""
    conn = get_connection()
    if notes is not None and position_id is not None:
        conn.execute(
            "UPDATE order_intents SET completed_at=?, outcome=?, notes=?, position_id=? WHERE id=?",
            (now_ms(), outcome, notes, position_id, intent_id),
        )
    elif notes is not None:
        conn.execute(
            "UPDATE order_intents SET completed_at=?, outcome=?, notes=? WHERE id=?",
            (now_ms(), outcome, notes, intent_id),
        )
    elif position_id is not None:
        conn.execute(
            "UPDATE order_intents SET completed_at=?, outcome=?, position_id=? WHERE id=?",
            (now_ms(), outcome, position_id, intent_id),
        )
    else:
        conn.execute(
            "UPDATE order_intents SET completed_at=?, outcome=? WHERE id=?",
            (now_ms(), outcome, intent_id),
        )
    conn.commit()
    conn.close()


def get_incomplete_intents(paper: bool) -> list[dict]:
    """Return all intents with completed_at IS NULL for the given mode."""
    conn = get_connection()
    paper_val = 1 if paper else 0
    rows = conn.execute(
        "SELECT id, position_id, symbol, venue, action, direction, side, qty, "
        "ref_price, baseline_szi, created_at, notes "
        "FROM order_intents WHERE completed_at IS NULL AND paper=? "
        "ORDER BY created_at",
        (paper_val,),
    ).fetchall()
    conn.close()
    cols = ("id", "position_id", "symbol", "venue", "action", "direction",
            "side", "qty", "ref_price", "baseline_szi", "created_at", "notes")
    return [dict(zip(cols, r)) for r in rows]

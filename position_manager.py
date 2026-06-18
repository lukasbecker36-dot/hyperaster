"""
Position state management for equity perp arb.

State machine per position:
  entering  → HL IOC filled; Aster GTX order resting, polling for fill
  open      → both sides filled; monitoring spread for exit
  exiting   → exit HL IOC done; Aster exit GTX resting, polling
  closed    → complete
  error     → needs manual intervention
"""

import logging
from dataclasses import dataclass
from typing import Optional

from auth import now_ms
from database import get_connection
from config import (
    ASTER_MAKER_FEE, ASTER_TAKER_FEE, HL_MAKER_FEE, HL_TAKER_FEE,
)

log = logging.getLogger(__name__)


def estimate_funding_pnl(
    direction: str, hours_held: float, notional: float,
    hl_funding_rate: float, aster_funding_rate: float,
) -> float:
    """
    Estimate net funding carry (USD) over a hold from entry-snapshot rates.

    Convention: a positive funding rate means longs pay shorts. So a leg we are
    LONG accrues -rate (we pay when positive); a leg we are SHORT accrues +rate.
    HL settles hourly (rate is per-1h); Aster every 8h (rate is per-8h), so we
    normalise each to the actual hours held.

    This is a continuous-accrual approximation — funding really settles at
    discrete times, but for paper P&L (and a first live estimate) rate × elapsed
    fraction of the period is the standard, sufficiently-accurate model.
    """
    if direction == "long_hl_short_aster":
        hl_sign, aster_sign = -1.0, +1.0   # long HL, short Aster
    else:
        hl_sign, aster_sign = +1.0, -1.0   # short HL, long Aster
    hl_carry = hl_sign * hl_funding_rate * hours_held * notional
    aster_carry = aster_sign * aster_funding_rate * (hours_held / 8.0) * notional
    return hl_carry + aster_carry


@dataclass
class Position:
    id: int = 0
    symbol: str = ""
    hl_coin: str = ""
    aster_symbol: str = ""
    direction: str = ""           # 'long_hl_short_aster' | 'long_aster_short_hl'
    status: str = "entering"

    entry_time: int = 0
    entry_spread_bps: float = 0.0
    entry_baseline_bps: float = 0.0
    hl_entry_price: float = 0.0
    aster_entry_price: float = 0.0
    hl_entry_order_id: str = ""
    aster_entry_order_id: str = ""   # resting GTX order id while entering
    qty: float = 0.0
    notional_usd: float = 0.0

    exit_time: int = 0
    exit_spread_bps: float = 0.0
    hl_exit_order_id: str = ""
    aster_exit_order_id: str = ""
    hl_exit_price: float = 0.0
    aster_exit_price: float = 0.0

    gross_pnl: float = 0.0
    fee_cost: float = 0.0
    net_pnl: float = 0.0
    exit_reason: str = ""

    # Funding: rates snapshotted at entry (HL hourly, Aster 8h),
    # funding_pnl = estimated net carry over the hold (folded into net_pnl).
    hl_funding_rate: float = 0.0
    aster_funding_rate: float = 0.0
    funding_pnl: float = 0.0

    # True for manual funding-carry holds — the monitor holds these for carry
    # and only applies safety exits (timeout, mark-to-market stop), never the
    # basis target/convergence exits.
    hold_for_funding: bool = False

    # Maker-first execution (carry trades): "hl" means the HL leg rests as a
    # post-only maker and the Aster leg crosses (IOC) to hedge each HL fill.
    # "" = legacy flow (HL IOC taker first, Aster GTX maker rests).
    entry_maker_venue: str = ""
    hl_baseline_szi: float = 0.0   # HL signed size before the resting maker order
    aster_hedged_qty: float = 0.0  # Aster qty already hedged against HL fills
    aster_baseline_amt: float = 0.0  # Aster positionAmt before entry (hedge reconcile)
    aster_hedge_attempts: int = 0  # circuit breaker: total Aster hedge IOCs placed
    # Scale-in tracking: when >0, this 'entering' record is adding to an existing
    # open position. On completion the increment is blended into the pre-scale
    # qty/prices; if it fails to fill, the position reverts to its pre-scale state.
    scale_pre_qty: float = 0.0
    scale_pre_hl_px: float = 0.0
    scale_pre_aster_px: float = 0.0
    scale_pre_notional: float = 0.0


class PositionManager:
    def __init__(self, paper_mode: bool = False):
        self.paper_mode = paper_mode
        # symbol -> Position (only one per symbol at a time)
        self.positions: dict[str, Position] = {}
        self._load_open_positions()

    def _load_open_positions(self):
        """Only load positions matching the current run mode — live runs ignore paper rows, vice versa."""
        paper_val = 1 if self.paper_mode else 0
        conn = get_connection()
        rows = conn.execute(
            "SELECT id, symbol, hl_coin, aster_symbol, direction, status, "
            "entry_time, entry_spread_bps, hl_entry_price, aster_entry_price, "
            "hl_entry_order_id, aster_entry_order_id, qty, notional_usd, "
            "exit_time, hl_exit_order_id, aster_exit_order_id, "
            "hl_funding_rate, aster_funding_rate, "
            "COALESCE(entry_baseline_bps, 0), COALESCE(hold_for_funding, 0), "
            "COALESCE(entry_maker_venue, ''), COALESCE(hl_baseline_szi, 0), "
            "COALESCE(aster_hedged_qty, 0), COALESCE(aster_baseline_amt, 0), "
            "COALESCE(scale_pre_qty, 0), COALESCE(scale_pre_hl_px, 0), "
            "COALESCE(scale_pre_aster_px, 0), COALESCE(scale_pre_notional, 0) "
            "FROM positions WHERE status NOT IN ('closed', 'error') AND paper=?",
            (paper_val,)
        ).fetchall()
        conn.close()
        for r in rows:
            p = Position(
                id=r[0], symbol=r[1], hl_coin=r[2], aster_symbol=r[3],
                direction=r[4], status=r[5],
                entry_time=r[6], entry_spread_bps=r[7],
                hl_entry_price=r[8] or 0.0, aster_entry_price=r[9] or 0.0,
                hl_entry_order_id=r[10] or "", aster_entry_order_id=r[11] or "",
                qty=r[12] or 0.0, notional_usd=r[13] or 0.0,
                exit_time=r[14] or 0,
                hl_exit_order_id=r[15] or "", aster_exit_order_id=r[16] or "",
                hl_funding_rate=r[17] or 0.0, aster_funding_rate=r[18] or 0.0,
                entry_baseline_bps=r[19] or 0.0,
                hold_for_funding=bool(r[20]),
                entry_maker_venue=r[21] or "",
                hl_baseline_szi=r[22] or 0.0,
                aster_hedged_qty=r[23] or 0.0,
                aster_baseline_amt=r[24] or 0.0,
                scale_pre_qty=r[25] or 0.0,
                scale_pre_hl_px=r[26] or 0.0,
                scale_pre_aster_px=r[27] or 0.0,
                scale_pre_notional=r[28] or 0.0,
            )
            self.positions[p.symbol] = p
            log.warning(
                f"Crash recovery: {p.symbol} position #{p.id} status={p.status}"
            )

    @property
    def active_count(self) -> int:
        return len(self.positions)

    def has_position(self, symbol: str) -> bool:
        return symbol in self.positions

    def get(self, symbol: str) -> Optional[Position]:
        return self.positions.get(symbol)

    def open_entering(
        self,
        symbol: str, hl_coin: str, aster_symbol: str,
        direction: str, entry_spread_bps: float,
        hl_entry_price: float, hl_order_id: str,
        aster_entry_order_id: str,  # resting GTX order
        qty: float, notional_usd: float,
        hl_funding_rate: float = 0.0, aster_funding_rate: float = 0.0,
        entry_baseline_bps: float = 0.0,
        hold_for_funding: bool = False,
    ) -> Position:
        entry_time = now_ms()
        conn = get_connection()
        cur = conn.execute(
            "INSERT INTO positions "
            "(symbol, hl_coin, aster_symbol, direction, status, entry_time, "
            "entry_spread_bps, hl_entry_price, hl_entry_order_id, "
            "aster_entry_order_id, qty, notional_usd, paper, "
            "hl_funding_rate, aster_funding_rate, entry_baseline_bps, hold_for_funding) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, hl_coin, aster_symbol, direction, "entering", entry_time,
             entry_spread_bps, hl_entry_price, hl_order_id,
             aster_entry_order_id, qty, notional_usd,
             1 if self.paper_mode else 0,
             hl_funding_rate, aster_funding_rate, entry_baseline_bps,
             1 if hold_for_funding else 0),
        )
        conn.commit()
        pid = cur.lastrowid
        conn.close()

        pos = Position(
            id=pid, symbol=symbol, hl_coin=hl_coin, aster_symbol=aster_symbol,
            direction=direction, status="entering",
            entry_time=entry_time, entry_spread_bps=entry_spread_bps,
            entry_baseline_bps=entry_baseline_bps,
            hl_entry_price=hl_entry_price, hl_entry_order_id=hl_order_id,
            aster_entry_order_id=aster_entry_order_id,
            qty=qty, notional_usd=notional_usd,
            hl_funding_rate=hl_funding_rate, aster_funding_rate=aster_funding_rate,
            hold_for_funding=hold_for_funding,
        )
        self.positions[symbol] = pos
        log.info(
            f"Position #{pid} ENTERING: {symbol} {direction} | "
            f"spread={entry_spread_bps:.1f}bps | qty={qty} | "
            f"HL filled @ {hl_entry_price:.2f} | Aster GTX resting {aster_entry_order_id}"
        )
        return pos

    def open_hl_maker_entering(
        self, *, symbol: str, aster_symbol: str, direction: str,
        hl_maker_order_id: str, hl_baseline_szi: float, qty: float,
        notional_usd: float, entry_spread_bps: float, hl_ref_price: float,
        hl_funding_rate: float = 0.0, aster_funding_rate: float = 0.0,
        hold_for_funding: bool = True, entry_baseline_bps: float = 0.0,
        aster_baseline_amt: float = 0.0,
    ) -> Position:
        """Record a maker-first entry: HL post-only order resting, nothing filled
        yet. poll_hl_maker advances it as the HL leg fills and the Aster taker
        hedges each increment. Used for both carry holds (hold_for_funding=True)
        and convergence arbs (hold_for_funding=False, with entry_baseline_bps set
        for the convergence exit)."""
        entry_time = now_ms()
        conn = get_connection()
        cur = conn.execute(
            "INSERT INTO positions "
            "(symbol, hl_coin, aster_symbol, direction, status, entry_time, "
            "entry_spread_bps, hl_entry_price, hl_entry_order_id, "
            "aster_entry_order_id, qty, notional_usd, paper, "
            "hl_funding_rate, aster_funding_rate, hold_for_funding, entry_baseline_bps, "
            "entry_maker_venue, hl_baseline_szi, aster_hedged_qty, aster_baseline_amt) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, f"xyz:{symbol}", aster_symbol, direction, "entering", entry_time,
             entry_spread_bps, hl_ref_price, hl_maker_order_id,
             "", qty, notional_usd, 1 if self.paper_mode else 0,
             hl_funding_rate, aster_funding_rate, 1 if hold_for_funding else 0,
             entry_baseline_bps, "hl", hl_baseline_szi, 0.0, aster_baseline_amt),
        )
        conn.commit()
        pid = cur.lastrowid
        conn.close()
        pos = Position(
            id=pid, symbol=symbol, hl_coin=f"xyz:{symbol}", aster_symbol=aster_symbol,
            direction=direction, status="entering", entry_time=entry_time,
            entry_spread_bps=entry_spread_bps, entry_baseline_bps=entry_baseline_bps,
            hl_entry_price=hl_ref_price,
            hl_entry_order_id=hl_maker_order_id, qty=qty, notional_usd=notional_usd,
            hl_funding_rate=hl_funding_rate, aster_funding_rate=aster_funding_rate,
            hold_for_funding=hold_for_funding, entry_maker_venue="hl",
            hl_baseline_szi=hl_baseline_szi, aster_hedged_qty=0.0,
            aster_baseline_amt=aster_baseline_amt,
        )
        self.positions[symbol] = pos
        log.info(
            f"Position #{pid} ENTERING (HL maker): {symbol} {direction} | "
            f"qty target={qty} | HL maker resting {hl_maker_order_id} @ {hl_ref_price:.2f}"
        )
        return pos

    def record_hl_maker_progress(
        self, symbol: str, hl_filled: float, aster_hedged: float,
        hl_avg_price: float, aster_avg_price: float,
    ):
        """Persist running fill/hedge state for an in-flight maker-first entry."""
        pos = self.positions.get(symbol)
        if not pos:
            return
        pos.aster_hedged_qty = aster_hedged
        if hl_avg_price > 0:
            pos.hl_entry_price = hl_avg_price
        if aster_avg_price > 0:
            pos.aster_entry_price = aster_avg_price
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET aster_hedged_qty=?, hl_entry_price=?, "
            "aster_entry_price=? WHERE id=?",
            (aster_hedged, pos.hl_entry_price, pos.aster_entry_price, pos.id),
        )
        conn.commit()
        conn.close()

    def confirm_hl_maker_open(
        self, symbol: str, final_qty: float, hl_avg_price: float, aster_avg_price: float,
    ):
        """Finalize a maker-first entry once the HL leg is filled and hedged.

        For a scale-in (scale_pre_qty>0), `final_qty` is the INCREMENT that
        filled — blend it into the pre-scale position rather than replacing it."""
        pos = self.positions.get(symbol)
        if not pos:
            return

        if pos.scale_pre_qty > 0:
            # Blend the increment into the existing position by notional weight.
            inc_qty = final_qty
            inc_hl = hl_avg_price if hl_avg_price > 0 else pos.hl_entry_price
            inc_aster = aster_avg_price if aster_avg_price > 0 else pos.aster_entry_price
            inc_notional = inc_qty * ((inc_hl + inc_aster) / 2)
            old_n = pos.scale_pre_notional or (pos.scale_pre_qty *
                    ((pos.scale_pre_hl_px + pos.scale_pre_aster_px) / 2))
            tot_n = old_n + inc_notional
            w_old = old_n / tot_n if tot_n > 0 else 0.0
            w_new = inc_notional / tot_n if tot_n > 0 else 1.0
            total_qty = pos.scale_pre_qty + inc_qty
            blended_hl = pos.scale_pre_hl_px * w_old + inc_hl * w_new
            blended_aster = pos.scale_pre_aster_px * w_old + inc_aster * w_new
            pos.qty = total_qty
            pos.aster_hedged_qty = total_qty
            pos.hl_entry_price = blended_hl
            pos.aster_entry_price = blended_aster
            pos.notional_usd = tot_n
            pos.status = "open"
            # Clear scale-in markers.
            pos.scale_pre_qty = pos.scale_pre_hl_px = 0.0
            pos.scale_pre_aster_px = pos.scale_pre_notional = 0.0
            conn = get_connection()
            conn.execute(
                "UPDATE positions SET status='open', qty=?, aster_hedged_qty=?, "
                "hl_entry_price=?, aster_entry_price=?, notional_usd=?, "
                "scale_pre_qty=0, scale_pre_hl_px=0, scale_pre_aster_px=0, "
                "scale_pre_notional=0 WHERE id=?",
                (total_qty, total_qty, blended_hl, blended_aster, tot_n, pos.id),
            )
            conn.commit()
            conn.close()
            log.warning(
                f"Position #{pos.id} SCALE-IN complete: {symbol} +{inc_qty} → "
                f"total qty={total_qty} ${tot_n:.0f} | HL @ {blended_hl:.2f} | "
                f"Aster @ {blended_aster:.2f}"
            )
            return

        pos.qty = final_qty
        pos.aster_hedged_qty = final_qty
        if hl_avg_price > 0:
            pos.hl_entry_price = hl_avg_price
        if aster_avg_price > 0:
            pos.aster_entry_price = aster_avg_price
        pos.notional_usd = final_qty * ((pos.hl_entry_price + pos.aster_entry_price) / 2)
        pos.status = "open"
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='open', qty=?, aster_hedged_qty=?, "
            "hl_entry_price=?, aster_entry_price=?, notional_usd=? WHERE id=?",
            (final_qty, final_qty, pos.hl_entry_price, pos.aster_entry_price,
             pos.notional_usd, pos.id),
        )
        conn.commit()
        conn.close()
        log.info(
            f"Position #{pos.id} OPEN (HL maker): {symbol} | qty={final_qty} | "
            f"HL @ {pos.hl_entry_price:.2f} | Aster @ {pos.aster_entry_price:.2f}"
        )

    def start_scale_in(
        self, *, symbol: str, hl_maker_order_id: str, increment_qty: float,
        hl_ref_price: float, hl_baseline_szi: float, aster_baseline_amt: float,
        hl_funding_rate: float = 0.0, aster_funding_rate: float = 0.0,
    ) -> bool:
        """Put an OPEN position into scale-in 'entering' mode for an increment.

        Stashes the pre-scale qty/prices so the increment can be blended in on
        completion (or reverted if it fails). poll_hl_maker then drives the
        increment exactly like a fresh maker entry, measuring fills from the
        supplied baselines (which already include the existing position)."""
        pos = self.positions.get(symbol)
        if not pos or pos.status != "open":
            return False
        pos.scale_pre_qty = pos.qty
        pos.scale_pre_hl_px = pos.hl_entry_price
        pos.scale_pre_aster_px = pos.aster_entry_price
        pos.scale_pre_notional = pos.notional_usd or (
            pos.qty * ((pos.hl_entry_price + pos.aster_entry_price) / 2))
        pos.qty = increment_qty           # poll completion target = the increment
        pos.aster_hedged_qty = 0.0        # track only the new hedge
        pos.aster_hedge_attempts = 0
        pos.hl_baseline_szi = hl_baseline_szi
        pos.aster_baseline_amt = aster_baseline_amt
        pos.hl_entry_price = hl_ref_price  # increment's resting price (blends in poll)
        pos.hl_entry_order_id = hl_maker_order_id
        pos.entry_time = now_ms()
        if hl_funding_rate:
            pos.hl_funding_rate = hl_funding_rate
        if aster_funding_rate:
            pos.aster_funding_rate = aster_funding_rate
        pos.status = "entering"
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='entering', qty=?, aster_hedged_qty=0, "
            "hl_baseline_szi=?, aster_baseline_amt=?, "
            "hl_entry_price=?, hl_entry_order_id=?, entry_time=?, "
            "scale_pre_qty=?, scale_pre_hl_px=?, scale_pre_aster_px=?, "
            "scale_pre_notional=? WHERE id=?",
            (increment_qty, hl_baseline_szi, aster_baseline_amt, hl_ref_price,
             hl_maker_order_id, pos.entry_time, pos.scale_pre_qty, pos.scale_pre_hl_px,
             pos.scale_pre_aster_px, pos.scale_pre_notional, pos.id),
        )
        conn.commit()
        conn.close()
        log.warning(
            f"Position #{pos.id} SCALE-IN started: {symbol} +{increment_qty} "
            f"(existing {pos.scale_pre_qty}) | HL maker @ {hl_ref_price:.2f}"
        )
        return True

    def revert_scale_in(self, symbol: str):
        """Increment filled nothing — restore the position to its pre-scale state."""
        pos = self.positions.get(symbol)
        if not pos or pos.scale_pre_qty <= 0:
            return
        pos.qty = pos.scale_pre_qty
        pos.aster_hedged_qty = pos.scale_pre_qty
        pos.hl_entry_price = pos.scale_pre_hl_px
        pos.aster_entry_price = pos.scale_pre_aster_px
        pos.notional_usd = pos.scale_pre_notional
        pos.status = "open"
        pos.hl_entry_order_id = ""
        pos.scale_pre_qty = pos.scale_pre_hl_px = 0.0
        pos.scale_pre_aster_px = pos.scale_pre_notional = 0.0
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='open', qty=?, aster_hedged_qty=?, "
            "hl_entry_price=?, aster_entry_price=?, notional_usd=?, "
            "hl_entry_order_id='', scale_pre_qty=0, scale_pre_hl_px=0, "
            "scale_pre_aster_px=0, scale_pre_notional=0 WHERE id=?",
            (pos.qty, pos.qty, pos.hl_entry_price, pos.aster_entry_price,
             pos.notional_usd, pos.id),
        )
        conn.commit()
        conn.close()
        log.warning(
            f"Position #{pos.id} SCALE-IN reverted: {symbol} — increment unfilled, "
            f"restored to qty={pos.qty} ${pos.notional_usd:.0f}"
        )

    def scale_in(
        self, symbol: str, add_qty: float, add_notional: float,
        hl_price: float, aster_price: float,
        hl_funding_rate: float = 0.0, aster_funding_rate: float = 0.0,
    ):
        """Add to an existing open position (blended VWAP entry prices)."""
        pos = self.positions.get(symbol)
        if not pos or pos.status != "open":
            return
        old_n = pos.notional_usd or 1.0
        new_n = old_n + add_notional
        w_old, w_new = old_n / new_n, add_notional / new_n
        pos.hl_entry_price = pos.hl_entry_price * w_old + hl_price * w_new
        pos.aster_entry_price = pos.aster_entry_price * w_old + aster_price * w_new
        pos.hl_funding_rate = pos.hl_funding_rate * w_old + hl_funding_rate * w_new
        pos.aster_funding_rate = pos.aster_funding_rate * w_old + aster_funding_rate * w_new
        pos.qty += add_qty
        pos.notional_usd = new_n
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET qty=?, notional_usd=?, hl_entry_price=?, "
            "aster_entry_price=?, hl_funding_rate=?, aster_funding_rate=? WHERE id=?",
            (pos.qty, pos.notional_usd, pos.hl_entry_price, pos.aster_entry_price,
             pos.hl_funding_rate, pos.aster_funding_rate, pos.id),
        )
        conn.commit()
        conn.close()
        log.warning(
            f"Position #{pos.id} SCALE-IN: {symbol} +{add_qty} qty +${add_notional:.0f} → "
            f"total qty={pos.qty} ${pos.notional_usd:.0f}"
        )

    def confirm_aster_entry(self, symbol: str, aster_fill_price: float):
        """Called when the Aster GTX maker order fills. Position becomes open."""
        pos = self.positions.get(symbol)
        if not pos or pos.status != "entering":
            return
        pos.aster_entry_price = aster_fill_price
        pos.status = "open"
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='open', aster_entry_price=? WHERE id=?",
            (aster_fill_price, pos.id),
        )
        conn.commit()
        conn.close()
        log.info(
            f"Position #{pos.id} OPEN: {symbol} | "
            f"HL @ {pos.hl_entry_price:.2f} | Aster @ {aster_fill_price:.2f}"
        )

    def confirm_aster_entry_partial(
        self, symbol: str, aster_fill_price: float, matched_qty: float
    ):
        """Aster maker partial-fill at entry timeout: shrink position to matched qty, open it."""
        pos = self.positions.get(symbol)
        if not pos or pos.status != "entering":
            return
        pos.qty = matched_qty
        pos.aster_entry_price = aster_fill_price
        pos.status = "open"
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='open', aster_entry_price=?, qty=? WHERE id=?",
            (aster_fill_price, matched_qty, pos.id),
        )
        conn.commit()
        conn.close()
        log.warning(
            f"Position #{pos.id} OPEN (partial): {symbol} | qty shrunk to {matched_qty} | "
            f"HL @ {pos.hl_entry_price:.2f} | Aster @ {aster_fill_price:.2f}"
        )

    def start_exiting_hl_maker(
        self, symbol: str, hl_maker_order_id: str, exit_baseline_szi: float,
        hl_ref_price: float, exit_spread_bps: float,
    ):
        """Begin a maker-first carry exit: HL post-only close order resting,
        nothing closed yet. poll_hl_maker_exit advances it as the HL leg fills
        and the Aster taker closes each increment. Reuses hl_baseline_szi to
        snapshot the szi at exit-start and aster_hedged_qty to track how much of
        the Aster leg has been closed back."""
        pos = self.positions.get(symbol)
        if not pos:
            return
        pos.status = "exiting"
        pos.exit_time = now_ms()
        pos.hl_exit_order_id = hl_maker_order_id
        pos.aster_exit_order_id = ""
        pos.hl_exit_price = hl_ref_price
        pos.exit_spread_bps = exit_spread_bps
        pos.hl_baseline_szi = exit_baseline_szi
        pos.aster_hedged_qty = 0.0
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='exiting', exit_time=?, "
            "hl_exit_order_id=?, aster_exit_order_id='', hl_exit_price=?, "
            "exit_spread_bps=?, hl_baseline_szi=?, aster_hedged_qty=0 WHERE id=?",
            (pos.exit_time, hl_maker_order_id, hl_ref_price, exit_spread_bps,
             exit_baseline_szi, pos.id),
        )
        conn.commit()
        conn.close()
        log.info(
            f"Position #{pos.id} EXITING (HL maker): {symbol} | "
            f"HL maker resting {hl_maker_order_id} @ {hl_ref_price:.2f} | "
            f"spread={exit_spread_bps:.1f}bps"
        )

    def record_hl_maker_exit_progress(
        self, symbol: str, aster_closed: float, aster_avg_price: float,
        hl_avg_price: float | None = None,
    ):
        """Persist running close/hedge state for an in-flight maker-first exit."""
        pos = self.positions.get(symbol)
        if not pos:
            return
        pos.aster_hedged_qty = aster_closed
        if aster_avg_price > 0:
            pos.aster_exit_price = aster_avg_price
        if hl_avg_price and hl_avg_price > 0:
            pos.hl_exit_price = hl_avg_price
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET aster_hedged_qty=?, aster_exit_price=?, "
            "hl_exit_price=? WHERE id=?",
            (aster_closed, pos.aster_exit_price, pos.hl_exit_price, pos.id),
        )
        conn.commit()
        conn.close()

    def confirm_hl_maker_exit(
        self, symbol: str, final_qty: float, hl_exit_price: float,
        aster_exit_price: float, exit_reason: str,
    ):
        """Finalize a maker-first carry exit: set the close prices/qty, compute
        P&L, and close the position. Mirrors confirm_aster_exit's accounting."""
        pos = self.positions.get(symbol)
        if not pos:
            return
        if final_qty > 0:
            pos.qty = final_qty
        if hl_exit_price > 0:
            pos.hl_exit_price = hl_exit_price
        self.confirm_aster_exit(symbol, aster_exit_price, exit_reason)

    def start_exiting(
        self, symbol: str, hl_exit_order_id: str, aster_exit_order_id: str,
        hl_exit_price: float, exit_spread_bps: float,
    ):
        pos = self.positions.get(symbol)
        if not pos:
            return
        pos.status = "exiting"
        pos.exit_time = now_ms()
        pos.hl_exit_order_id = hl_exit_order_id
        pos.aster_exit_order_id = aster_exit_order_id
        pos.hl_exit_price = hl_exit_price
        pos.exit_spread_bps = exit_spread_bps
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='exiting', exit_time=?, "
            "hl_exit_order_id=?, aster_exit_order_id=?, "
            "hl_exit_price=?, exit_spread_bps=? WHERE id=?",
            (pos.exit_time, hl_exit_order_id, aster_exit_order_id,
             hl_exit_price, exit_spread_bps, pos.id),
        )
        conn.commit()
        conn.close()
        log.info(f"Position #{pos.id} EXITING: {symbol} | spread={exit_spread_bps:.1f}bps")

    def confirm_aster_exit(
        self, symbol: str, aster_exit_price: float, exit_reason: str
    ):
        pos = self.positions.get(symbol)
        if not pos:
            return
        pos.aster_exit_price = aster_exit_price
        pos.status = "closed"

        # P&L calculation from actual fill prices
        if pos.direction == "long_hl_short_aster":
            hl_pnl = (pos.hl_exit_price - pos.hl_entry_price) * pos.qty
            aster_pnl = (pos.aster_entry_price - aster_exit_price) * pos.qty
        else:
            hl_pnl = (pos.hl_entry_price - pos.hl_exit_price) * pos.qty
            aster_pnl = (aster_exit_price - pos.aster_entry_price) * pos.qty
        gross = hl_pnl + aster_pnl

        # Maker-first trades (carry + convergence) pay HL-maker/Aster-taker;
        # legacy taker convergence pays HL-taker/Aster-maker.
        notional = pos.notional_usd
        if pos.entry_maker_venue == "hl":
            fees = notional * 2 * HL_MAKER_FEE + notional * 2 * ASTER_TAKER_FEE
        else:
            fees = notional * 2 * HL_TAKER_FEE + notional * 2 * ASTER_MAKER_FEE

        # Funding carry over the hold (estimated from entry-snapshot rates).
        hours_held = max(0.0, (pos.exit_time - pos.entry_time) / 3_600_000)
        funding = estimate_funding_pnl(
            pos.direction, hours_held, notional,
            pos.hl_funding_rate, pos.aster_funding_rate,
        )

        net = gross - fees + funding

        pos.gross_pnl = round(gross, 4)
        pos.fee_cost = round(fees, 4)
        pos.funding_pnl = round(funding, 4)
        pos.net_pnl = round(net, 4)
        pos.exit_reason = exit_reason

        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='closed', aster_exit_price=?, "
            "gross_pnl=?, fee_cost=?, funding_pnl=?, net_pnl=?, exit_reason=? WHERE id=?",
            (aster_exit_price, pos.gross_pnl, pos.fee_cost, pos.funding_pnl,
             pos.net_pnl, exit_reason, pos.id),
        )
        conn.commit()
        conn.close()

        log.info(
            f"Position #{pos.id} CLOSED: {symbol} | {exit_reason} | "
            f"gross=${gross:.2f} fees=${fees:.2f} funding=${funding:.2f} net=${net:.2f}"
        )
        del self.positions[symbol]

    def mark_error(self, symbol: str, reason: str):
        pos = self.positions.get(symbol)
        if not pos:
            return
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='error', exit_reason=? WHERE id=?",
            (reason, pos.id),
        )
        conn.commit()
        conn.close()
        log.error(f"Position #{pos.id} ERROR: {symbol} | {reason}")
        del self.positions[symbol]

    def drop_entering(self, symbol: str, reason: str = "maker_entry_unfilled"):
        """Close out a never-filled entry record (no exposure was ever taken)."""
        pos = self.positions.get(symbol)
        if not pos:
            return
        conn = get_connection()
        conn.execute(
            "UPDATE positions SET status='closed', exit_reason=?, net_pnl=0 WHERE id=?",
            (reason, pos.id),
        )
        conn.commit()
        conn.close()
        log.info(f"Position #{pos.id} dropped ({reason}): {symbol}")
        del self.positions[symbol]

    def import_position(
        self, *, symbol: str, hl_coin: str, aster_symbol: str,
        direction: str, qty: float, hl_price: float, aster_price: float,
        hl_funding_rate: float = 0.0, aster_funding_rate: float = 0.0,
    ) -> Position:
        """Import an existing venue position pair into the DB as 'open'.

        Used when the user has offsetting perp positions on HL and Aster that
        were created outside the bot (manual trades, prior runaway, etc.) and
        wants to manage them via /positions and /close."""
        entry_time = now_ms()
        notional = qty * ((hl_price + aster_price) / 2)
        conn = get_connection()
        cur = conn.execute(
            "INSERT INTO positions "
            "(symbol, hl_coin, aster_symbol, direction, status, entry_time, "
            "entry_spread_bps, hl_entry_price, aster_entry_price, "
            "qty, notional_usd, paper, "
            "hl_funding_rate, aster_funding_rate, hold_for_funding, "
            "entry_maker_venue) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, hl_coin, aster_symbol, direction, "open", entry_time,
             0.0, hl_price, aster_price,
             qty, notional, 1 if self.paper_mode else 0,
             hl_funding_rate, aster_funding_rate, 1, "hl"),
        )
        conn.commit()
        pid = cur.lastrowid
        conn.close()
        pos = Position(
            id=pid, symbol=symbol, hl_coin=hl_coin, aster_symbol=aster_symbol,
            direction=direction, status="open", entry_time=entry_time,
            hl_entry_price=hl_price, aster_entry_price=aster_price,
            qty=qty, notional_usd=notional,
            hl_funding_rate=hl_funding_rate, aster_funding_rate=aster_funding_rate,
            hold_for_funding=True, entry_maker_venue="hl",
        )
        self.positions[symbol] = pos
        log.warning(
            f"Position #{pid} IMPORTED: {symbol} {direction} | "
            f"qty={qty} ${notional:.0f} | HL @ {hl_price:.2f} | Aster @ {aster_price:.2f}"
        )
        return pos

    def log_trade(self, position_id: int, exchange: str, side: str,
                  order_type: str, order_id: str = "", qty: float = 0,
                  fill_price: float = 0, fee: float = 0, status: str = "",
                  notes: str = ""):
        conn = get_connection()
        conn.execute(
            "INSERT INTO trade_log "
            "(position_id, timestamp, exchange, side, order_type, order_id, "
            "qty, fill_price, fee, status, notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (position_id, now_ms(), exchange, side, order_type, order_id,
             qty, fill_price, fee, status, notes),
        )
        conn.commit()
        conn.close()

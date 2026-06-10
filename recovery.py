"""
Crash recovery: reconcile incomplete order intents against venue state.

Called once at monitor startup. For each intent without a completion record,
queries the relevant venue and decides:
  - the order actually filled  → recover the position into the right state
  - the order didn't fill      → close the intent as no_fill, no exposure
  - state is ambiguous         → mark position as 'error', refuse to start

Refusing-to-start is intentional: an autonomous bot resuming on top of a state
it can't explain is how naked legs accumulate. Operator runs flatten.py and
clears the symbol manually.
"""

import logging
from dataclasses import dataclass

from database import get_connection
from intents import complete_intent, get_incomplete_intents

log = logging.getLogger(__name__)


@dataclass
class ReconcileReport:
    auto_recovered: list[str]   # symbols brought back online cleanly
    no_fills: list[str]         # intents resolved as never-executed
    requires_manual: list[str]  # symbols left in 'error' — refuse to start


async def reconcile_incomplete_intents(client, pm, paper_mode: bool) -> ReconcileReport:
    """Walk every incomplete intent and resolve or error each."""
    report = ReconcileReport([], [], [])
    incomplete = get_incomplete_intents(paper=paper_mode)
    if not incomplete:
        return report

    log.warning(f"Crash recovery: {len(incomplete)} incomplete intent(s) found, reconciling")

    for intent in incomplete:
        symbol = intent["symbol"]
        venue = intent["venue"]
        action = intent["action"]
        try:
            if venue == "hl" and action == "entry_ioc":
                await _handle_hl_entry_intent(client, pm, intent, report)
            elif venue == "hl" and action == "exit_ioc":
                await _handle_hl_exit_intent(client, pm, intent, report)
            elif venue == "aster" and action == "force_exit_ioc":
                await _handle_aster_force_exit_intent(client, pm, intent, report)
            else:
                log.error(
                    f"Crash recovery: unknown intent {intent['id']} {venue}/{action} "
                    f"on {symbol} — marking ambiguous"
                )
                complete_intent(intent["id"], "ambiguous_unknown_action")
                report.requires_manual.append(symbol)
        except Exception as e:
            log.critical(
                f"Crash recovery: error reconciling intent {intent['id']} on {symbol}: {e} — "
                f"marking ambiguous, requires manual intervention"
            )
            complete_intent(intent["id"], "ambiguous_error", notes=str(e)[:200])
            if symbol not in report.requires_manual:
                report.requires_manual.append(symbol)

    return report


async def _handle_hl_entry_intent(client, pm, intent: dict, report: ReconcileReport):
    """Entry HL IOC that we don't know the outcome of."""
    symbol = intent["symbol"]
    intent_id = intent["id"]
    expected_signed = intent["qty"] if intent["side"] == "buy" else -intent["qty"]
    baseline = intent["baseline_szi"] or 0.0

    # If the position is already tracked, this intent corresponds to an entry
    # that DID complete — just close out the bookkeeping.
    if pm.has_position(symbol):
        log.info(
            f"Crash recovery: HL entry intent {intent_id} on {symbol} already has "
            f"position in DB — closing intent as filled"
        )
        pos = pm.get(symbol)
        complete_intent(intent_id, "reconciled_filled",
                        notes="position already in DB",
                        position_id=pos.id if pos else None)
        return

    # Query HL to see whether the order filled
    pos_data = await client.get_hl_position(symbol)
    current_szi = float(pos_data.get("szi", 0) or 0)
    delta = current_szi - baseline

    same_sign = (delta * expected_signed) > 0
    magnitude_ok = abs(delta) >= abs(expected_signed) * 0.95

    if not same_sign and abs(delta) < abs(expected_signed) * 0.05:
        # No movement — order didn't fill
        log.info(
            f"Crash recovery: HL entry intent {intent_id} on {symbol} — "
            f"no position change (szi {baseline}->{current_szi}), marking no_fill"
        )
        complete_intent(intent_id, "no_fill", notes="HL position unchanged")
        report.no_fills.append(symbol)
        return

    if not (same_sign and magnitude_ok):
        # Ambiguous — partial fill, wrong direction, or pre-existing position
        log.critical(
            f"Crash recovery: HL entry intent {intent_id} on {symbol} AMBIGUOUS — "
            f"expected {expected_signed} but szi moved {delta} ({baseline}->{current_szi}). "
            f"Manual intervention required: run flatten.py --symbols {symbol} --reconcile"
        )
        complete_intent(intent_id, "ambiguous",
                        notes=f"expected {expected_signed}, got delta {delta}")
        report.requires_manual.append(symbol)
        return

    # HL filled. Reconstruct the position row in 'entering' state and look for
    # the Aster GTX. If no Aster order, this is operator territory.
    log.critical(
        f"Crash recovery: HL entry intent {intent_id} on {symbol} FILLED while crashed "
        f"(szi {baseline}->{current_szi}, delta {delta}) — reconstructing position row"
    )
    aster_orders = await client.get_aster_open_orders(symbol)
    expected_aster_side = "sell" if intent["direction"] == "long_hl_short_aster" else "buy"
    matching = [o for o in aster_orders
                if str(o.get("side", "")).upper() == expected_aster_side.upper()]
    aster_order_id = str(matching[0].get("orderId", "")) if matching else "RECOVERED_NONE"

    pos = pm.open_entering(
        symbol=symbol,
        hl_coin=f"xyz:{symbol}",
        aster_symbol=f"{symbol}USDT",
        direction=intent["direction"],
        entry_spread_bps=0.0,  # unknown after the fact
        hl_entry_price=intent["ref_price"] or 0.0,
        hl_order_id="RECOVERED",
        aster_entry_order_id=aster_order_id,
        qty=abs(delta),
        notional_usd=abs(delta) * (intent["ref_price"] or 0.0),
    )
    complete_intent(intent_id, "reconciled_filled",
                    notes=f"reconstructed pos #{pos.id} aster_oid={aster_order_id}",
                    position_id=pos.id)

    if not matching:
        # HL filled, no Aster order found → naked HL. Mark error so operator
        # flattens; refuse to start on this symbol.
        log.critical(
            f"Crash recovery: {symbol} HL position {delta} exists but NO matching Aster "
            f"GTX found — NAKED HL LEG. Run flatten.py --symbols {symbol} --reconcile"
        )
        pm.mark_error(symbol, "recovery_naked_hl")
        report.requires_manual.append(symbol)
    else:
        report.auto_recovered.append(symbol)


async def _handle_hl_exit_intent(client, pm, intent: dict, report: ReconcileReport):
    """Exit HL IOC that we don't know the outcome of."""
    symbol = intent["symbol"]
    intent_id = intent["id"]
    expected_signed = intent["qty"] if intent["side"] == "buy" else -intent["qty"]
    baseline = intent["baseline_szi"] or 0.0

    pos = pm.get(symbol)
    if not pos:
        # The position row disappeared between intent creation and now —
        # extremely unusual. Just close the intent and alert.
        log.critical(
            f"Crash recovery: HL exit intent {intent_id} on {symbol} but no position "
            f"row exists — DB inconsistency. Marking intent ambiguous."
        )
        complete_intent(intent_id, "ambiguous_no_position")
        report.requires_manual.append(symbol)
        return

    pos_data = await client.get_hl_position(symbol)
    current_szi = float(pos_data.get("szi", 0) or 0)
    delta = current_szi - baseline

    same_sign = (delta * expected_signed) > 0
    magnitude_ok = abs(delta) >= abs(expected_signed) * 0.95

    if abs(delta) < abs(expected_signed) * 0.05:
        # No movement — exit didn't fire. Position is still genuinely open;
        # leave it alone, the monitor will re-attempt exit.
        log.warning(
            f"Crash recovery: HL exit intent {intent_id} on {symbol} — "
            f"HL position unchanged ({current_szi}), exit will retry naturally"
        )
        complete_intent(intent_id, "no_fill", notes="HL position unchanged, retry")
        report.no_fills.append(symbol)
        return

    if not (same_sign and magnitude_ok):
        log.critical(
            f"Crash recovery: HL exit intent {intent_id} on {symbol} AMBIGUOUS — "
            f"expected delta {expected_signed} but got {delta} ({baseline}->{current_szi}). "
            f"Position marked error. Run flatten.py --symbols {symbol} --reconcile"
        )
        complete_intent(intent_id, "ambiguous",
                        notes=f"expected delta {expected_signed}, got {delta}",
                        position_id=pos.id)
        pm.mark_error(symbol, "recovery_exit_ambiguous")
        report.requires_manual.append(symbol)
        return

    # HL exit fired. Advance position from 'open' to 'exiting' state.
    log.critical(
        f"Crash recovery: HL exit intent {intent_id} on {symbol} FILLED while crashed "
        f"(szi {baseline}->{current_szi}) — advancing position to 'exiting'"
    )
    conn = get_connection()
    conn.execute(
        "UPDATE positions SET status='exiting', exit_time=?, hl_exit_order_id=?, "
        "hl_exit_price=?, exit_reason=? WHERE id=?",
        (intent["created_at"], "RECOVERED", intent["ref_price"] or 0.0,
         "recovered_crash_exit", pos.id),
    )
    conn.commit()
    conn.close()
    pos.status = "exiting"
    pos.exit_time = intent["created_at"]
    pos.hl_exit_order_id = "RECOVERED"
    pos.hl_exit_price = intent["ref_price"] or 0.0
    pos.exit_reason = "recovered_crash_exit"

    # Check Aster side
    aster_pos = await client.get_aster_position(symbol)
    aster_qty = float(aster_pos.get("positionAmt", 0) or 0)
    if abs(aster_qty) < pos.qty * 0.05:
        # Aster also closed — alert operator to manually close out via flatten,
        # the bookkeeping is too fragile to auto-close P&L here.
        log.critical(
            f"Crash recovery: {symbol} HL and Aster both closed during crash — "
            f"P&L books unreliable. Position marked error. Run flatten.py to clear."
        )
        pm.mark_error(symbol, "recovery_both_closed")
        complete_intent(intent_id, "reconciled_filled",
                        notes="both legs closed during crash", position_id=pos.id)
        report.requires_manual.append(symbol)
    else:
        # Aster still has the leg — monitor's normal exit flow will pick it up
        log.warning(
            f"Crash recovery: {symbol} HL closed, Aster still has {aster_qty} — "
            f"monitor will continue exit via poll_aster_maker"
        )
        complete_intent(intent_id, "reconciled_filled",
                        notes=f"HL closed, Aster {aster_qty} pending",
                        position_id=pos.id)
        report.auto_recovered.append(symbol)


async def _handle_aster_force_exit_intent(client, pm, intent: dict, report: ReconcileReport):
    """Aster IOC force-close that we don't know the outcome of."""
    symbol = intent["symbol"]
    intent_id = intent["id"]
    pos = pm.get(symbol)

    aster_pos = await client.get_aster_position(symbol)
    aster_qty = float(aster_pos.get("positionAmt", 0) or 0)

    if abs(aster_qty) < intent["qty"] * 0.05:
        # Aster position is closed — the IOC fired
        log.critical(
            f"Crash recovery: Aster force-exit intent {intent_id} on {symbol} FILLED "
            f"(aster_qty={aster_qty}). Marking position error so operator verifies P&L."
        )
        complete_intent(intent_id, "reconciled_filled",
                        notes=f"aster closed (qty={aster_qty})",
                        position_id=pos.id if pos else None)
        if pos:
            pm.mark_error(symbol, "recovery_aster_force_closed")
        report.requires_manual.append(symbol)
    else:
        log.warning(
            f"Crash recovery: Aster force-exit intent {intent_id} on {symbol} — "
            f"aster_qty={aster_qty} still open, exit didn't fire, will retry"
        )
        complete_intent(intent_id, "no_fill",
                        notes=f"aster still open ({aster_qty})",
                        position_id=pos.id if pos else None)
        report.no_fills.append(symbol)

"""
Execution logic for equity perp arb: Aster GTX maker + Hyperliquid IOC taker.

Entry flow:
  1. Re-fetch both books and confirm spread still valid
  2. Place HL IOC (taker) - fills immediately or not at all
  3. If HL fills: place Aster GTX at best bid/ask (maker, resting)
  4. Position enters 'entering' state; Aster fill is polled in main loop
  5. If HL does NOT fill: abort cleanly (no positions opened)

Exit flow:
  1. Place HL IOC to close the HL leg
  2. If HL fills: place Aster GTX to close the Aster leg
  3. Position enters 'exiting' state; Aster fill polled in main loop

Aster GTX repricing:
  Each poll tick while 'entering' or 'exiting', if the Aster GTX order price
  is no longer at best bid/ask, cancel it and repost at the new best price.
  (Same principle as unwind.py, but Aster handles the API differences.)
"""

import asyncio
import logging

from exchange_client import ExchangeClient, OrderBook, OrderResult
from position_manager import PositionManager
from intents import record_intent, complete_intent
from config import (
    ENTRY_THRESHOLD_BPS, ENTRY_THRESHOLD_BPS_BY_SYMBOL, EXIT_THRESHOLD_BPS,
    NOTIONAL_PER_LEG, ENTRY_TIMEOUT_MINUTES, EXIT_TIMEOUT_MINUTES,
    MAX_PRICE_RATIO_DIVERGENCE, BLOCKED_SYMBOLS, MIN_EXECUTABLE_PREMIUM_BPS,
    ENTRY_CONFIRM_TICKS, ENTRY_COST_MARGIN_BPS, ROUND_TRIP_FEE,
    EXIT_TARGET_NET_USD, EXIT_TARGET_NET_USD_BY_SYMBOL,
    aster_symbol_for,
)
from auth import now_ms

log = logging.getLogger(__name__)

ASTER_TERMINAL_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}


class Executor:
    def __init__(
        self,
        client: ExchangeClient,
        pm: PositionManager,
        paper_mode: bool = False,
    ):
        self.client = client
        self.pm = pm
        self.paper_mode = paper_mode
        self._price_mismatch_warned: set[str] = set()
        # Entry persistence: symbol -> (direction, consecutive qualifying ticks).
        # An entry only commits once the same-direction signal has held for
        # ENTRY_CONFIRM_TICKS consecutive scans, filtering out stale-feed phantoms.
        self._entry_streak: dict[str, tuple[str, int]] = {}

    # ── Entry ──

    async def try_entry(
        self,
        symbol: str,
        aster_book: OrderBook,
        hl_book: OrderBook,
    ) -> bool:
        """
        Attempt to enter a position for the given symbol.
        Returns True if entry orders were placed.
        """
        if symbol in BLOCKED_SYMBOLS:
            return False

        if self.pm.has_position(symbol):
            # Position already open (or mid-entry) — drop any stale streak so a
            # later re-entry must re-confirm from scratch.
            self._entry_streak.pop(symbol, None)
            return False

        mid = (aster_book.mid + hl_book.mid) / 2
        if mid <= 0:
            return False

        # Reject if prices are so far apart the instruments are likely mismatched
        # (also catches stale Aster orderbooks outside market hours)
        price_ratio = abs(aster_book.mid - hl_book.mid) / min(aster_book.mid, hl_book.mid)
        if price_ratio > MAX_PRICE_RATIO_DIVERGENCE:
            if symbol not in self._price_mismatch_warned:
                log.warning(
                    f"{symbol}: price mismatch {aster_book.mid:.4f} (Aster) vs "
                    f"{hl_book.mid:.4f} (HL) — {price_ratio*100:.0f}% apart, skipping"
                )
                self._price_mismatch_warned.add(symbol)
            return False
        # Clear warn flag once prices re-align (market opens)
        self._price_mismatch_warned.discard(symbol)

        # Entry signal = deviation of the book mid-spread from its own rolling
        # median. We trade the books, so we baseline against the books (not the
        # venue oracle feeds). spread_bps > baseline => Aster rich vs HL => short
        # Aster / long HL; spread_bps < baseline => the reverse.
        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
        self.client.record_book_spread(symbol, spread_bps)
        baseline_bps = self.client.get_book_spread_baseline(symbol)
        # No baseline yet (cold start, not enough samples) = no entry. Substituting
        # 0 would treat the entire structural gap as tradeable edge — the failure
        # mode that bled fees before.
        if baseline_bps is None:
            log.debug(
                f"{symbol}: baseline not ready "
                f"({self.client.book_spread_sample_count(symbol)} samples) — skipping"
            )
            return False

        # raw_premium_bps removed — the cost floor (fees + book spreads) is the
        # real protection. The old abs(spread) guard blocked legitimate baseline-
        # deviation trades on names with negative baselines (e.g. AMZN at 69bps
        # excess but only 10bps raw spread due to -59bps baseline).

        aster_excess_bps = spread_bps - baseline_bps   # long_hl_short_aster
        hl_excess_bps = -(spread_bps - baseline_bps)    # long_aster_short_hl

        base_threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(symbol, ENTRY_THRESHOLD_BPS)
        # Dynamic cost floor: on exit we pay round-trip fees plus cross the bid-ask
        # spread on both venues again. Require the edge to clear that, so a wide
        # statistical spread on a thin book doesn't get traded into a loss.
        aster_spread_bps = (aster_book.ask - aster_book.bid) / mid * 10000 if mid > 0 else 0
        hl_spread_bps = (hl_book.ask - hl_book.bid) / mid * 10000 if mid > 0 else 0
        cost_floor = (
            ROUND_TRIP_FEE * 10000
            + aster_spread_bps + hl_spread_bps
            + ENTRY_COST_MARGIN_BPS
        )
        threshold = max(base_threshold, cost_floor)
        if cost_floor > base_threshold:
            log.debug(
                f"{symbol}: cost floor {cost_floor:.0f}bps > base {base_threshold:.0f}bps "
                f"(HL sprd={hl_spread_bps:.0f} Ast sprd={aster_spread_bps:.0f})"
            )

        # `excess_bps` is the tradeable deviation for the chosen direction;
        # `spread_bps` stays the raw book mid-spread (used for logging / records).
        if aster_excess_bps >= threshold:
            direction = "long_hl_short_aster"
            excess_bps = aster_excess_bps
            # HL: buy (long) at ask — taker
            hl_side = "buy"
            hl_ref_price = hl_book.ask
            # Aster: sell (short) at bid — maker
            aster_side = "sell"
            aster_ref_price = aster_book.bid
        elif hl_excess_bps >= threshold:
            direction = "long_aster_short_hl"
            excess_bps = hl_excess_bps
            hl_side = "sell"
            hl_ref_price = hl_book.bid
            aster_side = "buy"
            aster_ref_price = aster_book.ask
        else:
            # No qualifying direction this tick — reset the persistence streak.
            self._entry_streak.pop(symbol, None)
            return False

        # Hard floor — ensures the edge exceeds fee cost even before the rolling
        # median fully stabilises.
        if excess_bps < MIN_EXECUTABLE_PREMIUM_BPS:
            log.debug(
                f"{symbol}: excess {excess_bps:.1f}bps below MIN_EXECUTABLE floor "
                f"({MIN_EXECUTABLE_PREMIUM_BPS}bps), skipping"
            )
            return False

        # Persistence filter — the signal must hold the same direction for
        # ENTRY_CONFIRM_TICKS consecutive scans before we commit. Stale-feed phantoms
        # evaporate within a tick or two; real dislocations persist.
        prev_dir, prev_count = self._entry_streak.get(symbol, ("", 0))
        count = prev_count + 1 if prev_dir == direction else 1
        self._entry_streak[symbol] = (direction, count)
        if count < ENTRY_CONFIRM_TICKS:
            log.info(
                f"{symbol}: {direction} excess={excess_bps:.1f}bps (thr={threshold:.0f}) "
                f"confirming {count}/{ENTRY_CONFIRM_TICKS}"
            )
            return False

        # Size in base tokens, capped by top-of-book liquidity on both venues.
        target_qty = NOTIONAL_PER_LEG / mid
        if direction == "long_hl_short_aster":
            hl_avail, ast_avail = hl_book.ask_size, aster_book.bid_size
        else:
            hl_avail, ast_avail = hl_book.bid_size, aster_book.ask_size
        max_qty = min(target_qty, hl_avail, ast_avail)
        qty = self.client.snap_aster_qty(symbol, max_qty)
        if qty <= 0:
            log.debug(f"{symbol}: qty snapped to 0 (target={target_qty:.2f} hl={hl_avail:.2f} ast={ast_avail:.2f})")
            self._entry_streak.pop(symbol, None)
            return False
        actual_notional = qty * mid

        # Reject if max theoretical profit can't reach target. Full reversion
        # of excess_bps on this notional is the ceiling; require 1.5× target
        # so we're not entering trades that can only breakeven at best.
        sym_target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(symbol, EXIT_TARGET_NET_USD)
        est_fees = actual_notional * ROUND_TRIP_FEE
        max_gross = actual_notional * excess_bps / 10000
        if max_gross < sym_target * 1.5 + est_fees:
            log.debug(
                f"{symbol}: max gross ${max_gross:.2f} < 1.5×target+fees "
                f"${sym_target * 1.5 + est_fees:.2f} on ${actual_notional:.0f} notional — skipping"
            )
            return False

        log.info(
            f"ENTRY {symbol}: {direction} | excess={excess_bps:.1f}bps "
            f"spread={spread_bps:+.1f}bps base={baseline_bps:+.1f}bps | "
            f"qty={qty} | HL {hl_side} @ {hl_ref_price:.2f} | Aster {aster_side} @ {aster_ref_price:.2f}"
        )

        # Snapshot funding rates at entry (HL hourly, Aster 8h) for carry accounting.
        hl_fr = self.client.get_hl_funding_rate(symbol)
        aster_fr = self.client.get_aster_funding_rate(symbol)

        if self.paper_mode:
            log.info(
                f"[PAPER] ENTRY {symbol}: {direction} | excess={excess_bps:.1f}bps | "
                f"qty={qty} | HL {hl_side} @ {hl_ref_price:.2f} | Aster {aster_side} @ {aster_ref_price:.2f}"
            )
            self.pm.open_entering(
                symbol=symbol,
                hl_coin=f"xyz:{symbol}",
                aster_symbol=aster_symbol_for(symbol),
                direction=direction,
                entry_spread_bps=excess_bps,
                hl_entry_price=hl_ref_price,
                hl_order_id="PAPER",
                aster_entry_order_id="PAPER",
                qty=qty,
                notional_usd=actual_notional,
                hl_funding_rate=hl_fr,
                aster_funding_rate=aster_fr,
                entry_baseline_bps=baseline_bps,
            )
            self.pm.confirm_aster_entry(symbol, aster_ref_price)
            return True

        # Pre-trade recheck — re-confirm the deviation against the rolling baseline
        try:
            fresh_aster, fresh_hl = await self.client.get_both_books(symbol)
            fresh_mid = (fresh_aster.mid + fresh_hl.mid) / 2
            if fresh_mid <= 0:
                log.warning(f"{symbol}: empty books on recheck, aborting")
                return False
            fresh_baseline = self.client.get_book_spread_baseline(symbol)
            if fresh_baseline is None:
                log.warning(f"{symbol}: baseline vanished on recheck, aborting")
                return False
            fresh_spread = (fresh_aster.mid - fresh_hl.mid) / fresh_mid * 10000
            if direction == "long_hl_short_aster":
                fresh_excess = fresh_spread - fresh_baseline
            else:
                fresh_excess = -(fresh_spread - fresh_baseline)
            if fresh_excess < max(threshold, MIN_EXECUTABLE_PREMIUM_BPS):
                log.warning(
                    f"{symbol}: excess collapsed to {fresh_excess:.1f}bps on recheck, aborting"
                )
                return False
            # Use fresh prices
            if direction == "long_hl_short_aster":
                hl_ref_price = fresh_hl.ask
                aster_ref_price = fresh_aster.bid
            else:
                hl_ref_price = fresh_hl.bid
                aster_ref_price = fresh_aster.ask
        except Exception as e:
            log.error(f"{symbol}: pre-trade recheck failed ({e}), aborting")
            return False

        # Snapshot HL position BEFORE placing the order so we can reconcile if
        # the order call returns ambiguously (network timeout / non-JSON) OR
        # we crash before completing the intent.
        try:
            pre_pos = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre_pos.get("szi", 0) or 0)
        except Exception as e:
            log.warning(f"{symbol}: HL pre-position snapshot failed ({e}) — aborting entry to stay safe")
            return False

        # Record intent BEFORE placing the order — crash-safety for the window
        # between order-sent and DB-row-written. recovery.py reconciles on next start.
        hl_intent_id = record_intent(
            symbol=symbol, venue="hl", action="entry_ioc",
            direction=direction, side=hl_side, qty=qty,
            ref_price=hl_ref_price, baseline_szi=baseline_szi,
            paper=self.paper_mode,
        )

        # Step 1: HL IOC (taker) — fills immediately or not at all
        hl_result = await self.client.place_hl_ioc(symbol, hl_side, qty, hl_ref_price)

        if hl_result.ambiguous:
            # Don't know if it filled. Query position to find out.
            expected_signed = qty if hl_side == "buy" else -qty
            log.warning(
                f"{symbol}: HL IOC ambiguous ({hl_result.error}) — reconciling against position"
            )
            filled, actual_signed, current_szi = await self.client.reconcile_hl_position_delta(
                symbol, baseline_szi, expected_signed,
            )
            if not filled:
                log.warning(
                    f"{symbol}: HL ambiguous reconciled as NOT filled "
                    f"(szi {baseline_szi}->{current_szi}) — entry aborted"
                )
                complete_intent(hl_intent_id, "no_fill", notes="ambiguous reconciled as unfilled")
                return False
            # It did fill. Reconstruct a synthetic OrderResult so the rest of
            # the flow can proceed. Use ref price as fill price (no avg available).
            log.critical(
                f"{symbol}: HL ambiguous reconciled as FILLED {abs(actual_signed)} "
                f"(szi {baseline_szi}->{current_szi}) — continuing entry"
            )
            hl_result = OrderResult(
                success=True,
                order_id="RECONCILED",
                filled_qty=abs(actual_signed),
                fill_price=hl_ref_price,
                ambiguous=False,
            )

        if not hl_result.success or hl_result.filled_qty <= 0:
            log.info(f"{symbol}: HL IOC did not fill ({hl_result.error}) — entry aborted")
            complete_intent(hl_intent_id, "no_fill", notes=hl_result.error[:200])
            return False

        # HL filled successfully — close out the intent before anything else can crash
        complete_intent(hl_intent_id, "filled",
                        notes=f"order_id={hl_result.order_id} qty={hl_result.filled_qty}")

        actual_qty = hl_result.filled_qty
        log.info(f"{symbol}: HL {hl_side} filled {actual_qty} @ {hl_result.fill_price:.2f}")

        # Step 2: Aster GTX (maker) — rests at best bid/ask
        aster_result = await self.client.place_aster_gtx(
            symbol, aster_side, actual_qty, aster_ref_price
        )

        # If ambiguous, check whether an order actually landed before deciding
        # to emergency-close HL. A 30s timeout that actually placed an order
        # would otherwise leave us racing two opposite Aster fills.
        if aster_result.ambiguous:
            log.warning(
                f"{symbol}: Aster GTX ambiguous ({aster_result.error}) — checking open orders"
            )
            await asyncio.sleep(1.5)
            try:
                open_orders = await self.client.get_aster_open_orders(symbol)
            except Exception as e:
                open_orders = []
                log.error(f"{symbol}: open orders query failed after ambiguous GTX: {e}")
            matching = [
                o for o in open_orders
                if str(o.get("side", "")).upper() == aster_side.upper()
            ]
            if matching:
                o = matching[0]
                aster_result = OrderResult(
                    success=True,
                    order_id=str(o.get("orderId", "")),
                    filled_qty=float(o.get("executedQty", 0) or 0),
                    fill_price=float(o.get("price", 0) or aster_ref_price),
                    ambiguous=False,
                )
                log.critical(
                    f"{symbol}: Aster GTX ambiguous reconciled as PLACED "
                    f"(oid={aster_result.order_id}) — continuing"
                )
            else:
                # No matching open order — likely the request never landed, or
                # it filled instantly. Check position to disambiguate.
                aster_pos = await self.client.get_aster_position(symbol)
                aster_qty = float(aster_pos.get("positionAmt", 0) or 0)
                expected_sign = -1 if aster_side == "sell" else 1
                if aster_qty * expected_sign > 0 and abs(aster_qty) >= actual_qty * 0.95:
                    # Filled instantly (GTX crossed). Treat as filled order.
                    aster_result = OrderResult(
                        success=True,
                        order_id="RECONCILED",
                        filled_qty=abs(aster_qty),
                        fill_price=aster_ref_price,
                        ambiguous=False,
                    )
                    log.critical(
                        f"{symbol}: Aster GTX ambiguous reconciled as INSTANT FILL "
                        f"qty={aster_qty} — continuing"
                    )
                # Else fall through to the failure branch below

        if not aster_result.success:
            # HL filled but Aster failed — emergency close HL
            log.error(f"{symbol}: Aster GTX failed after HL fill — emergency closing HL")
            close_side = "sell" if hl_side == "buy" else "buy"
            close_result = await self.client.place_hl_ioc(
                symbol, close_side, actual_qty, hl_result.fill_price
            )
            if not close_result.success:
                log.critical(f"{symbol}: HL emergency close also failed! Manual intervention needed.")
            return False

        # Record position as 'entering' — Aster GTX is resting, awaiting fill
        pos = self.pm.open_entering(
            symbol=symbol,
            hl_coin=f"xyz:{symbol}",
            aster_symbol=aster_symbol_for(symbol),
            direction=direction,
            entry_spread_bps=excess_bps,
            hl_entry_price=hl_result.fill_price,
            hl_order_id=hl_result.order_id,
            aster_entry_order_id=aster_result.order_id,
            qty=actual_qty,
            notional_usd=actual_notional,
            hl_funding_rate=hl_fr,
            aster_funding_rate=aster_fr,
            entry_baseline_bps=baseline_bps,
        )
        self.pm.log_trade(
            pos.id, "hl", hl_side, "ioc_limit",
            hl_result.order_id, actual_qty, hl_result.fill_price, notes="entry"
        )
        self.pm.log_trade(
            pos.id, "aster", aster_side, "gtx_limit",
            aster_result.order_id, actual_qty, aster_ref_price, notes="entry resting"
        )
        return True

    # ── Manual forced entry (funding-carry holds) ──

    async def force_entry(
        self, symbol: str, direction: str, notional: float,
        hold_for_funding: bool = True,
    ) -> tuple[bool, str]:
        """
        Place ONE delta-neutral position on demand, bypassing all signal gating
        (no threshold, no cost floor, no confirm streak). Used by the manual
        /enter command for funding-carry trades. Reuses the same leg-risk flow
        as try_entry: HL IOC taker first, then Aster GTX maker; emergency-close
        HL if the Aster leg can't be placed.

        Returns (ok, message). `message` is surfaced to the operator via alerting.
        """
        if direction not in ("long_hl_short_aster", "long_aster_short_hl"):
            return False, f"bad direction {direction!r}"
        if self.pm.has_position(symbol):
            return False, f"{symbol}: position already open"
        if notional <= 0:
            return False, f"{symbol}: notional must be > 0"

        try:
            aster_book, hl_book = await self.client.get_both_books(symbol)
        except Exception as e:
            return False, f"{symbol}: book fetch failed ({e})"
        if aster_book.bid <= 0 or hl_book.bid <= 0:
            return False, f"{symbol}: empty book"
        mid = (aster_book.mid + hl_book.mid) / 2
        if mid <= 0:
            return False, f"{symbol}: bad mid"

        price_ratio = abs(aster_book.mid - hl_book.mid) / min(aster_book.mid, hl_book.mid)
        if price_ratio > MAX_PRICE_RATIO_DIVERGENCE:
            return False, (
                f"{symbol}: price mismatch {aster_book.mid:.4f} (Ast) vs "
                f"{hl_book.mid:.4f} (HL), {price_ratio*100:.0f}% apart — refusing"
            )

        if direction == "long_hl_short_aster":
            hl_side, aster_side = "buy", "sell"
            hl_ref_price, aster_ref_price = hl_book.ask, aster_book.bid
            hl_avail, ast_avail = hl_book.ask_size, aster_book.bid_size
        else:
            hl_side, aster_side = "sell", "buy"
            hl_ref_price, aster_ref_price = hl_book.bid, aster_book.ask
            hl_avail, ast_avail = hl_book.bid_size, aster_book.ask_size

        # Size from notional, capped by HL taker-side top-of-book (the leg that
        # must fill immediately). The Aster maker leg rests, so it can exceed
        # Aster's top-of-book size.
        target_qty = notional / mid
        capped = min(target_qty, hl_avail) if hl_avail > 0 else target_qty
        qty = self.client.snap_aster_qty(symbol, capped)
        if qty <= 0:
            return False, f"{symbol}: qty snapped to 0 (HL avail {hl_avail:.2f})"
        actual_notional = qty * mid

        baseline = self.client.get_book_spread_baseline(symbol)
        baseline_bps = baseline if baseline is not None else 0.0
        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
        hl_fr = self.client.get_hl_funding_rate(symbol)
        aster_fr = self.client.get_aster_funding_rate(symbol)

        log.warning(
            f"MANUAL ENTRY {symbol}: {direction} | notional≈${actual_notional:.0f} "
            f"qty={qty} | HL {hl_side} @ {hl_ref_price:.2f} | "
            f"Aster {aster_side} @ {aster_ref_price:.2f} | hold_for_funding={hold_for_funding}"
        )

        if self.paper_mode:
            self.pm.open_entering(
                symbol=symbol, hl_coin=f"xyz:{symbol}",
                aster_symbol=aster_symbol_for(symbol),
                direction=direction, entry_spread_bps=spread_bps,
                hl_entry_price=hl_ref_price, hl_order_id="PAPER",
                aster_entry_order_id="PAPER", qty=qty, notional_usd=actual_notional,
                hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
                entry_baseline_bps=baseline_bps, hold_for_funding=hold_for_funding,
            )
            self.pm.confirm_aster_entry(symbol, aster_ref_price)
            return True, (
                f"[PAPER] entered {symbol} {direction} ${actual_notional:.0f} "
                f"(hold_for_funding={hold_for_funding})"
            )

        # ── Live placement (same leg-risk flow as try_entry) ──
        try:
            pre_pos = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre_pos.get("szi", 0) or 0)
        except Exception as e:
            return False, f"{symbol}: HL pre-position snapshot failed ({e}) — aborted"

        hl_intent_id = record_intent(
            symbol=symbol, venue="hl", action="entry_ioc",
            direction=direction, side=hl_side, qty=qty,
            ref_price=hl_ref_price, baseline_szi=baseline_szi, paper=False,
        )
        hl_result = await self.client.place_hl_ioc(symbol, hl_side, qty, hl_ref_price)

        if hl_result.ambiguous:
            expected_signed = qty if hl_side == "buy" else -qty
            filled, actual_signed, current_szi = await self.client.reconcile_hl_position_delta(
                symbol, baseline_szi, expected_signed,
            )
            if not filled:
                complete_intent(hl_intent_id, "no_fill", notes="ambiguous reconciled as unfilled")
                return False, f"{symbol}: HL ambiguous, reconciled as NOT filled — aborted"
            hl_result = OrderResult(
                success=True, order_id="RECONCILED",
                filled_qty=abs(actual_signed), fill_price=hl_ref_price, ambiguous=False,
            )

        if not hl_result.success or hl_result.filled_qty <= 0:
            complete_intent(hl_intent_id, "no_fill", notes=hl_result.error[:200])
            return False, f"{symbol}: HL IOC did not fill ({hl_result.error}) — aborted"

        complete_intent(hl_intent_id, "filled",
                        notes=f"order_id={hl_result.order_id} qty={hl_result.filled_qty}")
        actual_qty = hl_result.filled_qty
        log.warning(f"{symbol}: HL {hl_side} filled {actual_qty} @ {hl_result.fill_price:.2f}")

        aster_result = await self.client.place_aster_gtx(
            symbol, aster_side, actual_qty, aster_ref_price
        )
        if aster_result.ambiguous:
            await asyncio.sleep(1.5)
            try:
                open_orders = await self.client.get_aster_open_orders(symbol)
            except Exception:
                open_orders = []
            matching = [o for o in open_orders
                        if str(o.get("side", "")).upper() == aster_side.upper()]
            if matching:
                o = matching[0]
                aster_result = OrderResult(
                    success=True, order_id=str(o.get("orderId", "")),
                    filled_qty=float(o.get("executedQty", 0) or 0),
                    fill_price=float(o.get("price", 0) or aster_ref_price), ambiguous=False,
                )
            else:
                aster_pos = await self.client.get_aster_position(symbol)
                aster_qty = float(aster_pos.get("positionAmt", 0) or 0)
                expected_sign = -1 if aster_side == "sell" else 1
                if aster_qty * expected_sign > 0 and abs(aster_qty) >= actual_qty * 0.95:
                    aster_result = OrderResult(
                        success=True, order_id="RECONCILED",
                        filled_qty=abs(aster_qty), fill_price=aster_ref_price, ambiguous=False,
                    )

        if not aster_result.success:
            log.error(f"{symbol}: Aster GTX failed after HL fill — emergency closing HL")
            close_side = "sell" if hl_side == "buy" else "buy"
            close_result = await self.client.place_hl_ioc(
                symbol, close_side, actual_qty, hl_result.fill_price
            )
            if not close_result.success:
                log.critical(f"{symbol}: HL emergency close also FAILED — manual intervention!")
                return False, f"{symbol}: Aster failed AND HL emergency close failed — MANUAL FIX"
            return False, f"{symbol}: Aster leg failed, HL emergency-closed — no position"

        pos = self.pm.open_entering(
            symbol=symbol, hl_coin=f"xyz:{symbol}",
            aster_symbol=aster_symbol_for(symbol),
            direction=direction, entry_spread_bps=spread_bps,
            hl_entry_price=hl_result.fill_price, hl_order_id=hl_result.order_id,
            aster_entry_order_id=aster_result.order_id, qty=actual_qty,
            notional_usd=actual_notional, hl_funding_rate=hl_fr,
            aster_funding_rate=aster_fr, entry_baseline_bps=baseline_bps,
            hold_for_funding=hold_for_funding,
        )
        self.pm.log_trade(
            pos.id, "hl", hl_side, "ioc_limit",
            hl_result.order_id, actual_qty, hl_result.fill_price, notes="manual entry",
        )
        self.pm.log_trade(
            pos.id, "aster", aster_side, "gtx_limit",
            aster_result.order_id, actual_qty, aster_ref_price, notes="manual entry resting",
        )
        return True, (
            f"entered {symbol} {direction} ${actual_notional:.0f} qty={actual_qty} "
            f"(Aster maker resting; you'll get a fill confirmation)"
        )

    # ── Poll Aster maker fill (entering / exiting states) ──

    async def poll_aster_maker(self, symbol: str):
        """
        Called each tick for positions in 'entering' or 'exiting' state.
        Checks Aster GTX order status; if filled, advances position state.
        If price has moved, reprices the GTX order.
        Also checks entry timeout.
        """
        pos = self.pm.get(symbol)
        if not pos:
            return
        if pos.status not in ("entering", "exiting"):
            return

        is_entry = pos.status == "entering"
        order_id = pos.aster_entry_order_id if is_entry else pos.aster_exit_order_id

        if not order_id:
            return

        # Check timeouts (entry → close HL; exit → force-close Aster as taker)
        if is_entry:
            elapsed_min = (now_ms() - pos.entry_time) / 60_000
            if elapsed_min > ENTRY_TIMEOUT_MINUTES:
                await self._handle_entry_timeout(pos, order_id, elapsed_min)
                return
        else:
            elapsed_min = (now_ms() - pos.exit_time) / 60_000 if pos.exit_time else 0
            if elapsed_min > EXIT_TIMEOUT_MINUTES:
                log.warning(
                    f"{symbol}: Aster exit GTX timed out after {elapsed_min:.0f}min — "
                    f"cancelling and force-closing with IOC (taker)"
                )
                await self.client.cancel_aster_order(symbol, order_id)
                await self._force_close_aster_exit(pos)
                return

        # Query current Aster order status
        q = await self.client.query_aster_order(symbol, order_id)
        status = q.get("status", "")
        executed_qty = float(q.get("executedQty", 0) or 0)
        avg_price = float(q.get("avgPrice", 0) or 0)

        if status == "FILLED":
            if is_entry:
                self.pm.confirm_aster_entry(symbol, avg_price)
                self.pm.log_trade(
                    pos.id, "aster",
                    "sell" if pos.direction == "long_hl_short_aster" else "buy",
                    "gtx_limit", order_id, executed_qty, avg_price, notes="entry filled"
                )
            else:
                self.pm.confirm_aster_exit(symbol, avg_price, pos.exit_reason or "converged")
                self.pm.log_trade(
                    pos.id, "aster",
                    "buy" if pos.direction == "long_hl_short_aster" else "sell",
                    "gtx_limit", order_id, executed_qty, avg_price, notes="exit filled"
                )
            return

        if status in ASTER_TERMINAL_STATUSES - {"FILLED"}:
            log.warning(f"{symbol}: Aster GTX {order_id} is {status} — repricing")
            order_id = None  # fall through to repost below

        # Order still resting — check if price needs updating
        await self._reprice_aster_gtx(pos, is_entry, order_id)

    async def _handle_entry_timeout(self, pos, order_id: str, elapsed_min: float):
        """
        Entry maker timed out. Query for partial fills before cancelling so we
        can keep the matched portion hedged and only close the unmatched HL.
        """
        symbol = pos.symbol
        q = await self.client.query_aster_order(symbol, order_id)
        executed = float(q.get("executedQty", 0) or 0)
        avg_price = float(q.get("avgPrice", 0) or 0)
        await self.client.cancel_aster_order(symbol, order_id)

        spec = self.client.aster_specs.get(symbol)
        min_qty = spec.min_qty if spec else 0.0

        if executed >= min_qty and avg_price > 0:
            unmatched = pos.qty - executed
            log.warning(
                f"{symbol}: Aster entry timed out after {elapsed_min:.0f}min with PARTIAL fill "
                f"{executed}/{pos.qty} — keeping matched portion, closing HL remainder {unmatched}"
            )
            # Close unmatched HL portion (we have full HL qty but only `executed` Aster)
            if unmatched > 0:
                close_side = "sell" if pos.direction == "long_hl_short_aster" else "buy"
                hl_book = await self.client._get_hl_book(symbol)
                ref_price = hl_book.bid if close_side == "sell" else hl_book.ask
                if ref_price > 0:
                    res = await self.client.place_hl_ioc(symbol, close_side, unmatched, ref_price)
                    if not res.success:
                        log.critical(
                            f"{symbol}: HL partial-close FAILED ({res.error}) — "
                            f"position has {unmatched} unhedged HL. Manual intervention!"
                        )
            # Shrink position to matched qty and mark as open
            self.pm.confirm_aster_entry_partial(symbol, avg_price, executed)
        else:
            log.warning(
                f"{symbol}: Aster entry GTX timed out after {elapsed_min:.0f}min (0 fill) — "
                f"closing HL leg"
            )
            await self._emergency_close_hl(pos)

    async def _force_close_aster_exit(self, pos):
        """Force-close Aster exit leg with a taker IOC after maker timeout."""
        symbol = pos.symbol
        close_side = "buy" if pos.direction == "long_hl_short_aster" else "sell"
        aster_book = await self.client._get_aster_book(symbol)
        ref_price = aster_book.ask if close_side == "buy" else aster_book.bid
        if ref_price <= 0:
            log.critical(f"{symbol}: empty Aster book on force exit — manual intervention!")
            self.pm.mark_error(symbol, "force_exit_empty_book")
            return
        force_intent_id = record_intent(
            symbol=symbol, venue="aster", action="force_exit_ioc",
            direction=pos.direction, side=close_side, qty=pos.qty,
            ref_price=ref_price, position_id=pos.id, paper=self.paper_mode,
        )
        result = await self.client.place_aster_ioc(symbol, close_side, pos.qty, ref_price)
        if result.success and result.filled_qty > 0:
            log.info(
                f"{symbol}: Aster force-exit filled {result.filled_qty} @ {result.fill_price:.4f}"
            )
            complete_intent(force_intent_id, "filled",
                            notes=f"oid={result.order_id} qty={result.filled_qty}",
                            position_id=pos.id)
            self.pm.log_trade(
                pos.id, "aster", close_side, "ioc_limit",
                result.order_id, result.filled_qty, result.fill_price, notes="exit forced taker",
            )
            self.pm.confirm_aster_exit(
                symbol, result.fill_price, (pos.exit_reason or "converged") + "_taker"
            )
        else:
            log.critical(
                f"{symbol}: Aster force-exit FAILED ({result.error}) — UNHEDGED. "
                f"Manual intervention!"
            )
            complete_intent(force_intent_id, "rejected",
                            notes=(result.error or "no_fill")[:200], position_id=pos.id)
            self.pm.mark_error(symbol, f"force_exit_failed: {result.error}")

    async def _reprice_aster_gtx(self, pos, is_entry: bool, current_order_id: str | None):
        """
        Cancel existing GTX and repost at new best price — with a race guard for
        the case where the order fills between query and cancel (we'd otherwise
        repost and double the position).
        """
        symbol = pos.symbol
        aster_book = await self.client._get_aster_book(symbol)
        if aster_book.bid <= 0:
            return

        if is_entry:
            side = "sell" if pos.direction == "long_hl_short_aster" else "buy"
            target_price = aster_book.bid if side == "sell" else aster_book.ask
        else:
            side = "buy" if pos.direction == "long_hl_short_aster" else "sell"
            target_price = aster_book.ask if side == "buy" else aster_book.bid

        spec = self.client.aster_specs.get(symbol)
        tick = spec.tick_size if spec else 0.01

        # If we have an existing order, check its state first
        new_qty = pos.qty
        if current_order_id:
            q = await self.client.query_aster_order(symbol, current_order_id)
            current_price = float(q.get("price", 0) or 0)
            current_status = q.get("status", "")
            executed = float(q.get("executedQty", 0) or 0)
            avg_price = float(q.get("avgPrice", 0) or 0)

            # Race: filled between last poll and now → advance state, don't repost
            if current_status == "FILLED":
                log.info(f"{symbol}: GTX {current_order_id} filled during reprice check")
                if is_entry:
                    self.pm.confirm_aster_entry(symbol, avg_price)
                else:
                    self.pm.confirm_aster_exit(symbol, avg_price, pos.exit_reason or "converged")
                return

            # Already at best price (within half a tick) — nothing to do
            if current_price > 0 and abs(target_price - current_price) < tick / 2:
                return

            # Cancel and re-query to capture any fill that landed during cancel
            await self.client.cancel_aster_order(symbol, current_order_id)
            q2 = await self.client.query_aster_order(symbol, current_order_id)
            final_executed = float(q2.get("executedQty", executed) or executed)
            final_avg = float(q2.get("avgPrice", avg_price) or avg_price)

            if final_executed >= pos.qty * 0.999:
                # Fully filled in the race — advance state, don't repost
                log.warning(
                    f"{symbol}: GTX {current_order_id} fully filled during cancel race"
                )
                if is_entry:
                    self.pm.confirm_aster_entry(symbol, final_avg)
                else:
                    self.pm.confirm_aster_exit(symbol, final_avg, pos.exit_reason or "converged")
                return

            if final_executed > executed and final_executed > 0:
                # Partial fill during cancel — repost only for the remainder
                remainder = pos.qty - final_executed
                snapped = self.client.snap_aster_qty(symbol, remainder)
                if snapped > 0:
                    log.warning(
                        f"{symbol}: GTX partial-fill {final_executed} during cancel, "
                        f"reposting remainder {snapped}"
                    )
                    new_qty = snapped
                else:
                    log.warning(
                        f"{symbol}: GTX partial-fill {final_executed} during cancel, "
                        f"remainder too small to repost — treating as filled"
                    )
                    if is_entry:
                        self.pm.confirm_aster_entry_partial(symbol, final_avg, final_executed)
                    else:
                        self.pm.confirm_aster_exit(symbol, final_avg, pos.exit_reason or "converged")
                    return

        new_result = await self.client.place_aster_gtx(symbol, side, new_qty, target_price)
        if new_result.success:
            new_oid = new_result.order_id
            if is_entry:
                pos.aster_entry_order_id = new_oid
            else:
                pos.aster_exit_order_id = new_oid

            from database import get_connection
            conn = get_connection()
            if is_entry:
                conn.execute(
                    "UPDATE positions SET aster_entry_order_id=? WHERE id=?",
                    (new_oid, pos.id)
                )
            else:
                conn.execute(
                    "UPDATE positions SET aster_exit_order_id=? WHERE id=?",
                    (new_oid, pos.id)
                )
            conn.commit()
            conn.close()
            log.info(
                f"{symbol}: Aster GTX repriced {side.upper()} @ {target_price:.2f} -> {new_oid}"
            )
        else:
            log.error(f"{symbol}: Aster GTX reprice failed: {new_result.error}")

    # ── Exit ──

    async def try_exit(
        self,
        symbol: str,
        aster_book: OrderBook,
        hl_book: OrderBook,
        reason: str,
    ) -> bool:
        pos = self.pm.get(symbol)
        if not pos or pos.status != "open":
            return False

        if self.paper_mode:
            mid = (aster_book.mid + hl_book.mid) / 2
            exit_spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000 if mid > 0 else 0
            if pos.direction == "long_hl_short_aster":
                hl_exit_px = hl_book.bid
                aster_exit_px = aster_book.ask
            else:
                hl_exit_px = hl_book.ask
                aster_exit_px = aster_book.bid
            self.pm.start_exiting(symbol, "PAPER", "PAPER", hl_exit_px, exit_spread_bps)
            self.pm.confirm_aster_exit(symbol, aster_exit_px, reason)
            log.info(f"[PAPER] EXIT {symbol} ({reason}) | spread={exit_spread_bps:.1f}bps")
            return True

        # HL close (taker)
        if pos.direction == "long_hl_short_aster":
            hl_close_side = "sell"
            hl_ref_price = hl_book.bid
            aster_close_side = "buy"
            aster_ref_price = aster_book.ask
        else:
            hl_close_side = "buy"
            hl_ref_price = hl_book.ask
            aster_close_side = "sell"
            aster_ref_price = aster_book.bid

        mid = (aster_book.mid + hl_book.mid) / 2
        exit_spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000 if mid > 0 else 0

        log.info(
            f"EXIT {symbol} ({reason}): spread={exit_spread_bps:.1f}bps | "
            f"HL {hl_close_side} @ {hl_ref_price:.2f} | Aster {aster_close_side} @ {aster_ref_price:.2f}"
        )

        # Snapshot HL baseline + record intent BEFORE the close call. Closing the
        # HL leg actually changes position; if we crash between fill and DB write
        # the bot would otherwise try to close again on restart and flip naked.
        try:
            pre_pos = await self.client.get_hl_position(symbol)
            exit_baseline_szi = float(pre_pos.get("szi", 0) or 0)
        except Exception as e:
            log.error(f"{symbol}: HL exit pre-snapshot failed ({e}) — aborting exit")
            return False

        exit_intent_id = record_intent(
            symbol=symbol, venue="hl", action="exit_ioc",
            direction=pos.direction, side=hl_close_side, qty=pos.qty,
            ref_price=hl_ref_price, baseline_szi=exit_baseline_szi,
            position_id=pos.id, paper=self.paper_mode,
        )

        # Step 1: HL IOC close — keep filling until we have the full qty (max 2 attempts)
        hl_filled = 0.0
        hl_avg_price = 0.0
        hl_order_id = ""
        last_err = ""
        for attempt in range(2):
            remaining = pos.qty - hl_filled
            if remaining <= 0:
                break
            res = await self.client.place_hl_ioc(symbol, hl_close_side, remaining, hl_ref_price)
            if res.success and res.filled_qty > 0:
                # Weighted-average the fill price across attempts
                new_total = hl_filled + res.filled_qty
                hl_avg_price = (hl_avg_price * hl_filled + res.fill_price * res.filled_qty) / new_total
                hl_filled = new_total
                hl_order_id = res.order_id or hl_order_id
            elif res.ambiguous and attempt == 0:
                # Don't risk a second IOC on an ambiguous first response — reconcile.
                expected_signed = -remaining if hl_close_side == "sell" else remaining
                filled_check, actual_signed, _ = await self.client.reconcile_hl_position_delta(
                    symbol, exit_baseline_szi - hl_filled * (-1 if hl_close_side == "sell" else 1),
                    expected_signed,
                )
                if filled_check:
                    new_total = hl_filled + abs(actual_signed)
                    hl_avg_price = (
                        (hl_avg_price * hl_filled + hl_ref_price * abs(actual_signed)) / new_total
                    )
                    hl_filled = new_total
                    hl_order_id = hl_order_id or "RECONCILED"
                    log.critical(
                        f"{symbol}: HL exit ambiguous reconciled as FILLED {abs(actual_signed)}"
                    )
                    break  # don't retry — we got what we expected
                last_err = "ambiguous_not_filled"
            else:
                last_err = res.error or last_err
                if attempt == 0:
                    log.warning(f"{symbol}: HL exit attempt 1 returned 0 fill ({res.error}), retrying")

        if hl_filled <= 0:
            log.critical(f"{symbol}: HL exit FAILED — manual intervention needed ({last_err})")
            complete_intent(exit_intent_id, "rejected", notes=last_err[:200], position_id=pos.id)
            self.pm.mark_error(symbol, f"HL exit failed: {last_err}")
            return False

        # HL leg closed (fully or partially) — close intent before any downstream work
        complete_intent(
            exit_intent_id, "filled",
            notes=f"hl_filled={hl_filled} qty={pos.qty}", position_id=pos.id,
        )

        partial_hl = hl_filled < pos.qty * 0.999  # tolerate 0.1% rounding
        if partial_hl:
            log.warning(
                f"{symbol}: HL exit PARTIAL {hl_filled}/{pos.qty} — closing matched Aster qty, "
                f"residual {pos.qty - hl_filled} will need manual close"
            )

        log.info(f"{symbol}: HL exit filled {hl_filled} @ {hl_avg_price:.2f}")

        # Step 2: Aster close — match the HL filled qty exactly to stay hedged
        aster_qty = self.client.snap_aster_qty(symbol, hl_filled)
        if aster_qty <= 0:
            log.critical(f"{symbol}: HL filled {hl_filled} but Aster snap returned 0 — UNHEDGED")
            self.pm.mark_error(symbol, "aster_qty_snap_zero")
            return False

        aster_result = await self.client.place_aster_gtx(
            symbol, aster_close_side, aster_qty, aster_ref_price
        )

        # If maker fails, escalate to real IOC (taker) — pays ~0.9bps to escape stuck exit
        if not aster_result.success:
            log.error(f"{symbol}: Aster exit GTX failed ({aster_result.error}) — forcing IOC taker")
            force_intent_id = record_intent(
                symbol=symbol, venue="aster", action="force_exit_ioc",
                direction=pos.direction, side=aster_close_side, qty=aster_qty,
                ref_price=aster_ref_price, position_id=pos.id, paper=self.paper_mode,
            )
            aster_result = await self.client.place_aster_ioc(
                symbol, aster_close_side, aster_qty, aster_ref_price
            )
            if not aster_result.success or aster_result.filled_qty <= 0:
                log.critical(f"{symbol}: Aster exit FAILED — UNHEDGED. Manual intervention!")
                complete_intent(force_intent_id, "rejected",
                                notes=(aster_result.error or "no_fill")[:200], position_id=pos.id)
                self.pm.mark_error(symbol, f"Aster exit failed: {aster_result.error}")
                return False
            complete_intent(force_intent_id, "filled",
                            notes=f"oid={aster_result.order_id} qty={aster_result.filled_qty}",
                            position_id=pos.id)
            # IOC filled immediately — start_exiting then confirm in one shot
            self.pm.start_exiting(
                symbol, hl_order_id, aster_result.order_id, hl_avg_price, exit_spread_bps,
            )
            self.pm.log_trade(
                pos.id, "hl", hl_close_side, "ioc_limit",
                hl_order_id, hl_filled, hl_avg_price, notes="exit" + (" partial" if partial_hl else ""),
            )
            self.pm.log_trade(
                pos.id, "aster", aster_close_side, "ioc_limit",
                aster_result.order_id, aster_result.filled_qty, aster_result.fill_price,
                notes="exit forced taker",
            )
            self.pm.confirm_aster_exit(symbol, aster_result.fill_price, reason + "_taker")
            if partial_hl:
                self.pm.mark_error(symbol, "hl_exit_partial_residual")
            return True

        # Maker order resting — wait for fill via poll_aster_maker
        pos.exit_reason = reason
        self.pm.start_exiting(
            symbol, hl_order_id, aster_result.order_id, hl_avg_price, exit_spread_bps,
        )
        self.pm.log_trade(
            pos.id, "hl", hl_close_side, "ioc_limit",
            hl_order_id, hl_filled, hl_avg_price, notes="exit" + (" partial" if partial_hl else ""),
        )
        self.pm.log_trade(
            pos.id, "aster", aster_close_side, "gtx_limit",
            aster_result.order_id, aster_qty, aster_ref_price, notes="exit resting",
        )
        # If HL partial, update pos.qty to match what's actually open on Aster
        if partial_hl:
            pos.qty = aster_qty
            from database import get_connection
            conn = get_connection()
            conn.execute("UPDATE positions SET qty=? WHERE id=?", (aster_qty, pos.id))
            conn.commit()
            conn.close()
        return True

    async def _emergency_close_hl(self, pos):
        """Close the HL leg after Aster entry timed out."""
        symbol = pos.symbol
        close_side = "sell" if pos.direction == "long_hl_short_aster" else "buy"
        hl_book = await self.client._get_hl_book(symbol)
        ref_price = hl_book.bid if close_side == "sell" else hl_book.ask
        log.warning(f"{symbol}: emergency closing HL leg ({close_side} @ {ref_price:.2f})")
        result = await self.client.place_hl_ioc(symbol, close_side, pos.qty, ref_price)
        if result.success:
            log.info(f"{symbol}: HL emergency close filled @ {result.fill_price:.2f}")
            self.pm.mark_error(symbol, "entry_timeout_hl_closed")
        else:
            log.critical(f"{symbol}: HL emergency close FAILED — {result.error}. Manual intervention!")
            self.pm.mark_error(symbol, f"entry_timeout_HL_close_failed: {result.error}")

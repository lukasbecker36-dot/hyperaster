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
    MAKER_ENTRY_TIMEOUT_SEC, MAKER_REPRICE_TICK_FRAC,
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
        existing = self.pm.get(symbol)
        if existing and existing.status != "open":
            return False, f"{symbol}: position in {existing.status} state — wait"
        if existing and existing.direction != direction:
            return False, f"{symbol}: already open in opposite direction"
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

        target_qty = notional / mid
        qty = self.client.snap_aster_qty(symbol, target_qty)
        if qty <= 0:
            return False, f"{symbol}: qty snapped to 0 (lot size too large for ${notional:.0f})"
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
            if existing:
                self.pm.scale_in(
                    symbol, qty, actual_notional, hl_ref_price, aster_ref_price,
                    hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
                )
                return True, (
                    f"[PAPER] scaled in {symbol} +${actual_notional:.0f} → "
                    f"${existing.notional_usd:.0f} total"
                )
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
        # Set 5x isolated leverage/margin on both venues before the first order
        # for this symbol (idempotent — no-op once done).
        await self.client.ensure_perp_margin(symbol)

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

        actual_notional = actual_qty * mid
        if existing:
            self.pm.scale_in(
                symbol, actual_qty, actual_notional,
                hl_result.fill_price, aster_ref_price,
                hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
            )
            self.pm.log_trade(
                existing.id, "hl", hl_side, "ioc_limit",
                hl_result.order_id, actual_qty, hl_result.fill_price, notes="scale-in",
            )
            self.pm.log_trade(
                existing.id, "aster", aster_side, "gtx_limit",
                aster_result.order_id, actual_qty, aster_ref_price, notes="scale-in resting",
            )
            return True, (
                f"scaled in {symbol} +${actual_notional:.0f} qty={actual_qty} → "
                f"${existing.notional_usd:.0f} total"
            )
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

    # ── Maker-first carry entry (HL post-only maker, Aster IOC taker hedge) ──

    async def force_entry_maker(
        self, symbol: str, direction: str, notional: float,
    ) -> tuple[bool, str]:
        """
        Open a funding-carry hold maker-first: rest a post-only HL order (the
        more-liquid venue) and let poll_hl_maker hedge each HL fill with an
        Aster IOC taker. Patient by design — you only pay the Aster spread, and
        capture the HL spread. Scale-in adds to an existing open position.

        Returns (ok, message). The HL maker rests across ticks; the operator
        gets a separate alert when it fully fills.
        """
        if direction not in ("long_hl_short_aster", "long_aster_short_hl"):
            return False, f"bad direction {direction!r}"
        if self.pm.has_position(symbol):
            # Maker-first rests the full notional (no top-of-book cap), so there's
            # no scale-in need — one position per symbol. Close it to resize.
            return False, f"{symbol}: position already exists — close it first to resize"
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

        # HL leg is the maker. Long HL -> buy resting at the bid; short HL ->
        # sell resting at the ask. (Aster taker hedges later, in poll_hl_maker.)
        if direction == "long_hl_short_aster":
            hl_side, hl_ref_price = "buy", hl_book.bid
        else:
            hl_side, hl_ref_price = "sell", hl_book.ask

        qty = self.client.snap_aster_qty(symbol, notional / mid)
        if qty <= 0:
            return False, f"{symbol}: qty snapped to 0 (lot too large for ${notional:.0f})"
        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
        hl_fr = self.client.get_hl_funding_rate(symbol)
        aster_fr = self.client.get_aster_funding_rate(symbol)

        log.warning(
            f"MAKER ENTRY {symbol}: {direction} | notional≈${qty*mid:.0f} qty={qty} | "
            f"HL {hl_side} maker @ {hl_ref_price:.2f}"
        )

        if self.paper_mode:
            # Paper: assume the maker fills at its resting price and the Aster
            # taker hedges instantly at the touch.
            aster_fill = aster_book.bid if direction == "long_hl_short_aster" else aster_book.ask
            self.pm.open_hl_maker_entering(
                symbol=symbol, aster_symbol=aster_symbol_for(symbol),
                direction=direction, hl_maker_order_id="PAPER",
                hl_baseline_szi=0.0, qty=qty, notional_usd=qty * mid,
                entry_spread_bps=spread_bps, hl_ref_price=hl_ref_price,
                hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
            )
            self.pm.confirm_hl_maker_open(symbol, qty, hl_ref_price, aster_fill)
            return True, f"[PAPER] entered {symbol} {direction} ${qty*mid:.0f} (maker-first)"

        # ── Live: rest the HL post-only maker; poll_hl_maker advances it ──
        await self.client.ensure_perp_margin(symbol)
        try:
            pre_pos = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre_pos.get("szi", 0) or 0)
        except Exception as e:
            return False, f"{symbol}: HL pre-position snapshot failed ({e})"

        alo = await self.client.place_hl_alo(symbol, hl_side, qty, hl_ref_price)
        if not alo.success:
            return False, f"{symbol}: HL maker not placed ({alo.error})"

        self.pm.open_hl_maker_entering(
            symbol=symbol, aster_symbol=aster_symbol_for(symbol),
            direction=direction, hl_maker_order_id=alo.order_id,
            hl_baseline_szi=baseline_szi, qty=qty, notional_usd=qty * mid,
            entry_spread_bps=spread_bps, hl_ref_price=hl_ref_price,
            hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
        )
        return True, (
            f"resting HL maker for {symbol} {direction} ${qty*mid:.0f} @ {hl_ref_price:.2f} "
            f"— hedging on fill"
        )

    async def poll_hl_maker(self, symbol: str):
        """
        Per-tick driver for a maker-first carry entry. Detects HL fills via the
        position szi delta, hedges each new increment with an Aster IOC taker,
        reprices the resting HL maker if the touch moves, and finalises on full
        fill or timeout. Only ever hedges what HL has actually filled, so there
        is no naked Aster exposure.
        """
        pos = self.pm.get(symbol)
        if not pos or pos.status != "entering" or pos.entry_maker_venue != "hl":
            return

        long_hl = pos.direction == "long_hl_short_aster"
        hl_side = "buy" if long_hl else "sell"
        aster_hedge_side = "sell" if long_hl else "buy"

        # How much of the HL maker has filled (signed szi delta from baseline).
        try:
            hlp = await self.client.get_hl_position(symbol)
            current_szi = float(hlp.get("szi", 0) or 0)
        except Exception as e:
            log.warning(f"{symbol}: maker poll szi fetch failed ({e})")
            return
        signed_delta = current_szi - pos.hl_baseline_szi
        hl_filled = signed_delta if long_hl else -signed_delta
        hl_filled = max(0.0, hl_filled)

        # Hedge any newly-filled HL qty with an Aster IOC taker.
        new_fill = hl_filled - pos.aster_hedged_qty
        min_lot = self.client.snap_aster_qty(symbol, new_fill) if new_fill > 0 else 0.0
        if min_lot > 0:
            try:
                aster_book = await self.client._get_aster_book(symbol)
            except Exception:
                aster_book = None
            if aster_book and aster_book.bid > 0:
                touch = aster_book.bid if aster_hedge_side == "sell" else aster_book.ask
                hedge_intent = record_intent(
                    symbol=symbol, venue="aster", action="maker_hedge_ioc",
                    direction=pos.direction, side=aster_hedge_side, qty=min_lot,
                    ref_price=touch, position_id=pos.id, paper=False,
                )
                res = await self.client.place_aster_ioc(symbol, aster_hedge_side, min_lot, touch)
                if res.success and res.filled_qty > 0:
                    complete_intent(hedge_intent, "filled",
                                    notes=f"qty={res.filled_qty} px={res.fill_price}",
                                    position_id=pos.id)
                    new_hedged = pos.aster_hedged_qty + res.filled_qty
                    # Running VWAPs: HL at its resting price, Aster at the fill.
                    old = pos.aster_hedged_qty
                    a_avg = ((pos.aster_entry_price * old + res.fill_price * res.filled_qty)
                             / new_hedged) if new_hedged > 0 else res.fill_price
                    h_avg = ((pos.hl_entry_price * old + pos.hl_entry_price * res.filled_qty)
                             / new_hedged) if new_hedged > 0 else pos.hl_entry_price
                    self.pm.record_hl_maker_progress(symbol, hl_filled, new_hedged, h_avg, a_avg)
                    self.pm.log_trade(pos.id, "aster", aster_hedge_side, "ioc_hedge",
                                      res.order_id, res.filled_qty, res.fill_price,
                                      notes="maker-entry hedge")
                    log.warning(f"{symbol}: hedged {res.filled_qty} on Aster @ {res.fill_price:.2f} "
                                f"({new_hedged}/{pos.qty})")
                else:
                    complete_intent(hedge_intent, "rejected",
                                    notes=(res.error or "no_fill")[:120], position_id=pos.id)
                    log.error(f"{symbol}: Aster hedge IOC failed ({res.error}) — will retry next tick")
                    return  # don't advance until the fill is hedged

        # Fully filled and hedged → open.
        if hl_filled >= pos.qty * 0.999 and pos.aster_hedged_qty >= hl_filled * 0.999:
            if pos.hl_entry_order_id:
                await self.client.cancel_hl_order(symbol, pos.hl_entry_order_id)
            self.pm.confirm_hl_maker_open(symbol, hl_filled, pos.hl_entry_price, pos.aster_entry_price)
            return

        elapsed = (now_ms() - pos.entry_time) / 1000
        if elapsed >= MAKER_ENTRY_TIMEOUT_SEC:
            # Give up the unfilled remainder. Cancel the maker, re-check szi for
            # any last fill, hedge it, and open at the partial qty (or drop the
            # record if nothing filled — no exposure was ever taken).
            if pos.hl_entry_order_id:
                await self.client.cancel_hl_order(symbol, pos.hl_entry_order_id)
            await asyncio.sleep(0.5)
            try:
                hlp = await self.client.get_hl_position(symbol)
                signed_delta = float(hlp.get("szi", 0) or 0) - pos.hl_baseline_szi
                hl_filled = max(0.0, signed_delta if long_hl else -signed_delta)
            except Exception:
                pass
            if hl_filled <= 0:
                log.warning(f"{symbol}: maker entry timed out unfilled — cancelling record")
                self.pm.drop_entering(symbol)
                return
            # Hedge any residual then open at the delta-neutral (hedged) qty.
            residual = self.client.snap_aster_qty(symbol, hl_filled - pos.aster_hedged_qty)
            if residual > 0:
                try:
                    aster_book = await self.client._get_aster_book(symbol)
                    touch = aster_book.bid if aster_hedge_side == "sell" else aster_book.ask
                    res = await self.client.place_aster_ioc(symbol, aster_hedge_side, residual, touch)
                    if res.success and res.filled_qty > 0:
                        new_hedged = pos.aster_hedged_qty + res.filled_qty
                        self.pm.record_hl_maker_progress(
                            symbol, hl_filled, new_hedged, pos.hl_entry_price, res.fill_price)
                except Exception as e:
                    log.critical(f"{symbol}: timeout residual hedge failed ({e}) — CHECK MANUALLY")
            # Open at the matched (hedged) qty so the record stays delta-neutral.
            final_qty = min(hl_filled, pos.aster_hedged_qty)
            naked_hl = hl_filled - final_qty
            if naked_hl > residual * 0.001 + 1e-9:
                log.critical(
                    f"{symbol}: maker entry timeout left {naked_hl:.4f} HL UNHEDGED "
                    f"(filled {hl_filled}, hedged {pos.aster_hedged_qty}) — CHECK MANUALLY"
                )
            if final_qty <= 0:
                self.pm.drop_entering(symbol, "maker_entry_unhedged")
                return
            log.warning(f"{symbol}: maker entry partial {final_qty}/{pos.qty} — opening")
            self.pm.confirm_hl_maker_open(symbol, final_qty, pos.hl_entry_price, pos.aster_entry_price)
            return

        # Reprice the resting maker if the touch has drifted away — but only
        # before any fill, so the recorded entry price stays clean and we don't
        # chase a moving market on a partially-filled order (the timeout opens
        # whatever filled).
        if hl_filled <= 0:
            await self._reprice_hl_maker(pos, hl_side, hl_filled)

    async def _reprice_hl_maker(self, pos, hl_side: str, hl_filled: float):
        """Cancel + repost the resting HL maker at the new touch for the unfilled
        remainder, if it has drifted more than a fraction of a tick."""
        symbol = pos.symbol
        try:
            _, hl_book = await self.client.get_both_books(symbol)
        except Exception:
            return
        touch = hl_book.bid if hl_side == "buy" else hl_book.ask
        if touch <= 0:
            return
        spec = self.client.hl_specs.get(symbol)
        tick = 10 ** (-spec.price_precision) if spec else 0.01
        if abs(touch - pos.hl_entry_price) < tick * MAKER_REPRICE_TICK_FRAC:
            return
        remainder = self.client.snap_aster_qty(symbol, pos.qty - hl_filled)
        if remainder <= 0:
            return
        if pos.hl_entry_order_id:
            await self.client.cancel_hl_order(symbol, pos.hl_entry_order_id)
        alo = await self.client.place_hl_alo(symbol, hl_side, remainder, touch)
        if alo.success:
            pos.hl_entry_order_id = alo.order_id
            pos.hl_entry_price = touch
            from database import get_connection
            conn = get_connection()
            conn.execute("UPDATE positions SET hl_entry_order_id=?, hl_entry_price=? WHERE id=?",
                         (alo.order_id, touch, pos.id))
            conn.commit()
            conn.close()
            log.info(f"{symbol}: repriced HL maker -> {alo.order_id} @ {touch:.2f} (rem {remainder})")
        else:
            log.warning(f"{symbol}: HL maker reprice failed ({alo.error})")

    # ── Maker-first carry exit (HL post-only maker, Aster IOC taker hedge) ──

    # User-initiated carry exits run maker-first for a good basis. The automated
    # safety bail (funding_stop) is a loss-cut and must cross immediately — never
    # rest a patient maker while a position bleeds.
    PATIENT_EXIT_REASONS = {"manual", "manual_target"}

    async def exit_position(
        self, symbol: str, aster_book: OrderBook, hl_book: OrderBook, reason: str,
    ) -> bool:
        """Dispatch an exit. A user-initiated carry close (hold_for_funding, manual
        reason) goes maker-first on HL with an Aster taker hedge — same patient
        convention as its entry, so the executable exit basis matches what
        /positions and /close display. Safety stops and everything else use the
        legacy taker-first try_exit for an immediate fill."""
        pos = self.pm.get(symbol)
        if pos and pos.hold_for_funding and reason in self.PATIENT_EXIT_REASONS:
            ok = await self.force_exit_maker(symbol, reason)
            if ok:
                return True
            # Maker exit couldn't even be placed — fall back to taker so we're
            # never stuck unable to close a carry hold.
            log.warning(f"{symbol}: maker exit unavailable — falling back to taker exit")
        return await self.try_exit(symbol, aster_book, hl_book, reason)

    async def force_exit_maker(self, symbol: str, reason: str) -> bool:
        """Close a carry hold maker-first: rest a post-only HL order on the close
        side (sell@ask for a long-HL leg, buy@bid for a short-HL leg) and let
        poll_hl_maker_exit cross Aster (IOC taker) to close each HL fill. Returns
        False if the HL maker can't be placed (caller falls back to taker)."""
        pos = self.pm.get(symbol)
        if not pos or pos.status != "open":
            return False
        try:
            aster_book, hl_book = await self.client.get_both_books(symbol)
        except Exception as e:
            log.error(f"{symbol}: maker-exit book fetch failed ({e})")
            return False
        if aster_book.bid <= 0 or hl_book.bid <= 0:
            log.warning(f"{symbol}: empty book on maker exit")
            return False
        mid = (aster_book.mid + hl_book.mid) / 2
        exit_spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000 if mid > 0 else 0.0

        closing_long = pos.direction == "long_hl_short_aster"  # long HL leg
        if closing_long:
            hl_side, hl_ref = "sell", hl_book.ask    # sell the long HL leg, maker @ ask
            aster_fill = aster_book.ask              # buy back the short Aster leg @ ask
        else:
            hl_side, hl_ref = "buy", hl_book.bid     # buy back the short HL leg, maker @ bid
            aster_fill = aster_book.bid              # sell the long Aster leg @ bid

        log.warning(
            f"MAKER EXIT {symbol} ({reason}): {pos.direction} | qty={pos.qty} | "
            f"HL {hl_side} maker @ {hl_ref:.2f}"
        )

        if self.paper_mode:
            self.pm.start_exiting_hl_maker(symbol, "PAPER", 0.0, hl_ref, exit_spread_bps)
            self.pm.confirm_hl_maker_exit(symbol, pos.qty, hl_ref, aster_fill, reason)
            log.info(f"[PAPER] MAKER EXIT {symbol} ({reason}) spread={exit_spread_bps:.1f}bps")
            return True

        try:
            pre = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre.get("szi", 0) or 0)
        except Exception as e:
            log.error(f"{symbol}: HL exit pre-snapshot failed ({e})")
            return False

        alo = await self.client.place_hl_alo(symbol, hl_side, pos.qty, hl_ref)
        if not alo.success:
            log.error(f"{symbol}: HL exit maker not placed ({alo.error})")
            return False

        pos.exit_reason = reason
        self.pm.start_exiting_hl_maker(symbol, alo.order_id, baseline_szi, hl_ref, exit_spread_bps)
        return True

    async def poll_hl_maker_exit(self, symbol: str):
        """Per-tick driver for a maker-first carry exit. Detects HL closes via the
        position szi delta, closes each new increment on Aster with an IOC taker,
        reprices the resting HL maker if the touch moves (before any fill), and
        finalises on full close. No timeout — the maker rests indefinitely. If the
        basis moves so favorably that a taker-taker exit is net-positive after fees
        and bid/offer, escalates to taker-taker to capture the windfall now.
        Only ever closes as much Aster as HL has closed — no naked exposure."""
        pos = self.pm.get(symbol)
        if (not pos or pos.status != "exiting" or not pos.hold_for_funding
                or pos.aster_exit_order_id):
            return

        closing_long = pos.direction == "long_hl_short_aster"
        hl_side = "sell" if closing_long else "buy"
        aster_hedge_side = "buy" if closing_long else "sell"  # close the Aster leg

        try:
            hlp = await self.client.get_hl_position(symbol)
            current_szi = float(hlp.get("szi", 0) or 0)
        except Exception as e:
            log.warning(f"{symbol}: maker exit poll szi fetch failed ({e})")
            return
        # Closing a long HL leg SELLS (szi falls from baseline); closing a short
        # HL leg BUYS (szi rises toward baseline). Both give a positive closed qty.
        signed = (pos.hl_baseline_szi - current_szi) if closing_long else (current_szi - pos.hl_baseline_szi)
        hl_closed = max(0.0, signed)

        # Close any newly-filled HL qty on Aster with an IOC taker.
        new_fill = hl_closed - pos.aster_hedged_qty
        lot = self.client.snap_aster_qty(symbol, new_fill) if new_fill > 0 else 0.0
        if lot > 0:
            try:
                aster_book = await self.client._get_aster_book(symbol)
            except Exception:
                aster_book = None
            if aster_book and aster_book.bid > 0:
                touch = aster_book.ask if aster_hedge_side == "buy" else aster_book.bid
                hedge_intent = record_intent(
                    symbol=symbol, venue="aster", action="maker_exit_hedge_ioc",
                    direction=pos.direction, side=aster_hedge_side, qty=lot,
                    ref_price=touch, position_id=pos.id, paper=False,
                )
                res = await self.client.place_aster_ioc(symbol, aster_hedge_side, lot, touch)
                if res.success and res.filled_qty > 0:
                    complete_intent(hedge_intent, "filled",
                                    notes=f"qty={res.filled_qty} px={res.fill_price}",
                                    position_id=pos.id)
                    new_closed = pos.aster_hedged_qty + res.filled_qty
                    old = pos.aster_hedged_qty
                    a_avg = ((pos.aster_exit_price * old + res.fill_price * res.filled_qty)
                             / new_closed) if new_closed > 0 else res.fill_price
                    self.pm.record_hl_maker_exit_progress(symbol, new_closed, a_avg)
                    self.pm.log_trade(pos.id, "aster", aster_hedge_side, "ioc_close",
                                      res.order_id, res.filled_qty, res.fill_price,
                                      notes="maker-exit close")
                    log.warning(f"{symbol}: closed {res.filled_qty} on Aster @ {res.fill_price:.2f} "
                                f"({new_closed}/{pos.qty})")
                else:
                    complete_intent(hedge_intent, "rejected",
                                    notes=(res.error or "no_fill")[:120], position_id=pos.id)
                    log.error(f"{symbol}: Aster exit IOC failed ({res.error}) — will retry next tick")
                    return

        # Fully closed and hedged → finalise.
        if hl_closed >= pos.qty * 0.999 and pos.aster_hedged_qty >= hl_closed * 0.999:
            if pos.hl_exit_order_id and pos.hl_exit_order_id != "PAPER":
                await self.client.cancel_hl_order(symbol, pos.hl_exit_order_id)
            self.pm.confirm_hl_maker_exit(
                symbol, hl_closed, pos.hl_exit_price, pos.aster_exit_price,
                pos.exit_reason or "manual")
            return

        # Taker-taker escalation: if the basis has moved so favorably that crossing
        # both books as taker (worst fills + conservative 9bps taker-model fees) is
        # still net-positive, stop waiting and take the money now. This is the
        # "unlikely but possible" windfall the user described.
        await self._check_taker_escalation(pos, closing_long, hl_side, aster_hedge_side)

        # Reprice the resting exit maker if the touch drifted — only before any
        # fill, so the recorded exit price stays clean.
        if hl_closed <= 0:
            await self._reprice_hl_maker_exit(pos, hl_side)

    async def _check_taker_escalation(self, pos, closing_long, hl_side, aster_hedge_side):
        """If the taker-taker exit is net-positive after fees+b/o, escalate from
        the patient maker exit to an immediate taker-taker close."""
        symbol = pos.symbol
        try:
            aster_book, hl_book = await self.client.get_both_books(symbol)
        except Exception:
            return
        mid = (aster_book.mid + hl_book.mid) / 2
        if mid <= 0:
            return
        # Taker-taker gross: both legs cross the book (worst-case fills).
        if closing_long:
            taker_gross = ((hl_book.bid - pos.hl_entry_price)
                           + (pos.aster_entry_price - aster_book.ask)) * pos.qty
        else:
            taker_gross = ((pos.hl_entry_price - hl_book.ask)
                           + (aster_book.bid - pos.aster_entry_price)) * pos.qty
        # Use the convergence (taker) fee model (9bps) — more conservative than the
        # actual mixed trip (entry-maker + exit-taker ≈ 7.8bps). This ensures we
        # only escalate when it's clearly worth it.
        from config import ROUND_TRIP_FEE
        taker_fees = (pos.notional_usd or pos.qty * mid) * ROUND_TRIP_FEE
        elapsed_hours = max(0.0, (now_ms() - pos.entry_time) / 3_600_000)
        from position_manager import estimate_funding_pnl
        taker_funding = estimate_funding_pnl(
            pos.direction, elapsed_hours, pos.notional_usd or pos.qty * mid,
            pos.hl_funding_rate, pos.aster_funding_rate,
        )
        taker_est_net = taker_gross - taker_fees + taker_funding
        if taker_est_net >= 0:
            log.warning(
                f"{symbol}: taker-taker exit viable (est_net=${taker_est_net:.2f}, "
                f"gross=${taker_gross:.2f}, fees=${taker_fees:.2f}, "
                f"funding=${taker_funding:.2f}) — escalating from maker to taker"
            )
            await self._complete_maker_exit_taker(pos, closing_long, hl_side, aster_hedge_side)

    async def _complete_maker_exit_taker(self, pos, closing_long, hl_side, aster_hedge_side):
        """Escalate from the patient maker exit to a taker-taker close. Cancel the
        resting HL maker, cross the unclosed HL remainder as a taker IOC, close the
        matching Aster, and finalise."""
        symbol = pos.symbol
        if pos.hl_exit_order_id and pos.hl_exit_order_id != "PAPER":
            await self.client.cancel_hl_order(symbol, pos.hl_exit_order_id)
        await asyncio.sleep(0.5)
        try:
            hlp = await self.client.get_hl_position(symbol)
            current_szi = float(hlp.get("szi", 0) or 0)
            signed = (pos.hl_baseline_szi - current_szi) if closing_long else (current_szi - pos.hl_baseline_szi)
            hl_closed = max(0.0, signed)
        except Exception:
            hl_closed = pos.aster_hedged_qty

        hl_exit_px = pos.hl_exit_price
        remaining_hl = self.client.snap_aster_qty(symbol, pos.qty - hl_closed)
        if remaining_hl > 0:
            try:
                hl_book = await self.client._get_hl_book(symbol)
                ref = hl_book.bid if hl_side == "sell" else hl_book.ask
                res = await self.client.place_hl_ioc(symbol, hl_side, remaining_hl, ref)
                if res.success and res.filled_qty > 0:
                    tot = hl_closed + res.filled_qty
                    hl_exit_px = ((pos.hl_exit_price * hl_closed + res.fill_price * res.filled_qty)
                                  / tot) if tot > 0 else pos.hl_exit_price
                    hl_closed = tot
                    self.pm.log_trade(pos.id, "hl", hl_side, "ioc_close",
                                      res.order_id, res.filled_qty, res.fill_price,
                                      notes="maker-exit taker complete")
                else:
                    log.critical(f"{symbol}: maker-exit taker-complete HL FAILED ({res.error}) — CHECK MANUALLY")
            except Exception as e:
                log.critical(f"{symbol}: maker-exit taker-complete errored ({e}) — CHECK MANUALLY")

        # Close any Aster residual to match what HL has now closed.
        residual = self.client.snap_aster_qty(symbol, hl_closed - pos.aster_hedged_qty)
        if residual > 0:
            try:
                aster_book = await self.client._get_aster_book(symbol)
                touch = aster_book.ask if aster_hedge_side == "buy" else aster_book.bid
                res = await self.client.place_aster_ioc(symbol, aster_hedge_side, residual, touch)
                if res.success and res.filled_qty > 0:
                    new_closed = pos.aster_hedged_qty + res.filled_qty
                    old = pos.aster_hedged_qty
                    a_avg = ((pos.aster_exit_price * old + res.fill_price * res.filled_qty)
                             / new_closed) if new_closed > 0 else res.fill_price
                    self.pm.record_hl_maker_exit_progress(symbol, new_closed, a_avg)
                    self.pm.log_trade(pos.id, "aster", aster_hedge_side, "ioc_close",
                                      res.order_id, res.filled_qty, res.fill_price,
                                      notes="maker-exit residual close")
            except Exception as e:
                log.critical(f"{symbol}: maker-exit residual Aster close failed ({e}) — CHECK MANUALLY")

        final_qty = min(hl_closed, pos.aster_hedged_qty)
        naked = abs(hl_closed - pos.aster_hedged_qty)
        if naked > final_qty * 0.001 + 1e-9:
            log.critical(
                f"{symbol}: maker exit left {naked:.4f} unhedged "
                f"(HL closed {hl_closed}, Aster closed {pos.aster_hedged_qty}) — CHECK MANUALLY"
            )
        if final_qty <= 0:
            log.critical(f"{symbol}: maker exit timed out with nothing closed — marking error")
            self.pm.mark_error(symbol, "maker_exit_nothing_closed")
            return
        log.warning(f"{symbol}: maker exit taker-completed {final_qty}/{pos.qty}")
        self.pm.confirm_hl_maker_exit(
            symbol, final_qty, hl_exit_px, pos.aster_exit_price,
            (pos.exit_reason or "manual") + "_taker")

    async def _reprice_hl_maker_exit(self, pos, hl_side: str):
        """Cancel + repost the resting HL exit maker at the new touch if it has
        drifted more than a fraction of a tick (only called before any fill)."""
        symbol = pos.symbol
        try:
            _, hl_book = await self.client.get_both_books(symbol)
        except Exception:
            return
        touch = hl_book.ask if hl_side == "sell" else hl_book.bid
        if touch <= 0:
            return
        spec = self.client.hl_specs.get(symbol)
        tick = 10 ** (-spec.price_precision) if spec else 0.01
        if abs(touch - pos.hl_exit_price) < tick * MAKER_REPRICE_TICK_FRAC:
            return
        if pos.hl_exit_order_id:
            await self.client.cancel_hl_order(symbol, pos.hl_exit_order_id)
        alo = await self.client.place_hl_alo(symbol, hl_side, pos.qty, touch)
        if alo.success:
            pos.hl_exit_order_id = alo.order_id
            pos.hl_exit_price = touch
            from database import get_connection
            conn = get_connection()
            conn.execute("UPDATE positions SET hl_exit_order_id=?, hl_exit_price=? WHERE id=?",
                         (alo.order_id, touch, pos.id))
            conn.commit()
            conn.close()
            log.info(f"{symbol}: repriced HL exit maker -> {alo.order_id} @ {touch:.2f}")
        else:
            log.warning(f"{symbol}: HL exit maker reprice failed ({alo.error})")

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

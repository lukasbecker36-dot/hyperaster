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

from exchange_client import ExchangeClient, OrderBook, OrderResult, ContractSpec
from position_manager import PositionManager
from intents import record_intent, complete_intent
from config import (
    ENTRY_THRESHOLD_BPS, ENTRY_THRESHOLD_BPS_BY_SYMBOL, EXIT_THRESHOLD_BPS,
    NOTIONAL_PER_LEG, ENTRY_TIMEOUT_MINUTES, EXIT_TIMEOUT_MINUTES,
    MAX_PRICE_RATIO_DIVERGENCE, BLOCKED_SYMBOLS, MIN_EXECUTABLE_PREMIUM_BPS,
    ENTRY_CONFIRM_TICKS, ENTRY_COST_MARGIN_BPS, ROUND_TRIP_FEE, CARRY_ROUND_TRIP_FEE,
    EXIT_TARGET_NET_USD, EXIT_TARGET_NET_USD_BY_SYMBOL,
    MAKER_ENTRY_TIMEOUT_SEC, MAKER_REPRICE_TICK_FRAC,
    LIQUIDITY_GUARD_ENABLED, MIN_TOB_NOTIONAL_USD, MAX_VENUE_SPREAD_BPS,
    ENTRY_TAKER_ESCALATION_ENABLED,
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
        # Symbols whose maker entry should be aborted on the next poll tick.
        self._abort_entering: set[str] = set()
        # Active drip orders: symbol -> {direction, target_notional, filled_notional,
        # min_basis_bps, fills}. Each tick places one taker-taker bite if basis is met.
        self._drips: dict[str, dict] = {}
        self._drip_exits: dict[str, dict] = {}
        self._partial_closes: dict[str, float] = {}

    # ── Entry ──

    async def try_entry(
        self,
        symbol: str,
        aster_book: OrderBook,
        hl_book: OrderBook,
        notional: float = NOTIONAL_PER_LEG,
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

        # Thin-book liquidity guard: skip names too thin/unstable to trade.
        if not self._book_liquid_enough(symbol, aster_book, hl_book, mid):
            self._entry_streak.pop(symbol, None)
            return False

        # Entry signal = deviation of the book mid-spread from its own rolling
        # median. We trade the books, so we baseline against the books (not the
        # venue oracle feeds). spread_bps > baseline => Aster rich vs HL => short
        # Aster / long HL; spread_bps < baseline => the reverse.
        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
        self.client.record_book_spread(symbol, spread_bps)
        self.client.record_oracle_delta(symbol)
        book_base = self.client.get_book_spread_baseline(symbol)
        # No baseline yet (cold start, not enough samples) = no entry. Substituting
        # 0 would treat the entire structural gap as tradeable edge — the failure
        # mode that bled fees before.
        if book_base is None:
            log.debug(
                f"{symbol}: baseline not ready "
                f"({self.client.book_spread_sample_count(symbol)} samples) — skipping"
            )
            return False

        # Oracle staleness guard: outside market hours the oracle freezes and a
        # book "dislocation" has no arbitrageable anchor — skip it.
        if self.client.oracle_is_stale(symbol):
            log.debug(f"{symbol}: HL oracle stale (market likely closed) — skipping entry")
            self._entry_streak.pop(symbol, None)
            return False

        # Fair-value correction: shift the book baseline by how far the oracle
        # delta has moved from its own norm, so a genuine fair-value move isn't
        # misread as tradeable excess. entry_baseline_bps is stored RAW (the
        # frozen book median) so the exit can re-apply the LIVE correction.
        correction = self.client.get_oracle_correction(symbol)
        baseline_bps = book_base          # stored at entry (raw book median)
        fair_spread = book_base + correction

        # Executable-basis refinement: the signal trades the HL-maker/Aster-taker
        # basis, not the mid. exec_dev is the deviation of the half-spread
        # differential from its norm — added to both directions so the excess
        # equals the executable basis' deviation, not the mid's. 0 until warm.
        exec_dev = self.client.executable_deviation_bps(symbol, aster_book, hl_book, mid)

        # raw_premium_bps removed — the cost floor (fees + book spreads) is the
        # real protection. The old abs(spread) guard blocked legitimate baseline-
        # deviation trades on names with negative baselines (e.g. AMZN at 69bps
        # excess but only 10bps raw spread due to -59bps baseline).

        aster_excess_bps = (spread_bps - fair_spread) + exec_dev   # long_hl_short_aster
        hl_excess_bps = -(spread_bps - fair_spread) + exec_dev      # long_aster_short_hl

        base_threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(symbol, ENTRY_THRESHOLD_BPS)
        # Maker-first cost floor: we rest the HL leg as a maker (no HL crossing) and
        # only cross the Aster leg as a taker. So the floor is the carry round-trip
        # (HL-maker + Aster-taker ≈ 4.8bps) plus the Aster spread once. (The taker
        # spread floor is reconstructed in the poll for the taker-taker escalation.)
        maker_floor, _ = self._entry_cost_floors(mid, aster_book, hl_book)
        threshold = max(base_threshold, maker_floor)
        if maker_floor > base_threshold:
            log.debug(f"{symbol}: maker cost floor {maker_floor:.0f}bps > base {base_threshold:.0f}bps")

        # `excess_bps` is the tradeable deviation for the chosen direction;
        # `spread_bps` stays the raw book mid-spread (used for logging / records).
        if aster_excess_bps >= threshold:
            direction = "long_hl_short_aster"
            excess_bps = aster_excess_bps
        elif hl_excess_bps >= threshold:
            direction = "long_aster_short_hl"
            excess_bps = hl_excess_bps
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
        # Maker-first sizes the full notional (the HL leg rests passively, no
        # top-of-book cap; the Aster taker hedges incrementally as HL fills).
        # `notional` is the per-leg USD size (runtime-overridable for live testing).
        target_qty = notional / mid
        qty = self.client.snap_aster_qty(symbol, target_qty)
        if qty <= 0:
            log.debug(f"{symbol}: qty snapped to 0 (target={target_qty:.2f})")
            self._entry_streak.pop(symbol, None)
            return False
        actual_notional = qty * mid

        # Reject if max theoretical profit can't reach target. Full reversion
        # of excess_bps on this notional is the ceiling; require 1.5× target
        # so we're not entering trades that can only breakeven at best. The
        # target scales with the chosen notional (vs the default), so the check
        # stays a fixed bps-edge requirement at any size — otherwise a small
        # test notional would fail this gate on every name.
        base_target = EXIT_TARGET_NET_USD_BY_SYMBOL.get(symbol, EXIT_TARGET_NET_USD)
        sym_target = base_target * (actual_notional / NOTIONAL_PER_LEG)
        est_fees = actual_notional * CARRY_ROUND_TRIP_FEE
        max_gross = actual_notional * excess_bps / 10000
        if max_gross < sym_target * 1.5 + est_fees:
            log.debug(
                f"{symbol}: max gross ${max_gross:.2f} < 1.5×target+fees "
                f"${sym_target * 1.5 + est_fees:.2f} on ${actual_notional:.0f} notional — skipping"
            )
            return False

        # HL is the maker leg: long HL rests a buy at the bid, short HL rests a
        # sell at the ask. The Aster taker hedges each HL fill (in poll_hl_maker).
        if direction == "long_hl_short_aster":
            hl_maker_side, hl_ref = "buy", hl_book.bid
        else:
            hl_maker_side, hl_ref = "sell", hl_book.ask

        log.info(
            f"ENTRY {symbol}: {direction} maker-first | excess={excess_bps:.1f}bps "
            f"spread={spread_bps:+.1f}bps base={baseline_bps:+.1f}bps "
            f"orac_corr={correction:+.1f}bps exec_dev={exec_dev:+.1f}bps "
            f"fair={fair_spread:+.1f}bps | "
            f"qty={qty} | HL {hl_maker_side} maker @ {hl_ref:.2f}"
        )

        # Snapshot funding rates at entry (HL hourly, Aster 8h) for carry accounting.
        # Actively fetched: a cold cache would silently snapshot 0.0 forever.
        hl_fr, aster_fr = await self.client.get_funding_rates_fresh(symbol)

        if self.paper_mode:
            aster_fill = aster_book.bid if direction == "long_hl_short_aster" else aster_book.ask
            self.pm.open_hl_maker_entering(
                symbol=symbol, aster_symbol=aster_symbol_for(symbol),
                direction=direction, hl_maker_order_id="PAPER",
                hl_baseline_szi=0.0, qty=qty, notional_usd=actual_notional,
                entry_spread_bps=excess_bps, hl_ref_price=hl_ref,
                hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
                hold_for_funding=False, entry_baseline_bps=baseline_bps,
            )
            self.pm.confirm_hl_maker_open(symbol, qty, hl_ref, aster_fill)
            self._entry_streak.pop(symbol, None)
            log.info(f"[PAPER] ENTRY {symbol}: {direction} maker-first | qty={qty}")
            return True

        # ── Live: rest the HL post-only maker; poll_hl_maker advances it ──
        await self.client.ensure_perp_margin(symbol)
        try:
            pre_pos = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre_pos.get("szi", 0) or 0)
        except Exception as e:
            log.warning(f"{symbol}: HL pre-position snapshot failed ({e}) — aborting entry to stay safe")
            return False

        alo = await self.client.place_hl_alo(symbol, hl_maker_side, qty, hl_ref)
        if not alo.success:
            log.warning(f"{symbol}: HL maker not placed ({alo.error}) — entry aborted")
            return False

        self.pm.open_hl_maker_entering(
            symbol=symbol, aster_symbol=aster_symbol_for(symbol),
            direction=direction, hl_maker_order_id=alo.order_id,
            hl_baseline_szi=baseline_szi, qty=qty, notional_usd=actual_notional,
            entry_spread_bps=excess_bps, hl_ref_price=hl_ref,
            hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
            hold_for_funding=False, entry_baseline_bps=baseline_bps,
        )
        self._entry_streak.pop(symbol, None)
        log.info(f"{symbol}: convergence maker resting {alo.order_id} @ {hl_ref:.2f} — hedging on fill")
        return True

    def _book_liquid_enough(self, symbol, aster_book, hl_book, mid) -> bool:
        """Reject entry when either venue's book is too thin or too wide — the
        thin/unstable-book class (e.g. ZHIPU) that produces wild tick-to-tick
        prices and big divergence losses. Checks top-of-book notional on the
        thinner side and each venue's own spread. Only gates AUTO entries."""
        if not LIQUIDITY_GUARD_ENABLED or mid <= 0:
            return True
        # Top-of-book notional on the thinner side of each venue.
        if MIN_TOB_NOTIONAL_USD > 0:
            a_depth = min(aster_book.bid_size, aster_book.ask_size) * aster_book.mid
            h_depth = min(hl_book.bid_size, hl_book.ask_size) * hl_book.mid
            # Only enforce where we actually have size data (>0); a zero could be
            # a missing field rather than a genuinely empty level.
            if 0 < a_depth < MIN_TOB_NOTIONAL_USD or 0 < h_depth < MIN_TOB_NOTIONAL_USD:
                log.debug(
                    f"{symbol}: thin book — top-of-book Aster ${a_depth:.0f} / HL ${h_depth:.0f} "
                    f"< ${MIN_TOB_NOTIONAL_USD:.0f} floor, skipping"
                )
                return False
        # Each venue's own bid-ask spread.
        if MAX_VENUE_SPREAD_BPS > 0:
            a_spread = (aster_book.ask - aster_book.bid) / mid * 10000
            h_spread = (hl_book.ask - hl_book.bid) / mid * 10000
            if a_spread > MAX_VENUE_SPREAD_BPS or h_spread > MAX_VENUE_SPREAD_BPS:
                log.debug(
                    f"{symbol}: wide book — Aster {a_spread:.0f}bps / HL {h_spread:.0f}bps "
                    f"> {MAX_VENUE_SPREAD_BPS:.0f}bps cap, skipping"
                )
                return False
        return True

    def _entry_cost_floors(self, mid, aster_book, hl_book) -> tuple[float, float]:
        """Return (maker_floor_bps, taker_floor_bps) the entry edge must clear.

        maker: HL rests (no HL crossing), only the Aster spread is crossed +
               carry round-trip fees (HL-maker/Aster-taker ≈4.8bps).
        taker: both books crossed + taker round-trip fees (9bps) — used by the
               poll's taker-taker escalation.
        """
        if mid <= 0:
            return 0.0, 0.0
        aster_spread_bps = (aster_book.ask - aster_book.bid) / mid * 10000
        hl_spread_bps = (hl_book.ask - hl_book.bid) / mid * 10000
        maker_floor = CARRY_ROUND_TRIP_FEE * 10000 + aster_spread_bps + ENTRY_COST_MARGIN_BPS
        taker_floor = (ROUND_TRIP_FEE * 10000 + aster_spread_bps + hl_spread_bps
                       + ENTRY_COST_MARGIN_BPS)
        return maker_floor, taker_floor

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
        hl_fr, aster_fr = await self.client.get_funding_rates_fresh(symbol)

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

        if existing:
            # Scale-in: the position must stay "open", so a resting Aster GTX
            # would be untracked (poll_aster_maker only polls entering/exiting
            # positions) — the old flow booked the hedge before it filled and
            # nothing ever completed or unwound it. Cross Aster as an IOC taker
            # (~0.9bps) and book only what actually fills; unwind any unmatched
            # HL increment immediately.
            aster_filled = 0.0
            a_avg = 0.0
            for attempt in range(3):
                remaining = self.client.snap_aster_qty(symbol, actual_qty - aster_filled)
                if remaining <= 0:
                    break
                try:
                    book = aster_book if attempt == 0 else await self.client._get_aster_book(symbol)
                except Exception:
                    break
                touch = book.bid if aster_side == "sell" else book.ask
                if touch <= 0:
                    break
                res = await self.client.place_aster_ioc(symbol, aster_side, remaining, touch)
                if res.success and res.filled_qty > 0:
                    tot = aster_filled + res.filled_qty
                    a_avg = (a_avg * aster_filled + res.fill_price * res.filled_qty) / tot
                    aster_filled = tot

            matched = min(actual_qty, aster_filled)
            unmatched = actual_qty - matched
            if unmatched > 0:
                log.critical(
                    f"{symbol}: scale-in Aster hedge only filled {aster_filled}/{actual_qty} "
                    f"— unwinding {unmatched} HL"
                )
                close_side = "sell" if hl_side == "buy" else "buy"
                unwind = await self.client.place_hl_ioc(
                    symbol, close_side, unmatched, hl_result.fill_price)
                if not unwind.success:
                    log.critical(f"{symbol}: scale-in HL unwind FAILED — {unmatched} NAKED HL. "
                                 f"Manual intervention!")
            if matched <= 0:
                return False, f"{symbol}: scale-in Aster leg unfilled — HL increment unwound"
            actual_notional = matched * mid
            self.pm.scale_in(
                symbol, matched, actual_notional,
                hl_result.fill_price, a_avg,
                hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
            )
            self.pm.log_trade(
                existing.id, "hl", hl_side, "ioc_limit",
                hl_result.order_id, matched, hl_result.fill_price, notes="scale-in",
            )
            self.pm.log_trade(
                existing.id, "aster", aster_side, "ioc_limit",
                "", matched, a_avg, notes="scale-in hedge",
            )
            return True, (
                f"scaled in {symbol} +${actual_notional:.0f} qty={matched} → "
                f"${existing.notional_usd:.0f} total"
            )

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

    # ── Drip entry (taker-taker small bites until target notional) ──

    def start_drip(
        self, symbol: str, direction: str, target_notional: float,
        min_basis_bps: float, bite_qty: float = 0.0,
    ) -> tuple[bool, str]:
        if direction not in ("long_hl_short_aster", "long_aster_short_hl"):
            return False, f"bad direction {direction!r}"
        if target_notional <= 0:
            return False, f"target notional must be > 0"
        if symbol in self._drips:
            return False, f"{symbol}: drip already running — /cancel first"
        if bite_qty <= 0:
            return False, "bite_qty must be > 0 (shares per bite)"
        self._drips[symbol] = {
            "direction": direction,
            "target_notional": target_notional,
            "filled_notional": 0.0,
            "min_basis_bps": min_basis_bps,
            "bite_qty": bite_qty,
            "fills": 0,
            "last_attempt_ms": 0,
            "cooldown_ms": 5_000,
        }
        return True, (
            f"drip started: {symbol} {direction} target=${target_notional:.0f} "
            f"bite={bite_qty}shares min_basis={min_basis_bps:.0f}bps"
        )

    def cancel_drip(self, symbol: str) -> tuple[bool, str]:
        drip = self._drips.pop(symbol, None)
        if not drip:
            return False, f"{symbol}: no active drip"
        return True, (
            f"drip cancelled: {symbol} filled ${drip['filled_notional']:.0f}"
            f"/${drip['target_notional']:.0f} ({drip['fills']} fills)"
        )

    # ── Drip exit (taker-taker unwind when spread narrows) ──

    def start_drip_exit(
        self, symbol: str, max_basis_bps: float, bite_qty: float,
        target_notional: float = 0.0,
    ) -> tuple[bool, str]:
        pos = self.pm.get(symbol)
        if not pos or pos.status != "open":
            return False, f"{symbol}: no open position to exit"
        if bite_qty <= 0:
            return False, "bite_qty must be > 0 (shares per bite)"
        if symbol in self._drip_exits:
            return False, f"{symbol}: drip_exit already running — /cancel first"
        if target_notional > 0:
            # Convert notional to qty using current position's avg price
            avg_price = (pos.hl_entry_price + pos.aster_entry_price) / 2
            if avg_price > 0:
                exit_qty = min(target_notional / avg_price, pos.qty)
            else:
                exit_qty = pos.qty
        else:
            exit_qty = pos.qty
        self._drip_exits[symbol] = {
            "direction": pos.direction,
            "total_qty": exit_qty,
            "exited_qty": 0.0,
            "exited_notional": 0.0,
            "max_basis_bps": max_basis_bps,
            "bite_qty": bite_qty,
            "fills": 0,
            "last_attempt_ms": 0,
            "cooldown_ms": 5_000,
            "unhedged_qty": 0.0,
            "last_hl_exit_price": 0.0,
            "last_aster_exit_price": 0.0,
        }
        partial = f" (partial, full pos={pos.qty:.4f})" if exit_qty < pos.qty else ""
        return True, (
            f"drip_exit started: {symbol} {pos.direction} qty={exit_qty:.4f}{partial} "
            f"bite={bite_qty}shares max_basis={max_basis_bps:.0f}bps"
        )

    def cancel_drip_exit(self, symbol: str) -> tuple[bool, str]:
        de = self._drip_exits.pop(symbol, None)
        if not de:
            return False, f"{symbol}: no active drip_exit"
        return True, (
            f"drip_exit cancelled: {symbol} exited {de['exited_qty']:.4f}"
            f"/{de['total_qty']:.4f} ({de['fills']} fills)"
        )

    async def drip_exit_tick(self, symbol: str):
        """One tick of drip exit: close a small bite when spread narrows."""
        de = self._drip_exits.get(symbol)
        if not de:
            return

        now = now_ms()
        elapsed = now - de.get("last_attempt_ms", 0)
        if elapsed < de.get("cooldown_ms", 5_000):
            return

        remaining_qty = de["total_qty"] - de["exited_qty"]
        if remaining_qty <= 0.0001:
            msg = (f"drip_exit complete: {symbol} exited {de['exited_qty']:.4f} "
                   f"({de['fills']} fills)")
            self._drip_exits.pop(symbol, None)
            # Close the position in the DB
            pos = self.pm.get(symbol)
            if pos and pos.status == "open":
                self.pm.start_exiting(
                    symbol, "drip_exit", "drip_exit",
                    de["last_hl_exit_price"], de["max_basis_bps"])
                self.pm.confirm_aster_exit(
                    symbol, de["last_aster_exit_price"], "drip_exit_converge")
            log.warning(msg)
            return msg

        log.info(f"drip_exit {symbol}: tick (elapsed={elapsed/1000:.1f}s, "
                 f"{de['exited_qty']:.3f}/{de['total_qty']:.3f})")
        de["last_attempt_ms"] = now

        direction = de["direction"]

        if not await self.client.ensure_symbol_loaded(symbol):
            log.warning(f"drip_exit {symbol}: ensure_symbol_loaded failed")
            return

        try:
            aster_book, hl_book = await self.client.get_both_books(symbol)
        except Exception as e:
            log.warning(f"drip_exit {symbol}: book fetch failed ({e})")
            return
        if aster_book.bid <= 0 or hl_book.bid <= 0:
            log.info(f"drip_exit {symbol}: empty book")
            return

        mid = (aster_book.mid + hl_book.mid) / 2
        if mid <= 0:
            return

        raw_qty = min(de["bite_qty"], remaining_qty)

        # Exit = reverse of entry. For long_hl_short_aster:
        #   entry was: buy HL ask, sell Aster bid → basis = (ast_bid - hl_ask)
        #   exit is:   sell HL bid, buy Aster ask → basis = (ast_ask - hl_bid)
        # We want to exit when the spread has NARROWED, i.e. basis ≤ max_basis.
        if direction == "long_hl_short_aster":
            aster_vwap = aster_book.vwap_buy(raw_qty)    # buying back Aster short
            hl_vwap = hl_book.vwap_sell(raw_qty)          # selling HL long
            basis_bps = (aster_vwap - hl_vwap) / mid * 10000
            hl_side, aster_side = "sell", "buy"
            hl_ref = hl_book.bid
            aster_ref = aster_book.ask
        else:
            hl_vwap = hl_book.vwap_buy(raw_qty)           # buying back HL short
            aster_vwap = aster_book.vwap_sell(raw_qty)     # selling Aster long
            basis_bps = (hl_vwap - aster_vwap) / mid * 10000
            hl_side, aster_side = "buy", "sell"
            hl_ref = hl_book.ask
            aster_ref = aster_book.bid

        if basis_bps > de["max_basis_bps"]:
            log.info(f"drip_exit {symbol}: basis {basis_bps:.0f}bps > max {de['max_basis_bps']:.0f}bps "
                     f"({de['exited_qty']:.3f}/{de['total_qty']:.3f} exited)")
            return

        bite_qty = self.client.snap_aster_qty(symbol, raw_qty)
        if bite_qty <= 0:
            log.info(f"drip_exit {symbol}: bite_qty snapped to 0")
            return

        if self.paper_mode:
            de["exited_qty"] += bite_qty
            de["exited_notional"] += bite_qty * mid
            de["fills"] += 1
            de["last_hl_exit_price"] = hl_ref
            de["last_aster_exit_price"] = aster_ref
            log.warning(f"drip_exit [PAPER] {symbol}: -{bite_qty} @ basis={basis_bps:.0f}bps "
                        f"({de['exited_qty']:.3f}/{de['total_qty']:.3f})")
            if de["exited_qty"] >= de["total_qty"] - 0.0001:
                return self._finish_drip_exit(symbol, de)
            return

        # Live: Aster first (buy back short / sell long), then HL hedge
        await self.client.ensure_perp_margin(symbol)

        # If there's an unhedged buffer, try HL first — don't add more Aster risk.
        unhedged = de.get("unhedged_qty", 0.0)
        if unhedged > 0.0001:
            hedge_qty = round(unhedged, 8)
            hedge_notional = hedge_qty * mid
            if hedge_notional >= 12.0:
                log.info(f"drip_exit {symbol}: retrying HL hedge for buffer {hedge_qty}")
                try:
                    pre_pos = await self.client.get_hl_position(symbol)
                    baseline_szi = float(pre_pos.get("szi", 0) or 0)
                except Exception:
                    baseline_szi = 0.0
                hl_intent = record_intent(
                    symbol=symbol, venue="hl", action="drip_exit_ioc",
                    direction=direction, side=hl_side, qty=hedge_qty,
                    ref_price=hl_ref, baseline_szi=baseline_szi, paper=False,
                )
                hl_res = await self.client.place_hl_ioc(symbol, hl_side, hedge_qty, hl_ref)
                if hl_res.ambiguous:
                    expected_signed = hedge_qty if hl_side == "buy" else -hedge_qty
                    filled, actual_signed, _ = await self.client.reconcile_hl_position_delta(
                        symbol, baseline_szi, expected_signed)
                    if filled:
                        hl_res = OrderResult(success=True, order_id="RECONCILED",
                                             filled_qty=abs(actual_signed), fill_price=hl_ref)
                if hl_res.success and hl_res.filled_qty > 0:
                    complete_intent(hl_intent, "filled",
                                    notes=f"qty={hl_res.filled_qty} px={hl_res.fill_price}")
                    de["unhedged_qty"] = 0.0
                    de["exited_qty"] += hedge_qty
                    de["exited_notional"] += hedge_qty * mid
                    de["fills"] += 1
                    de["last_hl_exit_price"] = hl_res.fill_price
                    pos = self.pm.get(symbol)
                    if pos:
                        self.pm.log_trade(pos.id, "hl", hl_side, "drip_exit_ioc",
                                          hl_res.order_id, hedge_qty, hl_res.fill_price)
                    log.warning(f"drip_exit {symbol}: -{hedge_qty} buffer hedged on HL "
                                f"({de['exited_qty']:.3f}/{de['total_qty']:.3f})")
                    if de["exited_qty"] >= de["total_qty"] - 0.0001:
                        return self._finish_drip_exit(symbol, de)
                else:
                    complete_intent(hl_intent, "no_fill", notes=hl_res.error[:200])
                    log.warning(f"drip_exit {symbol}: HL still no fill for buffer {hedge_qty} — waiting")
                return

        try:
            pre_ap = await self.client.get_aster_position(symbol)
            aster_pre_amt = float(pre_ap.get("positionAmt", 0) or 0)
        except Exception:
            aster_pre_amt = None

        ast_intent = record_intent(
            symbol=symbol, venue="aster", action="drip_exit_ioc",
            direction=direction, side=aster_side, qty=bite_qty,
            ref_price=aster_ref, paper=False,
        )
        ast_res = await self.client.place_aster_ioc(
            symbol, aster_side, bite_qty, aster_ref, reduce_only=True)

        if not ast_res.success or ast_res.filled_qty <= 0:
            if aster_pre_amt is not None:
                await asyncio.sleep(2.0)
                try:
                    post_ap = await self.client.get_aster_position(symbol)
                    aster_post_amt = float(post_ap.get("positionAmt", 0) or 0)
                    delta = abs(aster_post_amt - aster_pre_amt)
                    if delta >= bite_qty * 0.50:
                        log.warning(
                            f"drip_exit {symbol}: Aster IOC reconciled "
                            f"{aster_pre_amt} -> {aster_post_amt}")
                        ast_res = OrderResult(
                            success=True, order_id=ast_res.order_id or "RECONCILED",
                            filled_qty=delta, fill_price=aster_ref,
                        )
                except Exception as e:
                    log.warning(f"drip_exit {symbol}: Aster reconcile failed ({e})")

        if not ast_res.success or ast_res.filled_qty <= 0:
            complete_intent(ast_intent, "no_fill",
                            notes=(ast_res.error or "no fill")[:200])
            log.debug(f"drip_exit {symbol}: Aster IOC no fill — skipping tick")
            return

        complete_intent(ast_intent, "filled",
                        notes=f"qty={ast_res.filled_qty} px={ast_res.fill_price}")
        actual_qty = round(ast_res.filled_qty, 8)

        de["unhedged_qty"] = de.get("unhedged_qty", 0.0) + actual_qty
        hedge_qty = round(de["unhedged_qty"], 8)
        hedge_notional = hedge_qty * mid

        if hedge_notional < 12.0:
            log.info(f"drip_exit {symbol}: Aster filled {actual_qty}, buffer={hedge_qty} "
                     f"(${hedge_notional:.1f}) — accumulating for HL hedge")
            return

        # HL hedge
        try:
            pre_pos = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre_pos.get("szi", 0) or 0)
        except Exception:
            baseline_szi = 0.0

        hl_intent = record_intent(
            symbol=symbol, venue="hl", action="drip_exit_ioc",
            direction=direction, side=hl_side, qty=hedge_qty,
            ref_price=hl_ref, baseline_szi=baseline_szi, paper=False,
        )
        hl_res = await self.client.place_hl_ioc(symbol, hl_side, hedge_qty, hl_ref)

        if hl_res.ambiguous:
            expected_signed = hedge_qty if hl_side == "buy" else -hedge_qty
            filled, actual_signed, _ = await self.client.reconcile_hl_position_delta(
                symbol, baseline_szi, expected_signed)
            if filled:
                hl_res = OrderResult(success=True, order_id="RECONCILED",
                                     filled_qty=abs(actual_signed), fill_price=hl_ref)

        if not hl_res.success or hl_res.filled_qty <= 0:
            complete_intent(hl_intent, "no_fill", notes=hl_res.error[:200])
            log.error(f"drip_exit {symbol}: HL IOC failed for buffer {hedge_qty} — retry next tick")
            return

        complete_intent(hl_intent, "filled",
                        notes=f"qty={hl_res.filled_qty} px={hl_res.fill_price}")

        de["unhedged_qty"] = 0.0
        de["exited_qty"] += hedge_qty
        de["exited_notional"] += hedge_qty * mid
        de["fills"] += 1
        de["last_hl_exit_price"] = hl_res.fill_price
        de["last_aster_exit_price"] = ast_res.fill_price

        pos = self.pm.get(symbol)
        if pos:
            self.pm.log_trade(pos.id, "hl", hl_side, "drip_exit_ioc",
                              hl_res.order_id, hedge_qty, hl_res.fill_price)
            self.pm.log_trade(pos.id, "aster", aster_side, "drip_exit_ioc",
                              ast_res.order_id, hedge_qty, ast_res.fill_price)

        log.warning(
            f"drip_exit {symbol}: -{hedge_qty} @ basis={basis_bps:.0f}bps "
            f"HL@{hl_res.fill_price:.2f} Ast@{ast_res.fill_price:.2f} "
            f"({de['exited_qty']:.3f}/{de['total_qty']:.3f})"
        )

        if de["exited_qty"] >= de["total_qty"] - 0.0001:
            return self._finish_drip_exit(symbol, de)
        return None

    def _finish_drip_exit(self, symbol: str, de: dict) -> str:
        msg = (f"drip_exit complete: {symbol} exited {de['exited_qty']:.4f} "
               f"({de['fills']} fills)")
        self._drip_exits.pop(symbol, None)
        pos = self.pm.get(symbol)
        if pos and pos.status == "open":
            remaining_qty = pos.qty - de["exited_qty"]
            if remaining_qty <= 0.0001:
                # Full exit — close the position
                self.pm.start_exiting(
                    symbol, "drip_exit", "drip_exit",
                    de["last_hl_exit_price"], de["max_basis_bps"])
                self.pm.confirm_aster_exit(
                    symbol, de["last_aster_exit_price"], "drip_exit_converge")
            else:
                # Partial exit — reduce position qty in DB
                self.pm.scale_out(symbol, de["exited_qty"], de["exited_notional"],
                                  de["last_hl_exit_price"], de["last_aster_exit_price"])
                msg += f" (remaining: {remaining_qty:.4f})"
        log.warning(msg)
        return msg

    async def drip_tick(self, symbol: str):
        """One tick of the drip loop: check basis, place one taker-taker bite."""
        drip = self._drips.get(symbol)
        if not drip:
            return

        now = now_ms()
        elapsed = now - drip.get("last_attempt_ms", 0)
        if elapsed < drip.get("cooldown_ms", 5_000):
            return
        log.info(f"drip {symbol}: tick (elapsed={elapsed/1000:.1f}s, "
                 f"${drip['filled_notional']:.0f}/${drip['target_notional']:.0f})")
        drip["last_attempt_ms"] = now

        remaining = drip["target_notional"] - drip["filled_notional"]
        if remaining <= 0:
            msg = (f"drip complete: {symbol} filled ${drip['filled_notional']:.0f} "
                   f"({drip['fills']} fills)")
            self._drips.pop(symbol, None)
            log.warning(msg)
            return msg

        direction = drip["direction"]

        if not await self.client.ensure_symbol_loaded(symbol):
            log.warning(f"drip {symbol}: ensure_symbol_loaded failed — skipping tick")
            return

        try:
            aster_book, hl_book = await self.client.get_both_books(symbol)
        except Exception as e:
            log.warning(f"drip {symbol}: book fetch failed ({e})")
            return
        if aster_book.bid <= 0 or hl_book.bid <= 0:
            log.info(f"drip {symbol}: empty book (HL bid={hl_book.bid} Ast bid={aster_book.bid})")
            return

        mid = (aster_book.mid + hl_book.mid) / 2
        if mid <= 0:
            log.info(f"drip {symbol}: mid={mid:.4f} — skipping")
            return

        # Size this bite first so we can compute VWAP-based basis.
        remaining_qty = remaining / mid if mid > 0 else 0
        raw_qty = min(drip["bite_qty"], remaining_qty)

        # Compute executable basis using VWAP across the full bite qty.
        # This ensures the basis gate accounts for sweeping multiple levels.
        if direction == "long_hl_short_aster":
            aster_vwap = aster_book.vwap_sell(raw_qty)   # selling into Aster bids
            hl_vwap = hl_book.vwap_buy(raw_qty)          # buying from HL asks
            basis_bps = (aster_vwap - hl_vwap) / mid * 10000
        else:
            hl_vwap = hl_book.vwap_sell(raw_qty)          # selling into HL bids
            aster_vwap = aster_book.vwap_buy(raw_qty)     # buying from HL asks
            basis_bps = (hl_vwap - aster_vwap) / mid * 10000

        if basis_bps < drip["min_basis_bps"]:
            tob_basis = ((aster_book.bid - hl_book.ask) / mid * 10000
                         if direction == "long_hl_short_aster"
                         else (hl_book.bid - aster_book.ask) / mid * 10000)
            log.info(f"drip {symbol}: VWAP basis {basis_bps:.0f}bps (TOB {tob_basis:.0f}bps) "
                     f"< min {drip['min_basis_bps']:.0f}bps for {raw_qty} shares "
                     f"(${drip['filled_notional']:.0f}/${drip['target_notional']:.0f} filled)")
            return
        bite_qty = self.client.snap_aster_qty(symbol, raw_qty)
        bite_notional_actual = bite_qty * mid
        if bite_qty <= 0:
            log.info(f"drip {symbol}: bite_qty snapped to 0 (mid={mid:.2f})")
            return

        if direction == "long_hl_short_aster":
            hl_side, aster_side = "buy", "sell"
            hl_ref = hl_book.ask
            aster_ref = aster_book.bid
        else:
            hl_side, aster_side = "sell", "buy"
            hl_ref = hl_book.bid
            aster_ref = aster_book.ask

        if self.paper_mode:
            fill_notional = bite_qty * mid
            existing = self.pm.get(symbol)
            hl_fr, ast_fr = await self.client.get_funding_rates_fresh(symbol)
            if existing and existing.status == "open":
                self.pm.scale_in(symbol, bite_qty, fill_notional, hl_ref, aster_ref,
                                 hl_funding_rate=hl_fr, aster_funding_rate=ast_fr)
            else:
                self.pm.import_position(
                    symbol=symbol, hl_coin=f"xyz:{symbol}",
                    aster_symbol=aster_symbol_for(symbol),
                    direction=direction, qty=bite_qty,
                    hl_price=hl_ref, aster_price=aster_ref,
                    hl_funding_rate=hl_fr, aster_funding_rate=ast_fr,
                )
            drip["filled_notional"] += fill_notional
            drip["fills"] += 1
            log.warning(f"drip [PAPER] {symbol}: +{bite_qty} @ basis={basis_bps:.0f}bps "
                        f"(${drip['filled_notional']:.0f}/${drip['target_notional']:.0f})")
            if drip["filled_notional"] >= drip["target_notional"]:
                return self._drips.pop(symbol, None) and (
                    f"drip complete: {symbol} ${drip['filled_notional']:.0f} ({drip['fills']} fills)")
            return

        # ── Live: taker-taker (Aster first, then HL) ──
        await self.client.ensure_perp_margin(symbol)

        # If there's an unhedged buffer from a previous tick, try to hedge it
        # on HL first. Don't place new Aster IOCs until the buffer is cleared.
        unhedged = drip.get("unhedged_qty", 0.0)
        if unhedged > 0.0001:
            hedge_qty = round(unhedged, 8)
            hedge_notional = hedge_qty * mid
            if hedge_notional < 12.0:
                log.info(f"drip {symbol}: unhedged buffer {hedge_qty} (${hedge_notional:.1f}) "
                         f"still below $12 — placing Aster to top up")
            else:
                log.info(f"drip {symbol}: retrying HL hedge for buffer {hedge_qty} (${hedge_notional:.1f})")
                try:
                    pre_pos = await self.client.get_hl_position(symbol)
                    baseline_szi = float(pre_pos.get("szi", 0) or 0)
                except Exception:
                    baseline_szi = 0.0
                hl_intent = record_intent(
                    symbol=symbol, venue="hl", action="drip_ioc",
                    direction=direction, side=hl_side, qty=hedge_qty,
                    ref_price=hl_ref, baseline_szi=baseline_szi, paper=False,
                )
                hl_res = await self.client.place_hl_ioc(symbol, hl_side, hedge_qty, hl_ref)
                if hl_res.ambiguous:
                    expected_signed = hedge_qty if hl_side == "buy" else -hedge_qty
                    filled, actual_signed, _ = await self.client.reconcile_hl_position_delta(
                        symbol, baseline_szi, expected_signed)
                    if filled:
                        hl_res = OrderResult(success=True, order_id="RECONCILED",
                                             filled_qty=abs(actual_signed), fill_price=hl_ref)
                if hl_res.success and hl_res.filled_qty > 0:
                    complete_intent(hl_intent, "filled",
                                    notes=f"qty={hl_res.filled_qty} px={hl_res.fill_price}")
                    drip["unhedged_qty"] = 0.0
                    fill_notional = hedge_qty * mid
                    hl_fr, ast_fr = await self.client.get_funding_rates_fresh(symbol)
                    existing = self.pm.get(symbol)
                    if existing and existing.status == "open":
                        self.pm.scale_in(symbol, hedge_qty, fill_notional,
                                         hl_res.fill_price, hl_ref,
                                         hl_funding_rate=hl_fr, aster_funding_rate=ast_fr)
                    else:
                        self.pm.import_position(
                            symbol=symbol, hl_coin=f"xyz:{symbol}",
                            aster_symbol=aster_symbol_for(symbol),
                            direction=direction, qty=hedge_qty,
                            hl_price=hl_res.fill_price, aster_price=hl_ref,
                            hl_funding_rate=hl_fr, aster_funding_rate=ast_fr,
                        )
                    drip["filled_notional"] += fill_notional
                    drip["fills"] += 1
                    log.warning(f"drip {symbol}: +{hedge_qty} buffer hedged on HL "
                                f"@ {hl_res.fill_price:.2f} "
                                f"(${drip['filled_notional']:.0f}/${drip['target_notional']:.0f})")
                    if drip["filled_notional"] >= drip["target_notional"]:
                        msg = (f"drip complete: {symbol} ${drip['filled_notional']:.0f} "
                               f"({drip['fills']} fills)")
                        self._drips.pop(symbol, None)
                        return msg
                else:
                    complete_intent(hl_intent, "no_fill", notes=hl_res.error[:200])
                    log.warning(f"drip {symbol}: HL still no fill for buffer {hedge_qty} — "
                                f"waiting (no new Aster orders)")
                return

        # Snapshot Aster position before the order for reconciliation.
        try:
            pre_ap = await self.client.get_aster_position(symbol)
            aster_pre_amt = float(pre_ap.get("positionAmt", 0) or 0)
        except Exception:
            aster_pre_amt = None

        ast_intent = record_intent(
            symbol=symbol, venue="aster", action="drip_ioc",
            direction=direction, side=aster_side, qty=bite_qty,
            ref_price=aster_ref, paper=False,
        )
        ast_res = await self.client.place_aster_ioc(symbol, aster_side, bite_qty, aster_ref)

        # Aster IOC can return executedQty=0 but fill async as taker.
        # Wait and reconcile from the actual position delta.
        if not ast_res.success or ast_res.filled_qty <= 0:
            if aster_pre_amt is not None:
                await asyncio.sleep(2.0)
                try:
                    post_ap = await self.client.get_aster_position(symbol)
                    aster_post_amt = float(post_ap.get("positionAmt", 0) or 0)
                    delta = abs(aster_post_amt - aster_pre_amt)
                    if delta >= bite_qty * 0.50:
                        log.warning(
                            f"drip {symbol}: Aster IOC reported no fill but position "
                            f"moved {aster_pre_amt} -> {aster_post_amt} — reconciled"
                        )
                        ast_res = OrderResult(
                            success=True, order_id=ast_res.order_id or "RECONCILED",
                            filled_qty=delta, fill_price=aster_ref,
                        )
                except Exception as e:
                    log.warning(f"drip {symbol}: Aster reconcile failed ({e})")

        if not ast_res.success or ast_res.filled_qty <= 0:
            complete_intent(ast_intent, "no_fill",
                            notes=(ast_res.error or "no fill")[:200])
            log.debug(f"drip {symbol}: Aster IOC no fill — skipping this tick")
            return

        complete_intent(ast_intent, "filled",
                        notes=f"qty={ast_res.filled_qty} px={ast_res.fill_price}")
        actual_qty = round(ast_res.filled_qty, 8)

        # Accumulate into unhedged buffer. Only hedge on HL once buffer ≥ $12
        # (HL XYZ minimum order). This avoids repeated $6 fills that HL rejects.
        drip["unhedged_qty"] = drip.get("unhedged_qty", 0.0) + actual_qty
        hedge_qty = round(drip["unhedged_qty"], 8)
        hedge_notional = hedge_qty * mid

        if hedge_notional < 12.0:
            log.info(f"drip {symbol}: Aster filled {actual_qty}, buffer={hedge_qty} "
                     f"(${hedge_notional:.1f}) — accumulating until $12+ for HL hedge")
            return

        # HL hedge leg — hedge the full accumulated buffer.
        try:
            pre_pos = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre_pos.get("szi", 0) or 0)
        except Exception as e:
            log.warning(f"drip {symbol}: HL pre-position failed ({e})")
            # Aster already filled — must try HL anyway
            baseline_szi = 0.0

        hl_intent = record_intent(
            symbol=symbol, venue="hl", action="drip_ioc",
            direction=direction, side=hl_side, qty=hedge_qty,
            ref_price=hl_ref, baseline_szi=baseline_szi, paper=False,
        )
        hl_res = await self.client.place_hl_ioc(symbol, hl_side, hedge_qty, hl_ref)

        if hl_res.ambiguous:
            expected_signed = hedge_qty if hl_side == "buy" else -hedge_qty
            filled, actual_signed, _ = await self.client.reconcile_hl_position_delta(
                symbol, baseline_szi, expected_signed)
            if filled:
                hl_res = OrderResult(success=True, order_id="RECONCILED",
                                     filled_qty=abs(actual_signed), fill_price=hl_ref)

        if not hl_res.success or hl_res.filled_qty <= 0:
            complete_intent(hl_intent, "no_fill", notes=hl_res.error[:200])
            log.error(f"drip {symbol}: HL IOC failed for buffer {hedge_qty} "
                      f"(${hedge_notional:.1f}) — will retry next tick")
            return

        complete_intent(hl_intent, "filled",
                        notes=f"qty={hl_res.filled_qty} px={hl_res.fill_price}")

        # Buffer hedged successfully — clear it.
        drip["unhedged_qty"] = 0.0
        fill_notional = hedge_qty * mid
        hl_fr, ast_fr = await self.client.get_funding_rates_fresh(symbol)
        existing = self.pm.get(symbol)
        if existing and existing.status == "open":
            self.pm.scale_in(symbol, hedge_qty, fill_notional,
                             hl_res.fill_price, ast_res.fill_price,
                             hl_funding_rate=hl_fr, aster_funding_rate=ast_fr)
            self.pm.log_trade(existing.id, "hl", hl_side, "drip_ioc",
                              hl_res.order_id, hedge_qty, hl_res.fill_price)
            self.pm.log_trade(existing.id, "aster", aster_side, "drip_ioc",
                              ast_res.order_id, hedge_qty, ast_res.fill_price)
        else:
            pos = self.pm.import_position(
                symbol=symbol, hl_coin=f"xyz:{symbol}",
                aster_symbol=aster_symbol_for(symbol),
                direction=direction, qty=hedge_qty,
                hl_price=hl_res.fill_price, aster_price=ast_res.fill_price,
                hl_funding_rate=hl_fr, aster_funding_rate=ast_fr,
            )
            self.pm.log_trade(pos.id, "hl", hl_side, "drip_ioc",
                              hl_res.order_id, hedge_qty, hl_res.fill_price)
            self.pm.log_trade(pos.id, "aster", aster_side, "drip_ioc",
                              ast_res.order_id, hedge_qty, ast_res.fill_price)

        drip["filled_notional"] += fill_notional
        drip["fills"] += 1
        log.warning(
            f"drip {symbol}: +{hedge_qty} hedged @ basis={basis_bps:.0f}bps "
            f"HL@{hl_res.fill_price:.2f} Ast@{ast_res.fill_price:.2f} "
            f"(${drip['filled_notional']:.0f}/${drip['target_notional']:.0f})"
        )
        if drip["filled_notional"] >= drip["target_notional"]:
            msg = (f"drip complete: {symbol} ${drip['filled_notional']:.0f} "
                   f"({drip['fills']} fills)")
            self._drips.pop(symbol, None)
            return msg
        return None

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
        if notional <= 0:
            return False, f"{symbol}: notional must be > 0"

        # Scale-in: if an OPEN position already exists in the same direction, add
        # to it. A mid-flight (entering/exiting) position can't be scaled, and an
        # opposite-direction request is a reduction (use /close), not a scale.
        existing = self.pm.get(symbol)
        is_scale = False
        if existing is not None:
            if existing.status != "open":
                return False, (f"{symbol}: position is '{existing.status}', not open — "
                               f"wait for it to settle before scaling")
            if existing.direction != direction:
                return False, (f"{symbol}: existing position is {existing.direction}, "
                               f"opposite to {direction} — use /close to reduce, not /enter")
            if existing.entry_maker_venue != "hl":
                return False, (f"{symbol}: existing position isn't a maker-first carry "
                               f"trade — can't scale it this way")
            is_scale = True

        if not await self.client.ensure_symbol_loaded(symbol):
            return False, f"{symbol}: not found on one or both venues"

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
        # sell resting at the ask. Offset by one tick AWAY from the spread so the
        # post-only (Alo) never crosses even if the book moved between our fetch
        # and the order reaching HL. Repricing corrects on the next tick if the
        # touch drifted.
        hl_tick = self.client.hl_specs.get(symbol, ContractSpec()).tick_size
        if direction == "long_hl_short_aster":
            hl_side, hl_ref_price = "buy", hl_book.bid - hl_tick
        else:
            hl_side, hl_ref_price = "sell", hl_book.ask + hl_tick

        qty = self.client.snap_aster_qty(symbol, notional / mid)
        if qty <= 0:
            return False, f"{symbol}: qty snapped to 0 (lot too large for ${notional:.0f})"
        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
        hl_fr, aster_fr = await self.client.get_funding_rates_fresh(symbol)

        log.warning(
            f"MAKER ENTRY {symbol}: {direction} | notional≈${qty*mid:.0f} qty={qty} | "
            f"HL {hl_side} maker @ {hl_ref_price:.2f}"
        )

        if self.paper_mode:
            # Paper: assume the maker fills at its resting price and the Aster
            # taker hedges instantly at the touch.
            aster_fill = aster_book.bid if direction == "long_hl_short_aster" else aster_book.ask
            if is_scale:
                self.pm.start_scale_in(
                    symbol=symbol, hl_maker_order_id="PAPER", increment_qty=qty,
                    hl_ref_price=hl_ref_price, hl_baseline_szi=0.0,
                    aster_baseline_amt=0.0, hl_funding_rate=hl_fr,
                    aster_funding_rate=aster_fr,
                )
                self.pm.confirm_hl_maker_open(symbol, qty, hl_ref_price, aster_fill)
                return True, f"[PAPER] scaled {symbol} {direction} +${qty*mid:.0f} (maker-first)"
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

        # Snapshot the Aster position BEFORE we place anything, so poll_hl_maker
        # can measure how much has actually hedged from the live position delta
        # (not just the order-ack accumulator, which can miss async taker fills).
        try:
            pre_ap = await self.client.get_aster_position(symbol)
            aster_baseline_amt = float(pre_ap.get("positionAmt", 0) or 0)
        except Exception as e:
            return False, f"{symbol}: Aster pre-position snapshot failed ({e})"

        alo = await self.client.place_hl_alo(symbol, hl_side, qty, hl_ref_price)
        if not alo.success:
            return False, f"{symbol}: HL maker not placed ({alo.error})"

        if is_scale:
            ok = self.pm.start_scale_in(
                symbol=symbol, hl_maker_order_id=alo.order_id, increment_qty=qty,
                hl_ref_price=hl_ref_price, hl_baseline_szi=baseline_szi,
                aster_baseline_amt=aster_baseline_amt,
                hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
            )
            if not ok:
                await self.client.cancel_hl_order(symbol, alo.order_id)
                return False, f"{symbol}: scale-in could not start (position state changed)"
            return True, (
                f"scaling {symbol} {direction} +${qty*mid:.0f} @ {hl_ref_price:.2f} "
                f"(existing {existing.scale_pre_qty or existing.qty} qty) — hedging on fill"
            )

        self.pm.open_hl_maker_entering(
            symbol=symbol, aster_symbol=aster_symbol_for(symbol),
            direction=direction, hl_maker_order_id=alo.order_id,
            hl_baseline_szi=baseline_szi, qty=qty, notional_usd=qty * mid,
            entry_spread_bps=spread_bps, hl_ref_price=hl_ref_price,
            hl_funding_rate=hl_fr, aster_funding_rate=aster_fr,
            aster_baseline_amt=aster_baseline_amt,
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

        # Abort check: /cancel sets this flag to stop a running maker entry.
        if symbol in self._abort_entering:
            self._abort_entering.discard(symbol)
            long_hl = pos.direction == "long_hl_short_aster"
            aster_hedge_side = "sell" if long_hl else "buy"
            log.warning(f"{symbol}: maker entry ABORTED by /cancel")
            await self._finalize_partial_entry(pos, long_hl, aster_hedge_side, "manual_cancel")
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

        # Safety cap: if HL exposure exceeds target by >10%, something went wrong
        # (e.g. stale position API caused repricing to place duplicate Alo orders).
        # Emergency-finalize to stop the bleed.
        if hl_filled > pos.qty * 1.10:
            log.error(
                f"{symbol}: HL exposure {hl_filled:.4f} exceeds target {pos.qty:.4f} by "
                f"{(hl_filled/pos.qty - 1)*100:.0f}% — EMERGENCY FINALIZE"
            )
            await self._finalize_partial_entry(pos, long_hl, aster_hedge_side, "overexposure_cap")
            return

        # Reconcile hedged qty against the LIVE Aster position before deciding
        # what's left to hedge. The order-ack accumulator (pos.aster_hedged_qty)
        # can UNDER-count when Aster matches asynchronously — the POST response
        # comes back with executedQty=0 but the order then fills as a taker.
        # If we trusted only the accumulator, new_fill would stay positive and
        # we'd re-hedge the same fill every tick → runaway. The live position is
        # the source of truth and makes hedging idempotent.
        try:
            ap = await self.client.get_aster_position(symbol)
            aster_amt = float(ap.get("positionAmt", 0) or 0)
            # Hedge SELLS Aster when long_hl (amt falls below baseline), BUYS
            # when short_hl (amt rises above baseline). Either way → positive.
            hedged_live = ((pos.aster_baseline_amt - aster_amt) if long_hl
                           else (aster_amt - pos.aster_baseline_amt))
            hedged_live = max(0.0, hedged_live)
            if hedged_live > pos.aster_hedged_qty + 1e-12:
                log.warning(
                    f"{symbol}: Aster hedge reconciled "
                    f"{pos.aster_hedged_qty:.4f} -> {hedged_live:.4f} from live position"
                )
                # Take the price from the venue's position entry when the
                # order-ack accumulator never recorded one — otherwise
                # aster_entry_price stays 0, corrupting est_net and firing a
                # bogus immediate exit (part of the ARM incident).
                venue_px = float(ap.get("entryPrice", 0) or 0)
                aster_px = venue_px if venue_px > 0 else (pos.aster_entry_price or 0.0)
                self.pm.record_hl_maker_progress(
                    symbol, hl_filled, hedged_live,
                    pos.hl_entry_price, aster_px)
        except Exception as e:
            log.warning(f"{symbol}: Aster position reconcile failed ({e}) — using accumulator")

        # Hedge any newly-filled HL qty with an Aster IOC taker.
        new_fill = hl_filled - pos.aster_hedged_qty
        min_lot = self.client.snap_aster_qty(symbol, new_fill) if new_fill > 0 else 0.0
        if min_lot > 0:
            # Circuit breaker: cap total Aster hedge attempts to prevent runaway
            # order placement if IOC orders aren't working as expected.
            MAX_HEDGE_ATTEMPTS = 10
            if pos.aster_hedge_attempts >= MAX_HEDGE_ATTEMPTS:
                log.error(
                    f"{symbol}: Aster hedge circuit breaker tripped "
                    f"({pos.aster_hedge_attempts} attempts) — EMERGENCY FINALIZE"
                )
                await self._finalize_partial_entry(pos, long_hl, aster_hedge_side, "hedge_circuit_breaker")
                return

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
                pos.aster_hedge_attempts += 1
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

        if pos.hold_for_funding:
            # Carry hold: the operator deliberately chose this entry via /enter
            # with a basis gate. The maker rests indefinitely until filled — no
            # timeout. Use /cancel to abort manually if needed.
            pass
        else:
            # Convergence arb: re-measure the signal each tick. If the taker-taker
            # excess clears its (wider) gate, escalate — cross both as taker to lock
            # the fill before the maker can miss it. If the edge has faded below the
            # maker gate, cancel the unfilled remainder and keep any hedged partial.
            if await self._manage_convergence_entry(pos, long_hl, hl_side, aster_hedge_side, hl_filled):
                return

        # Reprice the resting maker if the touch has drifted away — but only
        # before any fill, so the recorded entry price stays clean and we don't
        # chase a moving market on a partially-filled order.
        if hl_filled <= 0:
            await self._reprice_hl_maker(pos, hl_side, hl_filled)

    async def _finalize_partial_entry(self, pos, long_hl, aster_hedge_side, reason):
        """Cancel the resting HL maker, re-check szi for a last fill, hedge any
        residual, and open at the delta-neutral (hedged) qty — or drop the record
        if nothing filled (no exposure was ever taken)."""
        symbol = pos.symbol
        if pos.hl_entry_order_id and pos.hl_entry_order_id != "PAPER":
            await self.client.cancel_hl_order(symbol, pos.hl_entry_order_id)
        await asyncio.sleep(0.5)
        try:
            hlp = await self.client.get_hl_position(symbol)
            signed_delta = float(hlp.get("szi", 0) or 0) - pos.hl_baseline_szi
            hl_filled = max(0.0, signed_delta if long_hl else -signed_delta)
        except Exception:
            hl_filled = pos.aster_hedged_qty
        if hl_filled <= 0:
            if pos.scale_pre_qty > 0:
                log.warning(f"{symbol}: scale-in increment unfilled ({reason}) — reverting")
                self.pm.revert_scale_in(symbol)
            else:
                log.warning(f"{symbol}: maker entry ended unfilled ({reason}) — cancelling record")
                self.pm.drop_entering(symbol, reason)
            return
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
                log.critical(f"{symbol}: partial-entry residual hedge failed ({e}) — CHECK MANUALLY")
        final_qty = min(hl_filled, pos.aster_hedged_qty)
        naked_hl = hl_filled - final_qty
        if naked_hl > residual * 0.001 + 1e-9:
            log.critical(
                f"{symbol}: maker entry left {naked_hl:.4f} HL UNHEDGED "
                f"(filled {hl_filled}, hedged {pos.aster_hedged_qty}) — CHECK MANUALLY"
            )
        if final_qty <= 0:
            if pos.scale_pre_qty > 0:
                self.pm.revert_scale_in(symbol)
            else:
                self.pm.drop_entering(symbol, "maker_entry_unhedged")
            return
        log.warning(f"{symbol}: maker entry partial {final_qty}/{pos.qty} ({reason}) — opening")
        self.pm.confirm_hl_maker_open(symbol, final_qty, pos.hl_entry_price, pos.aster_entry_price)

    async def _manage_convergence_entry(self, pos, long_hl, hl_side, aster_hedge_side, hl_filled) -> bool:
        """Per-tick signal management for a resting convergence entry maker.
        Returns True if the entry was resolved (escalated/opened/dropped)."""
        symbol = pos.symbol
        try:
            aster_book, hl_book = await self.client.get_both_books(symbol)
        except Exception:
            return False
        mid = (aster_book.mid + hl_book.mid) / 2
        baseline = self.client.get_book_spread_baseline(symbol)
        if mid <= 0 or baseline is None:
            return False
        # Same oracle-corrected fair spread + executable refinement the entry used.
        self.client.record_oracle_delta(symbol)
        fair_spread = baseline + self.client.get_oracle_correction(symbol)
        exec_dev = self.client.executable_deviation_bps(symbol, aster_book, hl_book, mid)
        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000
        raw_excess = spread_bps - fair_spread
        excess = (raw_excess if long_hl else -raw_excess) + exec_dev

        base_threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(symbol, ENTRY_THRESHOLD_BPS)
        maker_floor, taker_floor = self._entry_cost_floors(mid, aster_book, hl_book)
        threshold_maker = max(base_threshold, maker_floor)
        threshold_taker = max(base_threshold, taker_floor)

        # Edge so strong that even paying both taker spreads clears the gate →
        # stop waiting on the maker, cross both as taker now to guarantee the fill.
        # Disabled by default (see config): the cross could double-fill HL.
        if ENTRY_TAKER_ESCALATION_ENABLED and excess >= threshold_taker:
            log.warning(
                f"{symbol}: convergence excess {excess:.1f}bps ≥ taker gate "
                f"{threshold_taker:.0f}bps — escalating maker→taker entry"
            )
            await self._escalate_entry_taker(pos, long_hl, hl_side, aster_hedge_side, hl_filled)
            return True

        # Edge faded below the maker gate → don't enter. Cancel the unfilled
        # remainder; keep any already-filled-and-hedged portion as the position.
        if excess < threshold_maker:
            log.info(
                f"{symbol}: convergence excess {excess:.1f}bps fell below maker gate "
                f"{threshold_maker:.0f}bps — cancelling unfilled entry"
            )
            await self._finalize_partial_entry(pos, long_hl, aster_hedge_side, "edge_faded")
            return True

        return False  # edge still in the maker band — keep resting

    async def _escalate_entry_taker(self, pos, long_hl, hl_side, aster_hedge_side, hl_filled):
        """Cancel the resting HL maker, cross the unfilled HL remainder as a taker
        IOC, hedge the matching Aster, and open at the hedged qty."""
        symbol = pos.symbol
        if pos.hl_entry_order_id and pos.hl_entry_order_id != "PAPER":
            await self.client.cancel_hl_order(symbol, pos.hl_entry_order_id)
            # The resting maker may have filled between the poll's szi snapshot
            # and this cancel (or the cancel raced a fill). Re-read the TRUE HL
            # position so we cross only the genuine remainder — crossing
            # pos.qty − stale_filled double-fills HL (observed ARM: 0.06 → 0.12).
            await asyncio.sleep(0.5)
            try:
                hlp = await self.client.get_hl_position(symbol)
                signed_delta = float(hlp.get("szi", 0) or 0) - pos.hl_baseline_szi
                true_filled = max(0.0, signed_delta if long_hl else -signed_delta)
                if true_filled > hl_filled:
                    log.warning(f"{symbol}: escalation re-read HL fill {hl_filled:.4f} → "
                                f"{true_filled:.4f} (maker filled during cancel)")
                    hl_filled = true_filled
            except Exception as e:
                log.warning(f"{symbol}: escalation szi re-read failed ({e}) — using poll value")
        remaining = self.client.snap_aster_qty(symbol, pos.qty - hl_filled)
        if remaining > 0:
            try:
                _, hl_book = await self.client.get_both_books(symbol)
                cross = hl_book.ask if hl_side == "buy" else hl_book.bid
                res = await self.client.place_hl_ioc(symbol, hl_side, remaining, cross)
                if res.success and res.filled_qty > 0:
                    tot = hl_filled + res.filled_qty
                    h_avg = ((pos.hl_entry_price * hl_filled + res.fill_price * res.filled_qty)
                             / tot) if tot > 0 else pos.hl_entry_price
                    hl_filled = tot
                    pos.hl_entry_price = h_avg
                    self.pm.log_trade(pos.id, "hl", hl_side, "ioc_taker",
                                      res.order_id, res.filled_qty, res.fill_price,
                                      notes="entry escalate taker")
                else:
                    log.critical(f"{symbol}: entry escalation HL taker FAILED ({res.error}) — CHECK MANUALLY")
            except Exception as e:
                log.critical(f"{symbol}: entry escalation errored ({e}) — CHECK MANUALLY")
        # Hedge any residual then open at the hedged (delta-neutral) qty.
        residual = self.client.snap_aster_qty(symbol, hl_filled - pos.aster_hedged_qty)
        if residual > 0:
            try:
                aster_book = await self.client._get_aster_book(symbol)
                touch = aster_book.bid if aster_hedge_side == "sell" else aster_book.ask
                res = await self.client.place_aster_ioc(symbol, aster_hedge_side, residual, touch)
                if res.success and res.filled_qty > 0:
                    new_hedged = pos.aster_hedged_qty + res.filled_qty
                    old = pos.aster_hedged_qty
                    a_avg = ((pos.aster_entry_price * old + res.fill_price * res.filled_qty)
                             / new_hedged) if new_hedged > 0 else res.fill_price
                    self.pm.record_hl_maker_progress(symbol, hl_filled, new_hedged, pos.hl_entry_price, a_avg)
            except Exception as e:
                log.critical(f"{symbol}: entry escalation residual hedge failed ({e}) — CHECK MANUALLY")
        final_qty = min(hl_filled, pos.aster_hedged_qty)
        naked_hl = hl_filled - final_qty
        if naked_hl > 1e-9:
            # A no-fill IOC (not just an exception) also lands here — without
            # this check the naked remainder was silently absorbed.
            log.critical(
                f"{symbol}: entry escalation left {naked_hl:.4f} HL UNHEDGED "
                f"(filled {hl_filled}, hedged {pos.aster_hedged_qty}) — CHECK MANUALLY"
            )
        if final_qty <= 0:
            self.pm.drop_entering(symbol, "escalate_unfilled")
            return
        self.pm.confirm_hl_maker_open(symbol, final_qty, pos.hl_entry_price, pos.aster_entry_price)

    async def _reprice_hl_maker(self, pos, hl_side: str, hl_filled: float):
        """Cancel + repost the resting HL maker at the new touch for the unfilled
        remainder, if it has drifted more than a fraction of a tick.

        Before repricing, verifies the current order is still resting — if it
        already filled (position API was stale), we skip the reprice so we don't
        accumulate duplicate Alo orders that overshoot the target qty.
        """
        symbol = pos.symbol
        try:
            _, hl_book = await self.client.get_both_books(symbol)
        except Exception:
            return
        spec = self.client.hl_specs.get(symbol, ContractSpec())
        tick = spec.tick_size
        # Offset one tick away from spread so the Alo never crosses.
        touch = (hl_book.bid - tick) if hl_side == "buy" else (hl_book.ask + tick)
        if touch <= 0:
            return
        if abs(touch - pos.hl_entry_price) < tick * MAKER_REPRICE_TICK_FRAC:
            return
        remainder = self.client.snap_aster_qty(symbol, pos.qty - hl_filled)
        if remainder <= 0:
            return

        # Verify the current order is still resting before cancel+replace.
        # If it already filled (stale position API made hl_filled look like 0),
        # placing a new Alo would overshoot the target qty.
        if pos.hl_entry_order_id:
            try:
                resp = await self.client.query_hl_order(pos.hl_entry_order_id)
                # HL returns {"status": "order", "order": {"status": "open"|"filled"|...}}
                # or {"status": "unknownOid"}
                if resp.get("status") == "unknownOid":
                    log.warning(f"{symbol}: HL order {pos.hl_entry_order_id} unknown — skipping reprice")
                    return
                order_info = resp.get("order", {})
                order_status = order_info.get("status", "")
                if order_status != "open":
                    log.warning(
                        f"{symbol}: HL order {pos.hl_entry_order_id} already "
                        f"{order_status} — skipping reprice (position API may be stale)"
                    )
                    return
            except Exception as e:
                log.warning(f"{symbol}: HL order status check failed ({e}) — skipping reprice for safety")
                return
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

    # ── Maker-first exit (HL post-only maker, Aster IOC taker hedge) ──

    # Maker-first positions (carry + convergence) close maker-first for the good
    # basis, EXCEPT urgent exits — loss-cuts and forced closes must cross now, not
    # rest a patient maker. (funding_stop = carry bail; blocked/timeout/funding_drag
    # = convergence forced closes.)
    URGENT_EXIT_REASONS = {"funding_stop", "blocked", "timeout", "funding_drag"}

    async def partial_close(self, symbol: str, close_qty: float, reason: str = "manual_partial") -> bool:
        """Close a portion of a position taker-taker, Aster-first.

        Aster is the thin leg, so we close it FIRST: if the Aster IOC fills, the
        liquid HL leg can almost always hedge. Closing HL first risks the thin
        Aster leg failing and leaving naked exposure. Mirrors the drip-entry
        leg ordering. Only the matched (min of the two fills) qty is scaled out."""
        pos = self.pm.get(symbol)
        if not pos or pos.status != "open":
            return False
        close_qty = min(close_qty, pos.qty)
        if close_qty <= 0:
            return False

        try:
            aster_book, hl_book = await self.client.get_both_books(symbol)
        except Exception as e:
            log.error(f"{symbol}: partial close book fetch failed ({e})")
            return False

        # Closing reverses the entry sides.
        if pos.direction == "long_hl_short_aster":
            aster_side, hl_side = "buy", "sell"   # close short Aster, close long HL
            aster_ref, hl_ref = aster_book.ask, hl_book.bid
        else:
            aster_side, hl_side = "sell", "buy"
            aster_ref, hl_ref = aster_book.bid, hl_book.ask

        mid = (aster_book.mid + hl_book.mid) / 2
        spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000 if mid > 0 else 0

        log.info(
            f"PARTIAL CLOSE {symbol} ({reason}): qty={close_qty:.4f}/{pos.qty:.4f} "
            f"spread={spread_bps:.1f}bps | Aster {aster_side} @ {aster_ref:.4f} | HL {hl_side} @ {hl_ref:.2f}"
        )

        if self.paper_mode:
            remove_notional = close_qty * mid
            self.pm.scale_out(symbol, close_qty, remove_notional, hl_ref, aster_ref)
            log.info(f"[PAPER] PARTIAL CLOSE {symbol} qty={close_qty:.4f} ${remove_notional:.0f}")
            return True

        # Leg 1: close the thin Aster leg first (IOC taker).
        aster_qty = self.client.snap_aster_qty(symbol, close_qty)
        if aster_qty <= 0:
            log.error(f"{symbol}: partial close Aster snap=0")
            return False
        a_intent = record_intent(
            symbol=symbol, venue="aster", action="partial_close_ioc",
            direction=pos.direction, side=aster_side, qty=aster_qty,
            ref_price=aster_ref, position_id=pos.id, paper=self.paper_mode,
        )
        aster_res = await self.client.place_aster_ioc(
            symbol, aster_side, aster_qty, aster_ref, reduce_only=True)
        if not aster_res.success or aster_res.filled_qty <= 0:
            log.warning(f"{symbol}: partial close Aster IOC no fill ({aster_res.error}) — nothing closed")
            complete_intent(a_intent, "rejected", notes=(aster_res.error or "no_fill")[:200],
                            position_id=pos.id)
            return False
        complete_intent(a_intent, "filled", notes=f"qty={aster_res.filled_qty}", position_id=pos.id)
        aster_filled = aster_res.filled_qty
        log.info(f"{symbol}: partial close Aster filled {aster_filled} @ {aster_res.fill_price:.4f}")

        # Leg 2: hedge by closing the matched HL qty (IOC taker). Retry the liquid
        # leg a few times so an Aster fill is never left naked.
        hl_filled = 0.0
        hl_avg = 0.0
        for attempt in range(3):
            remaining = aster_filled - hl_filled
            if remaining <= 0:
                break
            book = hl_book if attempt == 0 else await self.client._get_hl_book(symbol)
            ref = book.bid if hl_side == "sell" else book.ask
            res = await self.client.place_hl_ioc(symbol, hl_side, remaining, ref)
            if res.success and res.filled_qty > 0:
                tot = hl_filled + res.filled_qty
                hl_avg = (hl_avg * hl_filled + res.fill_price * res.filled_qty) / tot
                hl_filled = tot

        if hl_filled <= 0:
            log.critical(f"{symbol}: partial close Aster closed {aster_filled} but HL hedge "
                         f"FAILED — NAKED. Manual intervention!")
            self.pm.mark_error(symbol, "partial_close_hl_hedge_failed")
            return False
        if hl_filled < aster_filled * 0.999:
            log.critical(f"{symbol}: partial close HL only hedged {hl_filled}/{aster_filled} — "
                         f"residual naked {aster_filled - hl_filled:.4f}. Manual check!")

        closed = min(aster_filled, hl_filled)
        remove_notional = closed * mid
        self.pm.log_trade(
            pos.id, "aster", aster_side, "ioc_limit",
            aster_res.order_id, aster_filled, aster_res.fill_price, notes="partial_close",
        )
        self.pm.log_trade(
            pos.id, "hl", hl_side, "ioc_limit",
            "", hl_filled, hl_avg, notes="partial_close",
        )
        self.pm.scale_out(symbol, closed, remove_notional, hl_avg, aster_res.fill_price)
        log.warning(
            f"PARTIAL CLOSE OK {symbol}: closed {closed:.4f} "
            f"(Aster @ {aster_res.fill_price:.4f}, HL @ {hl_avg:.2f}) "
            f"remaining={pos.qty:.4f}"
        )
        return True

    def _aster_exit_size(self, symbol: str, pos) -> float:
        """Qty the Aster exit leg should cover. Full close = pos.qty; a partial
        close only covers the partial target so we never over-close the Aster leg."""
        if symbol in self._partial_closes:
            return self.client.snap_aster_qty(symbol, self._partial_closes[symbol])
        return pos.qty

    def _advance_aster_exit_fill(self, symbol: str, closed_qty: float,
                                 hl_price: float, aster_price: float, exit_reason: str):
        """Finalize an Aster exit leg fill: partial closes scale out + revert to
        open; full closes book P&L and close the position.

        For partials, scale out by the partial TARGET, not the last fill. By the
        time we finalize, the whole target is closed on both legs (HL upfront,
        Aster via accumulated reposts), so using the target avoids under-scaling
        when the Aster GTX filled in pieces across reprices."""
        if symbol in self._partial_closes:
            target = self._partial_closes[symbol]
            self._finalize_partial_close(symbol, target, hl_price, aster_price)
        else:
            self.pm.confirm_aster_exit(symbol, aster_price, exit_reason)

    def _finalize_partial_close(self, symbol: str, closed_qty: float,
                                hl_price: float, aster_price: float):
        """Complete a partial close: scale_out + revert position to open."""
        self._partial_closes.pop(symbol, None)
        pos = self.pm.get(symbol)
        mid = (hl_price + aster_price) / 2 if (hl_price > 0 and aster_price > 0) else hl_price or aster_price
        remove_notional = closed_qty * mid
        self.pm.scale_out(symbol, closed_qty, remove_notional, hl_price, aster_price)
        self.pm.revert_partial_exit(symbol)
        remaining = self.pm.get(symbol)
        log.warning(f"PARTIAL CLOSE OK {symbol}: closed {closed_qty:.4f}, "
                    f"remaining={remaining.qty:.4f}" if remaining else f"PARTIAL CLOSE OK {symbol}")

    async def exit_position(
        self, symbol: str, aster_book: OrderBook, hl_book: OrderBook, reason: str,
        maker_venue: str = "", close_qty: float = 0.0,
    ) -> bool:
        """Dispatch an exit.
        maker_venue override: 'hl' = rest maker on HL, 'aster' = rest maker on Aster.
        Empty = auto (use entry_maker_venue if set). Urgent reasons always taker-taker.
        close_qty > 0: partial close (only close this many shares).

        Partial closes use ONLY the two qty-authoritative paths — HL-maker
        (szi-delta tracked, IOC Aster hedge) or taker-taker (immediate IOCs).
        The Aster resting-GTX path can't cleanly track partial fills across
        reprices, so it is never used for partials. Aster is 0% fee, so a
        taker-taker Aster leg costs no extra fee vs an Aster maker."""
        pos = self.pm.get(symbol)
        if not pos or pos.status != "open":
            return False
        effective_maker = maker_venue or (pos.entry_maker_venue if pos else "")
        is_partial = close_qty > 0 and close_qty < pos.qty

        if is_partial:
            if effective_maker == "hl" and reason not in self.URGENT_EXIT_REASONS:
                self._partial_closes[symbol] = close_qty
                ok = await self.force_exit_maker(symbol, reason, close_qty,
                                                 dec_aster_book=aster_book, dec_hl_book=hl_book)
                if ok:
                    return True
                self._partial_closes.pop(symbol, None)
                log.warning(f"{symbol}: HL maker partial unavailable — taker-taker partial")
            return await self.partial_close(symbol, close_qty, reason)

        # Full close
        if pos and effective_maker == "hl" and reason not in self.URGENT_EXIT_REASONS:
            ok = await self.force_exit_maker(symbol, reason,
                                             dec_aster_book=aster_book, dec_hl_book=hl_book)
            if ok:
                return True
            log.warning(f"{symbol}: maker exit unavailable — falling back to taker exit")
        return await self.try_exit(symbol, aster_book, hl_book, reason)

    async def force_exit_maker(self, symbol: str, reason: str, close_qty: float = 0.0,
                               dec_aster_book: OrderBook = None,
                               dec_hl_book: OrderBook = None) -> bool:
        """Close via HL maker: rest a post-only HL order on the close side
        (sell@ask for a long-HL leg, buy@bid for a short-HL leg) and let
        poll_hl_maker_exit cross Aster (IOC taker) to close each HL fill. Returns
        False if the HL maker can't be placed (caller falls back to taker).
        close_qty > 0: partial close (only close this many shares).

        dec_*_book: the book snapshot the EXIT DECISION was made on. In paper
        mode the fill is fabricated from the book, so it MUST reuse the decision
        snapshot — re-fetching gives a different (thin-book) reading than the one
        est_net was judged on, which is how a 'target' (profit) exit can book a
        loss. Live mode always re-fetches (real orders fill at the live book)."""
        pos = self.pm.get(symbol)
        if not pos or pos.status != "open":
            return False
        qty = close_qty if (close_qty > 0 and close_qty < pos.qty) else pos.qty
        if self.paper_mode and dec_aster_book is not None and dec_hl_book is not None:
            aster_book, hl_book = dec_aster_book, dec_hl_book
        else:
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
        hl_tick = self.client.hl_specs.get(symbol, ContractSpec()).tick_size
        if closing_long:
            hl_side, hl_ref = "sell", hl_book.ask + hl_tick
            aster_fill = aster_book.ask
        else:
            hl_side, hl_ref = "buy", hl_book.bid - hl_tick
            aster_fill = aster_book.bid

        partial = " (PARTIAL)" if qty < pos.qty else ""
        log.warning(
            f"MAKER EXIT{partial} {symbol} ({reason}): {pos.direction} | qty={qty}/{pos.qty} | "
            f"HL {hl_side} maker @ {hl_ref:.2f}"
        )

        if self.paper_mode:
            if qty < pos.qty:
                self._finalize_partial_close(symbol, qty, hl_ref, aster_fill)
            else:
                self.pm.start_exiting_hl_maker(symbol, "PAPER", 0.0, hl_ref, exit_spread_bps)
                self.pm.confirm_hl_maker_exit(symbol, pos.qty, hl_ref, aster_fill, reason)
            log.info(f"[PAPER] MAKER EXIT{partial} {symbol} ({reason}) spread={exit_spread_bps:.1f}bps")
            return True

        try:
            pre = await self.client.get_hl_position(symbol)
            baseline_szi = float(pre.get("szi", 0) or 0)
        except Exception as e:
            log.error(f"{symbol}: HL exit pre-snapshot failed ({e})")
            return False

        alo = await self.client.place_hl_alo(symbol, hl_side, qty, hl_ref)
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
        # An HL-maker exit is identified by the exiting state with a resting HL
        # exit order and NO resting Aster GTX (Aster is hedged via IOC per fill).
        # This holds whether the position was ENTERED maker-first on HL (carry)
        # or a taker-entered position is being CLOSED with an explicit HL-maker
        # exit (/close ... hl). Gating on entry_maker_venue would strand the
        # latter — the resting HL maker would never be polled → naked Aster leg.
        pos = self.pm.get(symbol)
        if (not pos or pos.status != "exiting" or pos.aster_exit_order_id
                or not pos.hl_exit_order_id):
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

        target_qty = self._partial_closes.get(symbol, pos.qty)

        # Reconcile the Aster close against the LIVE position — the exit IOC can
        # fill asynchronously (POST returns filled_qty=0, then fills as taker) and
        # once the leg is flat a reduce_only retry just rejects, so trusting only
        # the order-ack accumulator loops forever (observed DELL: circuit breaker
        # spamming while the venue was already closed). The venue is the truth.
        try:
            ap = await self.client.get_aster_position(symbol)
            aster_amt = abs(float(ap.get("positionAmt", 0) or 0))
            closed_live = min(max(0.0, pos.qty - aster_amt), target_qty)
            if closed_live > pos.aster_hedged_qty + 1e-9:
                px = pos.aster_exit_price if pos.aster_exit_price > 0 else 0.0
                if px <= 0:
                    try:
                        ab = await self.client._get_aster_book(symbol)
                        px = ab.bid if aster_hedge_side == "sell" else ab.ask
                    except Exception:
                        px = 0.0
                log.warning(
                    f"{symbol}: Aster exit reconciled {pos.aster_hedged_qty:.4f} -> "
                    f"{closed_live:.4f} from live position"
                )
                self.pm.record_hl_maker_exit_progress(symbol, closed_live, px)
        except Exception as e:
            log.warning(f"{symbol}: Aster exit reconcile failed ({e}) — using accumulator")

        # Close any newly-filled HL qty on Aster with an IOC taker.
        new_fill = hl_closed - pos.aster_hedged_qty
        lot = self.client.snap_aster_qty(symbol, new_fill) if new_fill > 0 else 0.0
        if lot > 0:
            MAX_HEDGE_ATTEMPTS = 10
            if pos.aster_hedge_attempts >= MAX_HEDGE_ATTEMPTS:
                # Terminal action (not a bare return that loops forever):
                # reconcile both legs and finalize if flat, else mark error.
                log.error(
                    f"{symbol}: Aster exit hedge circuit breaker tripped "
                    f"({pos.aster_hedge_attempts} attempts) — reconciling to finalize"
                )
                await self._force_finalize_maker_exit(symbol, closing_long)
                return

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
                pos.aster_hedge_attempts += 1
                res = await self.client.place_aster_ioc(
                    symbol, aster_hedge_side, lot, touch, reduce_only=True)
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

        # Fully closed and hedged → finalise. Also finalise when the HL leg is
        # done and the Aster leg is confirmed flat on the venue (reconcile above
        # credited it) even if hl_closed slightly exceeds due to szi rounding.
        if hl_closed >= target_qty * 0.999 and pos.aster_hedged_qty >= min(hl_closed, target_qty) * 0.999:
            if pos.hl_exit_order_id and pos.hl_exit_order_id != "PAPER":
                await self.client.cancel_hl_order(symbol, pos.hl_exit_order_id)
            if symbol in self._partial_closes:
                self._finalize_partial_close(
                    symbol, hl_closed, pos.hl_exit_price, pos.aster_exit_price)
            else:
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

    async def _force_finalize_maker_exit(self, symbol: str, closing_long: bool):
        """Terminal action for a stuck maker exit (circuit breaker): read BOTH
        legs from the venue. If both are flat, the close actually completed —
        book it and finalize. Otherwise a real leg is still open → mark error so
        it's surfaced, not looped on forever."""
        pos = self.pm.get(symbol)
        if not pos:
            return
        try:
            hlp = await self.client.get_hl_position(symbol)
            hl_amt = abs(float(hlp.get("szi", 0) or 0))
        except Exception:
            hl_amt = None
        try:
            ap = await self.client.get_aster_position(symbol)
            aster_amt = abs(float(ap.get("positionAmt", 0) or 0))
        except Exception:
            aster_amt = None
        dust = max(pos.qty * 0.05, 1e-9)
        hl_flat = hl_amt is not None and hl_amt < dust
        aster_flat = aster_amt is not None and aster_amt < dust
        if hl_flat and aster_flat:
            if pos.hl_exit_order_id and pos.hl_exit_order_id != "PAPER":
                await self.client.cancel_hl_order(symbol, pos.hl_exit_order_id)
            # Estimate the exit fills from the current book (the fills already
            # happened on the venue; pos.hl_exit_price may be 0/stale, which would
            # book fake P&L). Close side: long_hl sells HL @ bid / buys Aster @ ask.
            hl_px = pos.hl_exit_price
            ast_px = pos.aster_exit_price
            try:
                ab, hb = await self.client.get_both_books(symbol)
                hl_px = (hb.bid if closing_long else hb.ask) or hl_px
                ast_px = (ab.ask if closing_long else ab.bid) or ast_px
            except Exception:
                pass
            if hl_px <= 0:
                hl_px = pos.hl_entry_price
            if ast_px <= 0:
                ast_px = pos.aster_entry_price
            log.warning(f"{symbol}: stuck maker exit — both legs flat on venue, "
                        f"finalizing @ HL {hl_px:.2f} / Ast {ast_px:.2f}")
            if symbol in self._partial_closes:
                self._finalize_partial_close(symbol, pos.qty, hl_px, ast_px)
            else:
                self.pm.confirm_hl_maker_exit(
                    symbol, pos.qty, hl_px, ast_px,
                    (pos.exit_reason or "manual") + "_reconciled")
        else:
            log.critical(
                f"{symbol}: stuck maker exit — venue NOT flat (HL={hl_amt}, Aster={aster_amt}), "
                f"CHECK VENUE — marking error"
            )
            self.pm.mark_error(symbol, "maker_exit_stuck_leg")

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
        target_qty = self._partial_closes.get(symbol, pos.qty)
        remaining_hl = self.client.snap_aster_qty(symbol, target_qty - hl_closed)
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

        # Reconcile the Aster close against the live position first — an earlier
        # IOC may have filled async without being credited (and once flat a
        # reduce_only retry just rejects). Then close any residual, RETRYING with
        # a fresh book: a single missed IOC must not leave the leg naked (SNDK:
        # HL closed 0.01, Aster closed 0.0 -> naked, marked error after one try).
        try:
            ap = await self.client.get_aster_position(symbol)
            aster_amt = abs(float(ap.get("positionAmt", 0) or 0))
            closed_live = min(max(0.0, pos.qty - aster_amt), hl_closed)
            if closed_live > pos.aster_hedged_qty + 1e-9:
                self.pm.record_hl_maker_exit_progress(
                    symbol, closed_live, pos.aster_exit_price or 0.0)
        except Exception:
            pass

        for attempt in range(4):
            residual = self.client.snap_aster_qty(symbol, hl_closed - pos.aster_hedged_qty)
            if residual <= 0:
                break
            try:
                aster_book = await self.client._get_aster_book(symbol)
            except Exception:
                await asyncio.sleep(0.3)
                continue
            touch = aster_book.ask if aster_hedge_side == "buy" else aster_book.bid
            if touch <= 0:
                await asyncio.sleep(0.3)
                continue
            res = await self.client.place_aster_ioc(
                symbol, aster_hedge_side, residual, touch, reduce_only=True)
            if res.success and res.filled_qty > 0:
                new_closed = pos.aster_hedged_qty + res.filled_qty
                old = pos.aster_hedged_qty
                a_avg = ((pos.aster_exit_price * old + res.fill_price * res.filled_qty)
                         / new_closed) if new_closed > 0 else res.fill_price
                self.pm.record_hl_maker_exit_progress(symbol, new_closed, a_avg)
                self.pm.log_trade(pos.id, "aster", aster_hedge_side, "ioc_close",
                                  res.order_id, res.filled_qty, res.fill_price,
                                  notes="maker-exit residual close")
            else:
                log.warning(f"{symbol}: maker-exit Aster close attempt {attempt+1}/4 "
                            f"no fill ({res.error}) — retrying")
                await asyncio.sleep(0.3)

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
        if symbol in self._partial_closes:
            self._finalize_partial_close(
                symbol, final_qty, hl_exit_px, pos.aster_exit_price)
        else:
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
        spec = self.client.hl_specs.get(symbol, ContractSpec())
        tick = spec.tick_size
        touch = (hl_book.ask + tick) if hl_side == "sell" else (hl_book.bid - tick)
        if touch <= 0:
            return
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
                self.pm.log_trade(
                    pos.id, "aster",
                    "buy" if pos.direction == "long_hl_short_aster" else "sell",
                    "gtx_limit", order_id, executed_qty, avg_price, notes="exit filled"
                )
                # Include fills banked by earlier reprice cycles of this flow.
                self._advance_aster_exit_fill(
                    symbol, pos.aster_gtx_filled + executed_qty,
                    pos.hl_exit_price, avg_price,
                    pos.exit_reason or "converged")
            return

        if status in ASTER_TERMINAL_STATUSES - {"FILLED"}:
            log.warning(f"{symbol}: Aster GTX {order_id} is {status} — repricing")
            # Bank any partial fill the dead order accumulated, or the repost
            # below re-covers it and over-fills the leg. Keyed to the order id
            # so seeing the same dead order twice never double-counts.
            if executed_qty > 0 and order_id != pos.aster_gtx_banked_oid:
                self.pm.set_gtx_filled(
                    symbol, pos.aster_gtx_filled + executed_qty, banked_oid=order_id)
            # If the banked fills complete the flow (or leave sub-lot dust),
            # finalize here while the fill price is at hand — the reprice path
            # would otherwise stall with nothing left to repost.
            target = pos.qty if is_entry else self._aster_exit_size(symbol, pos)
            remaining = self.client.snap_aster_qty(symbol, target - pos.aster_gtx_filled)
            if pos.aster_gtx_filled > 0 and remaining <= 0:
                log.warning(
                    f"{symbol}: GTX flow complete at {pos.aster_gtx_filled}/{target} "
                    f"via dead-order fills — finalizing"
                )
                if is_entry:
                    if pos.aster_gtx_filled >= target * 0.999:
                        self.pm.confirm_aster_entry(symbol, avg_price)
                    else:
                        self.pm.confirm_aster_entry_partial(
                            symbol, avg_price, pos.aster_gtx_filled)
                else:
                    self._advance_aster_exit_fill(
                        symbol, pos.aster_gtx_filled, pos.hl_exit_price,
                        avg_price, pos.exit_reason or "converged")
                return
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
        target = self._aster_exit_size(symbol, pos)
        # Subtract fills already banked by the GTX flow's reprice cycles.
        force_qty = self.client.snap_aster_qty(symbol, target - pos.aster_gtx_filled)
        if force_qty <= 0 and pos.aster_gtx_filled > 0:
            log.warning(
                f"{symbol}: force-exit target already covered by GTX fills "
                f"({pos.aster_gtx_filled}/{target}) — finalizing"
            )
            self._advance_aster_exit_fill(
                symbol, pos.aster_gtx_filled, pos.hl_exit_price,
                pos.aster_exit_price or 0.0, (pos.exit_reason or "converged"))
            return
        aster_book = await self.client._get_aster_book(symbol)
        ref_price = aster_book.ask if close_side == "buy" else aster_book.bid
        if ref_price <= 0:
            log.critical(f"{symbol}: empty Aster book on force exit — manual intervention!")
            self.pm.mark_error(symbol, "force_exit_empty_book")
            return
        force_intent_id = record_intent(
            symbol=symbol, venue="aster", action="force_exit_ioc",
            direction=pos.direction, side=close_side, qty=force_qty,
            ref_price=ref_price, position_id=pos.id, paper=self.paper_mode,
        )
        result = await self.client.place_aster_ioc(
            symbol, close_side, force_qty, ref_price, reduce_only=True)
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
            self._advance_aster_exit_fill(
                symbol, pos.aster_gtx_filled + result.filled_qty,
                pos.hl_exit_price, result.fill_price,
                (pos.exit_reason or "converged") + "_taker")
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

        # Size the WHOLE flow must cover. Entry and full exit use pos.qty; a
        # partial exit only covers the partial target so we never over-close.
        order_size = pos.qty if is_entry else self._aster_exit_size(symbol, pos)
        # Fills already consumed by earlier orders in this flow (persisted so a
        # reprice — or a restart — reposts only the remainder, never the full
        # size; reposting the full size after a partial fill over-fills the leg
        # and flips the position past flat).
        cum_filled = pos.aster_gtx_filled

        new_qty = self.client.snap_aster_qty(symbol, order_size - cum_filled)
        if current_order_id:
            q = await self.client.query_aster_order(symbol, current_order_id)
            current_price = float(q.get("price", 0) or 0)
            current_status = q.get("status", "")
            executed = float(q.get("executedQty", 0) or 0)
            avg_price = float(q.get("avgPrice", 0) or 0)

            # Race: filled between last poll and now → advance state, don't repost
            if current_status == "FILLED":
                log.info(f"{symbol}: GTX {current_order_id} filled during reprice check")
                total = cum_filled + executed
                if is_entry:
                    self.pm.confirm_aster_entry(symbol, avg_price)
                else:
                    self._advance_aster_exit_fill(
                        symbol, total or order_size, pos.hl_exit_price,
                        avg_price, pos.exit_reason or "converged")
                return

            # Already at best price (within half a tick) — nothing to do
            if current_price > 0 and abs(target_price - current_price) < tick / 2:
                return

            # Cancel and re-query to capture any fill that landed during cancel
            await self.client.cancel_aster_order(symbol, current_order_id)
            q2 = await self.client.query_aster_order(symbol, current_order_id)
            final_executed = float(q2.get("executedQty", executed) or executed)
            final_avg = float(q2.get("avgPrice", avg_price) or avg_price)

            # Bank ANY fill this order accumulated — resting partials included,
            # not just fills that landed inside the cancel window. Keyed to the
            # order id: if this order was already banked (repost failed last
            # tick), its fill is already inside cum_filled — don't re-add.
            if final_executed > 0 and current_order_id != pos.aster_gtx_banked_oid:
                cum_filled += final_executed
                self.pm.set_gtx_filled(symbol, cum_filled, banked_oid=current_order_id)
                log.warning(
                    f"{symbol}: GTX {current_order_id} filled {final_executed} before "
                    f"reprice — cumulative {cum_filled}/{order_size}"
                )

            if cum_filled >= order_size * 0.999:
                # Flow fully filled across orders — advance state, don't repost
                log.warning(f"{symbol}: GTX flow complete at {cum_filled} across reprices")
                if is_entry:
                    self.pm.confirm_aster_entry(symbol, final_avg or avg_price)
                else:
                    self._advance_aster_exit_fill(
                        symbol, cum_filled, pos.hl_exit_price,
                        final_avg or avg_price, pos.exit_reason or "converged")
                return

            new_qty = self.client.snap_aster_qty(symbol, order_size - cum_filled)
            if new_qty <= 0 and cum_filled > 0:
                # Remainder is sub-lot dust — treat the flow as done at cum_filled.
                log.warning(
                    f"{symbol}: GTX remainder below lot size after {cum_filled} filled "
                    f"— treating as complete"
                )
                if is_entry:
                    self.pm.confirm_aster_entry_partial(symbol, final_avg, cum_filled)
                else:
                    self._advance_aster_exit_fill(
                        symbol, cum_filled, pos.hl_exit_price,
                        final_avg, pos.exit_reason or "converged")
                return

        if new_qty <= 0:
            return
        new_result = await self.client.place_aster_gtx(
            symbol, side, new_qty, target_price, reduce_only=not is_entry)
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
        close_qty: float = 0.0,
    ) -> bool:
        pos = self.pm.get(symbol)
        if not pos or pos.status != "open":
            return False
        qty = close_qty if (close_qty > 0 and close_qty < pos.qty) else pos.qty

        if self.paper_mode:
            mid = (aster_book.mid + hl_book.mid) / 2
            exit_spread_bps = (aster_book.mid - hl_book.mid) / mid * 10000 if mid > 0 else 0
            if pos.direction == "long_hl_short_aster":
                hl_exit_px = hl_book.bid
                aster_exit_px = aster_book.ask
            else:
                hl_exit_px = hl_book.ask
                aster_exit_px = aster_book.bid
            if qty < pos.qty:
                self._finalize_partial_close(symbol, qty, hl_exit_px, aster_exit_px)
            else:
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
            direction=pos.direction, side=hl_close_side, qty=qty,
            ref_price=hl_ref_price, baseline_szi=exit_baseline_szi,
            position_id=pos.id, paper=self.paper_mode,
        )

        # Step 1: HL IOC close — keep filling until we have the full qty (max 2 attempts)
        hl_filled = 0.0
        hl_avg_price = 0.0
        hl_order_id = ""
        last_err = ""
        for attempt in range(2):
            remaining = qty - hl_filled
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

        partial_hl = hl_filled < qty * 0.999  # tolerate 0.1% rounding
        if partial_hl:
            log.warning(
                f"{symbol}: HL exit PARTIAL {hl_filled}/{qty} — closing matched Aster qty, "
                f"residual {qty - hl_filled} will need manual close"
            )

        log.info(f"{symbol}: HL exit filled {hl_filled} @ {hl_avg_price:.2f}")

        # Step 2: Aster close — match the HL filled qty exactly to stay hedged
        aster_qty = self.client.snap_aster_qty(symbol, hl_filled)
        if aster_qty <= 0:
            log.critical(f"{symbol}: HL filled {hl_filled} but Aster snap returned 0 — UNHEDGED")
            self.pm.mark_error(symbol, "aster_qty_snap_zero")
            return False

        aster_result = await self.client.place_aster_gtx(
            symbol, aster_close_side, aster_qty, aster_ref_price, reduce_only=True
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
                symbol, aster_close_side, aster_qty, aster_ref_price, reduce_only=True
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
            # IOC filled immediately
            self.pm.log_trade(
                pos.id, "hl", hl_close_side, "ioc_limit",
                hl_order_id, hl_filled, hl_avg_price, notes="exit" + (" partial" if partial_hl else ""),
            )
            self.pm.log_trade(
                pos.id, "aster", aster_close_side, "ioc_limit",
                aster_result.order_id, aster_result.filled_qty, aster_result.fill_price,
                notes="exit forced taker",
            )
            if symbol in self._partial_closes:
                self._finalize_partial_close(
                    symbol, hl_filled, hl_avg_price, aster_result.fill_price)
            else:
                self.pm.start_exiting(
                    symbol, hl_order_id, aster_result.order_id, hl_avg_price, exit_spread_bps,
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
        # If HL partial on a FULL close, shrink pos.qty to what's actually open on
        # Aster. For a partial close we must NOT touch pos.qty here — finalisation
        # via _finalize_partial_close scales out only the actually-closed amount and
        # leaves the rest of the position open.
        if partial_hl and symbol not in self._partial_closes:
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

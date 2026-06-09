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

from exchange_client import ExchangeClient, OrderBook
from position_manager import PositionManager
from config import (
    ENTRY_THRESHOLD_BPS, ENTRY_THRESHOLD_BPS_BY_SYMBOL, EXIT_THRESHOLD_BPS,
    NOTIONAL_PER_LEG, ENTRY_TIMEOUT_MINUTES,
    MAX_PRICE_RATIO_DIVERGENCE, BLOCKED_SYMBOLS,
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

        # Oracle-adjusted effective entry spreads (bid/ask adjusted).
        # oracle_delta_bps = structural index price difference that won't converge;
        # only the excess beyond this is tradeable.
        aster_premium_bps = (aster_book.bid - hl_book.ask) / mid * 10000
        hl_premium_bps = (hl_book.bid - aster_book.ask) / mid * 10000

        aster_index = self.client.get_aster_index(symbol)
        hl_oracle = self.client.get_hl_oracle(symbol)
        oracle_delta_bps = (
            (aster_index - hl_oracle) / mid * 10000
            if aster_index > 0 and hl_oracle > 0 else 0.0
        )
        # For long_hl_short_aster: Aster premium minus the oracle delta
        # For long_aster_short_hl: HL premium plus the oracle delta (delta is negative here)
        aster_excess_bps = aster_premium_bps - oracle_delta_bps
        hl_excess_bps = hl_premium_bps + oracle_delta_bps

        threshold = ENTRY_THRESHOLD_BPS_BY_SYMBOL.get(symbol, ENTRY_THRESHOLD_BPS)

        if aster_excess_bps >= threshold:
            direction = "long_hl_short_aster"
            spread_bps = aster_excess_bps
            # HL: buy (long) at ask — taker
            hl_side = "buy"
            hl_ref_price = hl_book.ask
            # Aster: sell (short) at bid — maker
            aster_side = "sell"
            aster_ref_price = aster_book.bid
        elif hl_excess_bps >= threshold:
            direction = "long_aster_short_hl"
            spread_bps = hl_excess_bps
            hl_side = "sell"
            hl_ref_price = hl_book.bid
            aster_side = "buy"
            aster_ref_price = aster_book.ask
        else:
            return False

        # Size in base tokens
        qty = self.client.snap_aster_qty(symbol, NOTIONAL_PER_LEG / mid)
        if qty <= 0:
            log.warning(f"{symbol}: qty snapped to 0 at mid={mid:.2f}")
            return False

        log.info(
            f"ENTRY {symbol}: {direction} | excess={spread_bps:.1f}bps d={oracle_delta_bps:+.1f}bps | "
            f"qty={qty} | HL {hl_side} @ {hl_ref_price:.2f} | Aster {aster_side} @ {aster_ref_price:.2f}"
        )

        if self.paper_mode:
            log.info(
                f"[PAPER] ENTRY {symbol}: {direction} | excess={spread_bps:.1f}bps | "
                f"qty={qty} | HL {hl_side} @ {hl_ref_price:.2f} | Aster {aster_side} @ {aster_ref_price:.2f}"
            )
            self.pm.open_entering(
                symbol=symbol,
                hl_coin=f"xyz:{symbol}",
                aster_symbol=f"{symbol}USDT",
                direction=direction,
                entry_spread_bps=spread_bps,
                hl_entry_price=hl_ref_price,
                hl_order_id="PAPER",
                aster_entry_order_id="PAPER",
                qty=qty,
                notional_usd=NOTIONAL_PER_LEG,
            )
            self.pm.confirm_aster_entry(symbol, aster_ref_price)
            return True

        # Pre-trade recheck
        try:
            fresh_aster, fresh_hl = await self.client.get_both_books(symbol)
            fresh_mid = (fresh_aster.mid + fresh_hl.mid) / 2
            if fresh_mid <= 0:
                log.warning(f"{symbol}: empty books on recheck, aborting")
                return False
            fresh_oracle_delta = (
                (aster_index - hl_oracle) / fresh_mid * 10000
                if aster_index > 0 and hl_oracle > 0 else 0.0
            )
            if direction == "long_hl_short_aster":
                fresh_excess = (fresh_aster.bid - fresh_hl.ask) / fresh_mid * 10000 - fresh_oracle_delta
            else:
                fresh_excess = (fresh_hl.bid - fresh_aster.ask) / fresh_mid * 10000 + fresh_oracle_delta
            if fresh_excess < ENTRY_THRESHOLD_BPS:
                log.warning(
                    f"{symbol}: excess spread collapsed to {fresh_excess:.1f}bps on recheck, aborting"
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

        # Step 1: HL IOC (taker) — fills immediately or not at all
        hl_result = await self.client.place_hl_ioc(symbol, hl_side, qty, hl_ref_price)
        if not hl_result.success or hl_result.filled_qty <= 0:
            log.info(f"{symbol}: HL IOC did not fill ({hl_result.error}) — entry aborted")
            return False

        actual_qty = hl_result.filled_qty
        log.info(f"{symbol}: HL {hl_side} filled {actual_qty} @ {hl_result.fill_price:.2f}")

        # Step 2: Aster GTX (maker) — rests at best bid/ask
        aster_result = await self.client.place_aster_gtx(
            symbol, aster_side, actual_qty, aster_ref_price
        )
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
            aster_symbol=f"{symbol}USDT",
            direction=direction,
            entry_spread_bps=spread_bps,
            hl_entry_price=hl_result.fill_price,
            hl_order_id=hl_result.order_id,
            aster_entry_order_id=aster_result.order_id,
            qty=actual_qty,
            notional_usd=NOTIONAL_PER_LEG,
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

        # Check entry timeout
        if is_entry:
            elapsed_min = (now_ms() - pos.entry_time) / 60_000
            if elapsed_min > ENTRY_TIMEOUT_MINUTES:
                log.warning(
                    f"{symbol}: Aster entry GTX timed out after {elapsed_min:.0f}min — "
                    f"cancelling and closing HL"
                )
                await self.client.cancel_aster_order(symbol, order_id)
                await self._emergency_close_hl(pos)
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

    async def _reprice_aster_gtx(self, pos, is_entry: bool, current_order_id: str | None):
        """If the book has moved, cancel existing GTX and repost at new best price."""
        symbol = pos.symbol
        aster_book = await self.client._get_aster_book(symbol)
        if aster_book.bid <= 0:
            return

        if is_entry:
            side = "sell" if pos.direction == "long_hl_short_aster" else "buy"
            target_price = aster_book.bid if side == "sell" else aster_book.ask
            current_price = float(
                (await self.client.query_aster_order(symbol, current_order_id or "0")).get("price", 0) or 0
            ) if current_order_id else 0
        else:
            side = "buy" if pos.direction == "long_hl_short_aster" else "sell"
            target_price = aster_book.ask if side == "buy" else aster_book.bid
            current_price = float(
                (await self.client.query_aster_order(symbol, current_order_id or "0")).get("price", 0) or 0
            ) if current_order_id else 0

        spec = self.client.aster_specs.get(symbol)
        tick = spec.tick_size if spec else 0.01
        if current_order_id and abs(target_price - current_price) < tick / 2:
            return  # already at best price

        # Cancel old and repost
        if current_order_id:
            await self.client.cancel_aster_order(symbol, current_order_id)

        new_result = await self.client.place_aster_gtx(symbol, side, pos.qty, target_price)
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

        # Step 1: HL IOC close
        hl_result = await self.client.place_hl_ioc(
            symbol, hl_close_side, pos.qty, hl_ref_price
        )
        if not hl_result.success or hl_result.filled_qty <= 0:
            log.error(f"{symbol}: HL exit IOC failed ({hl_result.error})")
            # Retry once with market
            hl_result = await self.client.place_hl_ioc(
                symbol, hl_close_side, pos.qty, hl_ref_price
            )
            if not hl_result.success:
                log.critical(f"{symbol}: HL exit FAILED — manual intervention needed")
                self.pm.mark_error(symbol, f"HL exit failed: {hl_result.error}")
                return False

        log.info(f"{symbol}: HL exit filled {hl_result.filled_qty} @ {hl_result.fill_price:.2f}")

        # Step 2: Aster GTX close (maker, resting)
        aster_result = await self.client.place_aster_gtx(
            symbol, aster_close_side, pos.qty, aster_ref_price
        )
        if not aster_result.success:
            log.error(f"{symbol}: Aster exit GTX failed — retrying as IOC")
            # Try IOC as fallback (will be taker)
            aster_result = await self.client.place_aster_gtx(
                symbol, aster_close_side, pos.qty, aster_ref_price
            )
            if not aster_result.success:
                log.critical(f"{symbol}: Aster exit FAILED — UNHEDGED on Aster. Manual intervention!")
                self.pm.mark_error(symbol, f"Aster exit failed: {aster_result.error}")
                return False

        # Record exit as 'exiting' — wait for Aster GTX to fill
        pos.exit_reason = reason
        self.pm.start_exiting(
            symbol, hl_result.order_id, aster_result.order_id,
            hl_result.fill_price, exit_spread_bps,
        )
        self.pm.log_trade(
            pos.id, "hl", hl_close_side, "ioc_limit",
            hl_result.order_id, hl_result.filled_qty, hl_result.fill_price, notes="exit"
        )
        self.pm.log_trade(
            pos.id, "aster", aster_close_side, "gtx_limit",
            aster_result.order_id, pos.qty, aster_ref_price, notes="exit resting"
        )
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

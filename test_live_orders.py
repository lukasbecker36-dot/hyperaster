"""
Live order signing test — places and immediately cancels minimal orders on both
exchanges to verify API keys and signing work end-to-end.

Orders are priced 40% below market so they will never fill.
Uses the minimum possible quantity.

Run:  python test_live_orders.py
"""

import asyncio
import logging
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

from auth import load_api_keys
from exchange_client import ExchangeClient

TEST_SYMBOL = "AAPL"
PRICE_OFFSET = 0.05  # place 5% below market — will never fill but within exchange price limits


async def run():
    log.info("Loading API keys...")
    try:
        api_keys = load_api_keys()
    except EnvironmentError as e:
        log.error(f"Key load failed: {e}")
        return

    client = ExchangeClient(api_keys)
    log.info(f"Starting exchange client for {TEST_SYMBOL}...")
    await client.start([TEST_SYMBOL])

    if TEST_SYMBOL not in client.aster_specs:
        log.error(f"Aster specs not loaded for {TEST_SYMBOL} — aborting")
        await client.close()
        return

    if TEST_SYMBOL not in client.hl_specs:
        log.error(f"HL specs not loaded for {TEST_SYMBOL} — aborting")
        await client.close()
        return

    # ── Get current prices ──
    log.info("Fetching current order books...")
    aster_book, hl_book = await client.get_both_books(TEST_SYMBOL)

    if aster_book.mid <= 0:
        log.error("Aster order book empty — is the market open?")
        await client.close()
        return

    if hl_book.mid <= 0:
        log.error("HL order book empty")
        await client.close()
        return

    log.info(f"Aster mid: ${aster_book.mid:.2f}  HL mid: ${hl_book.mid:.2f}")

    # ── Test 1: Aster GTX ──
    log.info("=" * 50)
    log.info("TEST 1: Aster GTX order (post-only, far below market)")

    spec = client.aster_specs[TEST_SYMBOL]
    test_price = client.snap_aster_price(TEST_SYMBOL, aster_book.mid * (1 - PRICE_OFFSET))
    test_qty = client.snap_aster_qty(TEST_SYMBOL, spec.min_notional / test_price + spec.step_size)
    if test_qty <= 0:
        test_qty = spec.min_qty

    log.info(f"  Placing GTX BUY {test_qty} {TEST_SYMBOL} @ ${test_price:.2f} (market ~${aster_book.mid:.2f})")

    result = await client.place_aster_gtx(TEST_SYMBOL, "buy", test_qty, test_price)

    if result.success:
        log.info(f"  [PASS] Aster GTX placed: order_id={result.order_id}")
        log.info(f"  Cancelling order {result.order_id}...")
        cancelled = await client.cancel_aster_order(TEST_SYMBOL, result.order_id)
        if cancelled:
            log.info(f"  [PASS] Aster cancel successful")
        else:
            log.warning(f"  [WARN] Aster cancel returned false — check exchange manually for open order {result.order_id}")
    else:
        log.error(f"  [FAIL] Aster GTX failed: {result.error}")
        log.error("  Check: ASTER_API_KEY, ASTER_API_SECRET, ASTER_WALLET_ADDRESS")

    # ── Test 2: Hyperliquid IOC ──
    log.info("=" * 50)
    log.info("TEST 2: Hyperliquid XYZ IOC order (priced far below market, will not fill)")

    hl_spec = client.hl_specs[TEST_SYMBOL]
    hl_test_price = round(hl_book.mid * (1 - PRICE_OFFSET), hl_spec.price_precision)
    hl_test_qty = round(max(hl_spec.min_qty, hl_spec.min_notional / hl_test_price + hl_spec.step_size), hl_spec.qty_precision)

    log.info(f"  Placing IOC BUY {hl_test_qty} xyz:{TEST_SYMBOL} @ ${hl_test_price:.2f} (market ~${hl_book.mid:.2f})")

    hl_result = await client.place_hl_ioc(TEST_SYMBOL, "buy", hl_test_qty, hl_test_price)

    if hl_result.success:
        log.info(f"  [PASS] HL IOC filled (unexpectedly) @ {hl_result.fill_price} — check position!")
    elif hl_result.error == "ioc_not_filled":
        log.info(f"  [PASS] HL IOC submitted and expired unfilled (expected)")
    else:
        log.error(f"  [FAIL] HL IOC error: {hl_result.error}")
        log.error("  Check: HL_PRIVATE_KEY, HL_WALLET_ADDRESS")

    # ── Summary ──
    log.info("=" * 50)
    aster_ok = result.success
    hl_ok = hl_result.success or hl_result.error == "ioc_not_filled"

    if aster_ok and hl_ok:
        log.info("ALL TESTS PASSED — signing works on both exchanges")
        log.info("Safe to run: python live_monitor.py --live")
    else:
        if not aster_ok:
            log.error("ASTER SIGNING FAILED")
        if not hl_ok:
            log.error("HYPERLIQUID SIGNING FAILED")

    await client.close()


if __name__ == "__main__":
    asyncio.run(run())

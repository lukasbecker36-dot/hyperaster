#!/usr/bin/env python3
"""
Emergency flatten — close all open positions on both exchanges immediately.

Cancels any resting Aster orders, then queries the ACTUAL position on each
venue and market-closes whatever's open. Useful when:
  • a position is stuck (entering/exiting state) and you want out
  • the bot is in an error state and DB is out of sync with reality
  • you want to fully unwind before maintenance or stopping the bot

Usage:
    python flatten.py                    # close everything reported as open by DB
    python flatten.py --symbols AAPL MU  # close only specific symbols
    python flatten.py --reconcile        # query both venues and close whatever they say is open
                                         #   (DB ignored — use this when DB is corrupted)
    python flatten.py --yes              # skip the confirmation prompt
"""

import argparse
import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from auth import load_api_keys
from database import init_db, get_connection
from exchange_client import ExchangeClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("flatten")


async def flatten_symbol(client: ExchangeClient, symbol: str) -> dict:
    """Cancel all open Aster orders for symbol, then IOC-close any live position on either venue."""
    out = {
        "symbol": symbol,
        "aster_cancelled": 0,
        "aster_closed_qty": 0.0,
        "hl_closed_qty": 0.0,
        "errors": [],
    }

    # 1. Cancel all open Aster orders for this symbol
    try:
        orders = await client.get_aster_open_orders(symbol)
        for o in orders:
            oid = str(o.get("orderId"))
            if await client.cancel_aster_order(symbol, oid):
                out["aster_cancelled"] += 1
    except Exception as e:
        out["errors"].append(f"cancel orders: {e}")

    # 2. Read actual Aster position & close it
    try:
        ap = await client.get_aster_position(symbol)
        ast_qty = float(ap.get("positionAmt", 0) or 0)
        if abs(ast_qty) > 0:
            side = "sell" if ast_qty > 0 else "buy"
            book = await client.get_orderbook("aster", symbol)
            ref = book.bid if side == "sell" else book.ask
            if ref <= 0:
                out["errors"].append("aster: empty book")
            else:
                r = await client.place_aster_ioc(symbol, side, abs(ast_qty), ref)
                if r.success:
                    out["aster_closed_qty"] = r.filled_qty
                else:
                    out["errors"].append(f"aster IOC close: {r.error}")
    except Exception as e:
        out["errors"].append(f"aster close: {e}")

    # 3. Read actual HL position & close it
    try:
        hp = await client.get_hl_position(symbol)
        hl_qty = float(hp.get("szi", 0) or 0)
        if abs(hl_qty) > 0:
            side = "sell" if hl_qty > 0 else "buy"
            book = await client.get_orderbook("hl", symbol)
            ref = book.bid if side == "sell" else book.ask
            if ref <= 0:
                out["errors"].append("hl: empty book")
            else:
                r = await client.place_hl_ioc(symbol, side, abs(hl_qty), ref)
                if r.success:
                    out["hl_closed_qty"] = r.filled_qty
                else:
                    out["errors"].append(f"hl IOC close: {r.error}")
    except Exception as e:
        out["errors"].append(f"hl close: {e}")

    return out


def symbols_from_db() -> list[str]:
    init_db()
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM positions "
        "WHERE status NOT IN ('closed', 'error') AND paper=0"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


async def reconcile_symbols(api_keys: dict) -> list[str]:
    """Query both venues for live positions (DB-independent).

    HL is queried across BOTH dexes via get_all_hl_positions(). This used to post
    clearinghouseState with a hardcoded dex="xyz" and filter coin.startswith
    ("xyz:"), which made every main-dex CRYPTO position invisible to the
    emergency flatten — the one thing it exists to catch. A naked HL crypto leg
    (Aster hedge failed, or the Aster side already closed) was then found by
    neither venue's scan and flatten reported success with the leg still open.
    """
    import aiohttp

    found: set[str] = set()
    client = ExchangeClient(api_keys)
    client.session = aiohttp.ClientSession()
    try:
        # HL: both dexes. Coin is "xyz:AAPL" on the builder dex and bare "BTC" on
        # main, so the canonical base is whatever follows the last colon.
        try:
            hl_positions = await client.get_all_hl_positions()
            for p in hl_positions:
                coin = p.get("coin", "")
                if coin:
                    found.add(coin.split(":")[-1])
            if hl_positions:
                log.info(f"HL reconcile found: "
                         f"{[p.get('coin') for p in hl_positions]}")
        except Exception as e:
            log.error(f"HL reconcile failed: {e}")

        # Aster positions: ExchangeClient handles signing
        try:
            from config import ASTER_POSITION_URL, ASTER_BASE_TO_CANON
            from auth import sign_aster_request
            params = sign_aster_request(
                {},
                private_key=api_keys["aster_api_secret"],
                user_address=api_keys["aster_wallet_address"],
                signer_address=api_keys["aster_signer_address"],
            )
            async with client.session.get(ASTER_POSITION_URL, params=params) as r:
                data = await r.json()
            if isinstance(data, list):
                for p in data:
                    sym = p.get("symbol", "")
                    if abs(float(p.get("positionAmt", 0) or 0)) <= 0:
                        continue
                    for suffix in ("USDT", "USDC", "USD"):
                        if sym.endswith(suffix):
                            base = sym[: -len(suffix)]
                            # Aster's tradeable book uses the long name for a few
                            # equities (SAMSUNGUSDT, SKHYNIXUSDT, BBXUSDT). Without
                            # this map flatten got "SAMSUNG", which has no HL spec,
                            # so it closed the Aster leg and left HL naked.
                            found.add(ASTER_BASE_TO_CANON.get(base, base))
                            break
        except Exception as e:
            log.error(f"Aster reconcile failed: {e}")
    finally:
        await client.session.close()

    return sorted(found)


async def main_async(symbols: list[str] | None, reconcile: bool, auto_yes: bool):
    api_keys = load_api_keys()

    if reconcile:
        symbols = await reconcile_symbols(api_keys)
    elif not symbols:
        symbols = symbols_from_db()

    if not symbols:
        log.warning("No symbols to flatten. Use --symbols X Y or --reconcile.")
        return

    log.warning(f"FLATTEN target: {symbols}")
    if not auto_yes:
        confirm = input(f"Flatten {len(symbols)} symbol(s) with IOC market orders? Type 'yes': ")
        if confirm.strip().lower() != "yes":
            log.info("Aborted")
            return

    client = ExchangeClient(api_keys)
    await client.start(symbols)
    try:
        results = await asyncio.gather(*[flatten_symbol(client, s) for s in symbols])
    finally:
        await client.close()

    log.info("=" * 70)
    log.info("FLATTEN RESULTS")
    any_errors = False
    for r in results:
        log.info(
            f"  {r['symbol']}: cancelled={r['aster_cancelled']}  "
            f"aster_closed={r['aster_closed_qty']}  hl_closed={r['hl_closed_qty']}"
        )
        for e in r["errors"]:
            any_errors = True
            log.error(f"    {e}")
    log.info("=" * 70)

    # Mark all flattened DB rows as error/flattened so the live monitor won't try to resume them
    init_db()
    conn = get_connection()
    for s in symbols:
        conn.execute(
            "UPDATE positions SET status='error', exit_reason='flattened' "
            "WHERE symbol=? AND status NOT IN ('closed', 'error') AND paper=0",
            (s,),
        )
    conn.commit()
    conn.close()

    if any_errors:
        log.error("Flatten completed with errors — VERIFY positions on both venues manually!")
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser(description="Emergency flatten — close all open positions")
    ap.add_argument("--symbols", nargs="*", help="Specific symbols (default: read from DB)")
    ap.add_argument("--reconcile", action="store_true",
                    help="Query both venues for live positions, ignore DB")
    ap.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = ap.parse_args()
    asyncio.run(main_async(args.symbols, args.reconcile, args.yes))


if __name__ == "__main__":
    main()

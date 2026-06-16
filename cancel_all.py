#!/usr/bin/env python3
"""
Kill switch for RESTING ORDERS only — cancels every open order on both
venues. Does NOT touch positions (use flatten.py to close positions).

Why this exists separately from flatten.py: flatten cancels Aster orders
and closes positions, but never cancels resting HL orders. A maker-first
entry/exit can leave HL Alo orders resting; this cancels them.

Queries the live venues directly (DB-independent), so it works even when
the bot's state is out of sync.

Usage:
    python cancel_all.py            # cancel all resting orders, both venues
    python cancel_all.py --yes      # skip confirmation
    python cancel_all.py --venue hl # only one venue (hl | aster)
"""

import argparse
import asyncio
import logging
import os
import sys

import aiohttp
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from auth import load_api_keys, sign_aster_request
from config import HYPERLIQUID_API, ASTER_OPEN_ORDERS_URL
from exchange_client import ExchangeClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("cancel_all")

TIMEOUT = aiohttp.ClientTimeout(total=15)


async def fetch_hl_open_orders(session, api_keys) -> list[dict]:
    """All resting HL XYZ orders for the funded master account."""
    try:
        async with session.post(
            HYPERLIQUID_API,
            json={"type": "openOrders", "user": api_keys["hl_account_address"], "dex": "xyz"},
            timeout=TIMEOUT,
        ) as r:
            data = await r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        log.error(f"HL open-orders query failed: {e}")
        return []


async def fetch_aster_open_orders(session, api_keys) -> list[dict]:
    """All resting Aster orders (no symbol filter)."""
    params = sign_aster_request(
        {},
        private_key=api_keys["aster_api_secret"],
        user_address=api_keys["aster_wallet_address"],
        signer_address=api_keys["aster_signer_address"],
    )
    try:
        async with session.get(ASTER_OPEN_ORDERS_URL, params=params, timeout=TIMEOUT) as r:
            data = await r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        log.error(f"Aster open-orders query failed: {e}")
        return []


async def main_async(venue: str, auto_yes: bool):
    api_keys = load_api_keys()
    do_hl = venue in ("both", "hl")
    do_aster = venue in ("both", "aster")

    # ── Phase 1: discover resting orders (plain session, no specs needed) ──
    async with aiohttp.ClientSession() as session:
        hl_orders = await fetch_hl_open_orders(session, api_keys) if do_hl else []
        ast_orders = await fetch_aster_open_orders(session, api_keys) if do_aster else []

    total = len(hl_orders) + len(ast_orders)
    if total == 0:
        log.info("No resting orders on either venue. Nothing to cancel.")
        return

    log.warning(f"Found {len(hl_orders)} HL + {len(ast_orders)} Aster resting orders.")
    for o in hl_orders:
        log.info(f"  HL    {o.get('coin')}  oid={o.get('oid')}  "
                 f"{o.get('side')} sz={o.get('sz')} @ {o.get('limitPx')}")
    for o in ast_orders:
        log.info(f"  ASTER {o.get('symbol')}  id={o.get('orderId')}  "
                 f"{o.get('side')} {o.get('origQty')} @ {o.get('price')}")

    if not auto_yes:
        confirm = input(f"Cancel all {total} resting order(s)? Type 'yes': ")
        if confirm.strip().lower() != "yes":
            log.info("Aborted")
            return

    # Canonical symbols for every order, so client.start() loads their HL indices.
    hl_syms = {(c[4:] if (c := o.get("coin", "")).startswith("xyz:") else c) for o in hl_orders}
    ast_syms = {(s[:-4] if (s := o.get("symbol", "")).endswith("USDT") else s) for o in ast_orders}
    symbols = sorted(hl_syms | ast_syms)

    # ── Phase 2: start client (loads specs/indices) and cancel each ──
    client = ExchangeClient(api_keys)
    await client.start(symbols)
    try:
        hl_ok = 0
        for o in hl_orders:
            coin = o.get("coin", "")
            sym = coin[4:] if coin.startswith("xyz:") else coin
            if await client.cancel_hl_order(sym, str(o.get("oid"))):
                hl_ok += 1

        ast_ok = 0
        for o in ast_orders:
            sym = o.get("symbol", "")
            canon = sym[:-4] if sym.endswith("USDT") else sym
            if await client.cancel_aster_order(canon, str(o.get("orderId"))):
                ast_ok += 1
    finally:
        await client.close()

    log.info("=" * 60)
    log.info(f"Cancelled HL {hl_ok}/{len(hl_orders)}  |  Aster {ast_ok}/{len(ast_orders)}")
    log.info("=" * 60)

    if hl_ok < len(hl_orders) or ast_ok < len(ast_orders):
        log.error("Some cancels failed — VERIFY on both venues manually!")
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser(description="Cancel all resting orders on both venues")
    ap.add_argument("--venue", choices=["both", "hl", "aster"], default="both")
    ap.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = ap.parse_args()
    asyncio.run(main_async(args.venue, args.yes))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Top-of-book snapshot for one equity name on both venues.

Prints the top 5 levels of the Hyperliquid (USDC) and Aster (USDT) order
books side by side, plus the mid prices, each venue's own bid/ask spread, and
the cross-venue basis. Used by the /book Telegram command.

Run on the server (needs the venv's aiohttp + live API egress):
    .venv/bin/python scripts/book_snapshot.py NBIS
    .venv/bin/python scripts/book_snapshot.py NBIS --levels 5
"""

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

HL_URL = "https://api.hyperliquid.xyz/info"
ASTER_URL = "https://fapi.asterdex.com/fapi/v1/depth"

# canonical (HL) base -> Aster's tradeable base, when they differ.
_CANON_TO_ASTER = {"SMSN": "SAMSUNG", "SKHX": "SKHYNIX"}


async def hl_depth(session, coin: str, levels: int):
    async with session.post(HL_URL, json={"type": "l2Book", "coin": coin}) as r:
        book = await r.json()
    lv = book.get("levels") or [[], []]
    bids = [(Decimal(x["px"]), Decimal(x["sz"])) for x in lv[0][:levels]]
    asks = [(Decimal(x["px"]), Decimal(x["sz"])) for x in lv[1][:levels]]
    return bids, asks


async def aster_depth(session, aster_sym: str, levels: int):
    async with session.get(
        ASTER_URL, params={"symbol": aster_sym, "limit": max(levels, 5)}
    ) as r:
        book = await r.json()
    bids = [(Decimal(p), Decimal(q)) for p, q in book.get("bids", [])[:levels]]
    asks = [(Decimal(p), Decimal(q)) for p, q in book.get("asks", [])[:levels]]
    return bids, asks


async def usdc_usdt_rate(session) -> Decimal:
    try:
        async with session.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "USDCUSDT"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            return Decimal((await r.json())["price"])
    except Exception:
        return Decimal("1")


def _spread_bps(bid: Decimal, ask: Decimal) -> Decimal:
    if bid <= 0 or ask <= 0:
        return Decimal(0)
    return (ask - bid) / ((ask + bid) / 2) * 10000


async def run(symbol: str, levels: int) -> str:
    canon = symbol.upper()
    hl_coin = f"xyz:{canon}"
    aster_sym = _CANON_TO_ASTER.get(canon, canon) + "USDT"

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            (hl_b, hl_a), (ast_b, ast_a), usdc = await asyncio.gather(
                hl_depth(session, hl_coin, levels),
                aster_depth(session, aster_sym, levels),
                usdc_usdt_rate(session),
            )
        except Exception as e:
            return f"📕 {canon}: book fetch failed — {e}"

    if not hl_b or not hl_a:
        return f"📕 {canon}: no Hyperliquid book ({hl_coin})."
    if not ast_b or not ast_a:
        return f"📕 {canon}: no Aster book ({aster_sym})."

    hl_bid, hl_ask = hl_b[0][0], hl_a[0][0]
    ast_bid, ast_ask = ast_b[0][0], ast_a[0][0]
    hl_mid = (hl_bid + hl_ask) / 2
    ast_mid = (ast_bid + ast_ask) / 2
    hl_mid_usdt = hl_mid * usdc
    ref = (hl_mid_usdt + ast_mid) / 2

    n = levels

    def venue_block(title, asks, bids):
        # Stacked (not side-by-side) so nothing wraps on a phone. asks high→low
        # (best ask just above the mid line), bids high→low (best bid just below).
        out = [title, "  asks ↑"]
        for px, sz in reversed(asks[:n]):
            out.append(f"   {float(px):>9.2f}  × {float(sz):.3f}")
        out.append("  ── mid ──")
        for px, sz in bids[:n]:
            out.append(f"   {float(px):>9.2f}  × {float(sz):.3f}")
        out.append("  bids ↓")
        return out

    lines = [f"📕 {canon} order book (top {n})", ""]
    lines += venue_block("HYPERLIQUID (USDC)", hl_a, hl_b)
    lines.append("")
    lines += venue_block("ASTER (USDT)", ast_a, ast_b)
    lines.append("")

    # Two basis views: HL-maker/Aster-taker (passive, what you'd rest at) and
    # taker-taker (aggressive, both legs cross immediately).
    hl_ask_usdt = hl_ask * usdc
    hl_bid_usdt = hl_bid * usdc
    # HL-maker / Aster-taker (bid-bid for buy-HL, ask-ask for buy-AST)
    mk_buy_hl = (ast_bid - hl_bid_usdt) / ref * 10000
    mk_buy_ast = (hl_ask_usdt - ast_ask) / ref * 10000
    # Taker-taker (both cross)
    tk_buy_hl = (ast_bid - hl_ask_usdt) / ref * 10000
    tk_buy_ast = (hl_bid_usdt - ast_ask) / ref * 10000

    lines += [
        f"mid    HL {float(hl_mid):.2f}   Ast {float(ast_mid):.2f}",
        f"spread HL {float(_spread_bps(hl_bid, hl_ask)):.0f}bp  Ast {float(_spread_bps(ast_bid, ast_ask)):.0f}bp",
        f"USDC/USDT {float(usdc):.4f}",
        "basis (L-HL / L-AST):",
        f"  maker-taker  {float(mk_buy_hl):+.0f} / {float(mk_buy_ast):+.0f} bp",
        f"  taker-taker  {float(tk_buy_hl):+.0f} / {float(tk_buy_ast):+.0f} bp",
    ]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description="Order-book snapshot for one name")
    ap.add_argument("symbol")
    ap.add_argument("--levels", type=int, default=5)
    args = ap.parse_args()
    print(asyncio.run(run(args.symbol, max(1, min(args.levels, 10)))))


if __name__ == "__main__":
    main()

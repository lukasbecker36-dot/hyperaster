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

from config import aster_symbol_for, px_decimals

HL_URL = "https://api.hyperliquid.xyz/info"
ASTER_URL = "https://fapi.asterdex.com/fapi/v1/depth"


async def hl_depth(session, canon: str, levels: int):
    """HL top levels, dex-agnostic: try the xyz equity coin, then the bare
    main-dex crypto coin. Null-safe — HL returns a null body for a coin that
    doesn't exist on the queried dex (e.g. xyz:HMSTR), which used to crash on
    .get. Returns ([], []) if neither dex has a populated book."""
    for coin in (f"xyz:{canon}", canon):
        try:
            async with session.post(
                HL_URL, json={"type": "l2Book", "coin": coin}
            ) as r:
                book = await r.json(content_type=None)
        except Exception:
            book = None
        if not isinstance(book, dict):
            continue
        lv = book.get("levels") or [[], []]
        b0 = lv[0] if len(lv) > 0 else []
        a0 = lv[1] if len(lv) > 1 else []
        bids = [(Decimal(x["px"]), Decimal(x["sz"])) for x in b0[:levels]]
        asks = [(Decimal(x["px"]), Decimal(x["sz"])) for x in a0[:levels]]
        if bids or asks:
            return bids, asks
    return [], []


async def aster_depth(session, aster_sym: str, levels: int):
    async with session.get(
        ASTER_URL, params={"symbol": aster_sym, "limit": max(levels, 5)}
    ) as r:
        book = await r.json(content_type=None)
    if not isinstance(book, dict):
        return [], []
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


def _fmt_px(px) -> str:
    """Adaptive price precision so sub-dollar crypto (HMSTR ~$0.001) doesn't
    render as 0.00, while big equities stay readable. Column-padded for the
    table; the precision ladder itself lives in config.px_decimals so this and
    the log/alert formatter (config.fmt_px) can't drift apart."""
    p = float(px)
    return f"{p:>12.{px_decimals(p)}f}"


async def run(symbol: str, levels: int) -> str:
    canon = symbol.upper()
    aster_sym = aster_symbol_for(canon)

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            (hl_b, hl_a), (ast_b, ast_a), usdc = await asyncio.gather(
                hl_depth(session, canon, levels),
                aster_depth(session, aster_sym, levels),
                usdc_usdt_rate(session),
            )
        except Exception as e:
            return f"📕 {canon}: book fetch failed — {e}"

    if not hl_b or not hl_a:
        return f"📕 {canon}: no Hyperliquid book (xyz:{canon} or {canon})."
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
            out.append(f"  {_fmt_px(px)}  × {float(sz):.3f}")
        out.append("  ── mid ──")
        for px, sz in bids[:n]:
            out.append(f"  {_fmt_px(px)}  × {float(sz):.3f}")
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
        f"mid    HL {_fmt_px(hl_mid).strip()}   Ast {_fmt_px(ast_mid).strip()}",
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

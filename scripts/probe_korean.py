#!/usr/bin/env python3
"""Probe both venues for Samsung / SK Hynix (and other Korean) equity perps.

Run this ON THE SERVER (which has network access to the exchanges):

    cd /opt/hyperaster && .venv/bin/python scripts/probe_korean.py

It prints the exact ticker strings each venue uses and tells you whether the
bot's name-equality auto-discovery would match them. The whole "can we trade
Samsung/SK Hynix" question reduces to: do the base names match across venues?
"""
import json
import urllib.request

HL_API = "https://api.hyperliquid.xyz/info"
ASTER_INFO = "https://fapi.asterdex.com/fapi/v1/exchangeInfo"
NEEDLES = ("SAMSUNG", "HYNIX", "SKH", "HYUNDAI", "KOSPI", "KR", "005930", "000660")


def _post(url: str, body: dict) -> dict:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.load(r)


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=25) as r:
        return json.load(r)


def hl_xyz_names() -> list[str]:
    meta = _post(HL_API, {"type": "metaAndAssetCtxs", "dex": "xyz"})
    return [a.get("name", "").split(":")[-1] for a in meta[0].get("universe", [])]


def aster_bases() -> dict[str, str]:
    info = _get(ASTER_INFO)
    out = {}
    for s in info.get("symbols", []):
        raw = s.get("symbol", "")
        for suf in ("USDT", "USDC", "USD"):
            if raw.endswith(suf):
                out[raw[: -len(suf)]] = raw
                break
    return out


def _hits(names) -> list[str]:
    return sorted(n for n in names if any(k in n.upper() for k in NEEDLES))


def hl_mids() -> dict[str, float]:
    """xyz coin base -> mark price, from metaAndAssetCtxs."""
    meta = _post(HL_API, {"type": "metaAndAssetCtxs", "dex": "xyz"})
    universe, ctxs = meta[0].get("universe", []), meta[1]
    out = {}
    for i, a in enumerate(universe):
        base = a.get("name", "").split(":")[-1]
        if i < len(ctxs):
            px = ctxs[i].get("markPx") or ctxs[i].get("midPx") or ctxs[i].get("oraclePx")
            if px:
                out[base] = float(px)
    return out


def aster_book(symbol: str) -> dict:
    """Best bid/ask + 24h quote volume for an Aster symbol, or {} if missing."""
    try:
        bt = _get(f"https://fapi.asterdex.com/fapi/v1/ticker/bookTicker?symbol={symbol}")
        t24 = _get(f"https://fapi.asterdex.com/fapi/v1/ticker/24hr?symbol={symbol}")
        bid, ask = float(bt["bidPrice"]), float(bt["askPrice"])
        mid = (bid + ask) / 2 if bid and ask else 0.0
        spread_bps = (ask - bid) / mid * 1e4 if mid else 0.0
        return {"bid": bid, "ask": ask, "mid": mid, "spread_bps": spread_bps,
                "quote_vol_24h": float(t24.get("quoteVolume", 0))}
    except Exception as e:
        return {"error": str(e)}


def compare() -> None:
    """For Samsung/SK Hynix, line up HL mid vs each candidate Aster book so we
    can see which Aster contract matches HL on price and which one is liquid."""
    hl = hl_mids()
    groups = {
        "SAMSUNG  (HL xyz:SMSN)": ("SMSN", ["SMSNUSDT", "SAMSUNGUSDT"]),
        "SK HYNIX (HL xyz:SKHX)": ("SKHX", ["SKHXUSDT", "SKHYNIXUSDT"]),
    }
    print("\n" + "=" * 70)
    print("PRICE / LIQUIDITY COMPARISON  (pick the Aster book matching HL mid)")
    print("=" * 70)
    for label, (hl_base, aster_syms) in groups.items():
        hl_px = hl.get(hl_base)
        print(f"\n{label}")
        print(f"  HL mark px: {hl_px if hl_px else 'MISSING'}")
        for sym in aster_syms:
            b = aster_book(sym)
            if "error" in b:
                print(f"  Aster {sym:14s}: ERROR {b['error']}")
                continue
            div = abs(b["mid"] - hl_px) / hl_px * 100 if hl_px and b["mid"] else float("nan")
            print(f"  Aster {sym:14s}: mid={b['mid']:.4f}  spr={b['spread_bps']:.0f}bps  "
                  f"24hVol=${b['quote_vol_24h']:,.0f}  vs HL: {div:.1f}% off")


def main() -> None:
    hl = hl_xyz_names()
    aster = aster_bases()

    print(f"HL xyz universe: {len(hl)} assets | Aster: {len(aster)} symbols\n")
    print("HL korean-ish names:   ", _hits(hl))
    print("Aster korean-ish bases:", _hits(aster.keys()))

    overlap = sorted(set(hl) & set(aster.keys()))
    print(f"\nFull cross-venue name overlap ({len(overlap)}): {overlap}")

    for target in ("SAMSUNG", "SKHYNIX"):
        in_hl = target in hl
        in_aster = target in aster
        verdict = "AUTO-MATCHES ✅" if (in_hl and in_aster) else "needs alias ❌"
        print(
            f"\n{target}: HL={'yes' if in_hl else 'NO'}  "
            f"Aster={aster.get(target, 'NO')}  -> {verdict}"
        )
        if not in_hl:
            near = [n for n in hl if "SAMS" in n.upper() or "HYN" in n.upper() or "SKH" in n.upper()]
            if near:
                print(f"  (HL may list it as: {near})")

    # The real question isn't SAMSUNG/SKHYNIX (Aster's native names) but whether
    # HL's SMSN/SKHX line up with an Aster book on price + have liquidity.
    compare()


if __name__ == "__main__":
    main()

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


if __name__ == "__main__":
    main()

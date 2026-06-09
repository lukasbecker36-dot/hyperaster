#!/usr/bin/env python3
"""
Preflight connectivity check — run this FIRST on any new server.

Verifies the box can reach both exchanges (some datacenter/region IPs are
geo-blocked → 403) and measures round-trip latency. Uses only the standard
library, so it runs before you install requirements.

Usage:
    python3 check_connectivity.py
"""

import json
import time
import urllib.error
import urllib.request

CHECKS = [
    ("Hyperliquid info", "POST", "https://api.hyperliquid.xyz/info", {"type": "meta"}),
    ("Aster exchangeInfo", "GET", "https://fapi.asterdex.com/fapi/v1/exchangeInfo", None),
    ("Binance ping (USDC/USDT basis)", "GET", "https://api.binance.com/api/v3/ping", None),
]


def probe(method: str, url: str, body: dict | None) -> tuple[int, float, str]:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            ms = (time.monotonic() - t0) * 1000
            resp.read(200)
            return resp.status, ms, ""
    except urllib.error.HTTPError as e:
        ms = (time.monotonic() - t0) * 1000
        return e.code, ms, e.reason
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000
        return 0, ms, str(e)


def main() -> None:
    print("Connectivity preflight — both exchanges must return 200.\n")
    all_ok = True
    for name, method, url, body in CHECKS:
        status, ms, err = probe(method, url, body)
        ok = status == 200
        all_ok = all_ok and (ok or "Binance" in name)  # Binance is optional/basis-only
        mark = "OK " if ok else "FAIL"
        detail = f"{status}" + (f" {err}" if err else "")
        print(f"  [{mark}] {name:<34} {detail:<22} {ms:6.0f} ms")

    print()
    if all_ok:
        print("PASS — exchanges reachable. Safe to proceed with setup.")
    else:
        print("FAIL — an exchange is unreachable.")
        print("  • 403 = this server's IP/region is geo-blocked. Try a different")
        print("    Hetzner region (US locations often work for these venues), or")
        print("    route through an allowed region.")
        print("  • timeout/0 = firewall or no outbound network on this box.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""Names the session-flip strategy is trading LIVE on Hyperliquid.

HL nets every position on a coin into one per account, so an arb leg opened on a
name session_flip.py is flipping would merge with it and corrupt both books. The
arb's auto-entry asks here and skips those names. Cached for 30s; stdlib only.
"""
import json
import time
from pathlib import Path

_FILE = Path(__file__).resolve().parent / "data" / "sessionflip.json"
_cache: tuple[float, frozenset] = (0.0, frozenset())


def reserved_symbols() -> frozenset:
    global _cache
    if time.time() - _cache[0] < 30:
        return _cache[1]
    names: frozenset = frozenset()
    try:
        cfg = json.loads(_FILE.read_text())
        if cfg.get("enabled") and cfg.get("mode") == "live":
            names = frozenset(n.upper() for n in cfg.get("names", []))
    except Exception:
        pass
    _cache = (time.time(), names)
    return names

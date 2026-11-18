"""Names the session-flip and stock-carry strategies are trading LIVE on Hyperliquid.

HL nets every position on a coin into one per account, so an arb leg opened on a
name session_flip.py is flipping (or stock_carry.py holds) would merge with it and
corrupt both books. The arb's auto-entry asks here and skips those names. Cached
for 30s; stdlib only.
"""
import json
import time
from pathlib import Path

_DATA = Path(__file__).resolve().parent / "data"
_FILE = _DATA / "sessionflip.json"
_SC_STATE = _DATA / "stockcarry_state.json"
_cache: tuple[float, frozenset] = (0.0, frozenset())


def reserved_symbols() -> frozenset:
    global _cache
    if time.time() - _cache[0] < 30:
        return _cache[1]
    names: set = set()
    try:
        cfg = json.loads(_FILE.read_text())
        if cfg.get("enabled") and cfg.get("mode") == "live":
            names |= {n.upper() for n in cfg.get("names", [])}
    except Exception:
        pass
    try:   # stock carry: whatever it holds live right now (its universe changes daily)
        st = json.loads(_SC_STATE.read_text())
        names |= {n.upper() for n, p in st.get("pos", {}).items() if p.get("qty") and p.get("mode") == "live"}
    except Exception:
        pass
    _cache = (time.time(), frozenset(names))
    return _cache[1]

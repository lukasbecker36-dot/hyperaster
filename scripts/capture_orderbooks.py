#!/usr/bin/env python3
"""
High-frequency order-book + oracle capture daemon.

Records, for every overlapping equity name (HL XYZ ∩ Aster), a time series of
both venues' top-of-book (bid/ask/size), mids, oracle/index prices and funding
rates — one aligned snapshot per cycle (default every 3s). Runs unattended for a
day or two to build a dataset we can analyse offline (scripts/analyze_capture.py)
to work out, from real data, which names actually mean-revert and at what
threshold/horizon a maker-HL/taker-Aster round trip is net-positive after the
real bid-ask crossing cost.

Why this and not the live signal caches: the live bot samples ~1/min and only
the trading universe. This captures the FULL overlap (including currently
BLOCKED names, so we can re-evaluate them) at high frequency, straight from the
venues, independent of the trader.

Request budget per cycle (keeps us well under rate limits regardless of universe
size — only the HL books scale with N):
  - 1× HL  metaAndAssetCtxs (dex=xyz)  → oracle/mark/mid/funding for ALL names
  - 1× Aster ticker/bookTicker (all)   → true top-of-book for ALL names
  - 1× Aster premiumIndex (all)        → mark/index/funding for ALL names
  - N× HL  l2Book (per coin)           → true HL top-of-book (+ depth if --depth)

Storage: SQLite (WAL) at DATA_DIR/orderbook_capture.db, ~80 bytes/row →
~90MB/name-day is a wild overestimate; realistically the whole universe is a few
hundred MB for two days. Use --depth to also store the top-5 levels JSON (larger).

Usage:
    python scripts/capture_orderbooks.py                 # 3s cadence, auto-stops after 24h
    python scripts/capture_orderbooks.py --max-hours 48  # run for two days
    python scripts/capture_orderbooks.py --max-hours 0   # run until killed
    python scripts/capture_orderbooks.py --interval 2    # faster
    python scripts/capture_orderbooks.py --depth         # also store 5-level depth
    python scripts/capture_orderbooks.py --once          # single cycle (smoke test)

Deploy as a service (on the server) so it survives disconnects, e.g.:
    nohup python scripts/capture_orderbooks.py >/opt/hyperaster/data/capture.log 2>&1 &
  or a systemd unit mirroring the trader's.
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    HYPERLIQUID_API, ASTER_BASE, ASTER_EXCHANGE_INFO_URL, DATA_DIR, OUTPUT_DIR,
    NON_EQUITY_SYMBOLS, ASTER_BASE_TO_CANON, aster_symbol_for,
)

ASTER_BOOKTICKER_URL = f"{ASTER_BASE}/fapi/v1/ticker/bookTicker"
ASTER_PREMIUM_URL = f"{ASTER_BASE}/fapi/v1/premiumIndex"
ASTER_DEPTH_URL = f"{ASTER_BASE}/fapi/v1/depth"

log = logging.getLogger("capture")


def _load_universe_csv() -> list[str]:
    """Fallback universe: the overlap_symbols.csv the live bot maintains.
    Keeps BLOCKED names (we re-evaluate them); drops only NON_EQUITY."""
    import csv
    path = Path(OUTPUT_DIR) / "overlap_symbols.csv"
    if not path.exists():
        return []
    try:
        with open(path) as fh:
            return sorted({r["coin"] for r in csv.DictReader(fh)
                           if r.get("coin") and r["coin"] not in NON_EQUITY_SYMBOLS})
    except Exception as e:
        log.warning(f"overlap_symbols.csv read failed ({e})")
        return []

COLUMNS = [
    "ts", "symbol",
    "hl_bid", "hl_ask", "hl_bid_sz", "hl_ask_sz",
    "hl_mid", "hl_oracle", "hl_mark", "hl_funding",
    "aster_bid", "aster_ask", "aster_bid_sz", "aster_ask_sz",
    "aster_index", "aster_mark", "aster_funding",
    "hl_levels", "aster_levels",
]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS book_snaps (
            ts            INTEGER NOT NULL,
            symbol        TEXT    NOT NULL,
            hl_bid        REAL, hl_ask       REAL, hl_bid_sz    REAL, hl_ask_sz    REAL,
            hl_mid        REAL, hl_oracle    REAL, hl_mark      REAL, hl_funding   REAL,
            aster_bid     REAL, aster_ask    REAL, aster_bid_sz REAL, aster_ask_sz REAL,
            aster_index   REAL, aster_mark   REAL, aster_funding REAL,
            hl_levels     TEXT, aster_levels TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_sym_ts ON book_snaps(symbol, ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_ts ON book_snaps(ts)")
    # Covering index for the /opps + /basis screeners: they scan the last 24h of
    # the narrow top-of-book + funding columns. Without this the ts index still
    # forces a per-row lookup into the main table, whose rows carry fat
    # hl_levels/aster_levels JSON blobs — ~1M random page fetches that thrash the
    # cache into disk seeks on a memory-tight box (the /opps hang). With every
    # queried column in the index, that scan is index-only: no blob pages touched.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snap_cover ON book_snaps("
        "ts, symbol, hl_bid, hl_ask, aster_bid, aster_ask, hl_funding, aster_funding)")
    conn.commit()
    return conn


class Capturer:
    def __init__(self, session, interval, depth, aster_depth, max_hours=0.0,
                 retention_hours=72.0):
        self.session = session
        self.interval = interval
        self.depth = depth              # store HL top-5 levels JSON
        self.aster_depth = aster_depth  # also fetch + store Aster 5-level depth
        self.max_hours = max_hours      # auto-stop after this long (0 = run forever)
        self.retention_hours = retention_hours  # drop rows older than this (0 = keep all)
        self.timeout = aiohttp.ClientTimeout(total=8)
        self.symbols: list[str] = []    # canonical bases
        self._sem = asyncio.Semaphore(10)   # cap concurrent HL l2Book calls
        self.rows_written = 0
        self.cycles = 0

    # ── Universe discovery ──

    async def discover(self) -> list[str]:
        """HL XYZ ∩ Aster, equity only (NON_EQUITY excluded). BLOCKED names are
        intentionally KEPT — the point is to re-evaluate them from data.

        Live-queries both venues (retried — HL occasionally returns a null body
        on a single request); if that yields nothing, falls back to the
        overlap_symbols.csv the live bot maintains so a transient blip never
        aborts a capture run."""
        hl_bases = await self._hl_universe()
        aster_bases = await self._aster_universe()
        overlap = sorted((hl_bases & aster_bases) - set(NON_EQUITY_SYMBOLS))
        if overlap:
            return overlap
        # Fallback: the persisted universe from the live bot.
        csv_syms = _load_universe_csv()
        if csv_syms:
            log.warning(f"discover: live query empty (HL={len(hl_bases)} "
                        f"Aster={len(aster_bases)}) — using overlap_symbols.csv "
                        f"({len(csv_syms)} names)")
            return csv_syms
        return self.symbols

    async def _hl_universe(self) -> set[str]:
        for attempt in range(3):
            try:
                async with self.session.post(
                    HYPERLIQUID_API, json={"type": "metaAndAssetCtxs", "dex": "xyz"},
                    timeout=self.timeout,
                ) as r:
                    data = await r.json(content_type=None)
                # HL returns [meta, ctxs]; a transient failure can be null/dict.
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    out = {a.get("name", "").split(":")[-1]
                           for a in data[0].get("universe", []) if a.get("name")}
                    if out:
                        return out
                log.warning(f"discover: HL universe unexpected shape "
                            f"({type(data).__name__}) attempt {attempt+1}/3")
            except Exception as e:
                log.warning(f"discover: HL universe fetch failed ({e}) "
                            f"attempt {attempt+1}/3")
            await asyncio.sleep(1.5)
        return set()

    async def _aster_universe(self) -> set[str]:
        for attempt in range(3):
            try:
                async with self.session.get(ASTER_EXCHANGE_INFO_URL, timeout=self.timeout) as r:
                    info = await r.json(content_type=None)
                out = set()
                for si in (info or {}).get("symbols", []):
                    raw = si.get("symbol", "")
                    for suffix in ("USDT", "USDC", "USD"):
                        if raw.endswith(suffix):
                            base = raw[: -len(suffix)]
                            out.add(ASTER_BASE_TO_CANON.get(base, base))
                            break
                if out:
                    return out
                log.warning(f"discover: Aster universe empty attempt {attempt+1}/3")
            except Exception as e:
                log.warning(f"discover: Aster exchangeInfo fetch failed ({e}) "
                            f"attempt {attempt+1}/3")
            await asyncio.sleep(1.5)
        return set()

    # ── Batch fetchers (one call each, all names) ──

    async def _hl_ctxs(self) -> dict:
        """canonical base -> {mid, mark, oracle, funding}. One call."""
        out = {}
        try:
            async with self.session.post(
                HYPERLIQUID_API, json={"type": "metaAndAssetCtxs", "dex": "xyz"},
                timeout=self.timeout,
            ) as r:
                data = await r.json(content_type=None)
            meta, ctxs = data[0], data[1]
            for i, asset in enumerate(meta.get("universe", [])):
                base = asset.get("name", "").split(":")[-1]
                if i < len(ctxs) and base:
                    c = ctxs[i]
                    out[base] = {
                        "mid": _f(c.get("midPx")),
                        "mark": _f(c.get("markPx")),
                        "oracle": _f(c.get("oraclePx")),
                        "funding": _f(c.get("funding")),
                    }
        except Exception as e:
            log.warning(f"HL ctxs fetch failed ({e})")
        return out

    async def _aster_book_tickers(self) -> dict:
        """canonical base -> {bid, ask, bid_sz, ask_sz}. One call (all symbols)."""
        out = {}
        try:
            async with self.session.get(ASTER_BOOKTICKER_URL, timeout=self.timeout) as r:
                data = await r.json(content_type=None)
            rows = data if isinstance(data, list) else [data]
            for row in rows:
                base = _aster_to_canon(row.get("symbol", ""))
                if base:
                    out[base] = {
                        "bid": _f(row.get("bidPrice")), "ask": _f(row.get("askPrice")),
                        "bid_sz": _f(row.get("bidQty")), "ask_sz": _f(row.get("askQty")),
                    }
        except Exception as e:
            log.warning(f"Aster bookTicker fetch failed ({e})")
        return out

    async def _aster_premium(self) -> dict:
        """canonical base -> {mark, index, funding}. One call (all symbols)."""
        out = {}
        try:
            async with self.session.get(ASTER_PREMIUM_URL, timeout=self.timeout) as r:
                data = await r.json(content_type=None)
            rows = data if isinstance(data, list) else [data]
            for row in rows:
                base = _aster_to_canon(row.get("symbol", ""))
                if base:
                    out[base] = {
                        "mark": _f(row.get("markPrice")), "index": _f(row.get("indexPrice")),
                        "funding": _f(row.get("lastFundingRate")),
                    }
        except Exception as e:
            log.warning(f"Aster premiumIndex fetch failed ({e})")
        return out

    # ── Per-coin HL book (true top-of-book + optional depth) ──

    async def _hl_book(self, base: str) -> dict | None:
        hl_coin = f"xyz:{base}"
        async with self._sem:
            try:
                async with self.session.post(
                    HYPERLIQUID_API, json={"type": "l2Book", "coin": hl_coin},
                    timeout=self.timeout,
                ) as r:
                    data = await r.json(content_type=None)
            except Exception:
                return None
        levels = data.get("levels", [[], []])
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        if not bids or not asks:
            return None
        b0, a0 = _f(bids[0]["px"]), _f(asks[0]["px"])
        if b0 > a0:  # HL sometimes returns sides swapped
            bids, asks = asks, bids
            b0, a0 = a0, b0
        out = {
            "bid": b0, "ask": a0,
            "bid_sz": _f(bids[0]["sz"]), "ask_sz": _f(asks[0]["sz"]),
        }
        if self.depth:
            out["levels"] = json.dumps({
                "b": [[_f(l["px"]), _f(l["sz"])] for l in bids[:5]],
                "a": [[_f(l["px"]), _f(l["sz"])] for l in asks[:5]],
            })
        return out

    async def _aster_depth(self, base: str) -> str | None:
        if not self.aster_depth:
            return None
        try:
            async with self.session.get(
                ASTER_DEPTH_URL, params={"symbol": aster_symbol_for(base), "limit": 5},
                timeout=self.timeout,
            ) as r:
                d = await r.json(content_type=None)
            return json.dumps({
                "b": [[_f(x[0]), _f(x[1])] for x in d.get("bids", [])[:5]],
                "a": [[_f(x[0]), _f(x[1])] for x in d.get("asks", [])[:5]],
            })
        except Exception:
            return None

    def _prune(self, conn):
        """Drop rows older than retention_hours so the DB size plateaus instead
        of growing unbounded across restarts. Deleted pages are reused by later
        inserts, so no VACUUM is needed for the file to stay flat at steady
        state. Deletes in bounded batches (via the ts index) to avoid a long
        write lock stalling the capture loop."""
        if self.retention_hours <= 0:
            return
        cutoff = _now_ms() - int(self.retention_hours * 3600 * 1000)
        try:
            removed = 0
            while True:
                cur = conn.execute(
                    "DELETE FROM book_snaps WHERE rowid IN "
                    "(SELECT rowid FROM book_snaps WHERE ts < ? LIMIT 5000)",
                    (cutoff,))
                conn.commit()
                if cur.rowcount <= 0:
                    break
                removed += cur.rowcount
            if removed:
                log.info(f"prune: dropped {removed} rows older than "
                         f"{self.retention_hours}h")
        except Exception as e:
            log.warning(f"prune failed ({e})")

    # ── One capture cycle ──

    async def cycle(self, conn) -> int:
        ts = _now_ms()
        hl_ctxs, aster_bt, aster_pm = await asyncio.gather(
            self._hl_ctxs(), self._aster_book_tickers(), self._aster_premium(),
        )
        hl_books = await asyncio.gather(*[self._hl_book(s) for s in self.symbols])
        aster_depths = await asyncio.gather(*[self._aster_depth(s) for s in self.symbols])

        rows = []
        for s, hb, adep in zip(self.symbols, hl_books, aster_depths):
            ctx = hl_ctxs.get(s, {})
            bt = aster_bt.get(s, {})
            pm = aster_pm.get(s, {})
            hb = hb or {}
            # Skip a totally empty row (no data from any source this cycle).
            if not ctx and not bt and not hb:
                continue
            rows.append((
                ts, s,
                hb.get("bid"), hb.get("ask"), hb.get("bid_sz"), hb.get("ask_sz"),
                ctx.get("mid"), ctx.get("oracle"), ctx.get("mark"), ctx.get("funding"),
                bt.get("bid"), bt.get("ask"), bt.get("bid_sz"), bt.get("ask_sz"),
                pm.get("index"), pm.get("mark"), pm.get("funding"),
                hb.get("levels"), adep,
            ))
        if rows:
            placeholders = ",".join("?" * len(COLUMNS))
            conn.executemany(
                f"INSERT INTO book_snaps ({','.join(COLUMNS)}) VALUES ({placeholders})", rows
            )
            conn.commit()
        return len(rows)

    async def run(self, conn, run_once=False):
        self.symbols = await self.discover()
        if not self.symbols:
            log.error("No overlap universe discovered — aborting")
            return
        deadline = (time.time() + self.max_hours * 3600) if self.max_hours > 0 else None
        log.info(f"Capturing {len(self.symbols)} names @ {self.interval}s"
                 + (f" — auto-stop in {self.max_hours}h" if deadline else "")
                 + f": {', '.join(self.symbols)}")

        stop = {"flag": False}

        def _sig(*_):
            stop["flag"] = True
            log.info("Shutdown signal — finishing current cycle then exiting")
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _sig)
            except ValueError:
                pass  # not main thread

        self._prune(conn)  # clear any backlog from before retention existed
        last_report = time.time()
        last_discover = time.time()
        last_prune = time.time()
        while not stop["flag"]:
            if deadline and time.time() >= deadline:
                log.info(f"Reached {self.max_hours}h capture limit — stopping")
                break
            t0 = time.time()
            try:
                n = await self.cycle(conn)
                self.rows_written += n
                self.cycles += 1
            except Exception as e:
                log.error(f"cycle error: {e}")
            # Re-discover every 30 min to pick up new listings.
            if time.time() - last_discover > 1800:
                last_discover = time.time()
                try:
                    fresh = await self.discover()
                    added = [s for s in fresh if s not in self.symbols]
                    if added:
                        log.info(f"New names added to capture: {', '.join(added)}")
                        self.symbols = fresh
                except Exception as e:
                    log.warning(f"re-discover failed ({e})")
            # Prune aged-out rows every 30 min so the file size stays flat.
            if time.time() - last_prune > 1800:
                last_prune = time.time()
                self._prune(conn)
            # Health + periodic progress.
            now = time.time()
            elapsed = now - t0
            if now - last_report > 60:
                last_report = now
                log.info(f"cycles={self.cycles} rows={self.rows_written} "
                         f"names={len(self.symbols)} last_cycle={elapsed:.2f}s")
                _write_health(conn, self)
            if run_once:
                break
            await asyncio.sleep(max(0.0, self.interval - elapsed))
        _write_health(conn, self)
        log.info(f"Stopped. total cycles={self.cycles} rows={self.rows_written}")


def _f(v):
    try:
        f = float(v)
        return f if f == f else None  # drop NaN
    except (TypeError, ValueError):
        return None


def _aster_to_canon(raw: str) -> str | None:
    for suffix in ("USDT", "USDC", "USD"):
        if raw.endswith(suffix):
            base = raw[: -len(suffix)]
            return ASTER_BASE_TO_CANON.get(base, base)
    return None


def _write_health(conn, cap: Capturer):
    try:
        path = os.path.join(DATA_DIR, "capture_health.json")
        with open(path, "w") as fh:
            json.dump({
                "ts": _now_ms(), "cycles": cap.cycles, "rows": cap.rows_written,
                "names": len(cap.symbols), "interval": cap.interval,
            }, fh)
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=3.0, help="seconds between cycles")
    ap.add_argument("--db", default=os.path.join(DATA_DIR, "orderbook_capture.db"))
    ap.add_argument("--depth", action="store_true", help="also store HL top-5 levels JSON")
    ap.add_argument("--aster-depth", action="store_true", help="also fetch+store Aster 5-level depth")
    ap.add_argument("--once", action="store_true", help="single cycle then exit (smoke test)")
    ap.add_argument("--max-hours", type=float, default=24.0,
                    help="auto-stop after this many hours (0 = run until killed)")
    ap.add_argument("--retention-hours", type=float, default=72.0,
                    help="drop rows older than this so the DB size stays flat "
                         "(0 = keep everything)")
    a = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = _init_db(a.db)

    async def _run():
        async with aiohttp.ClientSession() as session:
            cap = Capturer(session, a.interval, a.depth, a.aster_depth, a.max_hours,
                           a.retention_hours)
            await cap.run(conn, run_once=a.once)

    try:
        asyncio.run(_run())
    finally:
        conn.close()


if __name__ == "__main__":
    main()

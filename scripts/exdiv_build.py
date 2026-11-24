"""Draft the ex-dividend calendar (next ~4 months) for xyz names whose dividends are >= 0.3% of the price.
Announced next ex-dates where Yahoo has one; otherwise the last same-season ex-date projected (+1y for
annual/semiannual payers, +3m for quarterly, +1m for monthly), as a +-4 day window."""
import json, sys
import pandas as pd, yfinance as yf

MAP = {n: n for n in ("AAPL MSFT ORCL GOOGL AMZN MU COST LLY TSM INTC META NFLX AMD HOOD BX XLE EWZ EWY EWJ EWT IBM "
       "AVGO QCOM AMAT CVX TLT SMH ICE GLW EBAY DELL MRVL ASML NOK UMC TTWO SOXL XBI URNM NVDA LRCX WDC GEV MAGS "
       "KORU STRC CRCL COIN PLTR ORCL NOW CRWD NET PANW BABA DKNG ZM ARM BE IREN RKLB HIMS GME TSLA").split()}
MAP.update({"SMSN": "005930.KS", "HYUNDAI": "005380.KS", "SKHX": "000660.KS", "SOFTBANK": "9984.T"})
today = pd.Timestamp.now().normalize()
horizon = today + pd.Timedelta(days=130)
out = {}
for n, yt in MAP.items():
    t = yf.Ticker(yt)
    d = t.dividends
    if d is None or d.empty:
        continue
    d.index = d.index.tz_localize(None).normalize()
    px = t.history(period="5d", auto_adjust=False)["Close"]
    if px.empty:
        continue
    p = float(px.iloc[-1])
    recent = d[d.index >= today - pd.Timedelta(days=400)]
    if recent.empty:
        continue
    gaps = recent.index.to_series().diff().dt.days.dropna()
    period = gaps.median() if len(gaps) else 365
    ents = []
    if period < 45:      # monthly (or more often): next one ~ a month after the last
        step = pd.DateOffset(months=1)
    elif period < 120:
        step = pd.DateOffset(months=3)
    else:
        step = pd.DateOffset(years=1)
    for ts, amt in recent.items():
        nxt = ts + step
        while nxt < today - pd.Timedelta(days=4):
            nxt += step
        if nxt <= horizon:
            ents.append((nxt, amt / p * 100))
    try:
        cal = t.calendar or {}
        ann = cal.get("Ex-Dividend Date")
    except Exception:
        ann = None
    ann = pd.Timestamp(ann) if ann else None
    rows = []
    for dte, pct in sorted(ents):
        if ann is not None and abs((dte - ann).days) <= 20:
            continue                                       # the announced date replaces this estimate
        rows.append({"from": (dte - pd.Timedelta(days=4)).date().isoformat(), "to": (dte + pd.Timedelta(days=4)).date().isoformat(),
                     "pct": round(pct, 2), "est": f"projected from {(dte - step).date()}"})
    if ann is not None and today <= ann <= horizon:
        pct = float(recent.iloc[-1]) / p * 100
        rows.append({"from": ann.date().isoformat(), "to": ann.date().isoformat(), "pct": round(pct, 2), "est": "announced"})
    # merge duplicates in the same window; keep >= 0.3%
    rows = sorted({r["from"]: r for r in rows if r["pct"] >= 0.3}.values(), key=lambda r: r["from"])
    if rows:
        out[n] = rows
        print(n, f"period~{period:.0f}d", rows, file=sys.stderr)
json.dump(out, open("exdiv_draft.json", "w"), indent=1)
print("wrote exdiv_draft.json -- dedupe and merge by hand into exdiv.json (Yahoo lists some dividends twice)", file=sys.stderr)

"""
Live scan: fetch current prices, spreads, order books, and funding rates
for all overlapping equity perps to find actionable opportunities right now.
"""

import requests
import urllib3
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timezone
from requests.exceptions import HTTPError

from config import (
    HYPERLIQUID_API, ASTER_API,
    HYPERLIQUID_MAKER_FEE, HYPERLIQUID_TAKER_FEE,
    ASTER_MAKER_FEE, ASTER_TAKER_FEE,
    NOTIONAL_PER_LEG,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SESSION = requests.Session()
SESSION.verify = False

OUTPUT_DIR = Path("output")


def load_overlap():
    df = pd.read_csv(OUTPUT_DIR / "overlap_symbols.csv")
    return {row["coin"]: {"hl_coin": row["hl_coin"], "aster_symbol": row["aster_symbol"]}
            for _, row in df.iterrows()}


def fetch_hl_all_mids():
    r = SESSION.post(HYPERLIQUID_API, json={"type": "allMids"})
    r.raise_for_status()
    return r.json()


def fetch_hl_l2(coin):
    r = SESSION.post(HYPERLIQUID_API, json={"type": "l2Book", "coin": coin})
    r.raise_for_status()
    return r.json()


def fetch_hl_predicted_fundings():
    r = SESSION.post(HYPERLIQUID_API, json={"type": "predictedFundings"})
    r.raise_for_status()
    return r.json()


def fetch_aster_ticker(symbol):
    r = SESSION.get(f"{ASTER_API}/fapi/v1/ticker/bookTicker", params={"symbol": symbol})
    r.raise_for_status()
    return r.json()


def fetch_aster_premium_index(symbol):
    r = SESSION.get(f"{ASTER_API}/fapi/v1/premiumIndex", params={"symbol": symbol})
    r.raise_for_status()
    return r.json()


def parse_hl_book(book):
    levels = book.get("levels", [[], []])
    if not levels or len(levels) < 2:
        return None, None, None, None
    bids = levels[0]
    asks = levels[1]
    if not bids or not asks:
        return None, None, None, None
    # HL returns [bids, asks] but ordering can vary — use price comparison
    p0 = float(bids[0]["px"])
    p1 = float(asks[0]["px"])
    if p0 > p1:
        best_bid, best_ask = p1, p0
        bid_size = float(asks[0]["sz"])
        ask_size = float(bids[0]["sz"])
    else:
        best_bid, best_ask = p0, p1
        bid_size = float(bids[0]["sz"])
        ask_size = float(asks[0]["sz"])
    return best_bid, best_ask, bid_size, ask_size


def main():
    overlap = load_overlap()
    now = datetime.now(timezone.utc)
    print(f"Live scan at {now:%Y-%m-%d %H:%M:%S} UTC")
    print(f"{'='*100}")

    # Fetch HL predicted fundings
    try:
        pred_fundings_raw = fetch_hl_predicted_fundings()
        # This returns nested structure — parse it
        hl_pred_funding = {}
        if isinstance(pred_fundings_raw, list):
            for item in pred_fundings_raw:
                if isinstance(item, list):
                    for entry in item:
                        if isinstance(entry, list) and len(entry) == 2:
                            coin_name = entry[0]
                            fund_data = entry[1]
                            if isinstance(fund_data, dict):
                                hl_pred_funding[coin_name] = float(fund_data.get("fundingRate", 0))
                elif isinstance(item, dict):
                    coin_name = item.get("coin", item.get("name", ""))
                    hl_pred_funding[coin_name] = float(item.get("fundingRate", 0))
    except Exception as e:
        print(f"Warning: could not fetch HL predicted fundings: {e}")
        hl_pred_funding = {}

    rows = []
    for base, info in sorted(overlap.items()):
        hl_coin = info["hl_coin"]
        aster_sym = info["aster_symbol"]

        try:
            # HL order book
            hl_book = fetch_hl_l2(hl_coin)
            hl_bid, hl_ask, hl_bid_sz, hl_ask_sz = parse_hl_book(hl_book)
            if hl_bid is None:
                continue
            hl_mid = (hl_bid + hl_ask) / 2
            hl_spread_bps = (hl_ask - hl_bid) / hl_mid * 10000

            # Aster book ticker
            aster_ticker = fetch_aster_ticker(aster_sym)
            if isinstance(aster_ticker, list):
                aster_ticker = aster_ticker[0] if aster_ticker else {}
            a_bid = float(aster_ticker.get("bidPrice", 0))
            a_ask = float(aster_ticker.get("askPrice", 0))
            if a_bid == 0 or a_ask == 0:
                continue
            a_mid = (a_bid + a_ask) / 2
            a_spread_bps = (a_ask - a_bid) / a_mid * 10000

            # Cross-exchange spread
            mid_avg = (hl_mid + a_mid) / 2
            cross_spread = hl_mid - a_mid
            cross_spread_bps = cross_spread / mid_avg * 10000

            # Aster funding
            try:
                aster_pi = fetch_aster_premium_index(aster_sym)
                if isinstance(aster_pi, list):
                    aster_pi = aster_pi[0] if aster_pi else {}
                aster_funding = float(aster_pi.get("lastFundingRate", 0))
                aster_next_funding = float(aster_pi.get("nextFundingRate", aster_pi.get("estimatedSettlePrice", 0)) or 0)
            except Exception:
                aster_funding = 0
                aster_next_funding = 0

            # HL funding
            hl_funding = hl_pred_funding.get(hl_coin, 0)

            # Trading costs (best scenario: maker HL, taker Aster)
            cost_bps = (
                hl_spread_bps / 2 + a_spread_bps / 2  # half-spread entry
                + hl_spread_bps / 2 + a_spread_bps / 2  # half-spread exit
                + (HYPERLIQUID_MAKER_FEE * 2 + ASTER_TAKER_FEE * 2) * 10000  # fees
            )

            # Net arb opportunity = |cross spread| - costs
            gross_bps = abs(cross_spread_bps)
            net_entry_bps = gross_bps - cost_bps

            # Funding differential (annualized for context)
            funding_diff = hl_funding - aster_funding
            funding_diff_annual_pct = funding_diff * 3 * 365 * 100

            # Direction
            if cross_spread_bps < 0:
                direction = "Long HL / Short Aster"
            else:
                direction = "Long Aster / Short HL"

            rows.append({
                "coin": base,
                "hl_mid": round(hl_mid, 4),
                "aster_mid": round(a_mid, 4),
                "cross_spread_bps": round(cross_spread_bps, 1),
                "abs_spread_bps": round(gross_bps, 1),
                "hl_spread_bps": round(hl_spread_bps, 1),
                "aster_spread_bps": round(a_spread_bps, 1),
                "round_trip_cost_bps": round(cost_bps, 1),
                "net_entry_bps": round(net_entry_bps, 1),
                "direction": direction,
                "hl_funding_bps": round(hl_funding * 10000, 2),
                "aster_funding_bps": round(aster_funding * 10000, 2),
                "funding_diff_bps": round(funding_diff * 10000, 2),
                "funding_annual_pct": round(funding_diff_annual_pct, 1),
                "net_entry_usd": round(net_entry_bps / 10000 * NOTIONAL_PER_LEG, 2),
            })

        except HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                try:
                    msg = e.response.json().get("msg", "")
                except Exception:
                    msg = ""
                if "pre-trading" in msg or "delivering" in msg or "settling" in msg:
                    print(f"  {base}: market closed (outside trading hours)")
                else:
                    print(f"  {base}: HTTP 400 - {msg or e}")
            else:
                print(f"  {base}: HTTP error - {e}")
        except Exception as e:
            print(f"  {base}: error - {e}")

    if not rows:
        print("No data retrieved.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values("net_entry_bps", ascending=False)

    # Print basis arb opportunities
    print(f"\n{'='*100}")
    print("BASIS ARB OPPORTUNITIES (sorted by net entry P&L)")
    print(f"{'='*100}")
    print(f"{'Coin':>6} {'HL Mid':>10} {'Aster Mid':>10} {'Spread':>8} "
          f"{'Cost':>7} {'Net':>7} {'Net $':>7} {'Direction':>25} "
          f"{'HL Sprd':>8} {'A Sprd':>8}")
    print("-" * 100)

    for _, r in df.iterrows():
        flag = " ***" if r["net_entry_bps"] > 0 else ""
        print(f"{r['coin']:>6} {r['hl_mid']:>10.2f} {r['aster_mid']:>10.2f} "
              f"{r['cross_spread_bps']:>+8.1f} {r['round_trip_cost_bps']:>7.1f} "
              f"{r['net_entry_bps']:>+7.1f} {r['net_entry_usd']:>+7.2f} "
              f"{r['direction']:>25} {r['hl_spread_bps']:>8.1f} {r['aster_spread_bps']:>8.1f}{flag}")

    profitable = df[df["net_entry_bps"] > 0]
    if not profitable.empty:
        print(f"\n>>> {len(profitable)} symbols with positive net entry right now <<<")
    else:
        print(f"\n>>> No symbols with positive net entry at this moment <<<")

    # ==========================================
    # FUNDING ARB with basis cost/gain
    # ==========================================
    # For each symbol with a funding differential:
    #   - The optimal funding direction tells us which exchange to be long/short on
    #   - The current basis (cross spread) is a cost if against us, or a gain if with us
    #   - We need entry cost (half-spreads + fees) + basis headwind, offset by funding income
    #   - Compute break-even periods and projected P&L at 24h and 48h

    print(f"\n{'='*120}")
    print("FUNDING ARB OPPORTUNITIES (with basis cost, sorted by 48h projected P&L)")
    print(f"{'='*120}")

    fund_rows = []
    for _, r in df.iterrows():
        hl_fund = r["hl_funding_bps"]
        aster_fund = r["aster_funding_bps"]
        fund_diff = abs(hl_fund - aster_fund)

        if fund_diff < 0.05:
            continue  # no meaningful differential

        # Optimal funding direction: we want to be short on the high-funding exchange
        # If Aster funding > HL funding: Short Aster (receive high funding), Long HL
        # If HL funding > Aster funding: Short HL (receive high funding), Long Aster
        if aster_fund > hl_fund:
            # Long HL / Short Aster
            direction = "Long HL / Short Aster"
            # Basis impact: we're buying HL, selling Aster
            # If HL < Aster (cross_spread < 0), we buy cheap / sell expensive = basis GAIN
            # If HL > Aster (cross_spread > 0), we buy expensive / sell cheap = basis COST
            basis_impact_bps = -r["cross_spread_bps"]  # positive = favorable
            # Net funding per period: we pay HL funding (long), receive Aster funding (short)
            net_funding_per_period = aster_fund - hl_fund  # positive = we earn
        else:
            # Long Aster / Short HL
            direction = "Long Aster / Short HL"
            basis_impact_bps = r["cross_spread_bps"]  # positive = favorable
            net_funding_per_period = hl_fund - aster_fund

        # Entry cost: half-spreads on both sides + fees (one-way, we exit later)
        entry_cost_bps = (
            r["hl_spread_bps"] / 2 + r["aster_spread_bps"] / 2  # entry spreads
            + (HYPERLIQUID_MAKER_FEE + ASTER_TAKER_FEE) * 10000   # entry fees
        )
        # Exit cost (same structure)
        exit_cost_bps = entry_cost_bps

        # Total initial outlay = entry cost - basis gain (or + basis cost)
        # basis_impact_bps > 0 means basis helps us (we buy cheap, sell expensive)
        net_entry_cost_bps = entry_cost_bps - basis_impact_bps

        # Funding income per 8h period
        funding_per_8h = net_funding_per_period  # bps per period

        # P&L at various horizons (3 periods per day)
        for hours, label in [(8, "8h"), (24, "24h"), (48, "48h")]:
            n_periods = hours / 8
            cum_funding = funding_per_8h * n_periods
            # P&L = cum funding - entry cost - exit cost
            pnl_bps = cum_funding - entry_cost_bps - exit_cost_bps + basis_impact_bps
            pnl_usd = pnl_bps / 10000 * NOTIONAL_PER_LEG

            fund_rows.append({
                "coin": r["coin"],
                "direction": direction,
                "horizon": label,
                "basis_bps": round(basis_impact_bps, 1),
                "entry_cost_bps": round(entry_cost_bps, 1),
                "exit_cost_bps": round(exit_cost_bps, 1),
                "funding_per_8h_bps": round(funding_per_8h, 2),
                "cum_funding_bps": round(cum_funding, 1),
                "net_pnl_bps": round(pnl_bps, 1),
                "net_pnl_usd": round(pnl_usd, 2),
            })

    if fund_rows:
        fund_df = pd.DataFrame(fund_rows)

        # Show 48h view sorted by P&L
        view_48h = fund_df[fund_df["horizon"] == "48h"].sort_values("net_pnl_bps", ascending=False)
        view_24h = fund_df[fund_df["horizon"] == "24h"].set_index("coin")
        view_8h = fund_df[fund_df["horizon"] == "8h"].set_index("coin")

        print(f"\n{'Coin':>6} {'Direction':>25}  {'Basis':>7} {'Entry':>7} {'Exit':>7} "
              f"{'Fund/8h':>8} | {'8h P&L':>8} {'24h P&L':>8} {'48h P&L':>8} {'48h $':>7}")
        print("-" * 120)

        for _, r in view_48h.iterrows():
            coin = r["coin"]
            pnl_8h = view_8h.loc[coin, "net_pnl_bps"] if coin in view_8h.index else 0
            pnl_24h = view_24h.loc[coin, "net_pnl_bps"] if coin in view_24h.index else 0
            flag = " ***" if r["net_pnl_bps"] > 0 else ""
            print(f"{coin:>6} {r['direction']:>25}  {r['basis_bps']:>+7.1f} "
                  f"{r['entry_cost_bps']:>7.1f} {r['exit_cost_bps']:>7.1f} "
                  f"{r['funding_per_8h_bps']:>+8.2f} | "
                  f"{pnl_8h:>+8.1f} {pnl_24h:>+8.1f} {r['net_pnl_bps']:>+8.1f} "
                  f"{r['net_pnl_usd']:>+7.2f}{flag}")

        profitable_48 = view_48h[view_48h["net_pnl_bps"] > 0]
        print(f"\n>>> {len(profitable_48)} symbols profitable at 48h horizon <<<")

        breakeven = []
        for _, r in view_48h.iterrows():
            total_cost = r["entry_cost_bps"] + r["exit_cost_bps"] - r["basis_bps"]
            if r["funding_per_8h_bps"] > 0:
                be_periods = total_cost / r["funding_per_8h_bps"]
                be_hours = be_periods * 8
                breakeven.append({"coin": r["coin"], "breakeven_hours": round(be_hours, 0)})
        if breakeven:
            be_df = pd.DataFrame(breakeven).sort_values("breakeven_hours")
            print(f"\nBreak-even times (funding to cover entry+exit costs+basis):")
            for _, b in be_df.iterrows():
                print(f"  {b['coin']:>6}: {b['breakeven_hours']:.0f}h")

        fund_df.to_csv(OUTPUT_DIR / "live_funding_arb.csv", index=False)
        print(f"\nSaved to output/live_funding_arb.csv")

    # Save
    df.to_csv(OUTPUT_DIR / "live_scan.csv", index=False)
    print(f"Saved to output/live_scan.csv")


if __name__ == "__main__":
    main()

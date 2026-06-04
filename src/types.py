from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class BookTicker:
    symbol: str
    bid: Decimal
    ask: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2

    @property
    def spread_bps(self) -> Decimal:
        if self.mid == 0:
            return Decimal("0")
        return (self.ask - self.bid) / self.mid * Decimal("10000")


@dataclass
class EquityPerp:
    canonical: str        # e.g. "NVDA"
    venue_symbol: str     # e.g. "xyz:NVDA" or "NVDAUSDT"
    mark_price: Optional[Decimal] = None
    funding_rate_hourly: Optional[Decimal] = None  # normalised to per-hour


@dataclass
class ArbResult:
    canonical: str
    hl_mid: Decimal
    aster_mid: Decimal
    mid_spread_bps: Decimal       # signed: positive = Aster more expensive
    abs_spread_bps: Decimal
    direction: str                # "BUY_HL" or "BUY_ASTER"
    hl_spread_bps: Decimal
    aster_spread_bps: Decimal
    bo_cost_bps: Decimal          # full round-trip bid-offer cost (both legs × 2)
    taker_fee_cost_bps: Decimal   # round-trip taker fees
    maker_fee_cost_bps: Decimal   # round-trip maker fees
    total_taker_cost_bps: Decimal
    total_maker_cost_bps: Decimal
    hl_funding_bps_hr: Decimal
    aster_funding_bps_hr: Decimal
    net_carry_bps_hr: Decimal     # positive = earn carry in chosen direction
    # Net P&L = abs_spread - total_cost + carry * hours
    net_pnl_taker_1h: Decimal
    net_pnl_taker_8h: Decimal
    net_pnl_taker_24h: Decimal
    net_pnl_taker_1wk: Decimal
    net_pnl_maker_1h: Decimal
    net_pnl_maker_8h: Decimal
    breakeven_taker_hours: Optional[Decimal]  # None = never profitable
    breakeven_maker_hours: Optional[Decimal]

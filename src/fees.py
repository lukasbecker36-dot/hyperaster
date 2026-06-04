"""Shared fee assumptions for both venues (bps per one-way trade).

Verify current rates before trading:
  Hyperliquid: https://app.hyperliquid.xyz  (HIP-3 deployer trade.xyz applies 2x base)
  Aster DEX:   0% maker/taker promo announced Dec 2025 — confirm still live
"""

from decimal import Decimal

HL_TAKER_BPS = Decimal("10")   # 0.10% per trade
HL_MAKER_BPS = Decimal("5")    # 0.05% per trade
ASTER_FEE_BPS = Decimal("0")   # 0% promotional

# Round-trip = entry + exit = 2 trades on each venue
ROUND_TRIP_TAKER_BPS = (HL_TAKER_BPS + ASTER_FEE_BPS) * 2   # 20 bps
ROUND_TRIP_MAKER_BPS = (HL_MAKER_BPS + ASTER_FEE_BPS) * 2   # 10 bps

from typing import Any

BASE_POSITION_PCT = 0.02
MAX_POSITION_PCT  = 0.05
TARGET_DAILY_RISK = 0.005  # risk no more than 0.5% of portfolio on one trade's daily move

TIER_MULTIPLIER: dict[str, float] = {
    "tier1": 1.0,
    "tier2": 0.6,
    "tier3": 0.4,
}

STOP_LOSS_ATR_MULTIPLE = 2.0


def compute_position_size(
    portfolio_value: float,
    entry_price: float,
    atr_14: float,
    size_modifier: float,
    tier: str,
) -> dict[str, Any]:
    """
    Return qty (shares), dollar_amount, and stop_loss_price for a trade.

    size_modifier comes from the agreement gate (1.0 for full, 0.6 for uncertain).
    tier adjusts for discovery confidence (tier1=1.0, tier2=0.6, tier3=0.4).
    Stop-loss is set at entry_price - (STOP_LOSS_ATR_MULTIPLE * atr_14).
    """
    tier_mult = TIER_MULTIPLIER.get(tier, 1.0)

    base_dollar = portfolio_value * BASE_POSITION_PCT
    max_dollar  = portfolio_value * MAX_POSITION_PCT

    # volatility-adjusted size: risk TARGET_DAILY_RISK of portfolio per ATR move
    if atr_14 and atr_14 > 0 and entry_price > 0:
        vol_adjusted_dollar = (portfolio_value * TARGET_DAILY_RISK) / (atr_14 / entry_price)
    else:
        vol_adjusted_dollar = base_dollar

    raw_dollar = min(base_dollar, vol_adjusted_dollar)
    adjusted   = raw_dollar * size_modifier * tier_mult
    final      = min(adjusted, max_dollar)

    qty = int(final / entry_price) if entry_price > 0 else 0
    stop_loss_price = round(entry_price - STOP_LOSS_ATR_MULTIPLE * atr_14, 2) if atr_14 else None

    return {
        "qty": qty,
        "dollar_amount": round(qty * entry_price, 2),
        "stop_loss_price": stop_loss_price,
    }

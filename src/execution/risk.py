import os
from typing import Any

HALT_FLAG_PATH = "/Volumes/bootcamp_students/trading_bd/landing/system_state/halt.flag"

MAX_OPEN_POSITIONS  = 10
MAX_SECTOR_EXPOSURE = 0.25
DAILY_LOSS_LIMIT    = 0.02
MAX_DRAWDOWN        = 0.10


def is_halted() -> bool:
    return os.path.exists(HALT_FLAG_PATH)


def set_halt(reason: str) -> None:
    os.makedirs(os.path.dirname(HALT_FLAG_PATH), exist_ok=True)
    with open(HALT_FLAG_PATH, "w") as f:
        f.write(reason)
    print(f"TRADING HALTED: {reason}")


def clear_halt() -> None:
    if os.path.exists(HALT_FLAG_PATH):
        os.remove(HALT_FLAG_PATH)
        print("Halt cleared — trading re-enabled")


def check_daily_loss(
    portfolio_value: float,
    peak_value: float,
    start_of_day_value: float,
) -> dict[str, Any]:
    daily_loss_pct = (portfolio_value - start_of_day_value) / start_of_day_value if start_of_day_value else 0.0
    drawdown_pct   = (portfolio_value - peak_value) / peak_value if peak_value else 0.0

    if daily_loss_pct <= -DAILY_LOSS_LIMIT:
        return {
            "halt": True,
            "reason": f"daily loss limit hit: {daily_loss_pct:.1%} (limit {DAILY_LOSS_LIMIT:.1%})",
        }
    if drawdown_pct <= -MAX_DRAWDOWN:
        return {
            "halt": True,
            "reason": f"max drawdown hit: {drawdown_pct:.1%} (limit {MAX_DRAWDOWN:.1%})",
        }
    return {"halt": False, "reason": None}


def check_pre_trade(
    ticker: str,
    open_positions: list[dict[str, Any]],
    qty: int,
    entry_price: float,
    portfolio_value: float,
) -> dict[str, Any]:
    if qty <= 0:
        return {"approved": False, "reason": "computed qty is zero — position too small"}

    if len(open_positions) >= MAX_OPEN_POSITIONS:
        return {
            "approved": False,
            "reason": f"max open positions reached ({MAX_OPEN_POSITIONS})",
        }

    already_holding = any(p.get("symbol") == ticker for p in open_positions)
    if already_holding:
        return {"approved": False, "reason": f"already holding {ticker}"}

    trade_pct = (qty * entry_price) / portfolio_value if portfolio_value else 0.0
    if trade_pct > MAX_SECTOR_EXPOSURE:
        return {
            "approved": False,
            "reason": f"single position would be {trade_pct:.1%} of portfolio (limit {MAX_SECTOR_EXPOSURE:.1%})",
        }

    return {"approved": True, "reason": None}

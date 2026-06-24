import pytest
from src.execution.sizing import compute_position_size, BASE_POSITION_PCT, MAX_POSITION_PCT


PORTFOLIO = 100_000.0
PRICE     = 150.0
ATR       = 3.0   # $3/day daily range


def test_basic_buy_returns_required_keys():
    result = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier1")
    assert "qty" in result
    assert "dollar_amount" in result
    assert "stop_loss_price" in result


def test_stop_loss_below_entry():
    result = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier1")
    assert result["stop_loss_price"] < PRICE


def test_stop_loss_is_two_atr_below_entry():
    result = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier1")
    assert abs(result["stop_loss_price"] - (PRICE - 2 * ATR)) < 0.01


def test_dollar_amount_does_not_exceed_max_pct():
    result = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier1")
    assert result["dollar_amount"] <= PORTFOLIO * MAX_POSITION_PCT


def test_size_modifier_reduces_qty():
    full     = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier1")
    reduced  = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=0.6, tier="tier1")
    assert reduced["qty"] <= full["qty"]


def test_tier2_discount_reduces_qty():
    tier1 = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier1")
    tier2 = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier2")
    assert tier2["qty"] <= tier1["qty"]


def test_tier3_discount_is_smaller_than_tier2():
    tier2 = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier2")
    tier3 = compute_position_size(PORTFOLIO, PRICE, ATR, size_modifier=1.0, tier="tier3")
    assert tier3["qty"] <= tier2["qty"]


def test_zero_qty_when_price_is_zero():
    result = compute_position_size(PORTFOLIO, 0.0, ATR, size_modifier=1.0, tier="tier1")
    assert result["qty"] == 0


def test_high_volatility_reduces_size():
    low_vol  = compute_position_size(PORTFOLIO, PRICE, 1.0, size_modifier=1.0, tier="tier1")
    high_vol = compute_position_size(PORTFOLIO, PRICE, 10.0, size_modifier=1.0, tier="tier1")
    assert high_vol["qty"] <= low_vol["qty"]

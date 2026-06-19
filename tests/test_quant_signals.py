from src.signals.quant import compute_quant_signal


def _row(rsi: float, macd: float, vol_ratio: float = 1.0, price_change_5d: float = 0.0) -> dict:
    return {
        "ticker": "TEST",
        "rsi_14": rsi,
        "macd": macd,
        "atr_14": 1.5,
        "vol_ratio_20d": vol_ratio,
        "price_change_5d": price_change_5d,
    }


# ── BUY signals ──────────────────────────────────────────────────────────────

def test_buy_high_confidence_rsi_deeply_oversold():
    result = compute_quant_signal(_row(rsi=28.0, macd=0.05, vol_ratio=2.0, price_change_5d=0.04))
    assert result["action"] == "BUY"
    assert result["confidence_tier"] == "HIGH"
    assert result["confidence"] >= 0.8


def test_buy_low_confidence_rsi_oversold_macd_positive():
    result = compute_quant_signal(_row(rsi=37.0, macd=0.02, vol_ratio=1.1))
    assert result["action"] == "BUY"
    assert result["confidence_tier"] == "LOW"


def test_buy_high_requires_volume_confirmation():
    # RSI < 30 + MACD positive but volume below 1.5x → LOW confidence, not HIGH
    result = compute_quant_signal(_row(rsi=28.0, macd=0.05, vol_ratio=1.2))
    assert result["action"] == "BUY"
    assert result["confidence_tier"] == "LOW"


# ── SELL signals ─────────────────────────────────────────────────────────────

def test_sell_high_confidence_rsi_deeply_overbought():
    result = compute_quant_signal(_row(rsi=72.0, macd=-0.05, vol_ratio=2.0, price_change_5d=-0.04))
    assert result["action"] == "SELL"
    assert result["confidence_tier"] == "HIGH"
    assert result["confidence"] >= 0.8


def test_sell_low_confidence_rsi_overbought_macd_negative():
    result = compute_quant_signal(_row(rsi=63.0, macd=-0.02, vol_ratio=1.0))
    assert result["action"] == "SELL"
    assert result["confidence_tier"] == "LOW"


# ── HOLD signals ─────────────────────────────────────────────────────────────

def test_hold_neutral_rsi():
    result = compute_quant_signal(_row(rsi=52.0, macd=0.01))
    assert result["action"] == "HOLD"
    assert result["confidence"] == 0.0


def test_hold_when_rsi_oversold_but_macd_negative():
    # Conflicting signals: oversold (BUY territory) but MACD negative — no trade
    result = compute_quant_signal(_row(rsi=37.0, macd=-0.01))
    assert result["action"] == "HOLD"


def test_hold_when_rsi_overbought_but_macd_positive():
    # Conflicting signals: overbought (SELL territory) but MACD positive — no trade
    result = compute_quant_signal(_row(rsi=63.0, macd=0.01))
    assert result["action"] == "HOLD"


def test_hold_missing_indicators():
    result = compute_quant_signal({"ticker": "TEST", "rsi_14": None, "macd": None})
    assert result["action"] == "HOLD"
    assert "insufficient" in result["reason"]


def test_hold_missing_macd_only():
    result = compute_quant_signal({"ticker": "TEST", "rsi_14": 30.0, "macd": None})
    assert result["action"] == "HOLD"


# ── Output structure ─────────────────────────────────────────────────────────

def test_output_always_has_required_keys():
    for row in [
        _row(28.0, 0.05, 2.0, 0.04),   # BUY HIGH
        _row(37.0, 0.02),               # BUY LOW
        _row(72.0, -0.05, 2.0, -0.04), # SELL HIGH
        _row(52.0, 0.01),               # HOLD
    ]:
        result = compute_quant_signal(row)
        assert "action" in result
        assert "confidence" in result
        assert "confidence_tier" in result
        assert "reason" in result
        assert result["action"] in ("BUY", "SELL", "HOLD")
        assert result["confidence_tier"] in ("HIGH", "LOW")
        assert 0.0 <= result["confidence"] <= 1.0

from typing import Any


def compute_quant_signal(row: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a quant trading signal from pre-computed technical indicators.

    Parameters
    ----------
    row : dict
        Must contain: rsi_14, macd, atr_14, vol_ratio_20d.
        Optional: price_change_5d.

    Returns
    -------
    dict with keys:
        action          : "BUY" | "SELL" | "HOLD"
        confidence      : float 0.0–1.0  (stored in decisions.quant_confidence)
        confidence_tier : "HIGH" | "LOW" (used in position size multiplier lookup)
        reason          : str            (stored in decisions.quant_reason)
    """
    rsi: float | None = row.get("rsi_14")
    macd: float | None = row.get("macd")
    vol_ratio: float = row.get("vol_ratio_20d") or 1.0
    price_change_5d: float = row.get("price_change_5d") or 0.0

    if rsi is None or macd is None:
        return {
            "action": "HOLD",
            "confidence": 0.0,
            "confidence_tier": "LOW",
            "reason": "insufficient indicator data",
        }

    # BUY — deeply oversold with volume and directional confirmation
    if rsi < 30 and vol_ratio >= 1.5 and (macd > 0 or price_change_5d > 0.03):
        return {
            "action": "BUY",
            "confidence": 0.85,
            "confidence_tier": "HIGH",
            "reason": (
                f"RSI {rsi:.1f} deeply oversold, volume {vol_ratio:.1f}x 20d avg, "
                f"MACD {macd:+.4f}"
            ),
        }

    # BUY — oversold with positive MACD momentum
    if rsi < 40 and macd > 0:
        return {
            "action": "BUY",
            "confidence": 0.60,
            "confidence_tier": "LOW",
            "reason": (
                f"RSI {rsi:.1f} oversold, MACD {macd:+.4f} positive, "
                f"volume {vol_ratio:.1f}x 20d avg"
            ),
        }

    # SELL — deeply overbought with volume and directional confirmation
    if rsi > 70 and vol_ratio >= 1.5 and (macd < 0 or price_change_5d < -0.03):
        return {
            "action": "SELL",
            "confidence": 0.85,
            "confidence_tier": "HIGH",
            "reason": (
                f"RSI {rsi:.1f} deeply overbought, volume {vol_ratio:.1f}x 20d avg, "
                f"MACD {macd:+.4f}"
            ),
        }

    # SELL — overbought with negative MACD momentum
    if rsi > 60 and macd < 0:
        return {
            "action": "SELL",
            "confidence": 0.60,
            "confidence_tier": "LOW",
            "reason": (
                f"RSI {rsi:.1f} overbought, MACD {macd:+.4f} negative, "
                f"volume {vol_ratio:.1f}x 20d avg"
            ),
        }

    return {
        "action": "HOLD",
        "confidence": 0.0,
        "confidence_tier": "LOW",
        "reason": f"RSI {rsi:.1f} neutral zone (40–60), no confirming signal",
    }

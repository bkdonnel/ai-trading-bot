from src.llm.prompt import PROMPT_VERSION, build_prompt


def _signal(action: str = "BUY", confidence: float = 0.85, reason: str = "RSI oversold") -> dict:
    return {"quant_action": action, "quant_confidence": confidence, "quant_reason": reason}


def _context(**kwargs) -> dict:
    base = {
        "tier": "tier1",
        "close": 150.25,
        "rsi_14": 32.5,
        "macd": 0.0123,
        "atr_14": 2.5,
        "vol_ratio_20d": 1.8,
        "price_change_5d": -0.04,
        "avg_sentiment_48h": -0.12,
        "news_count_48h": 3,
    }
    base.update(kwargs)
    return base


# ── Content checks ────────────────────────────────────────────────────────────

def test_prompt_contains_ticker():
    result = build_prompt("AAPL", _context(), _signal(), [], None)
    assert "AAPL" in result


def test_prompt_contains_quant_action():
    result = build_prompt("MSFT", _context(), _signal("SELL", 0.60), [], None)
    assert "SELL" in result


def test_prompt_contains_confidence_pct():
    result = build_prompt("NVDA", _context(), _signal("BUY", 0.85), [], None)
    assert "85%" in result


def test_prompt_contains_rsi():
    result = build_prompt("TSLA", _context(rsi_14=32.5), _signal(), [], None)
    assert "32.5" in result


def test_prompt_contains_news_headline():
    news = [{"headline": "Earnings beat expectations", "sentiment_score": 0.75}]
    result = build_prompt("AAPL", _context(), _signal(), news, None)
    assert "Earnings beat expectations" in result


def test_prompt_caps_news_at_10():
    news = [{"headline": f"Headline {i}", "sentiment_score": 0.1} for i in range(20)]
    result = build_prompt("AAPL", _context(), _signal(), news, None)
    assert result.count("Headline") == 10


def test_prompt_no_news_placeholder():
    result = build_prompt("AAPL", _context(), _signal(), [], None)
    assert "No recent news" in result


def test_prompt_fundamentals_when_present():
    fund = {
        "period": "2026-Q1",
        "eps_actual": 1.52,
        "eps_estimate": 1.40,
        "revenue_actual": 90.5,
        "revenue_estimate": 88.0,
        "analyst_target_price": 210.0,
        "analyst_rating": "Buy",
        "guidance_sentiment": "positive",
    }
    result = build_prompt("AAPL", _context(), _signal(), [], fund)
    assert "2026-Q1" in result
    assert "1.52" in result
    assert "210.00" in result


def test_prompt_no_fundamentals_placeholder():
    result = build_prompt("AAPL", _context(), _signal(), [], None)
    assert "No fundamentals data available" in result


def test_prompt_contains_tier():
    result = build_prompt("XYZ", _context(tier="tier2"), _signal(), [], None)
    assert "tier2" in result


def test_prompt_version_constant_is_string():
    assert isinstance(PROMPT_VERSION, str)
    assert len(PROMPT_VERSION) > 0

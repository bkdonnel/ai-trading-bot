from src.monitor.exits import build_exit_prompt, parse_exit_response


POSITION = {
    "ticker": "AAPL",
    "qty": 10,
    "avg_entry_price": 150.0,
    "current_price": 145.0,
    "unrealized_pl": -50.0,
}

ARTICLES = [
    {
        "headline": "Apple faces supply chain disruption",
        "summary": "Major factory shutdown expected to last weeks.",
        "published_at": "2026-06-24T10:00:00Z",
        "sentiment_score": -0.8,
    }
]


# ── build_exit_prompt ─────────────────────────────────────────────────────────

def test_prompt_contains_ticker():
    prompt = build_exit_prompt("AAPL", POSITION, ARTICLES)
    assert "AAPL" in prompt


def test_prompt_contains_entry_price():
    prompt = build_exit_prompt("AAPL", POSITION, ARTICLES)
    assert "150.00" in prompt


def test_prompt_contains_current_price():
    prompt = build_exit_prompt("AAPL", POSITION, ARTICLES)
    assert "145.00" in prompt


def test_prompt_contains_article_headline():
    prompt = build_exit_prompt("AAPL", POSITION, ARTICLES)
    assert "supply chain" in prompt


def test_prompt_contains_rules_section():
    prompt = build_exit_prompt("AAPL", POSITION, ARTICLES)
    assert "RULES" in prompt


def test_prompt_handles_empty_articles():
    prompt = build_exit_prompt("AAPL", POSITION, [])
    assert "AAPL" in prompt


# ── parse_exit_response ───────────────────────────────────────────────────────

def test_parse_exit_verdict():
    response = {
        "content": [{
            "type": "tool_use",
            "name": "submit_exit_verdict",
            "input": {"verdict": "EXIT", "confidence": 0.85, "reason": "Major factory shutdown"},
        }]
    }
    result = parse_exit_response(response)
    assert result["verdict"] == "EXIT"
    assert result["confidence"] == 0.85
    assert "factory" in result["reason"]


def test_parse_hold_verdict():
    response = {
        "content": [{
            "type": "tool_use",
            "name": "submit_exit_verdict",
            "input": {"verdict": "HOLD", "confidence": 0.6, "reason": "Noise only"},
        }]
    }
    result = parse_exit_response(response)
    assert result["verdict"] == "HOLD"


def test_parse_falls_back_to_hold_on_missing_tool():
    response = {"content": [{"type": "text", "text": "I think you should hold."}]}
    result = parse_exit_response(response)
    assert result["verdict"] == "HOLD"
    assert result["confidence"] == 0.0


def test_parse_falls_back_on_empty_content():
    result = parse_exit_response({"content": []})
    assert result["verdict"] == "HOLD"


def test_parse_result_has_required_keys():
    response = {
        "content": [{
            "type": "tool_use",
            "name": "submit_exit_verdict",
            "input": {"verdict": "EXIT", "confidence": 0.9, "reason": "Bad news"},
        }]
    }
    result = parse_exit_response(response)
    assert "verdict" in result
    assert "confidence" in result
    assert "reason" in result

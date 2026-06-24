from src.monitor.finbert import filter_negative, score_articles, SENTIMENT_THRESHOLD


def _article(headline: str, summary: str = "") -> dict:
    return {"headline": headline, "summary": summary, "published_at": "2026-06-24T10:00:00Z"}


# ── keyword scorer — neutral ──────────────────────────────────────────────────

def test_neutral_article_scores_zero():
    scored = score_articles([_article("Apple reports strong iPhone sales", "Revenue up 10%")])
    assert scored[0]["sentiment_score"] == 0.0


def test_scores_preserve_original_fields():
    article = _article("Good news")
    scored = score_articles([article])
    assert scored[0]["headline"] == "Good news"
    assert "sentiment_score" in scored[0]


# ── keyword scorer — single negative term ─────────────────────────────────────

def test_single_negative_term_is_below_threshold():
    scored = score_articles([_article("Apple warns of slower growth next quarter")])
    score = scored[0]["sentiment_score"]
    assert score < 0.0
    assert score >= SENTIMENT_THRESHOLD  # one term alone should NOT trigger alarm


# ── keyword scorer — multiple negative terms ──────────────────────────────────

def test_two_negative_terms_are_alarming():
    scored = score_articles([_article("Apple misses earnings, downgrade issued")])
    assert scored[0]["sentiment_score"] < SENTIMENT_THRESHOLD


def test_negative_terms_in_summary_are_caught():
    scored = score_articles([_article("Apple quarterly results", "Revenue miss disappoints investors")])
    assert scored[0]["sentiment_score"] < SENTIMENT_THRESHOLD


# ── filter_negative ───────────────────────────────────────────────────────────

def test_filter_negative_keeps_alarming():
    scored = [
        {"sentiment_score": -0.8},
        {"sentiment_score": 0.0},
        {"sentiment_score": -0.4},
    ]
    result = filter_negative(scored)
    assert len(result) == 1
    assert result[0]["sentiment_score"] == -0.8


def test_filter_negative_returns_empty_when_all_neutral():
    scored = [{"sentiment_score": 0.0}, {"sentiment_score": -0.3}]
    assert filter_negative(scored) == []


def test_filter_negative_on_empty_list():
    assert filter_negative([]) == []


def test_filter_negative_passes_all_fields_through():
    scored = [{"sentiment_score": -0.9, "headline": "Bad news", "ticker": "AAPL"}]
    result = filter_negative(scored)
    assert result[0]["headline"] == "Bad news"
    assert result[0]["ticker"] == "AAPL"

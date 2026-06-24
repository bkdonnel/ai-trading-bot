import requests
from typing import Any

SENTIMENT_THRESHOLD = -0.7

# Financial terms that suggest materially negative news.
# Two or more hits scores below the threshold; one hit scores mid-negative.
_NEGATIVE_TERMS = [
    "miss", "misses", "missed", "disappoints", "disappointing",
    "downgrade", "downgraded", "cuts guidance", "lowered guidance",
    "warning", "profit warning", "warns", "revenue decline",
    "recall", "investigation", "lawsuit", "layoffs", "bankruptcy",
    "default", "fraud", "scandal", "loss wider", "guidance cut",
]


def score_articles(
    articles: list[dict[str, Any]],
    endpoint_url: str | None = None,
    token: str | None = None,
) -> list[dict[str, Any]]:
    if endpoint_url and token:
        return _score_via_endpoint(articles, endpoint_url, token)
    return _score_via_keywords(articles)


def filter_negative(scored_articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [a for a in scored_articles if a.get("sentiment_score", 0.0) < SENTIMENT_THRESHOLD]


def _score_via_endpoint(
    articles: list[dict[str, Any]],
    endpoint_url: str,
    token: str,
) -> list[dict[str, Any]]:
    scored = []
    for article in articles:
        text = article.get("summary") or article.get("headline", "")
        resp = requests.post(
            endpoint_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"inputs": [{"text": text}]},
            timeout=10,
        )
        resp.raise_for_status()
        score = float(resp.json()["predictions"][0])
        scored.append({**article, "sentiment_score": score})
    return scored


def _score_via_keywords(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = []
    for article in articles:
        text = f"{article.get('headline', '')} {article.get('summary', '')}".lower()
        hits = sum(1 for term in _NEGATIVE_TERMS if term in text)
        if hits == 0:
            score = 0.0
        elif hits == 1:
            score = -0.4   # negative but below alarm threshold
        else:
            score = -0.75  # two or more hits — alarming
        scored.append({**article, "sentiment_score": score})
    return scored

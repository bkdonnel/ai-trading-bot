from typing import Any

import requests

_BASE = "https://financialmodelingprep.com/api/v3"


def fetch_fundamentals(ticker: str, api_key: str, limit: int = 8) -> list[dict[str, Any]]:
    """Return earnings rows enriched with current analyst consensus."""
    rows = _fetch_earnings(ticker, api_key, limit)
    analyst = _fetch_analyst_summary(ticker, api_key)
    for row in rows:
        row["analyst_target_price"] = analyst["target"]
        row["analyst_rating"] = analyst["rating"]
    return rows


def _fetch_earnings(ticker: str, api_key: str, limit: int) -> list[dict[str, Any]]:
    resp = requests.get(
        f"{_BASE}/earnings-surprises/{ticker}",
        params={"apiKey": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or not data:
        return []
    return [
        {
            "ticker": ticker,
            "period": (r.get("date") or "")[:7],
            "eps_actual": r.get("actualEarningResult"),
            "eps_estimate": r.get("estimatedEarning"),
            "revenue_actual": None,
            "revenue_estimate": None,
            "guidance_high": None,
            "guidance_low": None,
            "guidance_sentiment": None,
            "analyst_target_price": None,
            "analyst_rating": None,
            "updated_at": r.get("date"),
        }
        for r in data[:limit]
    ]


def _fetch_analyst_summary(ticker: str, api_key: str) -> dict[str, Any]:
    target: float | None = None
    rating: str | None = None

    try:
        resp = requests.get(
            f"{_BASE}/price-target-consensus/{ticker}",
            params={"apiKey": api_key},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        entry = data[0] if isinstance(data, list) and data else data
        target = (entry or {}).get("targetConsensus")
    except Exception:
        pass

    try:
        resp = requests.get(
            f"{_BASE}/analyst-stock-recommendations/{ticker}",
            params={"limit": "1", "apiKey": api_key},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        r = (data[0] if isinstance(data, list) and data else data) or {}
        buy = (r.get("analystRatingsBuy") or 0) + (r.get("analystRatingsStrongBuy") or 0)
        sell = (r.get("analystRatingsSell") or 0) + (r.get("analystRatingsStrongSell") or 0)
        hold = r.get("analystRatingsHold") or 0
        if buy > sell and buy > hold:
            rating = "BUY"
        elif sell > buy and sell > hold:
            rating = "SELL"
        elif buy or sell or hold:
            rating = "HOLD"
    except Exception:
        pass

    return {"target": target, "rating": rating}

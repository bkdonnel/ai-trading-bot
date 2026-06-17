import json
import time
from datetime import date, datetime, timezone
from typing import Any

import requests

_BASE = "https://api.polygon.io"


def _get(url: str, params: dict[str, Any]) -> requests.Response:
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code == 429:
        time.sleep(61)
        resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp


def fetch_bars(ticker: str, from_date: date, to_date: date, api_key: str) -> list[dict[str, Any]]:
    body = _get(
        f"{_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}",
        {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key},
    ).json()
    if body.get("status") not in ("OK", "DELAYED") or not body.get("results"):
        return []
    return [
        {
            "ticker": ticker,
            "date": _ms_to_date(r["t"]),
            "open": float(r["o"]),
            "high": float(r["h"]),
            "low": float(r["l"]),
            "close": float(r["c"]),
            "volume": int(r["v"]),
        }
        for r in body["results"]
    ]


def fetch_news(ticker: str, from_date: date, api_key: str, limit: int = 50) -> list[dict[str, Any]]:
    body = _get(
        f"{_BASE}/v2/reference/news",
        {
            "ticker": ticker,
            "published_utc.gte": from_date.isoformat(),
            "order": "desc",
            "limit": limit,
            "apiKey": api_key,
        },
    ).json()
    if not body.get("results"):
        return []
    return [
        {
            "id": f"{r['id']}_{ticker}",
            "ticker": ticker,
            "published_at": r.get("published_utc"),
            "headline": r.get("title"),
            "summary": r.get("description") or r.get("title"),
            "full_text": None,
            "source": (r.get("publisher") or {}).get("name"),
            "raw_json": json.dumps(r),
        }
        for r in body["results"]
    ]


def _ms_to_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

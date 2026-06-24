import os
from typing import Any

from src.execution.alpaca import (
    get_filled_orders,
    get_open_orders,
    get_open_positions,
    is_market_open,
    place_market_order,
)
from src.monitor.exits import invoke_exit_check
from src.monitor.finbert import filter_negative, score_articles

import requests

POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"


def get_position_state(api_key: str, secret_key: str) -> list[dict[str, Any]]:
    positions = get_open_positions(api_key, secret_key)
    if not positions:
        return []

    open_orders = get_open_orders(api_key, secret_key)
    stop_by_ticker = {
        o["symbol"]: o
        for o in open_orders
        if o.get("order_type") == "stop" and o.get("side") == "sell"
    }

    filled_orders = get_filled_orders(api_key, secret_key)
    buy_by_ticker: dict[str, dict[str, Any]] = {}
    for o in filled_orders:
        sym = o["symbol"]
        if o.get("side") == "buy" and sym not in buy_by_ticker:
            buy_by_ticker[sym] = o  # filled orders returned desc — first is most recent

    state = []
    for pos in positions:
        ticker     = pos["symbol"]
        stop_order = stop_by_ticker.get(ticker, {})
        buy_order  = buy_by_ticker.get(ticker, {})
        state.append({
            "ticker":          ticker,
            "qty":             float(pos.get("qty", 0)),
            "avg_entry_price": float(pos.get("avg_entry_price", 0)),
            "current_price":   float(pos.get("current_price", 0)),
            "unrealized_pl":   float(pos.get("unrealized_pl", 0)),
            "stop_price":      float(stop_order["stop_price"]) if stop_order.get("stop_price") else None,
            "entry_time":      buy_order.get("filled_at"),
        })
    return state


def fetch_breaking_news(ticker: str, since_iso: str, polygon_key: str) -> list[dict[str, Any]]:
    resp = requests.get(
        POLYGON_NEWS_URL,
        params={
            "ticker":           ticker,
            "published_utc.gte": since_iso,
            "limit":            10,
            "order":            "desc",
            "apiKey":           polygon_key,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return [
        {
            "headline":     a.get("title", ""),
            "summary":      a.get("description", ""),
            "published_at": a.get("published_utc", ""),
        }
        for a in resp.json().get("results", [])
    ]


def run() -> None:
    alpaca_key    = os.environ["ALPACA_API_KEY"]
    alpaca_secret = os.environ["ALPACA_SECRET_KEY"]
    polygon_key   = os.environ["POLYGON_API_KEY"]
    anthropic_key = os.environ["ANTHROPIC_API_KEY"]
    finbert_url   = os.environ.get("FINBERT_ENDPOINT_URL")
    finbert_token = os.environ.get("FINBERT_TOKEN")

    if not is_market_open(alpaca_key, alpaca_secret):
        print("Market is closed — nothing to do")
        return

    positions = get_position_state(alpaca_key, alpaca_secret)
    if not positions:
        print("No open positions")
        return

    print(f"Monitoring {len(positions)} open position(s)")

    for pos in positions:
        ticker        = pos["ticker"]
        current_price = pos["current_price"]
        stop_price    = pos["stop_price"]
        entry_time    = pos["entry_time"]

        print(f"\n{ticker}: current=${current_price:.2f}, stop={stop_price}, entry={entry_time}")

        # hard stop check — safety net on top of the broker GTC stop order placed at entry
        if stop_price and current_price <= stop_price:
            print(f"  STOP HIT — selling {pos['qty']} shares")
            try:
                place_market_order(ticker, "sell", pos["qty"], alpaca_key, alpaca_secret)
            except Exception as exc:
                print(f"  Sell order failed: {exc}")
            continue

        if not entry_time:
            print("  No entry_time — skipping news check")
            continue

        try:
            articles = fetch_breaking_news(ticker, entry_time, polygon_key)
        except Exception as exc:
            print(f"  News fetch failed: {exc}")
            continue

        if not articles:
            continue

        scored   = score_articles(articles, finbert_url, finbert_token)
        alarming = filter_negative(scored)

        if not alarming:
            continue

        print(f"  {len(alarming)} alarming article(s) — invoking Claude exit check")
        try:
            verdict = invoke_exit_check(ticker, pos, alarming, anthropic_key)
        except Exception as exc:
            print(f"  Exit check failed: {exc}")
            continue

        print(f"  Claude: {verdict['verdict']} ({verdict['confidence']:.0%}) — {verdict['reason']}")

        if verdict["verdict"] == "EXIT":
            try:
                place_market_order(ticker, "sell", pos["qty"], alpaca_key, alpaca_secret)
                print(f"  Sold {pos['qty']} shares of {ticker}")
            except Exception as exc:
                print(f"  Sell order failed: {exc}")


if __name__ == "__main__":
    run()

import re
import uuid
from datetime import datetime
from typing import Any

import requests


ALPACA_BASE_URL = "https://paper-api.alpaca.markets"


def parse_order_timestamp(value: str | None) -> datetime | None:
    """Parse an RFC 3339 timestamp from an Alpaca order, trimming nanosecond precision."""
    if not value:
        return None
    trimmed = re.sub(r"\.(\d{1,6})\d*", r".\1", value.replace("Z", "+00:00"))
    return datetime.fromisoformat(trimmed)


def order_to_trade_record(order: dict[str, Any], decision_id: str) -> dict[str, Any]:
    """Map an Alpaca order response to a row for the trades Delta table.

    Market orders are usually still unfilled when the response returns, so
    filled_price and filled_at are often None at write time.
    """
    return {
        "id": order.get("id") or str(uuid.uuid4()),
        "decision_id": decision_id,
        "ticker": order.get("symbol", ""),
        "side": order.get("side"),
        "order_type": order.get("type"),
        "quantity": float(order["qty"]) if order.get("qty") else None,
        "filled_price": float(order["filled_avg_price"]) if order.get("filled_avg_price") else None,
        "submitted_at": parse_order_timestamp(order.get("submitted_at")),
        "filled_at": parse_order_timestamp(order.get("filled_at")),
    }


def _headers(api_key: str, secret_key: str) -> dict[str, str]:
    return {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
        "Content-Type": "application/json",
    }


def get_account(api_key: str, secret_key: str) -> dict[str, Any]:
    resp = requests.get(
        f"{ALPACA_BASE_URL}/v2/account",
        headers=_headers(api_key, secret_key),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_open_positions(api_key: str, secret_key: str) -> list[dict[str, Any]]:
    resp = requests.get(
        f"{ALPACA_BASE_URL}/v2/positions",
        headers=_headers(api_key, secret_key),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_latest_quote(ticker: str, api_key: str, secret_key: str) -> float:
    resp = requests.get(
        f"{ALPACA_BASE_URL}/v2/stocks/{ticker}/quotes/latest",
        headers=_headers(api_key, secret_key),
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    ask = data.get("quote", {}).get("ap", 0.0)
    bid = data.get("quote", {}).get("bp", 0.0)
    return (ask + bid) / 2.0 if ask and bid else ask or bid


def place_market_order(
    ticker: str,
    side: str,
    qty: float,
    api_key: str,
    secret_key: str,
) -> dict[str, Any]:
    payload = {
        "symbol": ticker,
        "qty": str(round(qty, 0)),
        "side": side.lower(),
        "type": "market",
        "time_in_force": "day",
    }
    resp = requests.post(
        f"{ALPACA_BASE_URL}/v2/orders",
        headers=_headers(api_key, secret_key),
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def is_market_open(api_key: str, secret_key: str) -> bool:
    resp = requests.get(
        f"{ALPACA_BASE_URL}/v2/clock",
        headers=_headers(api_key, secret_key),
        timeout=10,
    )
    resp.raise_for_status()
    return bool(resp.json().get("is_open", False))


def get_open_orders(api_key: str, secret_key: str) -> list[dict[str, Any]]:
    resp = requests.get(
        f"{ALPACA_BASE_URL}/v2/orders",
        headers=_headers(api_key, secret_key),
        params={"status": "open", "limit": 500},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_filled_orders(api_key: str, secret_key: str, limit: int = 200) -> list[dict[str, Any]]:
    resp = requests.get(
        f"{ALPACA_BASE_URL}/v2/orders",
        headers=_headers(api_key, secret_key),
        params={"status": "closed", "direction": "desc", "limit": limit},
        timeout=10,
    )
    resp.raise_for_status()
    return [o for o in resp.json() if o.get("status") == "filled"]


def place_stop_order(
    ticker: str,
    qty: float,
    stop_price: float,
    api_key: str,
    secret_key: str,
) -> dict[str, Any]:
    """Place a sell stop order as the hard stop-loss for a long position."""
    payload = {
        "symbol": ticker,
        "qty": str(round(qty, 0)),
        "side": "sell",
        "type": "stop",
        "stop_price": str(round(stop_price, 2)),
        "time_in_force": "gtc",
    }
    resp = requests.post(
        f"{ALPACA_BASE_URL}/v2/orders",
        headers=_headers(api_key, secret_key),
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()

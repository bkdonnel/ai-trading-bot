import requests
from typing import Any


ALPACA_BASE_URL = "https://paper-api.alpaca.markets"


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

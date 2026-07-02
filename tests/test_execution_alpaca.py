from datetime import datetime, timezone

from src.execution.alpaca import order_to_trade_record, parse_order_timestamp


def test_parse_order_timestamp_none() -> None:
    assert parse_order_timestamp(None) is None
    assert parse_order_timestamp("") is None


def test_parse_order_timestamp_nanoseconds() -> None:
    parsed = parse_order_timestamp("2026-07-02T13:45:01.123456789Z")
    assert parsed == datetime(2026, 7, 2, 13, 45, 1, 123456, tzinfo=timezone.utc)


def test_parse_order_timestamp_no_fraction() -> None:
    parsed = parse_order_timestamp("2026-07-02T13:45:01Z")
    assert parsed == datetime(2026, 7, 2, 13, 45, 1, tzinfo=timezone.utc)


def test_order_to_trade_record_market_order() -> None:
    order = {
        "id": "abc-123",
        "symbol": "AAPL",
        "side": "buy",
        "type": "market",
        "qty": "12",
        "filled_avg_price": None,
        "submitted_at": "2026-07-02T13:45:01.123456789Z",
        "filled_at": None,
    }
    record = order_to_trade_record(order, "decision-1")
    assert record["id"] == "abc-123"
    assert record["decision_id"] == "decision-1"
    assert record["ticker"] == "AAPL"
    assert record["side"] == "buy"
    assert record["order_type"] == "market"
    assert record["quantity"] == 12.0
    assert record["filled_price"] is None
    assert record["submitted_at"] is not None
    assert record["filled_at"] is None


def test_order_to_trade_record_filled_stop() -> None:
    order = {
        "id": "def-456",
        "symbol": "MSFT",
        "side": "sell",
        "type": "stop",
        "qty": "5",
        "filled_avg_price": "412.37",
        "submitted_at": "2026-07-02T13:45:01Z",
        "filled_at": "2026-07-06T14:02:11Z",
    }
    record = order_to_trade_record(order, "decision-2")
    assert record["filled_price"] == 412.37
    assert record["filled_at"] == datetime(2026, 7, 6, 14, 2, 11, tzinfo=timezone.utc)


def test_order_to_trade_record_missing_id_generates_one() -> None:
    record = order_to_trade_record({"symbol": "NVDA"}, "decision-3")
    assert record["id"]
    assert record["quantity"] is None
    assert record["submitted_at"] is None

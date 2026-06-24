import os
import tempfile
import pytest

import src.execution.risk as risk_module
from src.execution.risk import (
    check_pre_trade,
    check_daily_loss,
    is_halted,
    set_halt,
    clear_halt,
    MAX_OPEN_POSITIONS,
)


PORTFOLIO = 100_000.0
PRICE     = 150.0
QTY       = 10


# ── halt flag ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def tmp_halt(tmp_path, monkeypatch):
    flag = str(tmp_path / "halt.flag")
    monkeypatch.setattr(risk_module, "HALT_FLAG_PATH", flag)
    return flag


def test_not_halted_by_default(tmp_halt):
    assert not is_halted()


def test_set_halt_creates_flag(tmp_halt):
    set_halt("daily loss limit")
    assert is_halted()


def test_clear_halt_removes_flag(tmp_halt):
    set_halt("test")
    clear_halt()
    assert not is_halted()


def test_clear_halt_is_idempotent(tmp_halt):
    clear_halt()  # should not raise


# ── daily loss / drawdown ─────────────────────────────────────────────────────

def test_no_halt_within_limits():
    result = check_daily_loss(
        portfolio_value=99_000.0,
        peak_value=100_000.0,
        start_of_day_value=100_000.0,
    )
    assert not result["halt"]


def test_halt_on_daily_loss_limit():
    result = check_daily_loss(
        portfolio_value=97_900.0,
        peak_value=100_000.0,
        start_of_day_value=100_000.0,
    )
    assert result["halt"]
    assert "daily loss" in result["reason"]


def test_halt_on_max_drawdown():
    # daily loss is within limit (-0.5%) but total drawdown from peak exceeds 10%
    result = check_daily_loss(
        portfolio_value=89_500.0,
        peak_value=100_000.0,
        start_of_day_value=90_000.0,
    )
    assert result["halt"]
    assert "drawdown" in result["reason"]


# ── pre-trade checks ──────────────────────────────────────────────────────────

def test_approved_when_all_clear():
    result = check_pre_trade("AAPL", [], QTY, PRICE, PORTFOLIO)
    assert result["approved"]


def test_rejected_when_already_holding():
    positions = [{"symbol": "AAPL"}]
    result = check_pre_trade("AAPL", positions, QTY, PRICE, PORTFOLIO)
    assert not result["approved"]
    assert "AAPL" in result["reason"]


def test_rejected_when_max_positions_reached():
    positions = [{"symbol": f"TICK{i}"} for i in range(MAX_OPEN_POSITIONS)]
    result = check_pre_trade("NEWT", positions, QTY, PRICE, PORTFOLIO)
    assert not result["approved"]


def test_rejected_when_qty_is_zero():
    result = check_pre_trade("AAPL", [], 0, PRICE, PORTFOLIO)
    assert not result["approved"]


def test_rejected_when_position_too_large():
    # 300 shares at $150 = $45,000 = 45% of $100k portfolio — exceeds 25% limit
    result = check_pre_trade("AAPL", [], 300, PRICE, PORTFOLIO)
    assert not result["approved"]

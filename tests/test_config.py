import inspect
import os

import pytest
from unittest.mock import patch

from src.data.config import get_secret, CATALOG, SCHEMA


def test_catalog_and_schema_are_set() -> None:
    assert CATALOG == "bootcamp_students"
    assert SCHEMA == "trading_bd"


def test_get_secret_reads_from_env() -> None:
    with patch.dict(os.environ, {"POLYGON_API_KEY": "test-key"}):
        assert get_secret("polygon_api_key") == "test-key"


def test_get_secret_raises_when_missing() -> None:
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="Secret 'polygon_api_key' not found"):
            get_secret("polygon_api_key")


def test_no_hardcoded_secrets() -> None:
    import src.data.config as config_module

    source = inspect.getsource(config_module)
    forbidden = ["sk-ant-", "pk-", "Bearer ", "password =", "api_key ="]
    for pattern in forbidden:
        assert pattern not in source, f"Possible hardcoded secret found: '{pattern}'"

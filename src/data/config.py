import os


CATALOG = "bootcamp_students"
SCHEMA = "trading_bd"


def get_secret(key: str) -> str:
    try:
        from databricks.sdk.runtime import dbutils
        return str(dbutils.secrets.get(scope="trading_bd", key=key))
    except (ImportError, Exception):
        pass

    env_key = key.upper().replace("-", "_")
    value = os.environ.get(env_key)
    if not value:
        raise RuntimeError(
            f"Secret '{key}' not found. "
            f"In Databricks: add to secrets scope 'trading_bd'. "
            f"Locally: set environment variable '{env_key}'."
        )
    return value


def get_polygon_api_key() -> str:
    return get_secret("polygon_api_key")


def get_alpaca_api_key() -> str:
    return get_secret("alpaca_api_key")


def get_alpaca_secret_key() -> str:
    return get_secret("alpaca_secret_key")


def get_anthropic_api_key() -> str:
    return get_secret("anthropic_api_key")


def get_fmp_api_key() -> str:
    return get_secret("fmp_api_key")

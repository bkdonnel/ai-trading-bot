import pandas as pd
from pyspark.sql import DataFrame

_OUTPUT_SCHEMA = (
    "ticker STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, "
    "close DOUBLE, volume LONG, rsi_14 DOUBLE, macd DOUBLE, atr_14 DOUBLE, vol_ratio_20d DOUBLE"
)


def _compute(pdf: pd.DataFrame) -> pd.DataFrame:
    from ta.momentum import RSIIndicator
    from ta.trend import MACD
    from ta.volatility import AverageTrueRange

    pdf = pdf.sort_values("date").copy()
    close = pdf["close"]

    pdf["rsi_14"] = RSIIndicator(close=close, window=14).rsi()
    pdf["macd"] = MACD(close=close, window_slow=26, window_fast=12, window_sign=9).macd()
    pdf["atr_14"] = AverageTrueRange(
        high=pdf["high"], low=pdf["low"], close=close, window=14
    ).average_true_range()

    volume = pdf["volume"].astype(float)
    pdf["vol_ratio_20d"] = volume / volume.rolling(20, min_periods=1).mean()

    return pdf[["ticker", "date", "open", "high", "low", "close", "volume",
                "rsi_14", "macd", "atr_14", "vol_ratio_20d"]]


def add_indicators(df: DataFrame) -> DataFrame:
    """Compute RSI-14, MACD, ATR-14, and 20-day volume ratio per ticker."""
    return df.groupby("ticker").applyInPandas(_compute, schema=_OUTPUT_SCHEMA)

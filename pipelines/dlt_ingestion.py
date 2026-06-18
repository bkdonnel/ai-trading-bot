# Databricks notebook source
# DLT ingestion pipeline — reads NDJSON files from DBFS landing zones
# written by jobs/fetch_market_data.py and produces cleaned, validated
# Delta tables in bootcamp_students.trading_bd.
#
# Pipeline runs in TRIGGERED mode (not continuous).  The nightly Workflow
# triggers fetch_market_data first, then this pipeline refresh.
#
# NOTE: On first run, DLT will attempt to create these tables.  If they
# already exist as non-DLT tables (from setup_schema.py), drop them first:
#   DROP TABLE IF EXISTS bootcamp_students.trading_bd.price_bars;
#   DROP TABLE IF EXISTS bootcamp_students.trading_bd.news;
#   DROP TABLE IF EXISTS bootcamp_students.trading_bd.fundamentals;
# DLT will recreate them with identical schemas under its management.

# COMMAND ----------

import dlt
import pandas as pd
from pyspark.sql import functions as F

_INDICATORS_SCHEMA = (
    "ticker STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, "
    "close DOUBLE, volume LONG, rsi_14 DOUBLE, macd DOUBLE, atr_14 DOUBLE, vol_ratio_20d DOUBLE"
)


def _compute_indicators(pdf: pd.DataFrame) -> pd.DataFrame:
    from ta.momentum import RSIIndicator
    from ta.trend import MACD
    from ta.volatility import AverageTrueRange

    pdf = pdf.sort_values("date").copy()
    close = pdf["close"]
    pdf["rsi_14"] = RSIIndicator(close=close, window=14).rsi()
    pdf["macd"] = MACD(close=close, window_slow=26, window_fast=12, window_sign=9).macd()
    pdf["atr_14"] = AverageTrueRange(high=pdf["high"], low=pdf["low"], close=close, window=14).average_true_range()
    volume = pdf["volume"].astype(float)
    pdf["vol_ratio_20d"] = volume / volume.rolling(20, min_periods=1).mean()
    return pdf[["ticker", "date", "open", "high", "low", "close", "volume",
                "rsi_14", "macd", "atr_14", "vol_ratio_20d"]]

# COMMAND ----------
# Landing zone paths (must match jobs/fetch_market_data.py)

BARS_LANDING  = "/Volumes/bootcamp_students/trading_bd/landing/bars"
NEWS_LANDING  = "/Volumes/bootcamp_students/trading_bd/landing/news"
FUNDS_LANDING = "/Volumes/bootcamp_students/trading_bd/landing/fundamentals"

# Schema checkpoints let Auto Loader evolve the schema without reprocessing
CHECKPOINT_BASE = "/Volumes/bootcamp_students/trading_bd/checkpoints"

# COMMAND ----------
# ── PRICE BARS ──────────────────────────────────────────────────────────────

_BARS_RAW_SCHEMA = (
    "ticker STRING, date DATE, open DOUBLE, high DOUBLE, "
    "low DOUBLE, close DOUBLE, volume LONG"
)


@dlt.table(
    name="raw_price_bars",
    comment="Bronze: raw EOD OHLCV bars from Polygon.io landing zone",
    table_properties={"quality": "bronze"},
)
def raw_price_bars():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bars_schema")
        .schema(_BARS_RAW_SCHEMA)
        .load(BARS_LANDING)
    )


@dlt.table(
    name="price_bars",
    comment="Silver: validated bars with RSI-14, MACD, ATR-14, vol_ratio_20d",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("positive_volume", "volume > 0")
@dlt.expect_or_drop("positive_close", "close > 0")
@dlt.expect_or_drop("ticker_present", "ticker IS NOT NULL")
def price_bars():
    # Batch read so add_indicators sees full history per ticker
    # (streaming would only get the micro-batch, breaking rolling windows)
    raw = dlt.read("raw_price_bars").dropDuplicates(["ticker", "date"])
    return raw.groupby("ticker").applyInPandas(_compute_indicators, schema=_INDICATORS_SCHEMA)


# COMMAND ----------
# ── NEWS ────────────────────────────────────────────────────────────────────

_NEWS_RAW_SCHEMA = (
    "id STRING, ticker STRING, published_at TIMESTAMP, headline STRING, "
    "summary STRING, full_text STRING, source STRING, raw_json STRING"
)


@dlt.table(
    name="raw_news",
    comment="Bronze: raw news articles from Polygon.io landing zone",
    table_properties={"quality": "bronze"},
)
def raw_news():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/news_schema")
        .schema(_NEWS_RAW_SCHEMA)
        .load(NEWS_LANDING)
    )


def _score_sentiment(pdf: pd.DataFrame, endpoint_url: str, token: str) -> pd.DataFrame:
    """Call FinBERT Model Serving endpoint; gracefully returns nulls if not configured."""
    import requests as _req

    scores, labels = [], []

    for text in pdf["summary"].fillna(""):
        if not endpoint_url or not text:
            scores.append(None)
            labels.append(None)
            continue
        try:
            resp = _req.post(
                endpoint_url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"inputs": [{"text": text[:512]}]},
                timeout=10,
            )
            pred = resp.json()["predictions"][0]
            label = (pred.get("label") or "neutral").lower()
            raw_score = float(pred.get("score", 0.0))
            score = -raw_score if label == "negative" else (0.0 if label == "neutral" else raw_score)
            scores.append(score)
            labels.append(label)
        except Exception:
            scores.append(None)
            labels.append(None)

    pdf["sentiment_score"] = scores
    pdf["sentiment_label"] = labels
    return pdf


@dlt.table(
    name="news",
    comment="Silver: news articles with FinBERT sentiment scores",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("has_id", "id IS NOT NULL")
@dlt.expect_or_drop("has_ticker", "ticker IS NOT NULL")
def news():
    # Retrieve FinBERT endpoint config; returns nulls gracefully when not yet deployed
    try:
        finbert_url = dbutils.secrets.get(scope="trading_bd", key="finbert_endpoint_url")
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    except Exception:
        finbert_url = ""
        token = ""

    _NEWS_OUT_SCHEMA = (
        "id STRING, ticker STRING, published_at TIMESTAMP, headline STRING, "
        "summary STRING, full_text STRING, source STRING, "
        "sentiment_score DOUBLE, sentiment_label STRING, raw_json STRING"
    )

    def _score(pdf: pd.DataFrame) -> pd.DataFrame:
        return _score_sentiment(pdf, finbert_url, token)

    raw = dlt.read_stream("raw_news").dropDuplicates(["id"])
    return raw.groupby("ticker").applyInPandas(_score, schema=_NEWS_OUT_SCHEMA)


# COMMAND ----------
# ── FUNDAMENTALS ─────────────────────────────────────────────────────────────

_FUNDS_RAW_SCHEMA = (
    "ticker STRING, period STRING, eps_actual DOUBLE, eps_estimate DOUBLE, "
    "revenue_actual DOUBLE, revenue_estimate DOUBLE, guidance_high DOUBLE, "
    "guidance_low DOUBLE, guidance_sentiment STRING, "
    "analyst_target_price DOUBLE, analyst_rating STRING, updated_at STRING"
)


@dlt.table(
    name="raw_fundamentals",
    comment="Bronze: raw earnings and analyst data from FMP landing zone",
    table_properties={"quality": "bronze"},
)
def raw_fundamentals():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/fundamentals_schema")
        .schema(_FUNDS_RAW_SCHEMA)
        .load(FUNDS_LANDING)
    )


@dlt.table(
    name="fundamentals",
    comment="Silver: cleaned fundamentals with TIMESTAMP updated_at",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("has_ticker", "ticker IS NOT NULL")
@dlt.expect_or_drop("has_period", "period IS NOT NULL")
def fundamentals():
    return (
        dlt.read_stream("raw_fundamentals")
        .dropDuplicates(["ticker", "period"])
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )

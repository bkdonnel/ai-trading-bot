# Databricks notebook source
# Fetches EOD bars, news, and fundamentals from external APIs and writes
# NDJSON files to DBFS landing zones for the DLT pipeline to consume.
#
# Run nightly BEFORE the DLT pipeline refresh.
# First run: set lookback_days widget to 90 to backfill enough history
# for indicator warmup (MACD needs 26+ trading days).
# Subsequent nightly runs: default of 2 days is sufficient.

# COMMAND ----------

import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, "/Workspace/Repos/bkdonnel/ai-trading-bot")

from src.data.config import get_polygon_api_key, get_fmp_api_key
from src.data.fetchers.polygon import fetch_bars, fetch_news
from src.data.fetchers.fmp import fetch_fundamentals

# COMMAND ----------

dbutils.widgets.text("lookback_days", "2", "Days of bar history to fetch")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))

TIER1_WATCHLIST: list[str] = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "META",
    "AMZN", "TSLA", "HD", "COST", "NKE",
    "UNH", "LLY", "JNJ", "ABBV",
    "JPM", "V", "MA", "GS",
    "XOM", "CVX",
]

BARS_LANDING  = "/Volumes/bootcamp_students/trading_bd/landing/bars"
NEWS_LANDING  = "/Volumes/bootcamp_students/trading_bd/landing/news"
FUNDS_LANDING = "/Volumes/bootcamp_students/trading_bd/landing/fundamentals"

POLYGON_KEY = get_polygon_api_key()
FMP_KEY     = get_fmp_api_key()

today     = date.today()
run_label = today.strftime("%Y%m%d")

# COMMAND ----------
# Ensure landing subdirectories exist (Volume root created via SQL before first run)

for path in (BARS_LANDING, NEWS_LANDING, FUNDS_LANDING):
    os.makedirs(path, exist_ok=True)

# COMMAND ----------
# Fetch EOD bars

bars_from = today - timedelta(days=LOOKBACK_DAYS)
bars_to   = today - timedelta(days=1)  # yesterday is the latest complete trading day

bars_fetched = 0
for ticker in TIER1_WATCHLIST:
    try:
        bars = fetch_bars(ticker, bars_from, bars_to, POLYGON_KEY)
        if bars:
            ndjson = "\n".join(json.dumps(r) for r in bars)
            with open(f"{BARS_LANDING}/{ticker}_{run_label}.jsonl", "w") as f:
                f.write(ndjson)
            bars_fetched += len(bars)
            print(f"  bars: {ticker} — {len(bars)} rows")
    except Exception as exc:
        print(f"  bars: {ticker} FAILED — {exc}")

print(f"Bars done: {bars_fetched} rows across {len(TIER1_WATCHLIST)} tickers")

# COMMAND ----------
# Fetch news (last 48h to catch anything published after yesterday's market close)

news_from = today - timedelta(days=2)

news_fetched = 0
for ticker in TIER1_WATCHLIST:
    try:
        articles = fetch_news(ticker, news_from, POLYGON_KEY)
        if articles:
            ndjson = "\n".join(json.dumps(a) for a in articles)
            with open(f"{NEWS_LANDING}/{ticker}_{run_label}.jsonl", "w") as f:
                f.write(ndjson)
            news_fetched += len(articles)
            print(f"  news: {ticker} — {len(articles)} articles")
    except Exception as exc:
        print(f"  news: {ticker} FAILED — {exc}")

print(f"News done: {news_fetched} articles across {len(TIER1_WATCHLIST)} tickers")

# COMMAND ----------
# Fetch fundamentals (earnings surprises + analyst consensus)
# These update infrequently; fetching every night is harmless and ensures
# we capture earnings releases within 24h.

funds_fetched = 0
for ticker in TIER1_WATCHLIST:
    try:
        rows = fetch_fundamentals(ticker, FMP_KEY)
        if rows:
            ndjson = "\n".join(json.dumps(r) for r in rows)
            with open(f"{FUNDS_LANDING}/{ticker}_{run_label}.jsonl", "w") as f:
                f.write(ndjson)
            funds_fetched += len(rows)
            print(f"  fundamentals: {ticker} — {len(rows)} periods")
    except Exception as exc:
        print(f"  fundamentals: {ticker} FAILED — {exc}")

print(f"Fundamentals done: {funds_fetched} rows across {len(TIER1_WATCHLIST)} tickers")

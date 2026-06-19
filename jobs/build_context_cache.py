# Databricks notebook source
# Builds the morning decision loop context.
# Combines the Tier 1 watchlist with last night's candidates, pulls latest
# indicators and aggregated news sentiment from Delta, and writes to a shared
# context_cache table that run_quant_signals reads.
#
# Runs at 9:45am as the first task in the decision_loop workflow.

# COMMAND ----------

from datetime import date, timedelta

from pyspark.sql import Row, functions as F

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"
TODAY   = date.today()

TIER1_WATCHLIST: list[str] = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "META",
    "AMZN", "TSLA", "HD", "COST", "NKE",
    "UNH", "LLY", "JNJ", "ABBV",
    "JPM", "V", "MA", "GS",
    "XOM", "CVX",
]

# COMMAND ----------
# Determine the full ticker universe: Tier 1 + today's candidates

candidates_rows = spark.sql(f"""
    SELECT ticker, tier
    FROM {CATALOG}.{SCHEMA}.candidates
    WHERE screen_date = DATE('{TODAY}')
""").collect()

# Build tier map — Tier 1 always wins if a ticker appears in both
tier_map: dict[str, str] = {t: "tier1" for t in TIER1_WATCHLIST}
for row in candidates_rows:
    if row.ticker not in tier_map:
        tier_map[row.ticker] = row.tier

all_tickers = list(tier_map.keys())
tickers_sql = ", ".join(f"'{t}'" for t in all_tickers)

print(f"Decision loop universe: {len(all_tickers)} tickers")
print(f"  Tier 1: {sum(1 for v in tier_map.values() if v == 'tier1')}")
print(f"  Tier 2: {sum(1 for v in tier_map.values() if v == 'tier2')}")
print(f"  Tier 3: {sum(1 for v in tier_map.values() if v == 'tier3')}")

# COMMAND ----------
# Batch pull — pays Delta latency once, not per ticker.
# Latest indicators from price_bars + 48h aggregated news sentiment.

news_cutoff = TODAY - timedelta(hours=48)

context_df = spark.sql(f"""
    WITH latest_bars AS (
        SELECT
            ticker, date, close, volume,
            rsi_14, macd, atr_14, vol_ratio_20d,
            LAG(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS close_5d_ago,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.price_bars
        WHERE ticker IN ({tickers_sql})
          AND rsi_14 IS NOT NULL
    ),
    bars AS (
        SELECT
            ticker,
            date        AS bar_date,
            close,
            volume,
            rsi_14,
            macd,
            atr_14,
            vol_ratio_20d,
            CASE
                WHEN close_5d_ago IS NOT NULL AND close_5d_ago > 0
                THEN (close - close_5d_ago) / close_5d_ago
            END         AS price_change_5d
        FROM latest_bars
        WHERE rn = 1
    ),
    recent_news AS (
        SELECT
            ticker,
            AVG(sentiment_score)  AS avg_sentiment_48h,
            COUNT(*)              AS news_count_48h
        FROM {CATALOG}.{SCHEMA}.news
        WHERE ticker IN ({tickers_sql})
          AND published_at >= TIMESTAMP('{news_cutoff}')
        GROUP BY ticker
    )
    SELECT
        b.ticker,
        b.bar_date,
        b.close,
        b.volume,
        b.rsi_14,
        b.macd,
        b.atr_14,
        b.vol_ratio_20d,
        b.price_change_5d,
        COALESCE(n.avg_sentiment_48h, 0.0) AS avg_sentiment_48h,
        COALESCE(n.news_count_48h,    0)   AS news_count_48h
    FROM bars b
    LEFT JOIN recent_news n ON b.ticker = n.ticker
""")

# Add tier column from the tier_map built above
tier_rows = [Row(ticker=t, tier=v) for t, v in tier_map.items()]
tier_df   = spark.createDataFrame(tier_rows)
context_df = context_df.join(tier_df, on="ticker", how="left").fillna({"tier": "tier1"})

ticker_count = context_df.count()
print(f"Context cache: {ticker_count} tickers with latest data")
context_df.show()

# COMMAND ----------
# Overwrite the context_cache table — fresh each morning.
# run_quant_signals reads directly from this table.

context_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.{SCHEMA}.context_cache")

print(f"Context cache written for {TODAY} ({ticker_count} tickers)")

# Databricks notebook source
# Reads the latest technical indicators per ticker from price_bars (silver)
# and writes them to the quant_features Delta table.
# Runs nightly after the DLT pipeline refresh (refresh_dlt_pipeline task).
#
# Writes directly to Delta rather than via FeatureEngineeringClient — the
# "DataExpert All Purpose" cluster does not have databricks.feature_engineering
# available. See CLAUDE.md Databricks Cluster Dependency Constraints.

# COMMAND ----------

from delta.tables import DeltaTable

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"
FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.quant_features"

# COMMAND ----------
# Pull the most recent indicator row per ticker.
# Include price_change_5d (5-day momentum) computed via a window function.
# Rows with null RSI are excluded — they lack enough history for indicator warmup.

features_df = spark.sql(f"""
    WITH ranked AS (
        SELECT
            ticker, date, close, volume,
            rsi_14, macd, atr_14, vol_ratio_20d,
            LAG(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS close_5d_ago,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.price_bars
        WHERE rsi_14 IS NOT NULL
    )
    SELECT
        ticker,
        date,
        close,
        volume,
        rsi_14,
        macd,
        atr_14,
        vol_ratio_20d,
        CASE
            WHEN close_5d_ago IS NOT NULL AND close_5d_ago > 0
            THEN (close - close_5d_ago) / close_5d_ago
        END AS price_change_5d
    FROM ranked
    WHERE rn = 1
""")

ticker_count = features_df.count()
print(f"Features computed for {ticker_count} tickers")
features_df.show()

# COMMAND ----------
# Write to quant_features Delta table.
# create on first run; merge (upsert on ticker, date) on subsequent runs.

if not spark.catalog.tableExists(FEATURE_TABLE):
    features_df.write.format("delta").saveAsTable(FEATURE_TABLE)
    print(f"Created {FEATURE_TABLE} ({ticker_count} rows)")
else:
    DeltaTable.forName(spark, FEATURE_TABLE).alias("target").merge(
        features_df.alias("src"),
        "target.ticker = src.ticker AND target.date = src.date"
    ).whenMatchedUpdate(set={
        "close":           "src.close",
        "volume":          "src.volume",
        "rsi_14":          "src.rsi_14",
        "macd":            "src.macd",
        "atr_14":          "src.atr_14",
        "vol_ratio_20d":   "src.vol_ratio_20d",
        "price_change_5d": "src.price_change_5d",
    }).whenNotMatchedInsertAll().execute()
    print(f"Updated {FEATURE_TABLE} ({ticker_count} rows)")

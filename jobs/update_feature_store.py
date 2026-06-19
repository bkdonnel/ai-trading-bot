# Databricks notebook source
# Reads the latest technical indicators per ticker from price_bars (silver)
# and writes them to the Databricks Feature Store.
# Runs nightly after the DLT pipeline refresh (refresh_dlt_pipeline task).

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

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
# Write to Feature Store.
# create_table on first run; write_table (merge) on subsequent runs.

fe = FeatureEngineeringClient()

if not spark.catalog.tableExists(FEATURE_TABLE):
    fe.create_table(
        name=FEATURE_TABLE,
        primary_keys=["ticker", "date"],
        df=features_df,
        description="Latest quant indicators per ticker for the decision loop",
    )
    print(f"Created Feature Store table: {FEATURE_TABLE} ({ticker_count} rows)")
else:
    fe.write_table(name=FEATURE_TABLE, df=features_df, mode="merge")
    print(f"Updated Feature Store table: {FEATURE_TABLE} ({ticker_count} rows)")

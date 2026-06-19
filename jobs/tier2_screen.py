# Databricks notebook source
# Tier 2 quant screener — runs across all tickers in price_bars using Spark SQL.
# Tickers passing all criteria are written to the candidates table for the
# morning decision loop.
#
# Screening criteria:
#   1. RSI outside neutral zone (< 40 oversold OR > 60 overbought)
#   2. 5-day price change > 3% in either direction (momentum confirmation)
#   3. Latest bar volume > 1M shares (liquidity floor)
#
# Market cap check (> $2B) is omitted until FMP Starter plan is active.
# Runs nightly after update_feature_store.

# COMMAND ----------

from datetime import date

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"
TODAY   = date.today()

# COMMAND ----------
# Run the screen via Spark SQL — all tickers processed in parallel.
# price_change_5d is computed on the fly via a 5-bar LAG window.

candidates_df = spark.sql(f"""
    WITH ranked AS (
        SELECT
            ticker, date, close, volume, rsi_14, macd, atr_14, vol_ratio_20d,
            LAG(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS close_5d_ago,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.price_bars
        WHERE rsi_14 IS NOT NULL
    ),
    latest AS (
        SELECT
            ticker, date, close, volume, rsi_14, macd, vol_ratio_20d,
            CASE
                WHEN close_5d_ago IS NOT NULL AND close_5d_ago > 0
                THEN (close - close_5d_ago) / close_5d_ago
            END AS price_change_5d
        FROM ranked
        WHERE rn = 1
    )
    SELECT
        ticker,
        DATE('{TODAY}') AS screen_date,
        'tier2' AS tier,
        CONCAT(
            'RSI ', ROUND(rsi_14, 1),
            CASE WHEN rsi_14 < 40 THEN ' oversold' ELSE ' overbought' END,
            ', vol ', ROUND(vol_ratio_20d, 1), 'x 20d avg',
            ', 5d chg ', ROUND(price_change_5d * 100, 1), '%'
        ) AS trigger_reason,
        CAST(NULL AS STRING) AS tier3_event,
        CAST(NULL AS STRING) AS tier3_event_detail
    FROM latest
    WHERE
        (rsi_14 < 40 OR rsi_14 > 60)
        AND price_change_5d IS NOT NULL
        AND ABS(price_change_5d) > 0.03
        AND volume > 1000000
""")

count = candidates_df.count()
print(f"Tier 2 screen ({TODAY}): {count} tickers passed")
candidates_df.show(truncate=False)

# COMMAND ----------
# Idempotent write — delete today's existing Tier 2 rows, then append fresh results.

spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.candidates
    WHERE screen_date = DATE('{TODAY}') AND tier = 'tier2'
""")

if count > 0:
    candidates_df.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA}.candidates"
    )

print(f"Wrote {count} Tier 2 candidates for {TODAY}")

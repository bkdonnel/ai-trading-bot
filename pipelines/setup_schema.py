# Databricks notebook source
# Run once to create the schema and all Delta tables in bootcamp_students.trading_bd

# COMMAND ----------

CATALOG = "bootcamp_students"
SCHEMA = "trading_bd"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.price_bars (
        ticker          STRING  NOT NULL,
        date            DATE    NOT NULL,
        open            DOUBLE,
        high            DOUBLE,
        low             DOUBLE,
        close           DOUBLE,
        volume          LONG,
        rsi_14          DOUBLE,
        macd            DOUBLE,
        atr_14          DOUBLE,
        vol_ratio_20d   DOUBLE
    )
    USING DELTA
    PARTITIONED BY (date)
    COMMENT 'EOD OHLCV bars with pre-computed technical indicators'
""")
print("price_bars ready")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.news (
        id               STRING    NOT NULL,
        ticker           STRING    NOT NULL,
        published_at     TIMESTAMP,
        headline         STRING,
        summary          STRING,
        full_text        STRING,
        source           STRING,
        sentiment_score  DOUBLE,
        sentiment_label  STRING,
        raw_json         STRING
    )
    USING DELTA
    PARTITIONED BY (ticker)
    COMMENT 'News articles with FinBERT sentiment scores computed at ingest'
""")
print("news ready")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.fundamentals (
        ticker               STRING  NOT NULL,
        period               STRING  NOT NULL,
        eps_actual           DOUBLE,
        eps_estimate         DOUBLE,
        revenue_actual       DOUBLE,
        revenue_estimate     DOUBLE,
        guidance_high        DOUBLE,
        guidance_low         DOUBLE,
        guidance_sentiment   STRING,
        analyst_target_price DOUBLE,
        analyst_rating       STRING,
        updated_at           TIMESTAMP
    )
    USING DELTA
    COMMENT 'Quarterly earnings, guidance, and analyst ratings per ticker'
""")
print("fundamentals ready")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.candidates (
        ticker             STRING  NOT NULL,
        screen_date        DATE    NOT NULL,
        tier               STRING,
        trigger_reason     STRING,
        tier3_event        STRING,
        tier3_event_detail STRING
    )
    USING DELTA
    PARTITIONED BY (screen_date)
    COMMENT 'Tickers passing nightly Tier 2 screen or Tier 3 event triggers'
""")
print("candidates ready")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.decisions (
        id               STRING    NOT NULL,
        timestamp        TIMESTAMP NOT NULL,
        ticker           STRING    NOT NULL,
        tier             STRING,
        quant_action     STRING,
        quant_confidence DOUBLE,
        quant_reason     STRING,
        llm_verdict      STRING,
        llm_confidence   DOUBLE,
        llm_thesis       STRING,
        llm_risk_flags   STRING,
        action_taken     STRING,
        skip_reason      STRING,
        entry_price      DOUBLE,
        position_size    DOUBLE,
        stop_loss_price  DOUBLE,
        exit_price       DOUBLE,
        exit_reason      STRING,
        pnl              DOUBLE,
        pnl_pct          DOUBLE,
        hold_days        INT,
        prompt_version   STRING
    )
    USING DELTA
    PARTITIONED BY (ticker)
    COMMENT 'Every decision the agent makes — the primary table for performance analysis'
""")
print("decisions ready")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.trades (
        id           STRING    NOT NULL,
        decision_id  STRING    NOT NULL,
        ticker       STRING    NOT NULL,
        side         STRING,
        order_type   STRING,
        quantity     DOUBLE,
        filled_price DOUBLE,
        submitted_at TIMESTAMP,
        filled_at    TIMESTAMP
    )
    USING DELTA
    COMMENT 'Executed Alpaca orders — reconciled against decisions table'
""")
print("trades ready")

# COMMAND ----------

print(f"\nAll tables created in {CATALOG}.{SCHEMA}")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show()

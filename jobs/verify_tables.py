# Databricks notebook source
# Verification queries for the Phase 5 shakedown period.
# Run manually after the workflows have executed to confirm tables are
# populating correctly. Not part of any scheduled workflow.
#
# Three layers, on different clocks:
#   Layer 1 — nightly pipeline tables (price_bars, news, quant_features,
#             candidates): verifiable after 2-3 nightly runs
#   Layer 2 — decision loop (context_cache, decisions): verifiable after
#             2-3 trading days
#   Layer 3 — exit reconciliation (exit_* columns on decisions): only
#             verifiable once a position has actually closed
#
# For the "offending rows" checks, an empty result means the check passed.
# Note: the trades table has no writer yet — order details are stored on
# the decisions row. An empty trades table is expected, not a failure.

# COMMAND ----------

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"

# COMMAND ----------

# ── Layer 1.1: price_bars freshness and coverage ──────────────────────
# Expect one row per ticker per trading day (~100 tickers: Tier 1 + Tier 2
# universe), no rows on weekends/holidays, and indicators mostly non-null
# (nulls are normal only in the warm-up window of a ticker's history).

display(spark.sql(f"""
    SELECT
        date,
        COUNT(*)                                    AS row_count,
        COUNT(DISTINCT ticker)                      AS tickers,
        SUM(CASE WHEN rsi_14        IS NULL THEN 1 ELSE 0 END) AS null_rsi,
        SUM(CASE WHEN macd          IS NULL THEN 1 ELSE 0 END) AS null_macd,
        SUM(CASE WHEN atr_14        IS NULL THEN 1 ELSE 0 END) AS null_atr,
        SUM(CASE WHEN vol_ratio_20d IS NULL THEN 1 ELSE 0 END) AS null_vol_ratio
    FROM {CATALOG}.{SCHEMA}.price_bars
    WHERE date >= CURRENT_DATE() - INTERVAL 10 DAYS
    GROUP BY date
    ORDER BY date DESC
"""))

# COMMAND ----------

# ── Layer 1.2: price_bars duplicates ──────────────────────────────────
# Empty result = pass. Duplicates mean the nightly merge is not idempotent.

display(spark.sql(f"""
    SELECT ticker, date, COUNT(*) AS copies
    FROM {CATALOG}.{SCHEMA}.price_bars
    GROUP BY ticker, date
    HAVING COUNT(*) > 1
    ORDER BY copies DESC, date DESC
"""))

# COMMAND ----------

# ── Layer 1.3: news freshness ─────────────────────────────────────────
# News is fetched for Tier 1 only (~20 tickers). Expect rows most days;
# sentiment columns are populated at ingest, so nulls there are a problem.

display(spark.sql(f"""
    SELECT
        DATE(published_at)     AS published_date,
        COUNT(*)               AS articles,
        COUNT(DISTINCT ticker) AS tickers,
        SUM(CASE WHEN sentiment_score IS NULL THEN 1 ELSE 0 END) AS null_sentiment
    FROM {CATALOG}.{SCHEMA}.news
    WHERE published_at >= CURRENT_DATE() - INTERVAL 10 DAYS
    GROUP BY DATE(published_at)
    ORDER BY published_date DESC
"""))

# COMMAND ----------

# ── Layer 1.4: news duplicates ────────────────────────────────────────
# Empty result = pass.

display(spark.sql(f"""
    SELECT id, COUNT(*) AS copies
    FROM {CATALOG}.{SCHEMA}.news
    GROUP BY id
    HAVING COUNT(*) > 1
"""))

# COMMAND ----------

# ── Layer 1.5: quant_features freshness and duplicates ────────────────
# Written by the rewritten update_feature_store.py (direct DeltaTable
# merge). Expect same ticker coverage as price_bars for each date, and
# exactly one row per ticker per date.

display(spark.sql(f"""
    SELECT
        date,
        COUNT(*)                                    AS row_count,
        COUNT(DISTINCT ticker)                      AS tickers,
        SUM(CASE WHEN price_change_5d IS NULL THEN 1 ELSE 0 END) AS null_chg_5d
    FROM {CATALOG}.{SCHEMA}.quant_features
    WHERE date >= CURRENT_DATE() - INTERVAL 10 DAYS
    GROUP BY date
    ORDER BY date DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT ticker, date, COUNT(*) AS copies
    FROM {CATALOG}.{SCHEMA}.quant_features
    GROUP BY ticker, date
    HAVING COUNT(*) > 1
"""))

# COMMAND ----------

# ── Layer 1.6: quant_features vs price_bars coverage gap ──────────────
# Tickers present in price_bars on the latest date but missing from
# quant_features. Empty result = pass.

display(spark.sql(f"""
    WITH latest AS (
        SELECT MAX(date) AS d FROM {CATALOG}.{SCHEMA}.price_bars
    )
    SELECT p.ticker, p.date
    FROM {CATALOG}.{SCHEMA}.price_bars p
    JOIN latest ON p.date = latest.d
    LEFT ANTI JOIN {CATALOG}.{SCHEMA}.quant_features q
        ON p.ticker = q.ticker AND p.date = q.date
    ORDER BY p.ticker
"""))

# COMMAND ----------

# ── Layer 1.7: candidates by screen date ──────────────────────────────
# Tier 2 passes + Tier 3 event triggers. Zero candidates on a quiet day
# is plausible; zero every day suggests the screens are not writing.

display(spark.sql(f"""
    SELECT
        screen_date,
        tier,
        COUNT(*)                 AS candidates,
        COUNT(DISTINCT ticker)   AS tickers
    FROM {CATALOG}.{SCHEMA}.candidates
    WHERE screen_date >= CURRENT_DATE() - INTERVAL 10 DAYS
    GROUP BY screen_date, tier
    ORDER BY screen_date DESC, tier
"""))

# COMMAND ----------

# ── Layer 2.1: context_cache from this morning ────────────────────────
# Overwritten each morning by build_context_cache.py. Expect Tier 1
# (~20 tickers) plus any current candidates. Stale or empty means the
# 9:45am workflow did not run or wrote nothing.

display(spark.sql(f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers
    FROM {CATALOG}.{SCHEMA}.context_cache
"""))

# COMMAND ----------

# ── Layer 2.2: decisions per day, by outcome ──────────────────────────
# Expect a batch each trading morning. PENDING rows should only exist for
# today (before/during the LLM task) — see 2.3 for stranded ones.

display(spark.sql(f"""
    SELECT
        DATE(timestamp)  AS decision_date,
        action_taken,
        COUNT(*)         AS decisions,
        COUNT(DISTINCT ticker) AS tickers
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE timestamp >= CURRENT_DATE() - INTERVAL 10 DAYS
    GROUP BY DATE(timestamp), action_taken
    ORDER BY decision_date DESC, action_taken
"""))

# COMMAND ----------

# ── Layer 2.3: stranded PENDING decisions ─────────────────────────────
# PENDING rows from before today mean decision_loop.py never picked them
# up (or failed mid-run). Empty result = pass.

display(spark.sql(f"""
    SELECT id, timestamp, ticker, tier, quant_action, quant_confidence
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE action_taken = 'PENDING'
      AND DATE(timestamp) < CURRENT_DATE()
    ORDER BY timestamp
"""))

# COMMAND ----------

# ── Layer 2.4: field consistency on resolved decisions ────────────────
# Rows that violate the invariants of decision_loop.py:
#   - resolved (non-PENDING) but no LLM verdict
#   - BUY but missing entry_price / position_size / stop_loss_price
#   - SKIP but no skip_reason
# Exception: skip_reason = 'market closed' rows are skipped before the LLM
# runs (holiday guard), so a null llm_verdict there is expected.
# Empty result = pass.

display(spark.sql(f"""
    SELECT id, DATE(timestamp) AS decision_date, ticker, action_taken,
        CASE
            WHEN llm_verdict IS NULL THEN 'missing llm_verdict'
            WHEN action_taken = 'BUY' AND entry_price     IS NULL THEN 'BUY missing entry_price'
            WHEN action_taken = 'BUY' AND position_size   IS NULL THEN 'BUY missing position_size'
            WHEN action_taken = 'BUY' AND stop_loss_price IS NULL THEN 'BUY missing stop_loss_price'
            WHEN action_taken = 'SKIP' AND skip_reason    IS NULL THEN 'SKIP missing skip_reason'
        END AS problem
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE action_taken IN ('BUY', 'SELL', 'SKIP')
      AND COALESCE(skip_reason, '') != 'market closed'
      AND (
            llm_verdict IS NULL
            OR (action_taken = 'BUY' AND (entry_price IS NULL
                                          OR position_size IS NULL
                                          OR stop_loss_price IS NULL))
            OR (action_taken = 'SKIP' AND skip_reason IS NULL)
      )
    ORDER BY timestamp DESC
"""))

# COMMAND ----------

# ── Layer 2.5: duplicate decisions per ticker per day ─────────────────
# More than one decision for the same ticker on the same day means
# run_quant_signals double-inserted (e.g. a workflow retry). Empty = pass.

display(spark.sql(f"""
    SELECT ticker, DATE(timestamp) AS decision_date, COUNT(*) AS copies
    FROM {CATALOG}.{SCHEMA}.decisions
    GROUP BY ticker, DATE(timestamp)
    HAVING COUNT(*) > 1
    ORDER BY decision_date DESC
"""))

# COMMAND ----------

# ── Layer 3.1: open positions per the decisions table ─────────────────
# BUYs that have not been closed by reconcile_exits.py yet. Cross-check
# this list against Alpaca open positions — they should match one-to-one.

display(spark.sql(f"""
    SELECT DATE(timestamp) AS entry_date, ticker, entry_price,
           position_size, stop_loss_price,
           DATEDIFF(CURRENT_DATE(), DATE(timestamp)) AS days_open
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE action_taken = 'BUY' AND exit_price IS NULL
    ORDER BY timestamp
"""))

# COMMAND ----------

# ── Layer 3.2: closed positions — exit field completeness ─────────────
# Once reconcile_exits.py has matched a sell fill, every exit field must
# be populated together, exit_reason must be a known value, and the pnl
# arithmetic must be self-consistent. Empty result = pass.

display(spark.sql(f"""
    SELECT id, ticker, exit_price, exit_reason, pnl, pnl_pct, hold_days,
        CASE
            WHEN exit_reason NOT IN ('STOP_LOSS_HIT', 'LLM_EXIT') THEN 'unknown exit_reason'
            WHEN pnl IS NULL OR pnl_pct IS NULL OR hold_days IS NULL THEN 'partial exit fields'
            WHEN entry_price IS NOT NULL
                 AND ABS(pnl_pct - (exit_price - entry_price) / entry_price) > 0.001
                THEN 'pnl_pct inconsistent with prices'
        END AS problem
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE exit_price IS NOT NULL
      AND (
            exit_reason NOT IN ('STOP_LOSS_HIT', 'LLM_EXIT')
            OR pnl IS NULL OR pnl_pct IS NULL OR hold_days IS NULL
            OR (entry_price IS NOT NULL
                AND ABS(pnl_pct - (exit_price - entry_price) / entry_price) > 0.001)
      )
    ORDER BY timestamp DESC
"""))

# COMMAND ----------

# ── Layer 3.3: closed positions summary ───────────────────────────────
# Informational — the first rows here are the proof that the full
# entry-to-exit lifecycle works end to end.

display(spark.sql(f"""
    SELECT DATE(timestamp) AS entry_date, ticker, entry_price, exit_price,
           exit_reason, pnl, pnl_pct, hold_days
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE exit_price IS NOT NULL
    ORDER BY timestamp DESC
"""))

# Databricks notebook source
%pip install "typing_extensions>=4.12.2" anthropic mlflow

# COMMAND ----------
# Reads today's PENDING decisions (written by run_quant_signals), calls Claude
# for each one, applies the agreement gate, and MERGEs the LLM verdicts back
# into the decisions table.  Also logs run metadata to MLflow.
#
# Runs at 9:45am as the third task in the decision_loop workflow,
# after run_quant_signals completes.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/bryankdonnelly@comcast.net/ai-trading-bot")

# COMMAND ----------

from datetime import date

import mlflow
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.data.config import get_anthropic_api_key
from src.llm.agreement import resolve_action
from src.llm.claude import invoke_llm
from src.llm.prompt import PROMPT_VERSION, build_prompt

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"
TODAY   = date.today()

# COMMAND ----------
# Load today's PENDING decisions

pending = spark.sql(f"""
    SELECT id, ticker, tier, quant_action, quant_confidence, quant_reason
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE action_taken = 'PENDING'
      AND DATE(timestamp) = '{TODAY}'
""").collect()

print(f"Found {len(pending)} PENDING decisions for {TODAY}")

if not pending:
    print("No PENDING decisions — nothing to do")
    dbutils.notebook.exit("no pending decisions")

# COMMAND ----------
# Batch-pull supporting context for PENDING tickers only

pending_tickers = [r.ticker for r in pending]
tickers_sql = ", ".join(f"'{t}'" for t in pending_tickers)

context_rows = spark.sql(f"""
    SELECT ticker, close, rsi_14, macd, atr_14, vol_ratio_20d,
           price_change_5d, avg_sentiment_48h, news_count_48h, tier
    FROM {CATALOG}.{SCHEMA}.context_cache
    WHERE ticker IN ({tickers_sql})
""").collect()
context_map = {r.ticker: r.asDict() for r in context_rows}

news_rows = spark.sql(f"""
    SELECT ticker, headline, sentiment_score, published_at
    FROM {CATALOG}.{SCHEMA}.news
    WHERE ticker IN ({tickers_sql})
      AND published_at >= CURRENT_TIMESTAMP() - INTERVAL 48 HOURS
    ORDER BY ticker, published_at DESC
""").collect()
news_map: dict[str, list[dict]] = {}
for r in news_rows:
    news_map.setdefault(r.ticker, []).append(r.asDict())

fund_rows = spark.sql(f"""
    WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated_at DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.fundamentals
        WHERE ticker IN ({tickers_sql})
    )
    SELECT ticker, period, eps_actual, eps_estimate, revenue_actual,
           revenue_estimate, guidance_sentiment, analyst_target_price, analyst_rating
    FROM ranked
    WHERE rn = 1
""").collect()
fund_map = {r.ticker: r.asDict() for r in fund_rows}

anthropic_key = get_anthropic_api_key()

# COMMAND ----------
# Call Claude for each PENDING decision

updates: list[dict] = []

with mlflow.start_run(run_name=f"llm_decisions_{TODAY}_prompt_{PROMPT_VERSION}"):
    mlflow.log_param("prompt_version", PROMPT_VERSION)
    mlflow.log_param("model", "claude-sonnet-4-6")
    mlflow.log_param("decision_date", str(TODAY))
    mlflow.log_param("pending_count", len(pending))

    for row in pending:
        ticker = row.ticker
        context = context_map.get(ticker, {})
        signal = row.asDict()
        news = news_map.get(ticker, [])
        fund = fund_map.get(ticker)

        prompt_text = build_prompt(ticker, context, signal, news, fund)
        print(f"\n--- {ticker} ({signal['quant_action']}) ---")

        try:
            verdict = invoke_llm(prompt_text, anthropic_key)
        except Exception as exc:
            print(f"  LLM call failed: {exc}")
            verdict = {
                "verdict": "UNCERTAIN",
                "confidence": 0.0,
                "thesis": f"LLM call failed: {exc}",
                "risk_flags": "[]",
            }

        resolution = resolve_action(signal["quant_action"], verdict["verdict"])

        print(f"  LLM: {verdict['verdict']} ({verdict['confidence']:.0%}) → {resolution['action_taken']}")
        print(f"  Thesis: {verdict['thesis']}")
        if resolution["skip_reason"]:
            print(f"  Skipped: {resolution['skip_reason']}")

        updates.append({
            "id":             row.id,
            "llm_verdict":    verdict["verdict"],
            "llm_confidence": verdict["confidence"],
            "llm_thesis":     verdict["thesis"],
            "llm_risk_flags": verdict["risk_flags"],
            "action_taken":   resolution["action_taken"],
            "skip_reason":    resolution["skip_reason"],
            "prompt_version": PROMPT_VERSION,
        })

    confirmed = sum(1 for u in updates if u["action_taken"] in ("BUY", "SELL"))
    skipped   = sum(1 for u in updates if u["action_taken"] == "SKIP")
    mlflow.log_metric("confirmed_count", confirmed)
    mlflow.log_metric("skipped_count", skipped)

print(f"\nSummary: {confirmed} confirmed, {skipped} skipped out of {len(updates)} decisions")

# COMMAND ----------
# Merge LLM verdicts back into the decisions table

updates_df = (
    spark.createDataFrame(updates)
    .withColumn("llm_confidence", F.col("llm_confidence").cast("double"))
)

DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.decisions").alias("target").merge(
    updates_df.alias("src"),
    "target.id = src.id"
).whenMatchedUpdate(set={
    "llm_verdict":    "src.llm_verdict",
    "llm_confidence": "src.llm_confidence",
    "llm_thesis":     "src.llm_thesis",
    "llm_risk_flags": "src.llm_risk_flags",
    "action_taken":   "src.action_taken",
    "skip_reason":    "src.skip_reason",
    "prompt_version": "src.prompt_version",
}).execute()

print(f"Merged {len(updates)} LLM verdicts into decisions table")

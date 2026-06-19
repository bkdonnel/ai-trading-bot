# Databricks notebook source
# Applies the quant signal model to each ticker in the morning context cache.
# Logs non-HOLD signals to the decisions table with action_taken = 'PENDING'
# so the Phase 3 LLM layer can process them next.
#
# Runs at 9:45am as the second task in the decision_loop workflow.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/bryankdonnelly@comcast.net/ai-trading-bot")

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from src.signals.quant import compute_quant_signal

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"

# COMMAND ----------

context = spark.sql(
    f"SELECT * FROM {CATALOG}.{SCHEMA}.context_cache"
).collect()

print(f"Processing {len(context)} tickers from context cache")

# COMMAND ----------

decisions: list[dict] = []

for row in context:
    data = row.asDict()
    signal = compute_quant_signal(data)

    print(f"  {row.ticker}: {signal['action']} (conf={signal['confidence']:.2f}) — {signal['reason']}")

    if signal["action"] == "HOLD":
        continue

    decisions.append({
        "id":               str(uuid.uuid4()),
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "ticker":           row.ticker,
        "tier":             row.tier,
        "quant_action":     signal["action"],
        "quant_confidence": signal["confidence"],
        "quant_reason":     signal["reason"],
        "llm_verdict":      None,
        "llm_confidence":   None,
        "llm_thesis":       None,
        "llm_risk_flags":   None,
        "action_taken":     "PENDING",
        "skip_reason":      None,
        "entry_price":      None,
        "position_size":    None,
        "stop_loss_price":  None,
        "exit_price":       None,
        "exit_reason":      None,
        "pnl":              None,
        "pnl_pct":          None,
        "hold_days":        None,
        "prompt_version":   None,
    })

print(f"\nQuant signals: {len(decisions)} non-HOLD ({len(context) - len(decisions)} HOLD)")
for d in decisions:
    print(f"  {d['ticker']}: {d['quant_action']} ({d['quant_confidence']:.0%} conf) — {d['quant_reason']}")

# COMMAND ----------

if not decisions:
    print("No actionable quant signals today — decisions table unchanged")
else:
    from pyspark.sql import functions as F

    decisions_df = (
        spark.createDataFrame(decisions)
        .withColumn("timestamp",        F.col("timestamp").cast("timestamp"))
        .withColumn("quant_confidence", F.col("quant_confidence").cast("double"))
        .withColumn("llm_confidence",   F.col("llm_confidence").cast("double"))
        .withColumn("entry_price",      F.col("entry_price").cast("double"))
        .withColumn("position_size",    F.col("position_size").cast("double"))
        .withColumn("stop_loss_price",  F.col("stop_loss_price").cast("double"))
        .withColumn("exit_price",       F.col("exit_price").cast("double"))
        .withColumn("pnl",              F.col("pnl").cast("double"))
        .withColumn("pnl_pct",          F.col("pnl_pct").cast("double"))
        .withColumn("hold_days",        F.col("hold_days").cast("int"))
    )

    decisions_df.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA}.decisions"
    )
    print(f"Wrote {len(decisions)} quant decisions to decisions table")

# Databricks notebook source
# Runs nightly after market close. Queries Alpaca for filled sell orders,
# matches them to open decisions (action_taken='BUY', exit_price IS NULL),
# and writes exit_price, exit_reason, pnl, pnl_pct, hold_days back to the
# decisions table.
#
# Add as a task at the end of the Nightly Pipeline workflow.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/bryankdonnelly@comcast.net/ai-trading-bot")

# COMMAND ----------

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.data.config import get_alpaca_api_key, get_alpaca_secret_key
from src.execution.alpaca import get_filled_orders, get_open_positions

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"

# COMMAND ----------
# fetch current Alpaca state

alpaca_key    = get_alpaca_api_key()
alpaca_secret = get_alpaca_secret_key()

open_positions = get_open_positions(alpaca_key, alpaca_secret)
open_tickers   = {p["symbol"] for p in open_positions}

print(f"Currently holding: {sorted(open_tickers) or 'nothing'}")

# COMMAND ----------
# find executed BUY decisions that have not been reconciled yet

open_decisions = spark.sql(f"""
    SELECT id, ticker, timestamp, entry_price, position_size
    FROM {CATALOG}.{SCHEMA}.decisions
    WHERE action_taken = 'BUY'
      AND exit_price IS NULL
      AND entry_price IS NOT NULL
""").collect()

# only reconcile tickers that are no longer in open positions — they were closed
closed_decisions = [r for r in open_decisions if r.ticker not in open_tickers]
print(f"Decisions to reconcile: {len(closed_decisions)} of {len(open_decisions)} open BUYs")

if not closed_decisions:
    print("Nothing to reconcile")
    dbutils.notebook.exit("nothing to reconcile")

# COMMAND ----------
# fetch Alpaca filled sell orders and index by symbol

filled_orders = get_filled_orders(alpaca_key, alpaca_secret, limit=500)
sell_fills    = [o for o in filled_orders if o.get("side") == "sell"]

sell_by_ticker: dict[str, list] = {}
for o in sell_fills:
    sell_by_ticker.setdefault(o["symbol"], []).append(o)

# COMMAND ----------
# match each closed decision to its Alpaca sell fill

updates = []

for decision in closed_decisions:
    ticker = decision.ticker
    sells  = sell_by_ticker.get(ticker, [])

    if not sells:
        print(f"  {ticker}: no Alpaca sell fill found — skipping")
        continue

    # entry_ts as comparable ISO string (Alpaca timestamps are UTC)
    entry_iso = decision.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

    # take the first sell fill that happened after this decision was entered
    matching = [o for o in sells if (o.get("filled_at") or "") > entry_iso]
    if not matching:
        print(f"  {ticker}: no sell fill after {entry_iso} — skipping")
        continue

    fill        = matching[0]
    exit_price  = float(fill["filled_avg_price"])
    qty         = float(fill["filled_qty"])
    entry_price = decision.entry_price

    order_type  = fill.get("order_type", "market")
    exit_reason = "STOP_LOSS_HIT" if order_type == "stop" else "LLM_EXIT"

    pnl       = round((exit_price - entry_price) * qty, 2)
    pnl_pct   = round((exit_price - entry_price) / entry_price, 4) if entry_price else 0.0
    filled_at = datetime.fromisoformat(fill["filled_at"].replace("Z", "+00:00"))
    hold_days = (filled_at.date() - decision.timestamp.date()).days

    print(f"  {ticker}: {exit_reason} @ ${exit_price:.2f}, PnL ${pnl:+.2f} ({pnl_pct:+.1%}), {hold_days}d held")

    updates.append({
        "id":          decision.id,
        "exit_price":  exit_price,
        "exit_reason": exit_reason,
        "pnl":         pnl,
        "pnl_pct":     pnl_pct,
        "hold_days":   hold_days,
    })

print(f"\nReady to reconcile {len(updates)} exit(s)")

# COMMAND ----------
# merge exit data back into the decisions table

if updates:
    _schema = StructType([
        StructField("id",          StringType(), False),
        StructField("exit_price",  DoubleType(), True),
        StructField("exit_reason", StringType(), True),
        StructField("pnl",         DoubleType(), True),
        StructField("pnl_pct",     DoubleType(), True),
        StructField("hold_days",   IntegerType(), True),
    ])

    updates_df = spark.createDataFrame(updates, schema=_schema)

    DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.decisions").alias("target").merge(
        updates_df.alias("src"),
        "target.id = src.id"
    ).whenMatchedUpdate(set={
        "exit_price":  "src.exit_price",
        "exit_reason": "src.exit_reason",
        "pnl":         "src.pnl",
        "pnl_pct":     "src.pnl_pct",
        "hold_days":   "src.hold_days",
    }).execute()

    print(f"Reconciled {len(updates)} exit(s) into decisions table")

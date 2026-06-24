# Databricks notebook source
# Reads today's PENDING decisions (written by run_quant_signals), calls Claude
# for each one, applies the agreement gate, runs pre-trade risk checks, places
# orders via Alpaca, and MERGEs all results back into the decisions table.
#
# Runs at 9:45am as the third task in the decision_loop workflow,
# after run_quant_signals completes.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/bryankdonnelly@comcast.net/ai-trading-bot")

# COMMAND ----------

from datetime import date

from delta.tables import DeltaTable
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.data.config import get_alpaca_api_key, get_alpaca_secret_key, get_anthropic_api_key
from src.execution.alpaca import get_account, get_latest_quote, get_open_positions, place_market_order, place_stop_order
from src.execution.risk import check_daily_loss, check_pre_trade, is_halted, set_halt
from src.execution.sizing import compute_position_size
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
alpaca_key    = get_alpaca_api_key()
alpaca_secret = get_alpaca_secret_key()

# COMMAND ----------
# Pre-flight: halt check and portfolio snapshot

if is_halted():
    print("Trading is HALTED — manual clearance required. Exiting.")
    dbutils.notebook.exit("halted")

account          = get_account(alpaca_key, alpaca_secret)
portfolio_value  = float(account["portfolio_value"])
start_of_day_val = float(account.get("last_equity", portfolio_value))
open_positions   = get_open_positions(alpaca_key, alpaca_secret)

halt_check = check_daily_loss(portfolio_value, portfolio_value, start_of_day_val)
if halt_check["halt"]:
    set_halt(halt_check["reason"])
    dbutils.notebook.exit(f"halted: {halt_check['reason']}")

print(f"Portfolio: ${portfolio_value:,.2f} | Open positions: {len(open_positions)} | Start of day: ${start_of_day_val:,.2f}")

# COMMAND ----------
# Call Claude for each PENDING decision

updates: list[dict] = []

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

    entry_price    = None
    position_size  = None
    stop_loss_price = None

    if resolution["action_taken"] in ("BUY", "SELL"):
        entry_price = get_latest_quote(ticker, alpaca_key, alpaca_secret)
        atr_14      = context.get("atr_14") or 0.0

        sizing = compute_position_size(
            portfolio_value=portfolio_value,
            entry_price=entry_price,
            atr_14=atr_14,
            size_modifier=resolution["size_modifier"],
            tier=signal.get("tier", "tier1"),
        )
        stop_loss_price = sizing["stop_loss_price"]
        position_size   = sizing["dollar_amount"]

        pre_trade = check_pre_trade(
            ticker=ticker,
            open_positions=open_positions,
            qty=sizing["qty"],
            entry_price=entry_price,
            portfolio_value=portfolio_value,
        )

        if not pre_trade["approved"]:
            print(f"  Pre-trade check failed: {pre_trade['reason']}")
            resolution["action_taken"] = "SKIP"
            resolution["skip_reason"]  = f"pre-trade: {pre_trade['reason']}"
            entry_price = position_size = stop_loss_price = None
        else:
            try:
                place_market_order(ticker, resolution["action_taken"], sizing["qty"], alpaca_key, alpaca_secret)
                if resolution["action_taken"] == "BUY" and stop_loss_price:
                    place_stop_order(ticker, sizing["qty"], stop_loss_price, alpaca_key, alpaca_secret)
                print(f"  Executed: {resolution['action_taken']} {sizing['qty']} shares @ ~${entry_price:.2f}, stop ${stop_loss_price}")
            except Exception as exc:
                print(f"  Order failed: {exc}")
                resolution["action_taken"] = "SKIP"
                resolution["skip_reason"]  = f"order error: {exc}"
                entry_price = position_size = stop_loss_price = None

    updates.append({
        "id":              row.id,
        "llm_verdict":     verdict["verdict"],
        "llm_confidence":  verdict["confidence"],
        "llm_thesis":      verdict["thesis"],
        "llm_risk_flags":  verdict["risk_flags"],
        "action_taken":    resolution["action_taken"],
        "skip_reason":     resolution["skip_reason"],
        "prompt_version":  PROMPT_VERSION,
        "entry_price":     entry_price,
        "position_size":   position_size,
        "stop_loss_price": stop_loss_price,
    })

confirmed = sum(1 for u in updates if u["action_taken"] in ("BUY", "SELL"))
skipped   = sum(1 for u in updates if u["action_taken"] == "SKIP")
print(f"\nSummary: {confirmed} confirmed, {skipped} skipped out of {len(updates)} decisions")

# COMMAND ----------
# Merge LLM verdicts back into the decisions table

_updates_schema = StructType([
    StructField("id",              StringType(), False),
    StructField("llm_verdict",     StringType(), True),
    StructField("llm_confidence",  DoubleType(), True),
    StructField("llm_thesis",      StringType(), True),
    StructField("llm_risk_flags",  StringType(), True),
    StructField("action_taken",    StringType(), True),
    StructField("skip_reason",     StringType(), True),
    StructField("prompt_version",  StringType(), True),
    StructField("entry_price",     DoubleType(), True),
    StructField("position_size",   DoubleType(), True),
    StructField("stop_loss_price", DoubleType(), True),
])

updates_df = spark.createDataFrame(updates, schema=_updates_schema)

DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.decisions").alias("target").merge(
    updates_df.alias("src"),
    "target.id = src.id"
).whenMatchedUpdate(set={
    "llm_verdict":     "src.llm_verdict",
    "llm_confidence":  "src.llm_confidence",
    "llm_thesis":      "src.llm_thesis",
    "llm_risk_flags":  "src.llm_risk_flags",
    "action_taken":    "src.action_taken",
    "skip_reason":     "src.skip_reason",
    "prompt_version":  "src.prompt_version",
    "entry_price":     "src.entry_price",
    "position_size":   "src.position_size",
    "stop_loss_price": "src.stop_loss_price",
}).execute()

print(f"Merged {len(updates)} LLM verdicts into decisions table")

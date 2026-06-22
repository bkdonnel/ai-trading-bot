# AI Trading Bot — Implementation Plan

## Overview

A hybrid AI trading agent that combines a classical quantitative signal filter with an LLM reasoning layer (Claude) to identify and execute stock trades. The system is designed for swing trading (daily decisions), not high-frequency trading. Databricks is the primary platform for storage, compute, orchestration, and monitoring.

---

## Architecture Summary

```
External Sources (Polygon.io, Alpaca, FMP, SEC EDGAR)
         ↓
Delta Live Tables (continuous ingestion + FinBERT scoring)
         ↓
Delta Lake (Unity Catalog)
  trading.price_bars   trading.news   trading.fundamentals
  trading.decisions    trading.trades trading.positions
         ↓
Databricks Workflows
  11:00pm — nightly pipeline (DLT refresh + Tier 2 screen + Feature Store)
  09:45am — decision loop (context pull → quant → LLM → Alpaca)
         ↓
Databricks SQL Dashboard + MLflow
  live performance metrics, prompt version tracking, weekly review

Separately (lightweight, not on Databricks):
  position_monitor.py — cloud function, runs every 30 min during market hours
  Alpaca             — all order execution
```

### Signal Flow

```
Market Data + News
       ↓
┌─────────────────┐     ┌──────────────────────┐
│  Classical      │     │  LLM Reasoning Layer  │
│  Signal Filter  │     │  (Claude)             │
│                 │     │                       │
│ • RSI/MACD      │     │ • News sentiment      │
│ • Volume trends │     │ • Earnings analysis   │
│ • Price action  │     │ • Macro context       │
│ • Mean revert.  │     │ • Thesis generation   │
└────────┬────────┘     └──────────┬────────────┘
         │                         │
         └──────────┬──────────────┘
                    ↓
            Agreement Gate
          (both must agree)
                    ↓
              Execute Trade
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| LLM reasoning | Claude Sonnet (claude-sonnet-4-6) via Anthropic SDK |
| Broker / execution | Alpaca (`alpaca-py`) |
| Market data | Polygon.io (real-time), Alpaca (EOD) |
| Fundamental data | Financial Modeling Prep |
| News / events | Polygon.io news feed, SEC EDGAR RSS |
| Sentiment model | FinBERT (deployed as Databricks Model Serving endpoint) |
| Storage | Delta Lake on Databricks (Unity Catalog) |
| Batch processing | Databricks Jobs + Delta Live Tables |
| Orchestration | Databricks Workflows |
| Feature management | Databricks Feature Store |
| Experiment tracking | MLflow (built into Databricks) |
| Vector search | Databricks Vector Search |
| Dashboards | Databricks SQL + Genie |
| Language | Python |
| Position monitor | Lightweight cloud function (not Databricks) |

---

## Stock Universe & Watchlist

Three-tier system to find trades beyond a manually curated list.

### Tier 1 — Core Watchlist
- ~20–50 stocks you know well
- Always monitored, full position sizing
- Manually curated and periodically reviewed

### Tier 2 — Broad Universe (Screened Nightly)
- S&P 500 or Russell 1000 (~500–1000 stocks)
- Run fast quantitative screener in parallel across full universe each night via Databricks
- Passes go into next day's decision loop

```python
def quick_screen(ticker: str) -> dict:
    return {
        "passes": all([
            avg_volume(ticker) > 1_000_000,
            market_cap(ticker) > 2_000_000_000,
            rsi(ticker) < 40 or rsi(ticker) > 60,
            abs(price_change_5d(ticker)) > 0.03,
        ])
    }
```

### Tier 3 — Event-Triggered Discovery
- Any stock triggering a specific event, regardless of watchlist
- Earnings surprises (beat/miss > 5%)
- Unusual volume (3x 30-day average)
- Material SEC 8-K filings
- Sector momentum spillover

**Position size discounts by tier:**
```python
TIER_SIZE_MULTIPLIER = {
    "tier1": 1.0,
    "tier2": 0.6,
    "tier3": 0.4,
}
```

---

## Data Layer

### Market Data
| Data | Refresh | Source |
|---|---|---|
| OHLCV bars (EOD) | Nightly | Alpaca / Polygon.io |
| Live quotes | At decision time | Alpaca websocket |
| Unusual volume | Nightly screen | Computed from bars in Feature Store |

### Fundamental Data
| Data | Refresh | Source |
|---|---|---|
| Earnings (EPS, revenue, guidance) | Quarterly / on release | Financial Modeling Prep |
| Analyst ratings / price targets | Weekly | Financial Modeling Prep |
| Insider transactions | Weekly | Quiver Quant |
| Short interest | Twice monthly | Quiver Quant |

### News & Sentiment Data
| Data | Refresh | Source |
|---|---|---|
| News headlines + summaries | Nightly + at decision time | Polygon.io news |
| SEC 8-K filings | Event-driven (RSS) | SEC EDGAR |
| Earnings call transcripts | On release | Scrape / Seeking Alpha |
| Macro indicators | On release schedule | FRED (free) |

### Sentiment Processing
- **FinBERT** runs as a Databricks Model Serving endpoint — scored at ingest time via DLT
- Fine-tuned on financial text, far better than general-purpose sentiment models
- Scores stored on the `news` Delta table at write time — no recomputation needed
- Claude makes the nuanced call only on articles that survive the quant filter
- Do not use Claude for bulk sentiment scoring (cost + latency)

---

## Storage — Delta Lake

All data lives in Delta Lake tables under a `trading` catalog in Unity Catalog. Delta gives you ACID transactions, time travel (query any table as of any past timestamp), and schema enforcement out of the box.

### Delta Table Schemas

```sql
-- price bars with pre-computed indicators (written by DLT)
CREATE TABLE trading.price_bars (
    ticker      STRING,
    date        DATE,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      LONG,
    rsi_14      DOUBLE,
    macd        DOUBLE,
    atr_14      DOUBLE,
    vol_ratio_20d DOUBLE
) USING DELTA
PARTITIONED BY (date);

-- news with FinBERT scores pre-computed at ingest
CREATE TABLE trading.news (
    id              STRING,
    ticker          STRING,
    published_at    TIMESTAMP,
    headline        STRING,
    summary         STRING,
    full_text       STRING,
    source          STRING,
    sentiment_score DOUBLE,
    sentiment_label STRING,
    raw_json        STRING
) USING DELTA
PARTITIONED BY (ticker);

-- fundamentals
CREATE TABLE trading.fundamentals (
    ticker      STRING,
    period      STRING,
    eps         DOUBLE,
    revenue     DOUBLE,
    guidance    STRING,
    updated_at  TIMESTAMP
) USING DELTA;

-- every decision the system makes, win or lose
CREATE TABLE trading.decisions (
    id                  STRING,
    timestamp           TIMESTAMP,
    ticker              STRING,
    tier                STRING,
    quant_action        STRING,
    quant_confidence    DOUBLE,
    quant_reason        STRING,
    llm_verdict         STRING,
    llm_confidence      DOUBLE,
    llm_thesis          STRING,
    llm_risk_flags      STRING,   -- JSON array stored as string
    action_taken        STRING,
    skip_reason         STRING,
    entry_price         DOUBLE,
    position_size       DOUBLE,
    stop_loss_price     DOUBLE,
    exit_price          DOUBLE,
    exit_reason         STRING,
    pnl                 DOUBLE,
    pnl_pct             DOUBLE,
    hold_days           INT
) USING DELTA
PARTITIONED BY (ticker);

-- executed trades
CREATE TABLE trading.trades (
    id          STRING,
    decision_id STRING,
    ticker      STRING,
    side        STRING,
    quantity    DOUBLE,
    price       DOUBLE,
    timestamp   TIMESTAMP
) USING DELTA;
```

### Time Travel (Audit)
Delta Lake retains historical versions automatically. Query any table as of any past point:

```sql
-- what did our decisions table look like before last week's bad run?
SELECT * FROM trading.decisions
TIMESTAMP AS OF '2026-06-08 09:00:00';
```

### Full Article Storage (DBFS)
Full article text and earnings transcripts are stored in DBFS (Databricks File System) and referenced by path in the `news` table:

```
dbfs:/trading/news/{TICKER}/{YYYY-MM-DD}_{source}_{id}.json
dbfs:/trading/transcripts/{TICKER}/{YYYY}-Q{N}-earnings.md
```

### Vector Search (Phase 2)
Databricks Vector Search is built into the platform — no separate service needed. Add when you have 6+ months of history:

```python
from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()

# at ingest: store embedding alongside article
client.upsert(
    index_name="trading.news_embeddings",
    inputs=[{"id": article_id, "text": summary, "ticker": ticker}]
)

# semantic search at decision time
results = client.similarity_search(
    index_name="trading.news_embeddings",
    query_text="supply chain disruption causing earnings miss",
    filters={"ticker": "AAPL"},
    num_results=5
)
```

---

## Databricks Platform Components

### Delta Live Tables (Data Ingestion Pipeline)

DLT manages the ingestion pipeline as continuously updated tables with schema validation, bad data quarantine, and lineage tracking:

```python
import dlt

@dlt.table(comment="Raw EOD bars from Polygon.io")
def raw_price_bars():
    return spark.readStream.format("cloudFiles") \
        .load("dbfs:/landing/price_bars/")

@dlt.table(comment="Cleaned bars with computed indicators")
@dlt.expect("valid_volume", "volume > 0")
@dlt.expect("valid_close", "close > 0")
def price_bars_clean():
    return dlt.read_stream("raw_price_bars") \
        .withColumn("rsi_14", compute_rsi("close", 14)) \
        .withColumn("atr_14", compute_atr("high", "low", "close", 14)) \
        .withColumn("macd", compute_macd("close")) \
        .withColumn("vol_ratio_20d", compute_vol_ratio("volume", 20))

@dlt.table(comment="News scored with FinBERT at ingest")
def news_scored():
    return dlt.read_stream("raw_news") \
        .withColumn("sentiment_score", score_with_finbert_endpoint("summary")) \
        .withColumn("sentiment_label", derive_label("sentiment_score"))
```

Indicators are computed once at ingest and stored — the decision loop reads them directly.

### Feature Store

Quant indicators are written to and read from the Databricks Feature Store, ensuring the live system and any future ML models read from the same consistent source (no training/serving skew):

```python
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# written nightly after DLT refresh
fe.write_table(
    name="trading.quant_features",
    df=features_df,  # ticker, date, rsi_14, macd, atr_14, vol_ratio_20d, ...
    mode="merge"
)

# read in decision loop
features = fe.read_table("trading.quant_features")
```

### MLflow (Prompt Version Tracking)

Every prompt change is tracked as an MLflow experiment run. After two weeks of live decisions under a new prompt, log the resulting metrics to compare versions directly in the MLflow UI:

```python
import mlflow

with mlflow.start_run(run_name="prompt_v3_sector_headwinds"):
    mlflow.log_param("prompt_version", "v3")
    mlflow.log_param("change", "added sector headwind instruction")
    mlflow.log_param("model", "claude-sonnet-4-6")
    mlflow.log_metric("llm_confirm_win_rate", 0.61)
    mlflow.log_metric("profit_factor", 1.72)
    mlflow.log_metric("llm_contradict_accuracy", 0.78)
```

### FinBERT Model Serving

FinBERT is deployed as a Databricks Model Serving endpoint — available to DLT, the position monitor, and any future component without a local dependency:

```python
# deploy once
mlflow.pyfunc.log_model("finbert", python_model=FinBERTWrapper())
mlflow.register_model("runs:/abc123/finbert", "finbert-sentiment")

# call from anywhere
import requests
response = requests.post(
    "https://{workspace}.databricks.com/serving-endpoints/finbert/invocations",
    json={"inputs": [{"text": article_summary}]}
)
score = response.json()["predictions"][0]
```

### Databricks SQL Dashboard (Weekly Review)

Performance metrics are queried live against Delta tables in Databricks SQL — no scripts needed for the weekly review:

```sql
SELECT
    quant_action,
    llm_verdict,
    COUNT(*)                                                              AS trades,
    ROUND(AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0.0 END), 3)             AS win_rate,
    ROUND(AVG(pnl_pct), 4)                                              AS avg_pnl_pct,
    ROUND(
        SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) /
        NULLIF(ABS(SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END)), 0), 2
    )                                                                    AS profit_factor
FROM trading.decisions
WHERE action_taken = 'BUY'
GROUP BY quant_action, llm_verdict
ORDER BY profit_factor DESC;
```

### Genie (Natural Language Querying)

Databricks Genie lets you query performance data in plain English against Delta tables — useful for ad hoc questions during the weekly review without writing SQL:

- "Which tickers had the best profit factor last month?"
- "Show me all trades where the LLM flagged a risk but we traded anyway"
- "What was our alpha vs SPY in Q1?"

---

## Databricks Workflows (Orchestration)

Replace cron with Databricks Workflows for dependency management, automatic retries, and alerting:

```
Workflow: Nightly Pipeline (11:00pm)
│
├── Task: refresh_dlt_pipeline        (runs first — bars, news, fundamentals)
├── Task: score_sentiment             (depends on refresh_dlt_pipeline)
├── Task: update_feature_store        (depends on refresh_dlt_pipeline)
├── Task: run_tier2_screen            (depends on update_feature_store)
└── Task: run_tier3_event_triggers    (depends on refresh_dlt_pipeline)

Workflow: Morning Decision Loop (9:45am)
│
├── Task: build_context_cache         (batch pull from Delta — pays latency once)
├── Task: run_quant_signals           (depends on build_context_cache)
└── Task: invoke_llm_and_execute      (depends on run_quant_signals)
```

If any upstream task fails, downstream tasks are blocked and an alert fires — you're not discovering a data gap at 9:45am.

---

## The Full Agent Loop

### Nightly Pipeline
1. DLT refreshes price bars, news (with FinBERT scores), fundamentals → Delta tables
2. Feature Store updated with computed indicators for full universe
3. Tier 2 screen runs in parallel across 500+ tickers using Spark
4. Tier 3 event triggers checked (earnings surprises, volume spikes, 8-K filings)
5. Candidate list written to Delta for morning decision loop

### Decision Loop (9:45am)

One batch pull from Delta at startup to avoid per-ticker query latency:

```python
async def run_decision_loop(watchlist: list[str]) -> None:
    # batch pull — pays Delta latency once, not per ticker
    context_cache = spark.sql("""
        SELECT
            p.ticker, p.close, p.rsi_14, p.macd, p.atr_14, p.vol_ratio_20d,
            n.headline, n.sentiment_score, n.published_at,
            f.eps, f.revenue, f.guidance
        FROM trading.price_bars p
        LEFT JOIN trading.news n
            ON p.ticker = n.ticker
            AND n.published_at > DATEADD(HOUR, -48, CURRENT_TIMESTAMP())
        LEFT JOIN trading.fundamentals f ON p.ticker = f.ticker
        WHERE p.date = CURRENT_DATE()
        AND p.ticker IN ({tickers})
    """.format(tickers=",".join(f"'{t}'" for t in watchlist))).collect()

    for row in context_cache:
        signal = compute_quant_signal(row)
        if signal["action"] == "HOLD":
            log_decision(row.ticker, "HOLD", "quant filter: no signal")
            continue

        verdict = await invoke_llm(row.ticker, row, signal)
        action = resolve_action(signal, verdict)

        if action["execute"] and pre_trade_checks(row.ticker, action, portfolio):
            size = compute_position_size(action, portfolio, row.ticker)
            place_order(row.ticker, action["side"], size)
            set_stop_loss(row.ticker, size, action["entry_price"])
            log_decision(row.ticker, signal, verdict, action)
```

### LLM Invocation

Claude is invoked via tool use so output is always structured — never free-form text:

```python
tools = [{
    "name": "submit_verdict",
    "input_schema": {
        "type": "object",
        "properties": {
            "verdict":    {"type": "string", "enum": ["CONFIRM", "CONTRADICT", "UNCERTAIN"]},
            "confidence": {"type": "number"},
            "thesis":     {"type": "string"},
            "risk_flags": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["verdict", "confidence", "thesis", "risk_flags"]
    }
}]
```

**Prompt structure:**
```
TICKER: {ticker}
QUANT SIGNAL: {action} (confidence: {confidence})
SIGNAL REASON: {reason}
CURRENT POSITION: {open_position or None}

RECENT NEWS (last 48h):
{news_summary}  ← pre-filtered by FinBERT score, only material articles

FUNDAMENTALS:
{latest_earnings_and_guidance}

RULES:
- Never invent prices or financial data
- Flag any news that contradicts the signal as a risk_flag
- If uncertain, say UNCERTAIN — do not force a verdict
- Consider whether an open position changes the risk profile
- Pay attention to sector-wide headwinds that may affect this stock
  even if company-specific news is positive
```

### Position Monitor (every 30 min — cloud function, not Databricks)

The position monitor runs as a lightweight cloud function during market hours. Databricks cluster startup time makes it unsuitable for 30-minute polling:

```python
async def monitor_positions() -> None:
    for position in get_open_positions():
        price = fetch_live_quote(position.ticker)

        # hard stop — no LLM involved, pure math
        if price <= position.stop_loss_price:
            place_order(position.ticker, "SELL", position.quantity)
            log_exit(position.ticker, "STOP_LOSS_HIT", price)
            continue

        # breaking bad news on a held position
        breaking = fetch_news_since(position.ticker, position.entry_time)
        if any(a["sentiment_score"] < -0.7 for a in breaking):
            verdict = await invoke_llm_exit_check(position, breaking)
            if verdict["verdict"] == "EXIT":
                place_order(position.ticker, "SELL", position.quantity)
                log_exit(position.ticker, "LLM_EXIT", price)
```

---

## Disagreement Between Layers

| Quant | LLM | Action |
|---|---|---|
| BUY | CONFIRM | Full trade |
| BUY | CONTRADICT | Skip — LLM veto is load-bearing (catches news quant can't see) |
| BUY | UNCERTAIN | Reduced size (60%) |
| HOLD | CONFIRM | Reduced size (40%) — event/catalyst not yet in price |
| HOLD | CONTRADICT | Skip |

Exception: LLM-only trades (HOLD + CONFIRM) are allowed for pre-earnings positioning or news catalysts not yet priced in, at reduced position size with a tighter stop.

---

## Position Sizing & Risk Management

### Per-Trade Sizing
```python
MAX_POSITION_PCT  = 0.05   # never more than 5% of portfolio in one stock
BASE_POSITION_PCT = 0.02   # default is 2%

MULTIPLIERS = {
    ("HIGH", "CONFIRM"):    1.5,
    ("HIGH", "UNCERTAIN"):  0.75,
    ("LOW",  "CONFIRM"):    0.75,
    ("HIGH", "CONTRADICT"): 0.0,
}
```

### Volatility Adjustment
```python
TARGET_DAILY_RISK = 0.005  # risk no more than 0.5% of portfolio on one trade's daily move
vol_adjusted_size = (portfolio_value * TARGET_DAILY_RISK) / daily_volatility
position_size = min(base_size, vol_adjusted_size)
```

### Tier Discount
```python
TIER_SIZE_MULTIPLIER = {"tier1": 1.0, "tier2": 0.6, "tier3": 0.4}
position_size = position_size * TIER_SIZE_MULTIPLIER[ticker_tier]
```

Tier 3 discoveries earn larger sizing as they build a track record. Promote to Tier 2 or Tier 1 manually after validated performance.

### Stop-Losses
- Set as a broker order at entry — not monitored by the LLM
- Default: ATR-based (2x 14-day ATR below entry price)
- Tighter stop on lower-confidence trades
- Never modified or overridden by the LLM

### Portfolio-Level Guardrails
```python
RISK_LIMITS = {
    "max_open_positions":      10,
    "max_sector_exposure":     0.25,
    "max_correlated_exposure": 0.30,
    "daily_loss_limit":        0.02,   # halt if portfolio drops 2% in a day
    "max_drawdown":            0.10,   # halt and review if down 10% from peak
}
```

### Daily Halt
If the daily loss limit is hit, all trading stops and requires manual re-enable. The LLM cannot override this. Automation cannot restart itself after a halt.

---

## Performance Evaluation

### Core Metrics
| Metric | Target | What It Tells You |
|---|---|---|
| Win rate | > 50% | Directional accuracy |
| Profit factor | > 1.5 | Total gains / total losses |
| Sharpe ratio | > 1.5 | Risk-adjusted return |
| Max drawdown | < 10% | Worst peak-to-trough loss |
| Alpha vs SPY | > 0 | Return above benchmark |

### System-Specific Diagnostics
```python
"llm_confirm_win_rate"      # if < 55%, LLM is adding noise not signal
"llm_contradict_accuracy"   # how often the LLM veto was correct
"tier3_discovery_win_rate"  # whether event-driven discovery is working
"by_signal_combination"     # which quant+llm pairs perform best
```

### Weekly Review Loop
1. Pull last week's closed trades from Databricks SQL dashboard
2. Read `llm_thesis` for every loser — find patterns in what Claude missed
3. Check `llm_contradict_accuracy` — are the vetoes justified?
4. Review trades blocked by risk rules — too conservative?
5. Update prompt, log new version in MLflow, track metric change over next two weeks

### Paper to Live Checklist
```
✅ 50+ closed trades
✅ Positive alpha vs SPY over 3+ months
✅ Profit factor > 1.5
✅ Max drawdown stayed within defined limit
✅ Daily halt triggered < once per month
✅ LLM confirm win rate > 55%

❌ Most returns from 1-2 huge wins (luck, not system)
❌ Performance degraded month-over-month (signal decaying)
❌ Win rate looks good but profit factor < 1.2
```

When going live: start at 10% of intended capital, run paper and live in parallel for the first month.

---

## Build Order

Build and validate each phase before moving to the next.

### Phase 1 — Databricks Foundation + Data Pipeline
- [x] Set up `trading_bd` schema and all six Delta tables in `bootcamp_students`
- [x] GitHub repo created: https://github.com/bkdonnel/ai-trading-bot
- [x] CI pipeline: gitleaks secret scan + lint + type check + tests
- [x] Git Folders connected in Databricks for deployment (path: `/Workspace/Users/bryankdonnelly@comcast.net/ai-trading-bot/`)
- [x] Centralized secrets management via `src/data/config.py`
- [x] API accounts set up + keys added to Databricks secrets scope `trading_bd`
  - alpaca_api_key, alpaca_secret_key, polygon_api_key, anthropic_api_key, fmp_api_key added
  - fmp_api_key is valid but free tier does not cover required endpoints — upgrade to Starter plan before Phase 3
- [x] Data fetchers: `src/data/fetchers/polygon.py` (bars + news), `src/data/fetchers/fmp.py`
- [x] Technical indicators: `src/data/indicators.py` (RSI-14, MACD, ATR-14, vol_ratio_20d via `ta` library + applyInPandas)
- [x] Nightly fetch job: `jobs/fetch_market_data.py` — writes NDJSON to UC Volumes landing zone
- [x] Delta Live Tables pipeline: `pipelines/dlt_ingestion.py` — Auto Loader → bronze → silver for bars, news, fundamentals
- [x] UC Volumes created for landing zone and checkpoints (`bootcamp_students.trading_bd.landing/checkpoints`)
- [x] `databricks.yml` updated: `fetch_market_data` task runs before DLT pipeline refresh
- [x] Initial backfill run: bars and news loaded successfully (lookback_days=90); fundamentals skipped (FMP plan limitation)
- [x] DLT pipeline run successfully: `price_bars` and `news` tables populated; `fundamentals` empty (expected)
  - Indicator UDFs inlined in `dlt_ingestion.py` — do not import from `src/` in applyInPandas (worker nodes cannot resolve it)
  - `ta` library installed via `%pip install ta` in first notebook cell
- [ ] Deploy FinBERT as Databricks Model Serving endpoint
- [ ] DLT scores news sentiment via FinBERT endpoint at ingest
- [ ] Validate end-to-end nightly Workflow run (low priority — not a blocker for Phase 2)

### Phase 2 — Quant Signal Layer
- [x] RSI, MACD, ATR, volume ratio computed in DLT and stored in Delta
- [x] Feature Store setup for quant indicators (`jobs/update_feature_store.py`)
- [x] Tier 2 screen (`jobs/tier2_screen.py`) — Spark SQL parallel screen, writes to candidates table
  - Market cap filter omitted until FMP Starter plan active
  - `TIER2_UNIVERSE` (~80 additional S&P 500 tickers) added to `jobs/fetch_market_data.py`
- [x] Tier 3 event triggers (`jobs/tier3_triggers.py`) — volume spikes (3x avg) + SEC 8-K RSS
  - Earnings surprises deferred (needs FMP Starter)
- [x] Quant signal logic (`src/signals/quant.py`) — RSI/MACD/volume signal with HIGH/LOW confidence tiers
- [x] Signal logging to `bootcamp_students.trading_bd.decisions` Delta table
  - `jobs/build_context_cache.py` — batch context pull at 9:45am (Tier 1 + candidates)
  - `jobs/run_quant_signals.py` — applies quant signal, writes PENDING decisions (LLM fields null until Phase 3)

### Phase 3 — LLM Reasoning Layer
- [x] Anthropic API integration via direct `requests` call (SDK avoided — pydantic-core conflicts with Databricks cluster's system `typing_extensions`)
- [x] Batch context pull from Delta for PENDING tickers only (news headlines + fundamentals)
- [x] Prompt builder (`src/llm/prompt.py`) — assembles technicals, news, fundamentals into structured prompt
- [x] Tool-use structured output (`submit_verdict`) — `src/llm/claude.py`
- [x] Agreement gate logic (`src/llm/agreement.py`) — BUY+CONFIRM=full, BUY+UNCERTAIN=60%, all CONTRADICT=SKIP
- [x] `jobs/decision_loop.py` — reads PENDING decisions, calls Claude, MERGEs verdicts into decisions table
- [ ] MLflow experiment setup for prompt version tracking (deferred — mlflow pip install also triggers pydantic-core conflict on this cluster)
- [ ] End-to-end test with live market data (pending: update `anthropic_api_key` secret with DataExpert proxy key `sk-de...`)

### Phase 4 — Execution & Risk
- [ ] Alpaca order placement
- [ ] Position sizing (volatility-adjusted + tier discount)
- [ ] Stop-loss orders set at entry via Alpaca
- [ ] Portfolio-level pre-trade checks
- [ ] Daily halt logic

### Phase 5 — Monitoring & Improvement
- [ ] Position monitor as cloud function (every 30 min, market hours)
- [ ] Breaking news exit check via FinBERT endpoint
- [ ] Databricks SQL dashboard (win rate, profit factor, signal combinations)
- [ ] Genie setup for natural language performance queries
- [ ] Weekly review workflow in notebook

### Phase 6 — Live Trading
- [ ] Paper trade 50+ decisions, validate all metrics
- [ ] Go live at 10% capital
- [ ] Run paper + live in parallel for first month
- [ ] Scale up once live performance matches paper
- [ ] Add Databricks Vector Search when 6+ months of history accumulated

---

## Key Rules (Never Break These)

1. Stop-losses are set in code as broker orders, not in prompts. The LLM cannot modify or override them.
2. Never trust LLM-generated prices or financial figures. Always fetch from APIs.
3. The daily halt requires manual re-enable. Automation cannot restart itself after a halt.
4. Tier 3 discoveries get 40% position size until they build a track record.
5. Paper trade first. The exact same code runs against Alpaca paper mode.
6. Log every decision with full reasoning to `trading.decisions`. You cannot improve what you did not capture.
7. The position monitor runs as a cloud function — never on Databricks (cluster latency is too high for 30-min polling).
8. Order execution goes directly to Alpaca — never routed through Databricks.

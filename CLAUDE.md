# Project Context

## Environment
- **Databricks workspace:** `https://dbc-7b106152-caf3.cloud.databricks.com` (AWS)
- **Cluster:** DataExpert All Purpose
- **Catalog:** `bootcamp_students`
- **Schema:** `trading_bd`
- **Full table path:** `bootcamp_students.trading_bd.<table>`
- **Secrets scope:** `trading_bd`
- **Deployment:** Git Folders — push to GitHub, then manually pull in Databricks UI
- **Git Folder path:** `/Workspace/Users/bryankdonnelly@comcast.net/ai-trading-bot/` (under Users, not Repos)

## Secrets
All API keys are stored in the Databricks secrets scope `trading_bd`.
Read them via `src/data/config.py` — never hardcode keys in any file.
Locally, set the corresponding environment variable (e.g. `POLYGON_API_KEY`).

Current secrets in scope:
- `alpaca_api_key` / `alpaca_secret_key` — Alpaca paper trading
- `polygon_api_key` — Polygon.io market data + news
- `anthropic_api_key` — Direct Anthropic API key (`sk-ant-...`). API URL: `https://api.anthropic.com/v1/messages`
- `fmp_api_key` — Financial Modeling Prep (upgraded to Starter plan 2026-06-22; covers earnings-surprises, price-target-consensus, analyst-stock-recommendations)

## File Storage
This workspace has Unity Catalog enabled with DBFS access restricted.
Use Unity Catalog Volumes for all file I/O — never `dbfs:/` paths.
- Landing zone: `/Volumes/bootcamp_students/trading_bd/landing/`
- Checkpoints: `/Volumes/bootcamp_students/trading_bd/checkpoints/`
Use Python `open()` and `os.makedirs()` for Volume file operations — not `dbutils.fs`.

## Notebook Imports
Databricks notebooks need this at the top to import from `src/`:
```python
import sys
sys.path.insert(0, "/Workspace/Users/bryankdonnelly@comcast.net/ai-trading-bot")
```

## DLT Pipelines
Do not import from `src/` inside `applyInPandas` UDFs in DLT pipelines. Worker nodes cannot resolve the `src` module via `sys.path` — only the driver node can. Define any pandas UDF functions inline in the pipeline notebook instead. The `sys.path.insert` is only needed if the driver itself needs to import from `src/`.

## Linting (ruff)
`pipelines/` is excluded from ruff entirely — Databricks `%pip` magic commands are not valid Python syntax and cause ruff to fail before per-file-ignores can apply. Do not add `pipelines/` back to ruff scope. `jobs/` is linted but with `F821` ignored (spark, dbutils injected at runtime).

## Ephemeral Tables
Some tables are created and overwritten by notebooks rather than pre-created in `setup_schema.py`. This is intentional for transient data:
- `context_cache` — overwritten each morning by `jobs/build_context_cache.py`; not in `setup_schema.py`

Use `mode("overwrite").option("overwriteSchema", "true")` when writing these.

## Databricks Account Limitations

This is an educational account via DataExpert.io. Two limitations apply:

- **No personal access tokens (PAT)** — cannot be generated. Any code that calls Databricks REST APIs from outside the workspace (SQL Statement Execution API, Files API, DBFS API) will not work. External processes must be fully Alpaca-native or use other non-Databricks storage.
- **No MLflow pip install** — triggers pydantic-core conflict (see cluster constraints below).

## Position Monitor — External Process Pattern

The position monitor runs outside Databricks (GitHub Actions, every 30 min during market hours). Because no PAT is available, it cannot query Delta tables directly. It is fully Alpaca-native:

- **Position state**: `GET /v2/positions` (open holdings) + `GET /v2/orders?status=open` (find GTC stop orders by ticker)
- **Entry time**: `GET /v2/orders?status=closed` — find most recent filled buy order per ticker
- **News**: Polygon.io news API (`/v2/reference/news?ticker=X&published_utc.gte=<entry_time>`)
- **Sentiment**: `src/monitor/finbert.py` keyword scorer by default; FinBERT endpoint opt-in via `FINBERT_ENDPOINT_URL` + `FINBERT_TOKEN` env vars
- **Exit check**: Claude via direct Anthropic API (`src/monitor/exits.py` — `submit_exit_verdict` tool, EXIT/HOLD)
- **Execution**: place market sell via Alpaca if Claude returns EXIT

Exit data is written back to Delta by `jobs/reconcile_exits.py` — a nightly Databricks job that matches Alpaca filled sell orders to open decisions and updates `exit_price`, `exit_reason`, `pnl`, `pnl_pct`, `hold_days`.

**GitHub Actions secrets required** (Settings → Secrets and variables → Actions):
`ALPACA_API_KEY`, `ALPACA_SECRET_KEY`, `POLYGON_API_KEY`, `ANTHROPIC_API_KEY`
Optional: `FINBERT_ENDPOINT_URL`, `FINBERT_TOKEN`

## Halt Flag

Trading halt state is persisted as a plain file — not a Delta table — so it is readable by both the Databricks job and future external processes without requiring a PAT:
- Path: `/Volumes/bootcamp_students/trading_bd/landing/system_state/halt.flag`
- If the file exists, trading is halted. `decision_loop.py` checks this at startup.
- Set via `src/execution/risk.py:set_halt(reason)`. Clear manually via `clear_halt()`.
- Never add automation to clear the halt — it requires manual intervention by design.

## Databricks Cluster — Dependency Constraints

The "DataExpert All Purpose" cluster has a system `typing_extensions` that is too old to support `deprecated` (needs 4.5.0+) or `Sentinel` (needs 4.12.2+). The system path takes precedence over pip-installed packages, so upgrading via `%pip install` does not fix it.

**Consequences:**
- Do NOT `%pip install anthropic` in notebooks — pydantic-core (a transitive dep) requires modern typing_extensions and will fail
- Do NOT `%pip install mlflow` in notebooks — same transitive pydantic-core conflict
- Use `requests` (pre-installed) to call the Anthropic API directly instead of the SDK — see `src/llm/claude.py`
- MLflow is removed from `jobs/decision_loop.py` for now; add it back only after resolving the cluster environment

**Databricks Workflows — per-task Environment setting:** Each task in a Workflow job can independently be set to either the job-level **Default** environment or its own **Notebook Environment** (a leftover, per-notebook environment that can carry stale pip dependencies from earlier manual/interactive testing). None of the `jobs/*.py` notebooks need any declared dependencies beyond what Databricks preinstalls (`requests`, `pyspark`, `delta-spark`) — every task should be set to Default. If a single task fails with a "Library installation failed... serverless compute" / `pydantic-core` error while sibling tasks in the same job succeed, check that task's "Environments and libraries" setting first — it is likely pinned to Notebook Environment instead of Default.

**Test PENDING decision:** To test `decision_loop.py` without waiting for a market day, insert a row via Spark SQL:
```python
import uuid
from datetime import datetime, timezone
decision_id = str(uuid.uuid4())
ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"""
    INSERT INTO bootcamp_students.trading_bd.decisions
    (id, timestamp, ticker, tier, quant_action, quant_confidence, quant_reason,
     llm_verdict, llm_confidence, llm_thesis, llm_risk_flags,
     action_taken, skip_reason, entry_price, position_size, stop_loss_price,
     exit_price, exit_reason, pnl, pnl_pct, hold_days, prompt_version)
    VALUES (
        '{decision_id}', TIMESTAMP('{ts}'), 'AAPL', 'tier1', 'BUY', 0.85,
        'RSI 28.5 deeply oversold, volume 2.1x 20d avg, MACD +0.0421',
        NULL, NULL, NULL, NULL, 'PENDING', NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL
    )
""")
```

## API Rate Limits
Tier 2 universe tickers (~80 stocks): fetch bars nightly but do NOT fetch news. News is only fetched for Tier 1 (20 stocks) to stay within Polygon API call budgets. Tier 3 SEC 8-K detection uses EDGAR RSS instead of the news API.

SEC EDGAR HTTP requests require a descriptive `User-Agent` header — not a generic string. Use: `{"User-Agent": "ai-trading-bot bryankdonnelly@comcast.net"}`

---

# Coding Standards & Conventions

## General Python
- Use functional, declarative programming — avoid classes where possible
- Prefer iteration and modularization over code duplication
- Use descriptive variable names with auxiliary verbs (e.g., `is_active`, `has_permission`)
- Use lowercase with underscores for directories and files (e.g., `routers/user_routes.py`)
- Favor named exports for routes and utility functions
- Use the Receive an Object, Return an Object (RORO) pattern
- Use `def` for pure/synchronous functions and `async def` for asynchronous operations
- Type hints required on all function signatures
- Prefer Pydantic `BaseModel` over raw dictionaries for input validation
- No emojis or symbols in any Python files

## Code Style
- Avoid unnecessary curly braces in conditional statements
- Omit curly braces for single-line conditionals
- Use concise one-line syntax for simple conditionals (e.g., `if condition: do_something()`)

## File Structure
Each module should follow this order:
1. Exported router
2. Sub-routes
3. Utilities
4. Static content
5. Types (models, schemas)

## FastAPI
- Use functional components (plain functions) and Pydantic models for validation and response schemas
- Use declarative route definitions with explicit return type annotations
- Prefer lifespan context managers over `@app.on_event("startup")` / `@app.on_event("shutdown")`
- Use middleware for logging, error monitoring, and performance optimization
- Use `HTTPException` for expected errors modeled as specific HTTP responses
- Use middleware for unexpected errors, logging, and error monitoring
- Refer to FastAPI docs for Data Models, Path Operations, and Middleware best practices

## Database & Async
- Use async functions for all I/O-bound tasks (database calls, external API requests)
- Minimize blocking I/O operations — async required for all DB and external requests
- Preferred async DB libraries: `asyncpg` or `aiomysql`
- Use SQLAlchemy 2.0 for ORM features when needed

## Performance
- Implement caching for static and frequently accessed data (Redis or in-memory)
- Use lazy loading for large datasets and substantial API responses
- Optimize data serialization/deserialization with Pydantic
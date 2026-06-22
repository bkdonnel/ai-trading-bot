# Databricks notebook source
# Tier 3 event-driven discovery — runs nightly after the DLT pipeline refresh.
#
# Triggers implemented:
#   1. Volume spike  — tickers with vol_ratio_20d > 3.0 (3x 20-day average)
#   2. SEC 8-K filings — material events from EDGAR RSS for universe tickers
#
# Earnings surprises (beat/miss > 5%) are deferred until FMP Starter plan is active.

# COMMAND ----------

import xml.etree.ElementTree as ET
from datetime import date

import requests
from pyspark.sql.types import (
    DateType, StringType, StructField, StructType,
)

CATALOG = "bootcamp_students"
SCHEMA  = "trading_bd"
TODAY   = date.today()

SEC_RSS_URL     = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type=8-K&dateb=&owner=include"
    "&count=40&search_text=&output=atom"
)
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
# SEC requires a descriptive User-Agent (not a generic string)
SEC_HEADERS     = {"User-Agent": "ai-trading-bot bryankdonnelly@comcast.net"}

_CANDIDATE_SCHEMA = StructType([
    StructField("ticker",             StringType(), False),
    StructField("screen_date",        DateType(),   False),
    StructField("tier",               StringType(), True),
    StructField("trigger_reason",     StringType(), True),
    StructField("tier3_event",        StringType(), True),
    StructField("tier3_event_detail", StringType(), True),
])

# COMMAND ----------
# Clear today's existing Tier 3 candidates (idempotent)

spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.candidates
    WHERE screen_date = DATE('{TODAY}') AND tier = 'tier3'
""")

# COMMAND ----------
# Trigger 1 — Volume spike detection
#
# Tickers where the latest bar's vol_ratio_20d > 3.0, not already in
# today's Tier 2 candidates (avoid duplicating the same ticker in both tiers).

volume_spike_df = spark.sql(f"""
    WITH ranked AS (
        SELECT
            ticker, date, volume, vol_ratio_20d,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.price_bars
        WHERE rsi_14 IS NOT NULL
    ),
    tier2_today AS (
        SELECT ticker FROM {CATALOG}.{SCHEMA}.candidates
        WHERE screen_date = DATE('{TODAY}') AND tier = 'tier2'
    )
    SELECT
        r.ticker,
        DATE('{TODAY}')                                                 AS screen_date,
        'tier3'                                                         AS tier,
        CONCAT('Volume spike: ', ROUND(r.vol_ratio_20d, 1),
               'x 20-day average')                                      AS trigger_reason,
        'VOLUME_SPIKE'                                                  AS tier3_event,
        CONCAT(ROUND(r.vol_ratio_20d, 2), 'x on ', CAST(r.date AS STRING))
                                                                        AS tier3_event_detail
    FROM ranked r
    LEFT JOIN tier2_today t2 ON r.ticker = t2.ticker
    WHERE r.rn = 1
      AND r.vol_ratio_20d > 3.0
      AND t2.ticker IS NULL
""")

spike_count = volume_spike_df.count()
print(f"Volume spikes: {spike_count} tickers")
volume_spike_df.show(truncate=False)

if spike_count > 0:
    volume_spike_df.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA}.candidates"
    )

# COMMAND ----------
# Trigger 2 — SEC 8-K filing detection
#
# Fetch the 40 most recent 8-K filings from EDGAR RSS and match CIKs against
# tickers in our current universe (any ticker in price_bars).

universe: set[str] = {
    row.ticker
    for row in spark.sql(
        f"SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.price_bars"
    ).collect()
}
print(f"Universe: {len(universe)} tickers in price_bars")

# COMMAND ----------
# Build CIK → ticker mapping using SEC's public company tickers JSON

cik_to_ticker: dict[str, str] = {}
try:
    resp = requests.get(SEC_TICKERS_URL, headers=SEC_HEADERS, timeout=15)
    resp.raise_for_status()
    for entry in resp.json().values():
        ticker = str(entry.get("ticker", "")).upper()
        cik    = str(entry.get("cik_str", "")).zfill(10)
        if ticker in universe:
            cik_to_ticker[cik] = ticker
    print(f"CIK→ticker map: {len(cik_to_ticker)} entries matched to universe")
except Exception as exc:
    print(f"WARNING: could not fetch SEC company tickers — {exc}")

# COMMAND ----------
# Parse the EDGAR 8-K Atom RSS feed and match to universe tickers

sec_rows: list[tuple] = []
try:
    resp = requests.get(SEC_RSS_URL, headers=SEC_HEADERS, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns   = {"atom": "http://www.w3.org/2005/Atom"}

    for entry in root.findall("atom:entry", ns):
        title   = (entry.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = entry.find("atom:link", ns)
        link    = link_el.get("href", "") if link_el is not None else ""

        # CIK appears in the href as CIK=0001234567
        cik = ""
        if "CIK=" in link:
            cik = link.split("CIK=")[1].split("&")[0].zfill(10)

        ticker = cik_to_ticker.get(cik)
        if not ticker:
            continue

        sec_rows.append((
            ticker,
            TODAY,
            "tier3",
            f"SEC 8-K: {title[:120]}",
            "SEC_8K",
            link[:500],
        ))
        print(f"  8-K match: {ticker} — {title[:80]}")

    print(f"SEC 8-K: {len(sec_rows)} filings matched to universe")

except Exception as exc:
    print(f"WARNING: could not parse SEC 8-K RSS — {exc}")

# COMMAND ----------

if sec_rows:
    sec_df = spark.createDataFrame(sec_rows, schema=_CANDIDATE_SCHEMA)
    # Deduplicate: one row per ticker (multiple 8-Ks collapse to the first match)
    sec_df = sec_df.dropDuplicates(["ticker", "screen_date", "tier3_event"])
    sec_df.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA}.candidates"
    )
    print(f"Wrote {len(sec_rows)} SEC 8-K candidates")
else:
    print("No SEC 8-K matches for today's universe")

# COMMAND ----------

total = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.candidates
    WHERE screen_date = DATE('{TODAY}') AND tier = 'tier3'
""").collect()[0].n

print(f"Tier 3 total for {TODAY}: {total} candidates")

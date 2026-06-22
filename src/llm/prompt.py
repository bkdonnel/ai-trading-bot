from typing import Any

PROMPT_VERSION = "v1"


def _fmt_pct(val: float | None) -> str:
    if val is None:
        return "N/A"
    return f"{val:+.1%}"


def _fmt_float(val: float | None, decimals: int = 2) -> str:
    if val is None:
        return "N/A"
    return f"{val:.{decimals}f}"


def _format_news(news_rows: list[dict[str, Any]]) -> str:
    if not news_rows:
        return "  No recent news"
    lines = []
    for row in news_rows[:10]:
        score = row.get("sentiment_score")
        score_str = f"{score:+.2f}" if score is not None else "unscored"
        lines.append(f"  [{score_str}] {row.get('headline', '')}")
    return "\n".join(lines)


def _format_fundamentals(fund_row: dict[str, Any] | None) -> str:
    if not fund_row:
        return "  No fundamentals data available"
    lines = [
        f"  Period: {fund_row.get('period', 'N/A')}",
        f"  EPS actual: {_fmt_float(fund_row.get('eps_actual'))} (est: {_fmt_float(fund_row.get('eps_estimate'))})",
        f"  Revenue actual: {_fmt_float(fund_row.get('revenue_actual'))} (est: {_fmt_float(fund_row.get('revenue_estimate'))})",
        f"  Analyst target price: {_fmt_float(fund_row.get('analyst_target_price'))}",
        f"  Analyst rating: {fund_row.get('analyst_rating', 'N/A')}",
    ]
    if fund_row.get("guidance_sentiment"):
        lines.append(f"  Guidance: {fund_row['guidance_sentiment']}")
    return "\n".join(lines)


def build_prompt(
    ticker: str,
    context: dict[str, Any],
    signal: dict[str, Any],
    news_rows: list[dict[str, Any]],
    fund_row: dict[str, Any] | None,
) -> str:
    news_block = _format_news(news_rows)
    fund_block = _format_fundamentals(fund_row)

    return f"""TICKER: {ticker}
QUANT SIGNAL: {signal['quant_action']} (confidence: {float(signal['quant_confidence']):.0%})
SIGNAL REASON: {signal['quant_reason']}
TIER: {context.get('tier', 'tier1')}

TECHNICALS:
  Close: ${_fmt_float(context.get('close'))}
  RSI-14: {_fmt_float(context.get('rsi_14'), 1)}
  MACD: {_fmt_float(context.get('macd'), 4)}
  ATR-14: {_fmt_float(context.get('atr_14'))}
  Volume ratio (20d avg): {_fmt_float(context.get('vol_ratio_20d'), 1)}x
  5d price change: {_fmt_pct(context.get('price_change_5d'))}
  Avg news sentiment (48h, {context.get('news_count_48h', 0)} articles): {_fmt_float(context.get('avg_sentiment_48h'), 3)}

RECENT NEWS (last 48h):
{news_block}

FUNDAMENTALS:
{fund_block}

RULES:
- Never invent prices or financial data
- Flag any news that contradicts the signal as a risk_flag
- If uncertain, say UNCERTAIN — do not force a verdict
- Consider whether macro or sector-wide headwinds affect this stock even if company-specific news is positive
- Tier 2 and Tier 3 discoveries require higher conviction to confirm"""

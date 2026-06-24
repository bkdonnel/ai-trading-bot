from typing import Any

import requests

_EXIT_TOOL: dict[str, Any] = {
    "name": "submit_exit_verdict",
    "description": "Submit your verdict on whether to exit this position.",
    "input_schema": {
        "type": "object",
        "properties": {
            "verdict": {
                "type": "string",
                "enum": ["EXIT", "HOLD"],
                "description": "EXIT to sell immediately, HOLD to keep the position",
            },
            "confidence": {
                "type": "number",
                "description": "Confidence in your verdict, 0.0 to 1.0",
            },
            "reason": {
                "type": "string",
                "description": "1-2 sentence rationale",
            },
        },
        "required": ["verdict", "confidence", "reason"],
    },
}

_SYSTEM_PROMPT = (
    "You are a risk manager monitoring an open trading position. "
    "Breaking news has been flagged as potentially negative. "
    "Decide whether to exit the position to protect capital. "
    "EXIT only if the news materially changes the investment thesis or signals "
    "significant downside risk. Ignore noise, minor sentiment, and already-known risks. "
    "You must call submit_exit_verdict — no free-form responses."
)

_API_URL = "https://api.anthropic.com/v1/messages"


def build_exit_prompt(
    ticker: str,
    position: dict[str, Any],
    articles: list[dict[str, Any]],
) -> str:
    entry  = position.get("avg_entry_price", 0.0)
    price  = position.get("current_price", 0.0)
    pl     = position.get("unrealized_pl", 0.0)
    qty    = position.get("qty", 0)
    pl_pct = ((price - entry) / entry * 100) if entry else 0.0

    news_lines = "\n".join(
        f"- [{a.get('published_at', '')}] {a.get('headline', '')} | {a.get('summary', '')}"
        for a in articles
    )

    return (
        f"TICKER: {ticker}\n"
        f"POSITION: {qty} shares, entered at ${entry:.2f}, now ${price:.2f} "
        f"(unrealized {pl_pct:+.1f}%, ${pl:+.2f})\n\n"
        f"BREAKING NEWS (flagged as negative):\n{news_lines}\n\n"
        "RULES:\n"
        "- EXIT only if news materially changes the thesis or signals severe downside\n"
        "- HOLD if news is noise, sector-level chatter, or already reflected in price\n"
        "- Never invent prices or financial data\n"
    )


def parse_exit_response(response_data: dict[str, Any]) -> dict[str, Any]:
    for block in response_data.get("content", []):
        if block.get("type") == "tool_use" and block.get("name") == "submit_exit_verdict":
            inp = block["input"]
            return {
                "verdict":    inp["verdict"],
                "confidence": float(inp["confidence"]),
                "reason":     inp["reason"],
            }
    return {"verdict": "HOLD", "confidence": 0.0, "reason": "no structured verdict returned"}


def invoke_exit_check(
    ticker: str,
    position: dict[str, Any],
    articles: list[dict[str, Any]],
    anthropic_key: str,
) -> dict[str, Any]:
    prompt = build_exit_prompt(ticker, position, articles)
    response = requests.post(
        _API_URL,
        headers={
            "x-api-key": anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 512,
            "system": _SYSTEM_PROMPT,
            "tools": [_EXIT_TOOL],
            "tool_choice": {"type": "tool", "name": "submit_exit_verdict"},
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    response.raise_for_status()
    return parse_exit_response(response.json())

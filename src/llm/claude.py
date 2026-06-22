import json
from typing import Any

import requests

_SUBMIT_VERDICT_TOOL: dict[str, Any] = {
    "name": "submit_verdict",
    "description": "Submit your trading verdict for this ticker.",
    "input_schema": {
        "type": "object",
        "properties": {
            "verdict": {
                "type": "string",
                "enum": ["CONFIRM", "CONTRADICT", "UNCERTAIN"],
                "description": "CONFIRM the quant signal, CONTRADICT it, or say UNCERTAIN",
            },
            "confidence": {
                "type": "number",
                "description": "Confidence in your verdict, 0.0 to 1.0",
            },
            "thesis": {
                "type": "string",
                "description": "1-3 sentence rationale for your verdict",
            },
            "risk_flags": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of risks or red flags — empty array if none",
            },
        },
        "required": ["verdict", "confidence", "thesis", "risk_flags"],
    },
}

_SYSTEM_PROMPT = (
    "You are a quantitative trading analyst. "
    "A classical signal model has flagged a ticker for a potential trade. "
    "Your job is to vet the signal using recent news, sentiment, and fundamentals. "
    "You must call submit_verdict — no free-form responses."
)

_API_URL = "https://www.dataexpert.io/api/v1/anthropic/messages"


def invoke_llm(prompt_text: str, api_key: str) -> dict[str, Any]:
    response = requests.post(
        _API_URL,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 1024,
            "system": _SYSTEM_PROMPT,
            "tools": [_SUBMIT_VERDICT_TOOL],
            "tool_choice": {"type": "tool", "name": "submit_verdict"},
            "messages": [{"role": "user", "content": prompt_text}],
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    for block in data.get("content", []):
        if block.get("type") == "tool_use" and block.get("name") == "submit_verdict":
            inp = block["input"]
            return {
                "verdict": inp["verdict"],
                "confidence": float(inp["confidence"]),
                "thesis": inp["thesis"],
                "risk_flags": json.dumps(inp.get("risk_flags", [])),
            }

    # Should not reach here with tool_choice forced, but handle gracefully
    return {
        "verdict": "UNCERTAIN",
        "confidence": 0.0,
        "thesis": "LLM did not return a structured verdict",
        "risk_flags": "[]",
    }

from typing import Any

# Size modifier by (quant_action, llm_verdict).
# 0.0 means skip the trade entirely.
_SIZE_MODIFIER: dict[tuple[str, str], float] = {
    ("BUY",  "CONFIRM"):    1.0,
    ("BUY",  "UNCERTAIN"):  0.6,
    ("BUY",  "CONTRADICT"): 0.0,
    ("SELL", "CONFIRM"):    1.0,
    ("SELL", "UNCERTAIN"):  0.0,   # skip uncertain sells — asymmetric conservatism
    ("SELL", "CONTRADICT"): 0.0,
}


def resolve_action(quant_action: str, llm_verdict: str) -> dict[str, Any]:
    modifier = _SIZE_MODIFIER.get((quant_action, llm_verdict), 0.0)

    if modifier == 0.0:
        return {
            "action_taken": "SKIP",
            "size_modifier": 0.0,
            "skip_reason": f"quant={quant_action} llm={llm_verdict}",
        }

    return {
        "action_taken": quant_action,
        "size_modifier": modifier,
        "skip_reason": None,
    }

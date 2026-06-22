from src.llm.agreement import resolve_action


# ── BUY combinations ─────────────────────────────────────────────────────────

def test_buy_confirm_executes():
    result = resolve_action("BUY", "CONFIRM")
    assert result["action_taken"] == "BUY"
    assert result["size_modifier"] == 1.0
    assert result["skip_reason"] is None


def test_buy_uncertain_executes_reduced():
    result = resolve_action("BUY", "UNCERTAIN")
    assert result["action_taken"] == "BUY"
    assert result["size_modifier"] == 0.6
    assert result["skip_reason"] is None


def test_buy_contradict_skips():
    result = resolve_action("BUY", "CONTRADICT")
    assert result["action_taken"] == "SKIP"
    assert result["size_modifier"] == 0.0
    assert result["skip_reason"] is not None


# ── SELL combinations ────────────────────────────────────────────────────────

def test_sell_confirm_executes():
    result = resolve_action("SELL", "CONFIRM")
    assert result["action_taken"] == "SELL"
    assert result["size_modifier"] == 1.0
    assert result["skip_reason"] is None


def test_sell_uncertain_skips():
    result = resolve_action("SELL", "UNCERTAIN")
    assert result["action_taken"] == "SKIP"
    assert result["size_modifier"] == 0.0


def test_sell_contradict_skips():
    result = resolve_action("SELL", "CONTRADICT")
    assert result["action_taken"] == "SKIP"
    assert result["size_modifier"] == 0.0


# ── Edge cases ───────────────────────────────────────────────────────────────

def test_unknown_combination_skips():
    result = resolve_action("BUY", "SOMETHING_UNEXPECTED")
    assert result["action_taken"] == "SKIP"
    assert result["size_modifier"] == 0.0


def test_skip_reason_includes_both_signals():
    result = resolve_action("BUY", "CONTRADICT")
    assert "BUY" in result["skip_reason"]
    assert "CONTRADICT" in result["skip_reason"]


# ── Output structure ─────────────────────────────────────────────────────────

def test_output_always_has_required_keys():
    for quant, llm in [
        ("BUY", "CONFIRM"),
        ("BUY", "UNCERTAIN"),
        ("BUY", "CONTRADICT"),
        ("SELL", "CONFIRM"),
        ("SELL", "UNCERTAIN"),
        ("SELL", "CONTRADICT"),
    ]:
        result = resolve_action(quant, llm)
        assert "action_taken" in result
        assert "size_modifier" in result
        assert "skip_reason" in result
        assert result["action_taken"] in ("BUY", "SELL", "SKIP")
        assert 0.0 <= result["size_modifier"] <= 1.0

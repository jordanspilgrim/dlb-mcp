"""dlb-session-recover hook + `dlb-mcp setup` wiring."""

from __future__ import annotations

import json

from dlb_mcp import recover_hook, setup, store

# ── recover_hook ─────────────────────────────────────────────────────────────


def test_recover_hook_lists_registered_names(capsys):
    store.register("program-manager")
    store.register("deployment-lead")
    assert recover_hook.registered_names() == ["deployment-lead", "program-manager"]

    recover_hook.main()
    out = capsys.readouterr().out
    assert "recover_token" in out
    assert "- program-manager" in out
    assert "- deployment-lead" in out


def test_recover_hook_is_silent_when_no_names(capsys):
    recover_hook.main()
    assert capsys.readouterr().out == ""


# ── setup.wire_session_hook ──────────────────────────────────────────────────


def test_setup_creates_and_wires_fresh_file(tmp_path):
    p = tmp_path / "settings.json"
    changed, msg = setup.wire_session_hook(p)
    assert changed and "wired" in msg
    data = json.loads(p.read_text())
    cmd = data["hooks"]["SessionStart"][0]["hooks"][0]["command"]
    assert cmd == "dlb-session-recover"


def test_setup_is_idempotent(tmp_path):
    p = tmp_path / "settings.json"
    setup.wire_session_hook(p)
    changed, msg = setup.wire_session_hook(p)
    assert not changed and "already wired" in msg
    # Exactly one SessionStart entry — no duplicate.
    data = json.loads(p.read_text())
    assert len(data["hooks"]["SessionStart"]) == 1


def test_setup_preserves_existing_hooks(tmp_path):
    p = tmp_path / "settings.json"
    p.write_text(json.dumps({"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": []}]}}))
    changed, _ = setup.wire_session_hook(p)
    assert changed
    data = json.loads(p.read_text())
    assert "PreToolUse" in data["hooks"]  # untouched
    assert "SessionStart" in data["hooks"]  # added


def test_setup_refuses_to_clobber_invalid_json(tmp_path):
    p = tmp_path / "settings.json"
    p.write_text("{ not valid json ")
    changed, msg = setup.wire_session_hook(p)
    assert not changed and "not valid JSON" in msg
    assert p.read_text() == "{ not valid json "  # untouched


def test_setup_backs_up_before_write(tmp_path):
    p = tmp_path / "settings.json"
    p.write_text(json.dumps({"permissions": {"allow": []}}))
    setup.wire_session_hook(p)
    assert (tmp_path / "settings.json.dlb-bak").exists()

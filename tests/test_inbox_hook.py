"""dlb-inbox-check hook and the three-hook `dlb-mcp setup` wiring.

Runs against the isolated tmp store from conftest, never ~/.dlb.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from dlb_mcp import inbox_hook, setup, store


def _reg(name: str) -> str:
    return store.register(name)["session_token"]


# --- unread_counts / render -----------------------------------------------------


def test_missing_store_reports_nothing() -> None:
    assert not store.store_path().exists()
    assert inbox_hook.unread_counts() == []
    # Read-only: it must not create the store either.
    assert not store.store_path().exists()


def test_counts_unread_work_per_recipient() -> None:
    _reg("a")
    b = _reg("b")
    store.send("a", "1")
    store.send("a", "2")
    store.send("b", "3")
    store.read("b", b)
    assert inbox_hook.unread_counts() == [("a", 2)]


def test_receipts_cancelled_and_expired_are_not_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    boss = _reg("boss")
    worker = _reg("worker")
    t = store.send("worker", "task", session_token=boss, msg_type="task")
    store.read("worker", worker)  # drops a receipt into boss's inbox
    doomed = store.send("worker", "cancel me", session_token=boss)
    store.cancel_task(doomed.id, boss)
    store.send("worker", "short-lived", expires_in_seconds=1)
    store.send("worker", "live")
    assert t.id  # sanity

    real = store._now_ms()
    monkeypatch.setattr(inbox_hook.time, "time", lambda: (real + 5_000) / 1000)
    assert inbox_hook.unread_counts() == [("worker", 1)]


def test_tolerates_pre_v9_store() -> None:
    _reg("a")
    store.send("a", "old-style")
    with sqlite3.connect(store.store_path()) as conn:
        conn.execute("ALTER TABLE messages DROP COLUMN cancelled_at_ms")
        conn.execute("ALTER TABLE messages DROP COLUMN deadline_ms")
    assert inbox_hook.unread_counts() == [("a", 1)]


def test_unreadable_store_reports_nothing() -> None:
    p = store.store_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"not a sqlite database at all")
    assert inbox_hook.unread_counts() == []


def test_render() -> None:
    assert inbox_hook.render("UserPromptSubmit", []) is None
    out = json.loads(inbox_hook.render("SessionStart", [("a", 1), ("b", 2)]))
    spec = out["hookSpecificOutput"]
    assert spec["hookEventName"] == "SessionStart"
    assert spec["additionalContext"].startswith("📬 DLB inbox status: a (1), b (2) unread.")


def test_main_prints_only_when_mail(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.argv", ["dlb-inbox-check", "SessionStart"])
    inbox_hook.main()
    assert capsys.readouterr().out == ""

    _reg("a")
    store.send("a", "hi")
    inbox_hook.main()
    out = json.loads(capsys.readouterr().out)
    assert out["hookSpecificOutput"]["hookEventName"] == "SessionStart"


# --- setup wiring -----------------------------------------------------------------


def _all_commands(data: dict, event: str) -> list[str]:
    return [h["command"] for g in data["hooks"].get(event, []) for h in g["hooks"]]


def test_setup_wires_all_three_hooks(tmp_path) -> None:
    p = tmp_path / "settings.json"
    changed, msg = setup.wire_session_hook(p)
    assert changed and msg.startswith("ok: wired DLB hooks")
    data = json.loads(p.read_text())
    assert set(_all_commands(data, "SessionStart")) == {
        "dlb-session-recover",
        "dlb-inbox-check SessionStart",
    }
    assert _all_commands(data, "UserPromptSubmit") == ["dlb-inbox-check UserPromptSubmit"]

    changed, msg = setup.wire_session_hook(p)
    assert not changed and "already wired" in msg
    assert json.loads(p.read_text()) == data


def test_setup_adds_only_missing_hooks(tmp_path) -> None:
    p = tmp_path / "settings.json"
    p.write_text(
        json.dumps(
            {
                "hooks": {
                    "SessionStart": [
                        {"hooks": [{"type": "command", "command": "/opt/bin/dlb-session-recover"}]}
                    ]
                }
            }
        )
    )
    changed, _ = setup.wire_session_hook(p)
    assert changed
    data = json.loads(p.read_text())
    assert _all_commands(data, "SessionStart") == [
        "/opt/bin/dlb-session-recover",
        "dlb-inbox-check SessionStart",
    ]


def test_legacy_sh_hook_is_noted_not_matched(tmp_path) -> None:
    p = tmp_path / "settings.json"
    legacy = "~/.claude/hooks/dlb-inbox-check.sh"
    p.write_text(
        json.dumps(
            {"hooks": {"UserPromptSubmit": [{"hooks": [{"type": "command", "command": legacy}]}]}}
        )
    )
    changed, msg = setup.wire_session_hook(p)
    assert changed
    assert "still wires the old dlb-inbox-check.sh on UserPromptSubmit" in msg
    data = json.loads(p.read_text())
    # Legacy entry left alone, real hook added alongside it.
    assert _all_commands(data, "UserPromptSubmit") == [
        legacy,
        "dlb-inbox-check UserPromptSubmit",
    ]


def test_setup_refuses_non_list_event(tmp_path) -> None:
    p = tmp_path / "settings.json"
    original = json.dumps({"hooks": {"UserPromptSubmit": {"oops": True}}})
    p.write_text(original)
    changed, msg = setup.wire_session_hook(p)
    assert not changed and "non-list 'hooks.UserPromptSubmit'" in msg
    assert p.read_text() == original

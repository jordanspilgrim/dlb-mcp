"""Regression tests for the PR #16 review findings.

Every test runs against the isolated tmp store from conftest, never ~/.dlb.
"""

from __future__ import annotations

import json
import os
import shlex
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dlb_mcp import inbox_hook, monitor, server, setup, store


def _reg(name: str) -> str:
    return store.register(name)["session_token"]


# --- serialization / timestamps ----------------------------------------------


def test_message_dict_carries_supersedes() -> None:
    a = _reg("a")
    old = store.send("b", "v1", session_token=a)
    new = store.send("b", "v2", session_token=a, supersedes=old.id)
    d = server._message_dict(new)
    assert d["supersedes"] == old.id
    assert "supersedes" not in server._message_dict(old)


def test_ms_to_dt_clamps_out_of_range() -> None:
    assert store._ms_to_dt(10**20) == datetime.max.replace(tzinfo=UTC)
    assert store._ms_to_dt(-(10**20)) == datetime.min.replace(tzinfo=UTC)
    assert store._ms_to_dt(None) is None


# --- rolling upgrade: v8 readers ---------------------------------------------


def test_store_stays_readable_by_v8_builds() -> None:
    store.init_schema()
    db = store.store_path()
    with sqlite3.connect(str(db)) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == 8
        row = c.execute("SELECT value FROM dlb_meta WHERE key='schema_version'").fetchone()
        assert int(row[0]) == 9


def test_newer_min_reader_is_refused() -> None:
    store.init_schema()
    with sqlite3.connect(str(store.store_path())) as c:
        c.execute(f"PRAGMA user_version = {store.SCHEMA_VERSION + 1}")
    with pytest.raises(store.DLBError):
        store.init_schema()


def test_stale_min_reader_stamp_is_restamped() -> None:
    store.init_schema()
    with sqlite3.connect(str(store.store_path())) as c:
        c.execute("PRAGMA user_version = 3")
    store.init_schema()
    with sqlite3.connect(str(store.store_path())) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == store.MIN_READER_VERSION


# --- inbox hook: scoping, quoting, caps --------------------------------------


def test_unread_counts_scoped_to_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_SESSION_ID", "s1")
    _reg("mine")
    monkeypatch.setenv("DLB_SESSION_ID", "s2")
    _reg("theirs")
    store.send("mine", "hi")
    store.send("theirs", "hi")
    assert inbox_hook.unread_counts("s1") == [("mine", 1)]
    assert inbox_hook.unread_counts(None) == [("mine", 1), ("theirs", 1)]


def test_unread_counts_skips_unregistered_recipients() -> None:
    _reg("real")
    store.send("real", "hi")
    store.send("ghost", "queued for a name nobody holds")
    assert inbox_hook.unread_counts(None) == [("real", 1)]


def test_huge_expiry_rejected_before_insert(monkeypatch: pytest.MonkeyPatch) -> None:
    # Even with the TTL env pushed far past the clamp, the deadline stays
    # within int64 and datetime range, and nothing is committed.
    monkeypatch.setenv("DLB_MESSAGE_TTL_DAYS", str(10**12))
    store.init_schema()
    for v in (10**12, 10**20):
        with pytest.raises(store.DLBError):
            store.send("b", "x", expires_in_seconds=v)
    with sqlite3.connect(str(store.store_path())) as c:
        assert c.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
    m = store.send("b", "x", expires_in_seconds=store.ttl_days() * 86400)
    assert m.id


def test_render_caps_listing() -> None:
    counts = [(f"n{i:03d}", 1) for i in range(inbox_hook.MAX_LISTED + 5)]
    out = render_text(inbox_hook.render("UserPromptSubmit", counts))
    assert "+5 more" in out
    assert f"n{inbox_hook.MAX_LISTED:03d}" not in out


def test_render_clips_long_names() -> None:
    long = "x" * 500
    out = render_text(inbox_hook.render("UserPromptSubmit", [(long, 1)]))
    assert long not in out
    assert "x" * (inbox_hook.MAX_NAME_CHARS - 1) + "…" in out


def test_render_quotes_hostile_unmonitored_name() -> None:
    name = 'bad name"; rm -rf ~ #'
    raw = inbox_hook.render("UserPromptSubmit", [], [name])
    assert raw is not None
    json.loads(raw)  # still valid JSON
    out = render_text(raw)
    # The command string embedded in the message is JSON-quoted, and inside it
    # the name is shell-quoted as a single argument.
    start = out.index("Monitor({command: ") + len("Monitor({command: ")
    cmd, _ = json.JSONDecoder().raw_decode(out[start:])
    assert shlex.split(cmd)[-1] == name


def test_render_notes_unmonitored_overflow() -> None:
    names = [f"n{i}" for i in range(inbox_hook.MAX_LISTED + 3)]
    out = render_text(inbox_hook.render("UserPromptSubmit", [], names))
    assert "3 more names" in out


def render_text(raw: str | None) -> str:
    """Every string in the hook's JSON payload, joined: the text the agent sees."""
    assert raw is not None

    def strings(v: object) -> list[str]:
        if isinstance(v, str):
            return [v]
        if isinstance(v, dict):
            return [s for x in v.values() for s in strings(x)]
        if isinstance(v, list):
            return [s for x in v for s in strings(x)]
        return []

    return "\n".join(strings(json.loads(raw)))


# --- setup: runnable hook commands -------------------------------------------


def _which(present: set[str]):
    return lambda exe: f"/usr/bin/{exe}" if exe in present else None


def test_runnable_prefers_console_script(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setup.shutil, "which", _which({"dlb-inbox-hook", "uvx"}))
    assert setup._runnable("dlb-inbox-hook --event X") == ("dlb-inbox-hook --event X", True)


def test_runnable_falls_back_to_uvx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setup.shutil, "which", _which({"uvx"}))
    assert setup._runnable("dlb-inbox-hook") == ("uvx --from dlb-mcp dlb-inbox-hook", True)


def test_runnable_flags_unresolvable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setup.shutil, "which", _which(set()))
    assert setup._runnable("dlb-inbox-hook") == ("dlb-inbox-hook", False)


@pytest.mark.parametrize(
    "argv",
    [
        ["uvx", "--from", "dlb-mcp", "dlb-inbox-hook"],
        ["uvx", "--from", "dlb-mcp==0.7.0", "dlb-inbox-hook"],
        ["uvx", "--from=dlb-mcp", "dlb-inbox-hook"],
        ["/opt/bin/uvx", "--from", "dlb-mcp@latest", "dlb-inbox-hook"],
    ],
)
def test_strip_uvx_recognizes_forms(argv: list[str]) -> None:
    assert setup._strip_uvx(argv) == ["dlb-inbox-hook"]


def test_strip_uvx_ignores_other_packages() -> None:
    argv = ["uvx", "--from", "other-pkg", "dlb-inbox-hook"]
    assert setup._strip_uvx(argv) == argv


def test_has_command_matches_uvx_form() -> None:
    settings = {
        "hooks": {
            "SessionStart": [
                {"hooks": [{"type": "command", "command": "uvx --from dlb-mcp dlb-inbox-hook"}]}
            ]
        }
    }
    assert setup._has_command(settings, "SessionStart", "dlb-inbox-hook")
    assert not setup._has_command(settings, "SessionStart", "dlb-session-recover")


@pytest.mark.parametrize(
    "cmd",
    [
        "dlb-session-recover  # print registered names after compaction",
        "/Users/x/.local/bin/dlb-session-recover #recover",
        "uvx --from dlb-mcp dlb-session-recover # via uvx",
    ],
)
def test_has_command_ignores_trailing_comment(cmd: str) -> None:
    settings = {"hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": cmd}]}]}}
    assert setup._has_command(settings, "SessionStart", "dlb-session-recover")


@pytest.mark.parametrize(
    "cmd",
    [
        "dlb-session-recover#disabled",
        "dlb-session-recover '# not a comment'",
        "dlb-session-recover \\#literal",
    ],
)
def test_has_command_keeps_hash_inside_a_word(cmd: str) -> None:
    # A # that does not start a word is part of the command, not a comment.
    settings = {"hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": cmd}]}]}}
    assert not setup._has_command(settings, "SessionStart", "dlb-session-recover")


def test_has_command_survives_unbalanced_quotes() -> None:
    settings = {
        "hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": "echo 'oops"}]}]}
    }
    assert not setup._has_command(settings, "SessionStart", "dlb-session-recover")


def test_setup_does_not_duplicate_commented_hooks(tmp_path: Path) -> None:
    # Every required hook already wired, each with a trailing comment: setup
    # must see them all and leave the file alone.
    hooks: dict[str, list] = {}
    for event, command in setup.REQUIRED_HOOKS:
        hooks.setdefault(event, []).append(
            {"hooks": [{"type": "command", "command": f"{command}  # wired by hand"}]}
        )
    path = tmp_path / "settings.json"
    path.write_text(json.dumps({"hooks": hooks}))
    before = path.read_text()
    changed, message = setup.wire_session_hook(path)
    assert not changed, message
    assert path.read_text() == before


# --- heartbeat auth ----------------------------------------------------------


def test_heartbeat_with_token() -> None:
    tok = _reg("a")
    assert store.record_monitor_heartbeat("a", tok) is True
    with pytest.raises(store.AuthError):
        store.record_monitor_heartbeat("a", "wrong")


def test_heartbeat_with_matching_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_SESSION_ID", "s1")
    _reg("a")
    monkeypatch.delenv("DLB_SESSION_ID")
    assert store.record_monitor_heartbeat("a", session_id="s1") is True
    with pytest.raises(store.AuthError):
        store.record_monitor_heartbeat("a", session_id="s2")
    with pytest.raises(store.AuthError):
        store.record_monitor_heartbeat("a")


def test_heartbeat_unregistered_returns_false() -> None:
    assert store.record_monitor_heartbeat("ghost", session_id="s1") is False


# --- monitor -----------------------------------------------------------------


def test_fetch_new_skips_cancelled_and_expired(monkeypatch: pytest.MonkeyPatch) -> None:
    a = _reg("a")
    _reg("b")
    live = store.send("b", "live", session_token=a)
    gone = store.send("b", "gone", session_token=a)
    store.cancel_task(gone.id, a, "nvm")
    old = store.send("b", "old", session_token=a)
    store.send("b", "new", session_token=a, supersedes=old.id)
    store.send("b", "soon", session_token=a, expires_in_seconds=1)
    real = monitor.time.time
    monkeypatch.setattr(monitor.time, "time", lambda: real() + 5)
    bodies = [r[4] for r in monitor._fetch_new(Path(store.store_path()), "b", 0)]
    assert bodies == ["live", "new"]
    assert live.id


def test_monitor_token_flag_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def fake_run(**kwargs: object) -> int:
        seen.update(kwargs)
        return 0

    monkeypatch.setattr(monitor, "run", lambda *a, **k: fake_run(**k) if k else fake_run(args=a))
    monkeypatch.setenv("DLB_MONITOR_TOKEN", "from-env")
    monitor.main(["--name", "a"])
    assert "from-env" in json.dumps(seen, default=str)
    seen.clear()
    monitor.main(["--name", "a", "--token", "from-flag"])
    assert "from-flag" in json.dumps(seen, default=str)


def test_session_id_falls_back_without_mutating_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DLB_SESSION_ID", raising=False)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "cc-1")
    assert monitor._session_id() == "cc-1"
    assert "DLB_SESSION_ID" not in os.environ
    monkeypatch.setenv("DLB_SESSION_ID", "dlb-1")
    assert monitor._session_id() == "dlb-1"
    monkeypatch.delenv("DLB_SESSION_ID")
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID")
    assert monitor._session_id() is None

"""Server and monitor must agree on the session id, and a refused heartbeat
must be loud (stdout event) and, when it persists, fatal (exit 77)."""

from __future__ import annotations

import threading

import pytest

from dlb_mcp import _version, monitor, store


def test_empty_dlb_session_id_falls_back_on_both_sides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The MCP env expands DLB_SESSION_ID='${CLAUDE_CODE_SESSION_ID:-}', which
    # can be empty. The server and the monitor must still resolve the same id.
    monkeypatch.setenv("DLB_SESSION_ID", "")
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-x")
    assert store.resolve_session_id() == "sess-x"
    assert monitor._session_id() == "sess-x"

    store.register("alpha")
    assert monitor._heartbeat("alpha", None, monitor._session_id()) == "ok"
    assert store.dispatch_assessment("alpha")["wakeable"] is True


def test_reclaim_rebinds_session_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-old")
    prior = store.register("alpha")["session_token"]

    # A new session reclaims the name with force + the prior token.
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-new")
    store.register("alpha", force=True, prior_token=prior)

    assert monitor._heartbeat("alpha", None, "sess-new") == "ok"
    assert store.dispatch_assessment("alpha")["wakeable"] is True
    with pytest.raises(store.AuthError):
        store.record_monitor_heartbeat("alpha", None, session_id="sess-old")


def test_refused_heartbeat_returns_refused(capsys: pytest.CaptureFixture[str]) -> None:
    store.register("alpha")
    assert monitor._heartbeat("alpha", None, "not-the-owner") == "refused"
    assert "heartbeat refused" in capsys.readouterr().err


def test_heartbeat_thread_reports_each_refusal_on_stdout_then_gives_up(
    capsys: pytest.CaptureFixture[str],
) -> None:
    store.register("alpha")
    beater = monitor._HeartbeatThread("alpha", None, 0.0, "not-the-owner")
    beater.start()
    beater.join(timeout=5.0)
    assert not beater.is_alive()
    assert beater.refused.is_set()
    out = capsys.readouterr().out
    assert out.count("heartbeat refused for alpha") == monitor.HEARTBEAT_REFUSAL_LIMIT
    assert f"({monitor.HEARTBEAT_REFUSAL_LIMIT}/{monitor.HEARTBEAT_REFUSAL_LIMIT})" in out


def test_success_resets_refusal_count(monkeypatch: pytest.MonkeyPatch) -> None:
    # refused, refused, ok, refused, refused: never 3 in a row, so no give-up.
    results = iter(["refused", "refused", "ok", "refused", "refused"])
    beater = monitor._HeartbeatThread("alpha", None, 0.0, None)

    def fake(*_a: object, **_k: object) -> str:
        try:
            return next(results)
        except StopIteration:
            beater.stop.set()
            return "ok"

    monkeypatch.setattr(monitor, "_heartbeat", fake)
    beater.start()
    beater.join(timeout=5.0)
    assert not beater.refused.is_set()


def test_transient_error_does_not_count_as_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    results = iter(["refused", "refused", "error", "error", "ok"])
    beater = monitor._HeartbeatThread("alpha", None, 0.0, None)

    def fake(*_a: object, **_k: object) -> str:
        try:
            return next(results)
        except StopIteration:
            beater.stop.set()
            return "ok"

    monkeypatch.setattr(monitor, "_heartbeat", fake)
    beater.start()
    beater.join(timeout=5.0)
    assert not beater.refused.is_set()


def test_run_exits_77_when_heartbeat_keeps_being_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-owner")
    store.register("alpha")
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-intruder")
    monkeypatch.setattr(monitor, "HEARTBEAT_INTERVAL_SECONDS", 0.0)
    stop = threading.Event()

    code = monitor.run(
        name="alpha",
        interval_seconds=0.01,
        include_senders=None,
        exclude_senders=None,
        stop_event=stop,
    )
    assert code == _version.EXIT_HEARTBEAT_REFUSED == 77
    captured = capsys.readouterr()
    assert "heartbeat refused for alpha" in captured.out
    assert "do not respawn" in captured.err
    assert store.dispatch_assessment("alpha")["wakeable"] is False


def test_run_with_matching_session_never_exits_77(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_SESSION_ID", "")
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-owner")
    store.register("alpha")
    monkeypatch.setattr(monitor, "HEARTBEAT_INTERVAL_SECONDS", 0.0)
    stop = threading.Event()
    calls = {"n": 0}
    real = monitor._poll_iteration

    def counting(*a: object, **k: object) -> int:
        calls["n"] += 1
        if calls["n"] >= 20:
            stop.set()
        return real(*a, **k)

    monkeypatch.setattr(monitor, "_poll_iteration", counting)
    code = monitor.run(
        name="alpha",
        interval_seconds=0.001,
        include_senders=None,
        exclude_senders=None,
        stop_event=stop,
    )
    assert code == 0

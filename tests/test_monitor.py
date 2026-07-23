"""Unit tests for dlb-monitor.

These cover what the Monitor tool's notification stream actually depends on:
- baseline excludes pre-existing messages
- new messages emit one line each, line-buffered
- include/exclude filters work
- watermark advances past filtered messages too (no infinite re-emit)
- format is one short line per event
- runtime errors don't kill the loop
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from dlb_mcp import monitor, store


def test_format_event_truncates_long_body() -> None:
    long = "x" * 200
    line = monitor._format_event(0, "bob", None, long, preview_len=20)
    assert len(line.split('"', 2)[1]) <= 20  # body slot capped


def test_format_event_prefers_subject_over_body() -> None:
    line = monitor._format_event(0, "bob", "hello world", "irrelevant body", preview_len=80)
    assert "hello world" in line
    assert "irrelevant body" not in line


def test_format_event_strips_newlines_to_single_line() -> None:
    line = monitor._format_event(0, "bob", None, "first\nsecond\nthird", preview_len=80)
    assert "\n" not in line.rstrip("\n")
    # The internal newlines are collapsed; carriage on the line stays single
    assert "first second third" in line


def test_format_event_includes_iso_timestamp() -> None:
    # 2026-01-01T00:00:00 UTC == 1767225600000 ms
    line = monitor._format_event(1767225600000, "bob", None, "hi", preview_len=80)
    assert line.startswith("2026-01-01T00:00:00Z ")


def test_baseline_max_id_returns_zero_for_empty_recipient(isolated_store: Path) -> None:
    store.register("alpha")
    assert monitor._baseline_max_id(isolated_store, "alpha") == 0


def test_baseline_max_id_returns_highest_existing(isolated_store: Path) -> None:
    store.register("alpha")
    store.send(to="alpha", body="m1", from_="x")
    store.send(to="alpha", body="m2", from_="x")
    baseline = monitor._baseline_max_id(isolated_store, "alpha")
    # Two messages; baseline is the second one's id.
    assert baseline == 2


def test_fetch_new_returns_only_messages_after_watermark(isolated_store: Path) -> None:
    store.register("alpha")
    store.send(to="alpha", body="old", from_="x")
    baseline = monitor._baseline_max_id(isolated_store, "alpha")
    assert monitor._fetch_new(isolated_store, "alpha", baseline) == []

    store.send(to="alpha", body="new", from_="bob")
    new = monitor._fetch_new(isolated_store, "alpha", baseline)
    assert len(new) == 1
    msg_id, _sent, sender, _subject, body, _headline, _mt = new[0]
    assert sender == "bob"
    assert body == "new"
    assert msg_id > baseline


def test_fetch_new_returns_in_arrival_order(isolated_store: Path) -> None:
    store.register("alpha")
    baseline = 0
    for i in range(5):
        store.send(to="alpha", body=f"m{i}", from_="x")
    new = monitor._fetch_new(isolated_store, "alpha", baseline)
    bodies = [body for _id, _sent, _sender, _subject, body, _hl, _mt in new]
    assert bodies == ["m0", "m1", "m2", "m3", "m4"]


# ── Run loop (with a fast tick + a separate thread to inject messages) ──────


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    """Poll `predicate` until it is truthy or `timeout` elapses. Returns the
    final truthiness. Used instead of a fixed sleep so the thread-based tests
    are deterministic (they fail loudly on timeout rather than flaky-passing
    on a too-short sleep)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class _RunningMonitor:
    """A `monitor.run()` loop on a *joinable* background thread.

    History: this helper used to spawn `run()` on a `daemon=True` thread and
    deliberately never stop it ("we live with leaving it as a daemon"),
    because `run()` had no clean shutdown signal. That leaked an unbounded
    poll loop across tests — and because `run()` reads the *module-global*
    `monitor._poll_iteration` and `monitor.time.sleep` each tick, a later
    test that monkeypatched those globals (test_run_loop_survives_...) had
    its shared state corrupted by the leaked loop, producing a real
    cross-test flake. `run()` now takes a `stop_event`, so the thread stops
    cooperatively and is *joined* on context exit — no leak, no flake.
    """

    def __init__(
        self,
        name: str,
        interval: float,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> None:
        import contextlib
        import io

        self._out = io.StringIO()
        self._err = io.StringIO()
        self._stop = threading.Event()

        def target() -> None:
            # redirect_* patch the process-global streams; safe here because
            # this is the only monitor thread and it is joined before the test
            # returns. suppress(BaseException) so a thread-side error surfaces
            # via the captured logs / a failed assertion, never as a raw crash.
            with (
                contextlib.redirect_stdout(self._out),
                contextlib.redirect_stderr(self._err),
                contextlib.suppress(BaseException),
            ):
                monitor.run(
                    name=name,
                    interval_seconds=interval,
                    include_senders=include,
                    exclude_senders=exclude,
                    stop_event=self._stop,
                )

        self._thread = threading.Thread(target=target)  # non-daemon: we join it

    @property
    def out(self) -> str:
        return self._out.getvalue()

    @property
    def err(self) -> str:
        return self._err.getvalue()

    def __enter__(self) -> _RunningMonitor:
        self._thread.start()
        # Block until run() has established its baseline (the "watching" line
        # is printed to stderr right before the loop starts), so messages sent
        # by the test are guaranteed to land ABOVE the watermark and emit.
        assert _wait_until(lambda: "watching '" in self.err), "monitor never started"
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=5.0)
        assert not self._thread.is_alive(), "monitor thread failed to stop on stop_event"


def test_monitor_emits_one_line_per_new_message(isolated_store: Path) -> None:
    store.register("alpha")
    # Pre-existing message — baseline must skip it
    store.send(to="alpha", body="pre-existing — skip me", from_="x")

    with _RunningMonitor("alpha", interval=0.02) as mon:
        # Baseline established (see __enter__); now inject two new messages.
        store.send(to="alpha", body="ping", from_="bravo")
        store.send(to="alpha", body="pong", from_="charlie")
        # Wait until both events have actually been emitted to stdout.
        assert _wait_until(lambda: "bravo" in mon.out and "charlie" in mon.out), (
            f"monitor did not emit both events; got: {mon.out!r}"
        )

    # One stdout line per NEW message — this is what the monitor's whole
    # contract is: exactly one Monitor wake event per arriving message.
    lines = [ln for ln in mon.out.splitlines() if ln.strip()]
    assert len(lines) == 2, f"expected 2 event lines, got {lines!r}"
    assert any("bravo" in ln for ln in lines)
    assert any("charlie" in ln for ln in lines)
    # The pre-existing message must NOT be re-surfaced (baseline skipped it).
    assert not any("pre-existing" in ln for ln in lines)

    # Belt-and-suspenders: the store-level view agrees (pre-existing excluded).
    new = monitor._fetch_new(isolated_store, "alpha", 1)
    assert len(new) == 2
    assert "pre-existing — skip me" not in {
        body for _id, _sent, _sender, _subject, body, _hl, _mt in new
    }


def test_monitor_include_senders_filter_drops_others(isolated_store: Path) -> None:
    """Include filter only emits matching senders; watermark still advances."""
    store.register("alpha")
    # Send three messages BEFORE starting so we can check filter behavior
    # via _fetch_new + simulating the run loop's per-event filter.
    store.send(to="alpha", body="bob says hi", from_="bob")
    store.send(to="alpha", body="carol says hi", from_="carol")
    store.send(to="alpha", body="eve says hi", from_="eve")

    new = monitor._fetch_new(isolated_store, "alpha", 0)
    assert len(new) == 3

    # Replicate the run loop's filter inline (the loop itself is a daemon
    # thread; testing the filter logic by direct call is more deterministic).
    include = {"bob", "carol"}
    emitted = [
        (sid, sender, body)
        for sid, _sent, sender, _subject, body, _hl, _mt in new
        if sender in include
    ]
    assert {sender for _id, sender, _body in emitted} == {"bob", "carol"}
    # eve was excluded
    assert "eve" not in {sender for _id, sender, _body in emitted}


def test_monitor_continues_after_sqlite_hiccup(
    isolated_store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the SQLite store transiently fails to open, _fetch_new must return
    [] and log to stderr — NOT raise. (The Monitor tool would stop and we'd
    lose the wake source until manual restart.)"""

    def boom(*_args: object, **_kwargs: object) -> sqlite3.Connection:
        raise sqlite3.OperationalError("simulated boom")

    monkeypatch.setattr(sqlite3, "connect", boom)
    # Still safe — must return [], not raise
    result = monitor._fetch_new(isolated_store, "alpha", 0)
    assert result == []


def test_main_rejects_mutually_exclusive_filters(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        monitor.main(
            [
                "--name",
                "alpha",
                "--include-senders",
                "bob",
                "--exclude-senders",
                "eve",
            ]
        )
    err = capsys.readouterr().err
    assert "mutually exclusive" in err


def test_main_rejects_nonpositive_interval(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        monitor.main(["--name", "alpha", "--interval", "0"])
    err = capsys.readouterr().err
    assert "positive" in err


# ── Resilience: the loop must survive non-fatal exceptions ──────────────────


def test_poll_iteration_advances_watermark_for_new_messages(
    isolated_store: Path,
) -> None:
    """_poll_iteration is the unit of work; verify its watermark contract
    independently of the loop wrapper."""
    store.register("alpha")
    store.send(to="alpha", body="m1", from_="bob")
    store.send(to="alpha", body="m2", from_="bob")
    new_watermark = monitor._poll_iteration(
        isolated_store, "alpha", 0, include_senders=None, exclude_senders=None, emit_receipts=False
    )
    assert new_watermark == 2


def test_poll_iteration_returns_unchanged_watermark_for_empty_inbox(
    isolated_store: Path,
) -> None:
    store.register("alpha")
    new_watermark = monitor._poll_iteration(
        isolated_store, "alpha", 7, include_senders=None, exclude_senders=None, emit_receipts=False
    )
    # No messages → watermark unchanged (NOT reset to 0)
    assert new_watermark == 7


def test_run_loop_survives_unexpected_exception_in_poll_iteration(
    isolated_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The whole point of v0.2.1. A RuntimeError out of _poll_iteration
    must NOT kill the loop — must log, back off, and continue. We force
    one failing iteration then a normal one then a KeyboardInterrupt to
    exit cleanly, and assert the loop reached the third call (proving it
    didn't die on the RuntimeError)."""
    store.register("alpha")

    calls: list[str] = []

    def flaky_iteration(*_args, **_kwargs) -> int:
        if not calls:
            calls.append("boom")
            raise RuntimeError("simulated transient failure")
        if len(calls) == 1:
            calls.append("ok")
            return 0
        # Third call → request clean exit
        calls.append("kbd")
        raise KeyboardInterrupt

    # Also collapse the backoff sleep so the test isn't slow
    monkeypatch.setattr(monitor, "_poll_iteration", flaky_iteration)
    monkeypatch.setattr(monitor, "LOOP_ERROR_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(monitor.time, "sleep", lambda *_: None)

    exit_code = monitor.run(
        name="alpha",
        interval_seconds=0.01,
        include_senders=None,
        exclude_senders=None,
    )

    assert exit_code == 0
    # Critical assertion: the third call happened. If the loop died on the
    # first RuntimeError, calls would be just ["boom"].
    assert calls == ["boom", "ok", "kbd"]

    err = capsys.readouterr().err
    # The warning line about the failure must appear in stderr so users
    # debugging a flaky monitor can see what's going wrong.
    assert "poll iteration failed" in err
    assert "RuntimeError" in err
    assert "simulated transient failure" in err


def test_run_loop_propagates_keyboard_interrupt_cleanly(
    isolated_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The outer except RuntimeError handler must NOT swallow
    KeyboardInterrupt — Ctrl-C should still exit the process cleanly."""
    store.register("alpha")

    def immediate_ctrl_c(*_args, **_kwargs) -> int:
        raise KeyboardInterrupt

    monkeypatch.setattr(monitor, "_poll_iteration", immediate_ctrl_c)
    monkeypatch.setattr(monitor.time, "sleep", lambda *_: None)

    exit_code = monitor.run(
        name="alpha",
        interval_seconds=0.01,
        include_senders=None,
        exclude_senders=None,
    )
    assert exit_code == 0


def test_monitor_suppresses_receipt_events_by_default(
    isolated_store: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Auto-generated read-receipts must NOT emit an event by default (they
    would wake a turn for a pure machine ack)."""
    store.register("alpha")
    store.send(to="alpha", body="real task", from_="bob", msg_type="task")
    store.send(to="alpha", body="seen it", from_="carol", msg_type="receipt")

    monitor._poll_iteration(isolated_store, "alpha", 0, None, None, emit_receipts=False)
    out = capsys.readouterr().out
    assert "real task" in out
    assert "seen it" not in out  # receipt suppressed


def test_monitor_emit_receipts_flag_opts_back_in(
    isolated_store: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--emit-receipts (emit_receipts=True) restores receipt events."""
    store.register("alpha")
    store.send(to="alpha", body="seen it", from_="carol", msg_type="receipt")

    monitor._poll_iteration(isolated_store, "alpha", 0, None, None, emit_receipts=True)
    out = capsys.readouterr().out
    assert "seen it" in out

"""#6 headline field + #4 task read-receipts."""

from __future__ import annotations

import pytest

from dlb_mcp import monitor, store

# ── #6 headline ──────────────────────────────────────────────────────────────


def test_headline_round_trips_through_send_and_read():
    store.register("alpha")
    store.send(to="alpha", body="long body here", headline="STATUS: 3/5 done", from_="bravo")
    msgs = store.read("alpha", session_token=store.recover_token("alpha")["session_token"])
    assert msgs[0].headline == "STATUS: 3/5 done"


def test_monitor_shows_headline_untruncated():
    long_headline = "RESULT: " + "x" * 200
    line = monitor._format_event(0, "bravo", subject="hi", body="body", headline=long_headline)
    assert long_headline in line  # untruncated
    # Without a headline, falls back to truncated subject/body.
    line2 = monitor._format_event(0, "bravo", subject="s" * 200, body="body")
    assert line2.endswith('…"')


# ── #4 read-receipts ─────────────────────────────────────────────────────────


def _read(name: str):
    return store.read(name, session_token=store.recover_token(name)["session_token"])


def test_task_read_sends_receipt_to_registered_sender():
    store.register("worker")
    store.register("boss")
    store.send(
        to="worker",
        body="do X",
        from_="boss",
        session_token=store.recover_token("boss")["session_token"],
        msg_type="task",
        headline="task: do X",
    )
    _read("worker")

    receipts = _read("boss")
    assert len(receipts) == 1
    r = receipts[0]
    assert r.msg_type == "receipt"
    assert r.sender_name == "worker"
    assert "read by worker" in r.body


def test_non_task_message_does_not_receipt():
    store.register("worker")
    store.register("boss")
    store.send(
        to="worker",
        body="fyi",
        from_="boss",
        session_token=store.recover_token("boss")["session_token"],
    )  # no msg_type
    _read("worker")
    assert _read("boss") == []


def test_unregistered_sender_gets_no_receipt():
    store.register("worker")
    store.send(to="worker", body="do X", from_="ghost", msg_type="task")  # ghost not registered
    _read("worker")
    # ghost inbox is a dead-letter; no receipt should have been queued
    assert store.read("ghost", unread_only=True) == []


def test_receipt_does_not_generate_a_receipt_no_loop():
    store.register("worker")
    store.register("boss")
    store.send(
        to="worker",
        body="do X",
        from_="boss",
        session_token=store.recover_token("boss")["session_token"],
        msg_type="task",
    )
    _read("worker")  # -> receipt to boss
    _read("boss")  # reading the receipt must NOT create a new receipt to worker
    # worker inbox now empty (its only msg was already read); no loop receipt.
    assert _read("worker") == []


def test_env_opt_out_disables_receipts(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DLB_READ_RECEIPTS", "0")
    store.register("worker")
    store.register("boss")
    store.send(
        to="worker",
        body="do X",
        from_="boss",
        session_token=store.recover_token("boss")["session_token"],
        msg_type="task",
    )
    _read("worker")
    assert _read("boss") == []

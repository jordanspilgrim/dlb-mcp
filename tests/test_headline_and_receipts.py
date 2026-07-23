"""#6 headline field + #4 task read-receipts.

Tokens are captured from register() (the DB stores only sha256(token), so there
is no store-level recover_token to fetch them back)."""

from __future__ import annotations

import pytest

from dlb_mcp import monitor, store

# ── #6 headline ──────────────────────────────────────────────────────────────


def test_headline_round_trips_through_send_and_read():
    tok = store.register("alpha")["session_token"]
    store.send(to="alpha", body="long body here", headline="STATUS: 3/5 done", from_="bravo")
    msgs = store.read("alpha", session_token=tok)
    assert msgs[0].headline == "STATUS: 3/5 done"


def test_monitor_shows_headline_untruncated():
    long_headline = "RESULT: " + "x" * 200
    line = monitor._format_event(0, "bravo", subject="hi", body="body", headline=long_headline)
    assert long_headline in line  # untruncated
    # Without a headline, falls back to truncated subject/body.
    line2 = monitor._format_event(0, "bravo", subject="s" * 200, body="body")
    assert line2.endswith('…"')


# ── #4 read-receipts ─────────────────────────────────────────────────────────


def test_task_read_sends_receipt_to_registered_sender():
    worker_t = store.register("worker")["session_token"]
    boss_t = store.register("boss")["session_token"]
    store.send(
        to="worker",
        body="do X",
        from_="boss",
        session_token=boss_t,
        msg_type="task",
        headline="task: do X",
    )
    store.read("worker", session_token=worker_t)

    receipts = store.read("boss", session_token=boss_t)
    assert len(receipts) == 1
    r = receipts[0]
    assert r.msg_type == "receipt"
    assert r.sender_name == "worker"
    assert "read by worker" in r.body


def test_non_task_message_does_not_receipt():
    worker_t = store.register("worker")["session_token"]
    boss_t = store.register("boss")["session_token"]
    store.send(to="worker", body="fyi", from_="boss", session_token=boss_t)  # no msg_type
    store.read("worker", session_token=worker_t)
    assert store.read("boss", session_token=boss_t) == []


def test_unregistered_sender_gets_no_receipt():
    worker_t = store.register("worker")["session_token"]
    store.send(to="worker", body="do X", from_="ghost", msg_type="task")  # ghost not registered
    store.read("worker", session_token=worker_t)
    # ghost inbox is a dead-letter; no receipt should have been queued
    assert store.read("ghost", unread_only=True) == []


def test_receipt_does_not_generate_a_receipt_no_loop():
    worker_t = store.register("worker")["session_token"]
    boss_t = store.register("boss")["session_token"]
    store.send(to="worker", body="do X", from_="boss", session_token=boss_t, msg_type="task")
    store.read("worker", session_token=worker_t)  # -> receipt to boss
    store.read("boss", session_token=boss_t)  # reading the receipt must NOT create a new receipt
    # worker inbox now empty (its only msg was already read); no loop receipt.
    assert store.read("worker", session_token=worker_t) == []


def test_env_opt_out_disables_receipts(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DLB_READ_RECEIPTS", "0")
    worker_t = store.register("worker")["session_token"]
    boss_t = store.register("boss")["session_token"]
    store.send(to="worker", body="do X", from_="boss", session_token=boss_t, msg_type="task")
    store.read("worker", session_token=worker_t)
    assert store.read("boss", session_token=boss_t) == []

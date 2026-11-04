"""v9 dispatch reliability: cancel/supersede, deadlines, read order and type
filtering, atomic claims, monitor heartbeat (wakeable), receipt opt-out, and
the send-time dispatch assessment.

Every test runs against the isolated tmp store from conftest, never ~/.dlb.
"""

from __future__ import annotations

import threading

import pytest

from dlb_mcp import store


def _reg(name: str) -> str:
    return store.register(name)["session_token"]


# --- cancel / supersede -------------------------------------------------------


def test_cancel_hides_message_and_is_idempotent() -> None:
    boss = _reg("boss")
    worker = _reg("worker")
    m = store.send("worker", "do X", session_token=boss, msg_type="task")

    out = store.cancel_task(m.id, boss, reason="no longer needed")
    assert out == {"cancelled": True, "already": False, "notified": False}
    assert store.read("worker", worker) == []

    shown = store.read("worker", worker, include_inactive=True, unread_only=False)
    assert [x.id for x in shown] == [m.id]
    assert shown[0].lifecycle == "cancelled"
    assert shown[0].cancel_reason == "no longer needed"

    again = store.cancel_task(m.id, boss)
    assert again["already"] is True


def test_cancel_after_read_notifies_recipient() -> None:
    boss = _reg("boss")
    worker = _reg("worker")
    m = store.send("worker", "do X", session_token=boss, msg_type="task")
    assert [x.id for x in store.read("worker", worker)] == [m.id]

    out = store.cancel_task(m.id, boss)
    assert out["notified"] is True
    inbox = store.read("worker", worker)
    notices = [x for x in inbox if x.msg_type == "cancel"]
    assert len(notices) == 1
    assert notices[0].in_reply_to == m.id


def test_cancel_requires_original_sender() -> None:
    boss = _reg("boss")
    other = _reg("other")
    _reg("worker")
    m = store.send("worker", "do X", session_token=boss)
    with pytest.raises(store.AuthError):
        store.cancel_task(m.id, other)
    with pytest.raises(store.AuthError):
        store.cancel_task(m.id, "not-a-token")
    anon = store.send("worker", "unsigned")
    with pytest.raises(store.AuthError):
        store.cancel_task(anon.id, boss)
    with pytest.raises(store.DLBError):
        store.cancel_task(999_999, boss)


def test_supersede_hides_old_and_links_new() -> None:
    boss = _reg("boss")
    worker = _reg("worker")
    old = store.send("worker", "v1", session_token=boss, msg_type="task")
    new = store.send("worker", "v2", session_token=boss, msg_type="task", supersedes=old.id)

    inbox = store.read("worker", worker)
    assert [x.id for x in inbox] == [new.id]
    old_row = next(
        x
        for x in store.read("worker", worker, include_inactive=True, unread_only=False)
        if x.id == old.id
    )
    assert old_row.superseded_by == new.id
    assert old_row.lifecycle == "superseded"


def test_supersede_requires_token() -> None:
    boss = _reg("boss")
    _reg("worker")
    old = store.send("worker", "v1", session_token=boss)
    with pytest.raises(store.AuthError):
        store.send("worker", "v2", supersedes=old.id)


# --- deadlines -----------------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -1, True, "5", 1.5])
def test_expires_in_seconds_validation(bad: object) -> None:
    with pytest.raises(store.DLBError):
        store.send("worker", "x", expires_in_seconds=bad)  # type: ignore[arg-type]


def test_past_deadline_is_hidden(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = _reg("worker")
    m = store.send("worker", "urgent", expires_in_seconds=60)
    assert m.deadline is not None
    real = store._now_ms()
    monkeypatch.setattr(store, "_now_ms", lambda: real + 120_000)
    assert store.read("worker", worker) == []
    shown = store.read("worker", worker, include_inactive=True)
    assert shown[0].lifecycle == "expired"


# --- read order and type filtering ----------------------------------------------


def test_read_order_oldest() -> None:
    worker = _reg("worker")
    ids = [store.send("worker", f"m{i}").id for i in range(3)]
    assert [x.id for x in store.read("worker", worker, order="oldest")] == ids


def test_read_order_validation() -> None:
    worker = _reg("worker")
    with pytest.raises(store.DLBError):
        store.read("worker", worker, order="sideways")
    with pytest.raises(store.DLBError):
        store.read("worker", worker, exclude_types="receipt")  # type: ignore[arg-type]


def test_exclude_types_leaves_rows_unread() -> None:
    worker = _reg("worker")
    note = store.send("worker", "fyi", msg_type="note")
    task = store.send("worker", "do it", msg_type="task")
    got = store.read("worker", worker, exclude_types=["note"])
    assert [x.id for x in got] == [task.id]
    rest = store.read("worker", worker)
    assert [x.id for x in rest] == [note.id]


# --- claims ---------------------------------------------------------------------


def test_claim_win_lose_renew_release() -> None:
    a = _reg("a")
    b = _reg("b")
    won = store.claim("task:1", "a", a, ttl_seconds=60, note="on it")
    assert won["claimed"] is True and won["renewed"] is False and won["holder"] == "a"

    lost = store.claim("task:1", "b", b)
    assert lost["claimed"] is False and lost["holder"] == "a"

    renewed = store.claim("task:1", "a", a, ttl_seconds=120)
    assert renewed["claimed"] is True and renewed["renewed"] is True

    not_mine = store.release("task:1", "b", b)
    assert not_mine == {"key": "task:1", "released": False, "holder": "a"}
    mine = store.release("task:1", "a", a)
    assert mine == {"key": "task:1", "released": True, "holder": None}
    assert store.claim("task:1", "b", b)["claimed"] is True


def test_expired_claim_is_taken_over(monkeypatch: pytest.MonkeyPatch) -> None:
    a = _reg("a")
    b = _reg("b")
    store.claim("pr:7", "a", a, ttl_seconds=10)
    real = store._now_ms()
    monkeypatch.setattr(store, "_now_ms", lambda: real + 11_000)
    out = store.claim("pr:7", "b", b)
    assert out["claimed"] is True and out["took_over_from"] == "a"


def test_claim_requires_auth_and_valid_ttl() -> None:
    a = _reg("a")
    with pytest.raises(store.AuthError):
        store.claim("k", "a", "wrong")
    for bad in (0, -5, True, "60", store.MAX_CLAIM_TTL_SECONDS + 1):
        with pytest.raises(store.DLBError):
            store.claim("k", "a", a, ttl_seconds=bad)  # type: ignore[arg-type]
    with pytest.raises(store.DLBError):
        store.claim("", "a", a)


def test_list_claims_prefix_and_unregister_releases() -> None:
    a = _reg("a")
    b = _reg("b")
    store.claim("task:1", "a", a)
    store.claim("task:2", "b", b)
    store.claim("file:x", "a", a)
    assert {c["key"] for c in store.list_claims()} == {"task:1", "task:2", "file:x"}
    assert {c["key"] for c in store.list_claims("task:")} == {"task:1", "task:2"}

    store.unregister("a", a)
    assert [c["key"] for c in store.list_claims()] == ["task:2"]


def test_list_claims_drops_expired(monkeypatch: pytest.MonkeyPatch) -> None:
    a = _reg("a")
    store.claim("k", "a", a, ttl_seconds=5)
    real = store._now_ms()
    monkeypatch.setattr(store, "_now_ms", lambda: real + 6_000)
    assert store.list_claims() == []


def test_claim_race_has_exactly_one_winner() -> None:
    tokens = {f"w{i}": _reg(f"w{i}") for i in range(8)}
    results: list[dict] = []
    lock = threading.Lock()
    barrier = threading.Barrier(len(tokens))

    def go(name: str, token: str) -> None:
        barrier.wait()
        r = store.claim("contested", name, token)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=go, args=item) for item in tokens.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [r for r in results if r["claimed"]]
    assert len(winners) == 1
    assert all(r["holder"] == winners[0]["holder"] for r in results)


# --- heartbeat / wakeable -------------------------------------------------------


def test_heartbeat_makes_agent_wakeable(monkeypatch: pytest.MonkeyPatch) -> None:
    w = _reg("worker")
    assert store.record_monitor_heartbeat("ghost") is False
    summary = {s.name: s for s in store.list_threads()}["worker"]
    assert summary.wakeable is False

    assert store.record_monitor_heartbeat("worker", w) is True
    summary = {s.name: s for s in store.list_threads()}["worker"]
    assert summary.wakeable is True
    assert store.dispatch_assessment("worker")["wakeable"] is True

    real = store._now_ms()
    window_ms = store.wake_window_seconds() * 1000
    monkeypatch.setattr(store, "_now_ms", lambda: real + window_ms + 5_000)
    assert store.dispatch_assessment("worker")["wakeable"] is False


# --- receipts -------------------------------------------------------------------


def test_receipts_counted_separately_and_opt_out() -> None:
    boss = _reg("boss")
    worker = _reg("worker")
    store.send("worker", "t1", session_token=boss, msg_type="task")
    store.read("worker", worker)

    summary = {s.name: s for s in store.list_threads()}["boss"]
    assert summary.unread_receipts == 1
    assert summary.unread_count == 0

    assert store.set_receipts("boss", boss, False) == {"name": "boss", "receipts": False}
    store.send("worker", "t2", session_token=boss, msg_type="task")
    store.read("worker", worker)
    summary = {s.name: s for s in store.list_threads()}["boss"]
    assert summary.unread_receipts == 1

    with pytest.raises(store.DLBError):
        store.set_receipts("boss", boss, "no")  # type: ignore[arg-type]
    with pytest.raises(store.AuthError):
        store.set_receipts("boss", worker, True)


# --- dispatch assessment --------------------------------------------------------


def test_dispatch_assessment_warnings() -> None:
    unreg = store.dispatch_assessment("nobody")
    assert unreg["registered"] is False
    assert "not registered" in unreg["warning"]

    w = _reg("worker")
    idle = store.dispatch_assessment("worker")
    assert idle["registered"] is True
    assert "no live dlb-monitor" in idle["warning"]

    store.record_monitor_heartbeat("worker", w)
    assert store.dispatch_assessment("worker")["warning"] is None

    store.set_status("worker", w, "done", "shipped PR")
    done = store.dispatch_assessment("worker")
    assert "'done'" in done["warning"] and "shipped PR" in done["warning"]

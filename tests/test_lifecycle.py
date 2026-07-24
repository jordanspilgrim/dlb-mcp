"""Tests for the v0.3.0 message-lifecycle feature:

    * msg_type / in_reply_to fields on send
    * update_status(msg_id, status, note, session_token) tool
    * get_task_status(msg_id) sender-side probe
    * v2 → v3 schema migration
    * Backward compat: existing send/read paths still work unchanged

The convention that ties these together (recipient MUST reply on task-shaped
mail) lives in ~/.claude/CLAUDE.md, not enforced by DLB itself. These tests
lock in the STORAGE + QUERY primitives that support the convention.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dlb_mcp import store
from dlb_mcp.store import AuthError, DLBError

# ── send: msg_type + in_reply_to ────────────────────────────────────────────


def test_send_defaults_leave_lifecycle_fields_null() -> None:
    """Backward-compat: existing callers get None for all new fields."""
    msg = store.send(to="alpha", body="plain message", from_="bob")
    assert msg.msg_type is None
    assert msg.in_reply_to is None
    assert msg.status is None
    assert msg.status_note is None
    assert msg.status_updated_at is None


def test_send_task_type_persists_and_is_readable() -> None:
    reg = store.register("alpha")
    msg = store.send(
        to="alpha",
        body="run /security-review",
        from_="qa-tester",
        msg_type="task",
    )
    assert msg.msg_type == "task"

    inbox = store.read("alpha", session_token=reg["session_token"])
    assert len(inbox) == 1
    assert inbox[0].msg_type == "task"


def test_send_in_reply_to_persists_and_is_readable() -> None:
    """A reply carries the original task's id so the sender can correlate."""
    store.register("alpha")
    task = store.send(to="alpha", body="please review", from_="qa", msg_type="task")

    reply = store.send(
        to="qa",
        body="accepted, starting now",
        from_="alpha",
        in_reply_to=task.id,
    )
    assert reply.in_reply_to == task.id

    qa_inbox = store.read("qa")  # unregistered, no auth
    assert len(qa_inbox) == 1
    assert qa_inbox[0].in_reply_to == task.id


def test_send_in_reply_to_stores_stale_id_without_fk_enforcement() -> None:
    """in_reply_to is a soft reference, NOT a FK. Storing a non-existent id
    is intentional so we don't fail sends due to lookup races."""
    msg = store.send(to="alpha", body="reply to nothing", in_reply_to=99999)
    assert msg.in_reply_to == 99999


# ── update_status ────────────────────────────────────────────────────────────


def test_update_status_by_recipient_records_state() -> None:
    reg = store.register("alpha")
    task = store.send(to="alpha", body="do X", from_="qa", msg_type="task")

    updated = store.update_status(
        message_id=task.id,
        status="accepted",
        session_token=reg["session_token"],
        note="ETA 20m",
    )
    assert updated.status == "accepted"
    assert updated.status_note == "ETA 20m"
    assert updated.status_updated_at is not None


def test_update_status_sets_read_at_if_still_unread() -> None:
    """A status update implies the recipient has read the message,
    replaces `ack` for the common case."""
    reg = store.register("alpha")
    task = store.send(to="alpha", body="task", from_="qa", msg_type="task")

    # Before update_status: read_at should be None (unread)
    assert task.read_at is None

    updated = store.update_status(
        message_id=task.id,
        status="accepted",
        session_token=reg["session_token"],
    )
    # After: read_at is set to the same instant as status_updated_at
    assert updated.read_at is not None


def test_update_status_preserves_existing_read_at() -> None:
    """If the message was already read, update_status must NOT clobber the
    original read_at (it just records the status update instant separately)."""
    reg = store.register("alpha")
    store.send(to="alpha", body="task", from_="qa", msg_type="task")

    inbox = store.read("alpha", session_token=reg["session_token"])
    original_read_at = inbox[0].read_at
    assert original_read_at is not None

    updated = store.update_status(
        message_id=inbox[0].id,
        status="running",
        session_token=reg["session_token"],
    )
    # read_at unchanged (in ms terms)
    assert updated.read_at == original_read_at


def test_update_status_rejects_wrong_token() -> None:
    reg_alpha = store.register("alpha")
    _reg_bravo = store.register("bravo")
    task = store.send(to="alpha", body="do X", from_="qa", msg_type="task")

    # Bravo cannot update alpha's message status
    with pytest.raises(AuthError):
        store.update_status(
            message_id=task.id, status="accepted", session_token=_reg_bravo["session_token"]
        )
    # But alpha can (control)
    updated = store.update_status(
        message_id=task.id, status="accepted", session_token=reg_alpha["session_token"]
    )
    assert updated.status == "accepted"


def test_update_status_rejects_unregistered_recipient() -> None:
    """Cannot update status when the recipient is unregistered: nobody
    to authenticate as."""
    task = store.send(to="ghost", body="do X", from_="qa", msg_type="task")
    with pytest.raises(AuthError):
        store.update_status(message_id=task.id, status="accepted", session_token="any")


def test_update_status_rejects_missing_message() -> None:
    reg = store.register("alpha")
    with pytest.raises(DLBError):
        store.update_status(
            message_id=99_999_999,
            status="accepted",
            session_token=reg["session_token"],
        )


def test_update_status_rejects_empty_status() -> None:
    reg = store.register("alpha")
    task = store.send(to="alpha", body="task", from_="qa", msg_type="task")
    with pytest.raises(DLBError):
        store.update_status(message_id=task.id, status="", session_token=reg["session_token"])


def test_update_status_free_string_accepts_arbitrary_values() -> None:
    """Convention (queued/accepted/running/done/blocked) is NOT enforced.
    Any string is stored. Locks in the "convention over enforcement" design
    so future statuses don't need a schema migration."""
    reg = store.register("alpha")
    task = store.send(to="alpha", body="task", from_="qa", msg_type="task")

    updated = store.update_status(
        message_id=task.id,
        status="triaging-and-cross-referencing",  # unlikely but legal
        session_token=reg["session_token"],
    )
    assert updated.status == "triaging-and-cross-referencing"


# ── get_task_status: sender-side probe, no auth ──────────────────────────────


def test_get_task_status_returns_current_state_without_auth() -> None:
    """This is the whole point: the sender can check task progress without
    holding the recipient's session_token. Fixes the 'is it running?' gap."""
    reg_alpha = store.register("alpha")
    task = store.send(to="alpha", body="do X", from_="qa", msg_type="task")

    # Sender (qa, no token, unregistered) can query
    initial = store.get_task_status(task.id)
    assert initial is not None
    assert initial["status"] is None  # not yet updated
    assert initial["msg_type"] == "task"
    assert initial["read_at"] is None
    # It is a pure lifecycle-status probe: parties are NOT exposed (shrinks the
    # auth-free enumeration-harvest surface). Use read() for content/parties.
    assert "recipient_name" not in initial
    assert "sender_name" not in initial

    # Recipient accepts
    store.update_status(
        message_id=task.id,
        status="accepted",
        session_token=reg_alpha["session_token"],
        note="starting now",
    )

    # Sender re-probes → sees update
    after = store.get_task_status(task.id)
    assert after is not None
    assert after["status"] == "accepted"
    assert after["status_note"] == "starting now"
    assert after["status_updated_at"] is not None
    assert after["read_at"] is not None


def test_get_task_status_returns_none_for_unknown_id() -> None:
    assert store.get_task_status(99_999_999) is None


def test_get_task_status_does_not_return_body() -> None:
    """Status probe is lightweight: no body/subject. Callers use `read`
    if they want content."""
    task = store.send(to="alpha", body="secret plan", from_="qa", msg_type="task")
    status = store.get_task_status(task.id)
    assert status is not None
    assert "body" not in status
    assert "subject" not in status


# ── Schema migration: v2 → v3 ────────────────────────────────────────────────


def test_v2_schema_migrates_to_v3_on_connect(isolated_store: Path) -> None:
    """Hand-create a v2 DB (INTEGER ms columns, no lifecycle fields),
    call init_schema(), verify the 5 new columns exist + user_version==3."""
    conn = sqlite3.connect(str(isolated_store))
    conn.executescript(
        """
        CREATE TABLE agents (
            name              TEXT PRIMARY KEY,
            working_on        TEXT,
            registered_at_ms  INTEGER NOT NULL,
            last_seen_ms      INTEGER NOT NULL,
            session_token     TEXT NOT NULL
        );
        CREATE TABLE messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_name  TEXT NOT NULL,
            sender_name     TEXT NOT NULL,
            subject         TEXT,
            body            TEXT NOT NULL,
            sent_at_ms      INTEGER NOT NULL,
            read_at_ms      INTEGER,
            expires_at_ms   INTEGER NOT NULL
        );
        PRAGMA user_version = 2;
        """
    )
    # Seed a message so we can prove the migration doesn't lose data
    conn.execute(
        "INSERT INTO messages (recipient_name, sender_name, body, "
        "sent_at_ms, expires_at_ms) VALUES "
        "('legacy', 'preexisting', 'v2 body', 1767225600000, 4070908800000)"
    )
    conn.commit()
    conn.close()

    store.init_schema()

    conn = sqlite3.connect(str(isolated_store))
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    # Migrates to the current schema (track SCHEMA_VERSION, not a stale literal).
    assert version == store.SCHEMA_VERSION

    cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)").fetchall()}
    for expected in (
        "msg_type",
        "in_reply_to",
        "status",
        "status_note",
        "status_updated_at_ms",
    ):
        assert expected in cols, f"missing column after migration: {expected}"

    # Data survived
    row = conn.execute(
        "SELECT body, msg_type, status FROM messages WHERE recipient_name='legacy'"
    ).fetchone()
    conn.close()
    assert row[0] == "v2 body"
    assert row[1] is None  # v2 rows have NULL lifecycle
    assert row[2] is None


def test_v2_to_v3_migration_is_idempotent(isolated_store: Path) -> None:
    """Calling init_schema() twice on an already-v3 DB is a no-op."""
    store.register("alpha")
    store.init_schema()
    store.init_schema()

    conn = sqlite3.connect(str(isolated_store))
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert version == store.SCHEMA_VERSION


def test_send_and_update_status_work_after_v2_migration(
    isolated_store: Path,
) -> None:
    """Post-migration sanity: BOTH the send-with-new-fields AND update_status
    paths must work on an upgraded v2 DB. Regression guard against the
    v1→v2 class of bug where migration produced a schema the new code
    couldn't write to."""
    # Set up v2 DB
    conn = sqlite3.connect(str(isolated_store))
    conn.executescript(
        """
        CREATE TABLE agents (
            name              TEXT PRIMARY KEY,
            working_on        TEXT,
            registered_at_ms  INTEGER NOT NULL,
            last_seen_ms      INTEGER NOT NULL,
            session_token     TEXT NOT NULL
        );
        CREATE TABLE messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_name  TEXT NOT NULL,
            sender_name     TEXT NOT NULL,
            subject         TEXT,
            body            TEXT NOT NULL,
            sent_at_ms      INTEGER NOT NULL,
            read_at_ms      INTEGER,
            expires_at_ms   INTEGER NOT NULL
        );
        PRAGMA user_version = 2;
        """
    )
    conn.commit()
    conn.close()

    store.init_schema()  # migrate

    reg = store.register("alpha")
    task = store.send(
        to="alpha",
        body="post-migration task",
        from_="qa",
        msg_type="task",
    )
    assert task.msg_type == "task"

    updated = store.update_status(
        message_id=task.id,
        status="running",
        session_token=reg["session_token"],
        note="in progress",
    )
    assert updated.status == "running"

    probe = store.get_task_status(task.id)
    assert probe is not None
    assert probe["status"] == "running"


# ── Full round-trip demonstrating the intended pattern ──────────────────────


def test_full_task_lifecycle_round_trip() -> None:
    """The pattern the DLB Inbox Protocol prescribes:
        1. Sender dispatches a task (msg_type='task')
        2. Recipient reads it (implicitly OR via read())
        3. Recipient replies with in_reply_to + updates_status('accepted')
        4. Recipient does the work, updates_status('running')
        5. Recipient completes, updates_status('done', note=<result>)
        6. Sender polls get_task_status throughout to see progress
    This test walks through steps 1-6 and asserts each observable state."""
    reg_recipient = store.register("worker")

    # 1. Sender dispatches
    task = store.send(
        to="worker",
        body="run /security-review on branch feat/x",
        from_="orchestrator",
        subject="security review",
        msg_type="task",
    )

    # Sender's initial view: dispatched but unaccepted
    view0 = store.get_task_status(task.id)
    assert view0["status"] is None
    assert view0["read_at"] is None

    # 2 + 3. Recipient reads + acknowledges via update_status('accepted')
    inbox = store.read("worker", session_token=reg_recipient["session_token"])
    assert inbox[0].msg_type == "task"
    ack_reply = store.send(
        to="orchestrator",
        body="accepted, ETA ~20m",
        from_="worker",
        session_token=reg_recipient["session_token"],
        in_reply_to=task.id,
    )
    assert ack_reply.in_reply_to == task.id
    store.update_status(
        message_id=task.id,
        status="accepted",
        session_token=reg_recipient["session_token"],
        note="ETA ~20m",
    )

    view1 = store.get_task_status(task.id)
    assert view1["status"] == "accepted"
    assert view1["read_at"] is not None

    # 4. Recipient starts work
    store.update_status(
        message_id=task.id,
        status="running",
        session_token=reg_recipient["session_token"],
        note="scanning src/",
    )
    view2 = store.get_task_status(task.id)
    assert view2["status"] == "running"
    assert view2["status_note"] == "scanning src/"

    # 5. Recipient completes
    store.send(
        to="orchestrator",
        body="done: 0 high, 2 medium findings; see PR #42",
        from_="worker",
        session_token=reg_recipient["session_token"],
        in_reply_to=task.id,
    )
    store.update_status(
        message_id=task.id,
        status="done",
        session_token=reg_recipient["session_token"],
        note="0 high, 2 medium; PR #42",
    )

    view3 = store.get_task_status(task.id)
    assert view3["status"] == "done"

    # Orchestrator's inbox now has 2 replies, both threaded to the task
    orch_replies = store.read("orchestrator")
    assert len(orch_replies) == 2
    assert all(r.in_reply_to == task.id for r in orch_replies)

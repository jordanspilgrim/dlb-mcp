"""Unit tests for the store layer.

These cover the 5 DLB differentiator promises directly:
    1. Names accepted as-given
    2. Send to nonexistent recipient persists (dead-letter)
    3. Conflict on register errors with suggestions
    4. Send is auth-free; sender can be "anonymous"
    5. TTL expires messages lazily

Plus the normal CRUD paths and the auth model on read/ack/unregister.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dlb_mcp import store
from dlb_mcp.store import AuthError, DLBError, NameConflict

# ── Differentiator #1: names accepted as-given ───────────────────────────────


def test_register_accepts_user_name_verbatim() -> None:
    """The name we pass is the name we get. No rename, no auto-suffix."""
    for name in ["alpha", "ThreadBeta", "worker-1", "thread.alpha", "X"]:
        r = store.register(name)
        assert r["name"] == name, f"DLB renamed '{name}' to '{r['name']}'"
        store.unregister(name, r["session_token"])


def test_register_rejects_empty_name() -> None:
    with pytest.raises(DLBError):
        store.register("")


# ── Differentiator #2: send-to-nonexistent persists (dead letter) ────────────


def test_send_to_nonexistent_recipient_succeeds_and_persists() -> None:
    """The whole point of DLB. Sending to 'ghost' must not fail."""
    msg = store.send(to="ghost", body="anybody home?", from_="me")
    assert msg.id > 0
    assert msg.recipient_name == "ghost"

    # And the message must be readable from the unregistered name
    inbox = store.read("ghost")
    assert len(inbox) == 1
    assert inbox[0].body == "anybody home?"


def test_late_registration_finds_waiting_messages() -> None:
    """Register AFTER messages have been sent; they should be in the inbox."""
    store.send(to="latecomer", body="msg 1", from_="early-bird")
    store.send(to="latecomer", body="msg 2", from_="early-bird")

    reg = store.register("latecomer", working_on="just got here")
    inbox = store.read("latecomer", session_token=reg["session_token"])
    assert len(inbox) == 2
    bodies = {m.body for m in inbox}
    assert bodies == {"msg 1", "msg 2"}


# ── Differentiator #3: conflict on register errors with suggestions ──────────


def test_register_conflict_raises_with_suggestions() -> None:
    """Honest error + suggested alternatives. No silent rewrite."""
    store.register("alpha")
    with pytest.raises(NameConflict) as exc:
        store.register("alpha")
    # The exception itself carries machine-readable suggestions
    assert exc.value.name == "alpha"
    assert exc.value.suggestions[0] == "alpha-2"
    assert "alpha-2" in str(exc.value)
    assert "force=true" in str(exc.value).lower()


def test_register_with_force_evicts_prior_session() -> None:
    r1 = store.register("alpha")
    r2 = store.register("alpha", working_on="taking over", force=True)
    assert r2["session_token"] != r1["session_token"]
    # Old token must no longer authenticate
    with pytest.raises(AuthError):
        store.read("alpha", session_token=r1["session_token"])
    # New token works
    store.read("alpha", session_token=r2["session_token"])


def test_suggestions_skip_already_taken_alternatives() -> None:
    store.register("alpha")
    store.register("alpha-2")
    with pytest.raises(NameConflict) as exc:
        store.register("alpha")
    # Should skip alpha-2 and suggest alpha-3, alpha-4, alpha-5
    assert "alpha-2" not in exc.value.suggestions
    assert "alpha-3" in exc.value.suggestions


# ── Differentiator #4: send is auth-free, anonymous sender allowed ───────────


def test_send_without_from_marks_as_anonymous() -> None:
    msg = store.send(to="recipient", body="who am I?")
    assert msg.sender_name == "anonymous"


def test_send_does_not_require_sender_to_be_registered() -> None:
    """Free sender string; no check that the sender exists."""
    msg = store.send(to="recipient", body="hi", from_="not-registered-name")
    assert msg.sender_name == "not-registered-name"


# ── Differentiator #5: TTL expires messages lazily ───────────────────────────


def test_ttl_purges_expired_messages_on_next_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Set TTL to 0 days; message is expired immediately. Lazy purge on read."""
    monkeypatch.setenv("DLB_MESSAGE_TTL_DAYS", "0")
    store.send(to="recipient", body="ephemeral", from_="sender")

    # The message exists in the DB right after send (expires_at is "now")
    # but it's already expired. Reading should purge and return nothing.
    inbox = store.read("recipient")
    assert inbox == []


def test_ttl_does_not_purge_unexpired_messages() -> None:
    """Default TTL is 7 days; sending and reading immediately should be fine."""
    store.send(to="recipient", body="fresh", from_="sender")
    inbox = store.read("recipient")
    assert len(inbox) == 1


# ── Auth model: read ──────────────────────────────────────────────────────────


def test_read_unregistered_name_needs_no_auth() -> None:
    store.send(to="ghost", body="open mail", from_="x")
    inbox = store.read("ghost")  # no token, no problem
    assert len(inbox) == 1


def test_read_registered_name_requires_session_token() -> None:
    reg = store.register("alpha")
    store.send(to="alpha", body="for alpha", from_="x")
    with pytest.raises(AuthError):
        store.read("alpha")  # no token
    with pytest.raises(AuthError):
        store.read("alpha", session_token="wrong-token")
    # Correct token works
    inbox = store.read("alpha", session_token=reg["session_token"])
    assert len(inbox) == 1


def test_read_marks_messages_as_read() -> None:
    reg = store.register("alpha")
    store.send(to="alpha", body="first", from_="x")
    store.send(to="alpha", body="second", from_="x")

    # First read: 2 unread
    inbox1 = store.read("alpha", session_token=reg["session_token"], unread_only=True)
    assert len(inbox1) == 2
    # Second read with unread_only=True: nothing
    inbox2 = store.read("alpha", session_token=reg["session_token"], unread_only=True)
    assert inbox2 == []
    # But unread_only=False returns them
    inbox3 = store.read("alpha", session_token=reg["session_token"], unread_only=False)
    assert len(inbox3) == 2
    assert all(m.read_at is not None for m in inbox3)


# ── Auth model: ack ───────────────────────────────────────────────────────────


def test_ack_requires_valid_session_token() -> None:
    reg = store.register("alpha")
    msg = store.send(to="alpha", body="x", from_="y")
    with pytest.raises(AuthError):
        store.ack(msg.id, "wrong-token")
    assert store.ack(msg.id, reg["session_token"]) is True


def test_ack_unknown_message_returns_false() -> None:
    reg = store.register("alpha")
    assert store.ack(999_999, reg["session_token"]) is False


def test_ack_on_unregistered_recipient_raises_auth_error() -> None:
    msg = store.send(to="ghost", body="x", from_="y")
    with pytest.raises(AuthError):
        store.ack(msg.id, "any-token")


# ── Auth model: unregister ────────────────────────────────────────────────────


def test_unregister_requires_session_token() -> None:
    reg = store.register("alpha")
    with pytest.raises(AuthError):
        store.unregister("alpha", "wrong-token")
    assert store.unregister("alpha", reg["session_token"]) is True


def test_unregister_preserves_messages_for_reregistration() -> None:
    """Messages survive unregister; re-registering reveals them."""
    reg1 = store.register("alpha")
    store.send(to="alpha", body="surviving message", from_="x")
    store.unregister("alpha", reg1["session_token"])

    # Re-register (anyone can claim the name now)
    reg2 = store.register("alpha")
    inbox = store.read("alpha", session_token=reg2["session_token"])
    assert len(inbox) == 1
    assert inbox[0].body == "surviving message"


def test_unregister_unknown_name_returns_false() -> None:
    assert store.unregister("never-existed", "any-token") is False


# ── list_threads ──────────────────────────────────────────────────────────────


def test_list_threads_returns_unread_counts_and_staleness() -> None:
    store.register("alpha", working_on="A")
    store.register("beta", working_on="B")
    store.send(to="alpha", body="x", from_="z")
    store.send(to="alpha", body="y", from_="z")

    threads = store.list_threads()
    by_name = {t.name: t for t in threads}
    assert by_name["alpha"].unread_count == 2
    assert by_name["alpha"].working_on == "A"
    assert by_name["beta"].unread_count == 0
    assert by_name["alpha"].stale is False
    assert by_name["beta"].stale is False


def test_list_threads_marks_stale_when_last_seen_outside_window() -> None:
    """An agent inactive longer than active_within is flagged stale."""
    store.register("oldtimer")
    # Manually backdate last_seen to a week ago
    import sqlite3

    from dlb_mcp.store import store_path

    conn = sqlite3.connect(str(store_path()))
    conn.execute(
        "UPDATE agents SET last_seen = ? WHERE name = ?",
        ((datetime.now(UTC) - timedelta(days=7)).isoformat(), "oldtimer"),
    )
    conn.commit()
    conn.close()

    threads = store.list_threads(active_within=timedelta(hours=24))
    by_name = {t.name: t for t in threads}
    assert by_name["oldtimer"].stale is True


# ── Concurrency smoke ─────────────────────────────────────────────────────────


def test_multiple_sends_get_distinct_ids() -> None:
    ids = {store.send(to="x", body=f"msg {i}", from_="s").id for i in range(20)}
    assert len(ids) == 20  # all unique

"""Tests for the P0+P1 security/correctness fixes:

#1 LIKE-metachar escape in _suggest_alternatives
#2 Epoch-ms integer timestamps (incl. v1→v2 migration)
#3 DLB_MAX_BODY_BYTES cap on send
#4 Stale-gated force=True takeover (+ prior_token bypass)
#5 Optional session_token on send binds from_ to token's name
"""

from __future__ import annotations

import sqlite3

import pytest

from dlb_mcp import store
from dlb_mcp.store import AuthError, DLBError, NameConflict, TakeoverDenied

# ── #1: LIKE-metachar escape ─────────────────────────────────────────────────


def test_suggestions_do_not_false_match_on_underscore_in_base() -> None:
    """A base ending in '_' would over-match without ESCAPE handling
    (underscore is a single-char wildcard in LIKE)."""
    store.register("a_")  # holds the literal name 'a_'
    store.register("ab-2")  # would FALSELY match `a_-%` if underscore unescaped

    with pytest.raises(NameConflict) as exc:
        store.register("a_")

    # 'ab-2' does NOT collide with 'a_-2'; the suggestion must still be 'a_-2'.
    assert "a_-2" in exc.value.suggestions
    # Sanity: the false-match would have skipped 'a_-2' and offered 'a_-3' instead.
    assert exc.value.suggestions[0] == "a_-2"


def test_suggestions_do_not_false_match_on_percent_in_base() -> None:
    """A base containing '%' is a 0+-char wildcard without escape."""
    store.register("a%")  # literal 'a%'
    store.register("axyz-2")  # would match `a%-%` if % unescaped
    store.register("axyz-3")

    with pytest.raises(NameConflict) as exc:
        store.register("a%")

    assert "a%-2" in exc.value.suggestions
    assert exc.value.suggestions[0] == "a%-2"


# ── #2: epoch-ms timestamps ──────────────────────────────────────────────────


def test_timestamps_stored_as_integer_ms(isolated_store) -> None:
    """Fresh DBs use INTEGER ms columns directly, not TEXT ISO strings."""
    store.register("alpha")
    store.send(to="alpha", body="x", from_="anon")

    conn = sqlite3.connect(str(isolated_store))
    conn.row_factory = sqlite3.Row
    agent_row = conn.execute(
        "SELECT registered_at_ms, last_seen_ms FROM agents WHERE name='alpha'"
    ).fetchone()
    msg_row = conn.execute(
        "SELECT sent_at_ms, expires_at_ms FROM messages WHERE recipient_name='alpha'"
    ).fetchone()
    conn.close()

    assert isinstance(agent_row["registered_at_ms"], int)
    assert isinstance(agent_row["last_seen_ms"], int)
    assert isinstance(msg_row["sent_at_ms"], int)
    assert isinstance(msg_row["expires_at_ms"], int)
    # Sanity: values are plausible (within a few seconds of now in ms)
    import time

    now_ms = int(time.time() * 1000)
    assert abs(msg_row["sent_at_ms"] - now_ms) < 5000


def test_v1_schema_migrates_to_v2_on_connect(isolated_store) -> None:
    """Hand-create a v1 DB (TEXT columns, no user_version), then call
    init_schema() and verify the *_ms columns are populated correctly."""
    # Build a v1-shaped DB manually
    conn = sqlite3.connect(str(isolated_store))
    conn.executescript(
        """
        CREATE TABLE agents (
            name           TEXT PRIMARY KEY,
            working_on     TEXT,
            registered_at  TEXT NOT NULL,
            last_seen      TEXT NOT NULL,
            session_token  TEXT NOT NULL
        );
        CREATE TABLE messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_name  TEXT NOT NULL,
            sender_name     TEXT NOT NULL,
            subject         TEXT,
            body            TEXT NOT NULL,
            sent_at         TEXT NOT NULL,
            read_at         TEXT,
            expires_at      TEXT NOT NULL
        );
        """
    )
    # Insert known timestamps (UTC ISO with explicit offset, as v1 produced)
    conn.execute(
        "INSERT INTO agents (name, working_on, registered_at, last_seen, "
        "session_token) VALUES "
        "('legacy', 'pre-migration', '2026-01-01T00:00:00+00:00', "
        "'2026-01-02T00:00:00+00:00', 'tok-legacy')"
    )
    conn.execute(
        "INSERT INTO messages (recipient_name, sender_name, subject, body, "
        "sent_at, read_at, expires_at) VALUES "
        "('legacy', 'oldsender', NULL, 'hello', '2026-01-03T00:00:00+00:00', "
        "NULL, '2099-12-31T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    # Trigger migration
    store.init_schema()

    # Re-open, verify *_ms columns populated, user_version stamped to 2
    conn = sqlite3.connect(str(isolated_store))
    conn.row_factory = sqlite3.Row
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 2

    agent = conn.execute(
        "SELECT registered_at_ms, last_seen_ms FROM agents WHERE name='legacy'"
    ).fetchone()
    msg = conn.execute(
        "SELECT sent_at_ms, read_at_ms, expires_at_ms FROM messages WHERE recipient_name='legacy'"
    ).fetchone()
    conn.close()

    # 2026-01-01T00:00:00 UTC == 1767225600000 ms
    assert agent["registered_at_ms"] == 1767225600000
    # 2026-01-02 == 1767225600000 + 86400000
    assert agent["last_seen_ms"] == 1767225600000 + 86_400_000
    # 2026-01-03
    assert msg["sent_at_ms"] == 1767225600000 + 2 * 86_400_000
    assert msg["read_at_ms"] is None
    # And the legacy row is now readable through the normal API
    msgs = store.read("legacy", session_token="tok-legacy")
    assert len(msgs) == 1
    assert msgs[0].body == "hello"


def test_init_schema_is_idempotent_post_migration(isolated_store) -> None:
    """Calling init_schema() twice on an already-v2 DB is a no-op."""
    store.register("alpha")
    # Second call must not error or duplicate data
    store.init_schema()
    store.init_schema()
    conn = sqlite3.connect(str(isolated_store))
    n = conn.execute("SELECT COUNT(*) FROM agents WHERE name='alpha'").fetchone()[0]
    conn.close()
    assert n == 1


def test_read_at_returned_matches_db_to_the_millisecond(isolated_store) -> None:
    """When read() marks messages as read, the read_at on returned objects
    must equal the read_at_ms stored in the DB — not a second 'now' read
    a few microseconds later that lexically/numerically diverges."""
    reg = store.register("alpha")
    store.send(to="alpha", body="m", from_="x")

    msgs = store.read("alpha", session_token=reg["session_token"])
    assert len(msgs) == 1
    returned_read_at = msgs[0].read_at
    assert returned_read_at is not None

    conn = sqlite3.connect(str(isolated_store))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT read_at_ms FROM messages WHERE id = ?", (msgs[0].id,)).fetchone()
    conn.close()

    db_read_at_ms = row["read_at_ms"]
    returned_read_at_ms = int(returned_read_at.timestamp() * 1000)
    assert returned_read_at_ms == db_read_at_ms


# ── #3: body size cap ────────────────────────────────────────────────────────


def test_send_rejects_oversized_body(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_MAX_BODY_BYTES", "100")
    with pytest.raises(DLBError) as exc:
        store.send(to="alpha", body="x" * 200, from_="me")
    assert "exceeds" in str(exc.value)
    assert "DLB_MAX_BODY_BYTES" in str(exc.value)


def test_send_accepts_body_exactly_at_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_MAX_BODY_BYTES", "100")
    msg = store.send(to="alpha", body="x" * 100, from_="me")
    assert msg.id > 0


def test_default_cap_is_256_kib(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DLB_MAX_BODY_BYTES", raising=False)
    assert store.max_body_bytes() == 256 * 1024


def test_send_counts_utf8_bytes_not_codepoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 4-byte emoji should count as 4 toward the cap, not 1."""
    monkeypatch.setenv("DLB_MAX_BODY_BYTES", "3")
    # 1 codepoint, 4 UTF-8 bytes — over the 3-byte cap
    with pytest.raises(DLBError):
        store.send(to="alpha", body="🔔", from_="me")


# ── #4: stale-gated force takeover ───────────────────────────────────────────


def test_force_takeover_denied_while_holder_is_active() -> None:
    """Default DLB_TAKEOVER_AFTER_SECONDS=86400; a just-registered holder
    is well within that window, so force=True without prior_token should
    raise TakeoverDenied — NOT silently hijack the name."""
    store.register("alpha")
    with pytest.raises(TakeoverDenied):
        store.register("alpha", force=True)


def test_force_takeover_allowed_when_holder_stale(
    monkeypatch: pytest.MonkeyPatch, isolated_store
) -> None:
    """Shorten the takeover window to 0 so the holder is immediately
    considered stale; force=True without prior_token now succeeds."""
    monkeypatch.setenv("DLB_TAKEOVER_AFTER_SECONDS", "0")
    r1 = store.register("alpha")
    r2 = store.register("alpha", force=True)
    assert r2["session_token"] != r1["session_token"]
    # Old token is now invalid
    with pytest.raises(AuthError):
        store.read("alpha", session_token=r1["session_token"])


def test_force_takeover_allowed_with_matching_prior_token() -> None:
    """A legitimate handoff (caller has the current token) should not
    have to wait for the stale window — passing prior_token bypasses it."""
    r1 = store.register("alpha")
    r2 = store.register("alpha", force=True, prior_token=r1["session_token"])
    assert r2["session_token"] != r1["session_token"]


def test_force_takeover_denied_with_wrong_prior_token() -> None:
    """A bogus prior_token does NOT bypass the stale-gate."""
    store.register("alpha")
    with pytest.raises(TakeoverDenied):
        store.register("alpha", force=True, prior_token="not-the-real-token")


def test_register_without_force_still_raises_nameconflict() -> None:
    """The stale-gate only affects force=True. force=False unchanged."""
    store.register("alpha")
    with pytest.raises(NameConflict):
        store.register("alpha")


# ── #5: session_token on send binds from_ ────────────────────────────────────


def test_send_without_token_keeps_anonymous_default() -> None:
    msg = store.send(to="ghost", body="hi")
    assert msg.sender_name == "anonymous"


def test_send_without_token_keeps_arbitrary_from_string() -> None:
    """Backward-compatible: tokenless callers can still spoof any from_."""
    msg = store.send(to="ghost", body="hi", from_="claims-to-be-bob")
    assert msg.sender_name == "claims-to-be-bob"


def test_send_with_token_auto_sets_from_to_registered_name() -> None:
    reg = store.register("alpha")
    msg = store.send(to="ghost", body="hi", session_token=reg["session_token"])
    assert msg.sender_name == "alpha"


def test_send_with_token_rejects_mismatched_from() -> None:
    """If you supply both, they must agree — no spoofing."""
    reg = store.register("alpha")
    with pytest.raises(AuthError):
        store.send(
            to="ghost",
            body="hi",
            from_="bravo",
            session_token=reg["session_token"],
        )


def test_send_with_token_accepts_matching_from() -> None:
    reg = store.register("alpha")
    msg = store.send(
        to="ghost",
        body="hi",
        from_="alpha",
        session_token=reg["session_token"],
    )
    assert msg.sender_name == "alpha"


def test_send_with_invalid_token_raises_autherror() -> None:
    """A garbage token shouldn't silently degrade to anonymous — it's a
    caller bug worth surfacing."""
    with pytest.raises(AuthError):
        store.send(to="ghost", body="hi", session_token="bogus-token")

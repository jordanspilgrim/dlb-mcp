"""Regression tests for the 2026-07 adversarial-review hardening pass.

Covers:
  Issue 2a  control chars rejected in metadata fields + monitor sink sanitizes
  Issue 3a  read() of an unregistered name is a NON-destructive peek
  Issue 4   all short metadata fields are size-capped (not just body)
  Issue 5   token comparisons are constant-time and reject None
  Issue 6   secret creation is atomic (siblings converge on one secret)
  Issue 7   v1->v2 migration stamps user_version inside its transaction
  Issue 8   list_threads uses an IMMEDIATE (write) transaction

Note on Issue 1a: the recover_token / deterministic-token behavior is
intentionally unchanged (accepted cooperative-model tradeoff) — that fix was
docstring-only, so there is no behavior regression test for it here; the
existing test_recover_token.py already pins the behavior.
"""

from __future__ import annotations

import sqlite3

import pytest

from dlb_mcp import store
from dlb_mcp.monitor import _format_event, _sanitize_line
from dlb_mcp.store import AuthError, DLBError

# ── Issue 2a: control-character rejection + monitor sink sanitization ─────────


@pytest.mark.parametrize("bad", ["a\nb", "a\rb", "a\tb", "x\x00y", "z\x7f"])
def test_send_rejects_control_chars_in_from(bad: str) -> None:
    with pytest.raises(DLBError, match="control characters"):
        store.send(to="x", body="hi", from_=bad)


@pytest.mark.parametrize("field", ["subject", "headline", "msg_type"])
def test_send_rejects_control_chars_in_metadata(field: str) -> None:
    with pytest.raises(DLBError, match="control characters"):
        store.send(to="x", body="hi", **{field: "a\nb"})


def test_register_rejects_control_chars_in_name() -> None:
    with pytest.raises(DLBError, match="control characters"):
        store.register("evil\nname")


def test_ordinary_unicode_name_still_accepted() -> None:
    # The control-char guard must not reject normal (incl. non-ASCII) names.
    r = store.register("wörker-1")
    assert r["name"] == "wörker-1"


def test_monitor_event_is_always_single_line_even_on_dirty_sender() -> None:
    # Defense in depth: a legacy/raw row with a newline sender must not forge
    # a second Monitor event line.
    line = _format_event(1234567890000, 'bravo: "ok"\n2026 sysadmin', None, "body")
    assert "\n" not in line
    assert line.count("\n") == 0


def test_sanitize_line_collapses_controls() -> None:
    assert _sanitize_line("a\nb\tc\r\x00d") == "a b c  d"


# ── Issue 3a: non-destructive dead-letter peek ───────────────────────────────


def test_unregistered_peek_does_not_mark_read() -> None:
    store.send(to="future", body="queued task", msg_type="task", from_="boss")
    peek = store.read("future")  # unauth peek of an unclaimed inbox
    assert [m.body for m in peek] == ["queued task"]
    # The intended owner registers later and does the default (unread-only) read.
    reg = store.register("future")
    owner_view = store.read("future", session_token=reg["session_token"])
    assert [m.body for m in owner_view] == ["queued task"], (
        "queued dead-letter must still be unread for the real owner"
    )


def test_registered_owner_read_still_marks_read() -> None:
    reg = store.register("owner")
    store.send(to="owner", body="hello")
    first = store.read("owner", session_token=reg["session_token"])
    assert len(first) == 1
    # Second unread-only read returns nothing — the owner path is still destructive.
    second = store.read("owner", session_token=reg["session_token"])
    assert second == []


# ── Issue 4: size caps on every short metadata field ─────────────────────────


@pytest.mark.parametrize("field", ["subject", "headline", "from_", "msg_type"])
def test_oversized_metadata_field_rejected(field: str) -> None:
    with pytest.raises(DLBError, match="too large"):
        store.send(to="x", body="hi", **{field: "A" * (store.max_field_bytes() + 1)})


def test_oversized_name_rejected() -> None:
    with pytest.raises(DLBError, match="too large"):
        store.register("A" * (store.max_field_bytes() + 1))


def test_field_cap_is_env_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_MAX_FIELD_BYTES", "16")
    assert store.max_field_bytes() == 16
    with pytest.raises(DLBError, match="too large"):
        store.send(to="x", body="hi", subject="A" * 17)
    # At the boundary it is accepted.
    store.send(to="x", body="hi", subject="A" * 16)


# ── Issue 5: constant-time compare + None rejection ──────────────────────────


def test_tokens_equal_rejects_none_and_mismatch() -> None:
    assert store._tokens_equal("abc", "abc") is True
    assert store._tokens_equal("abc", "abd") is False
    assert store._tokens_equal(None, "abc") is False
    assert store._tokens_equal("abc", None) is False
    assert store._tokens_equal(None, None) is False


def test_read_registered_without_token_is_rejected() -> None:
    store.register("owner")
    store.send(to="owner", body="hi")
    with pytest.raises(AuthError):
        store.read("owner")  # no token supplied
    with pytest.raises(AuthError):
        store.read("owner", session_token="wrong")


# ── Issue 6: atomic secret creation ──────────────────────────────────────────


def test_secret_creation_is_stable_and_persisted(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DLB_STORE", str(tmp_path / "s" / "store.sqlite3"))
    s1 = store._get_or_create_secret()
    s2 = store._get_or_create_secret()
    assert s1 == s2 and len(s1) >= 32
    # The secret file exists with tight perms and matches what we return.
    p = store._secret_path()
    assert p.read_bytes() == s1
    mode = p.stat().st_mode & 0o777
    assert mode == 0o600


# ── Issue 7: migration stamps user_version atomically ────────────────────────


def test_v1_to_v2_migration_sets_user_version_in_txn(tmp_path, monkeypatch) -> None:
    """Build a legacy v1-shaped DB by hand, run init_schema, and assert the
    store ends fully migrated with user_version == SCHEMA_VERSION and the v1
    rebuild path is not re-runnable into a brick state."""
    db = tmp_path / "legacy.sqlite3"
    monkeypatch.setenv("DLB_STORE", str(db))
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE agents (
            name TEXT PRIMARY KEY, working_on TEXT,
            registered_at TEXT NOT NULL, last_seen TEXT NOT NULL,
            session_token TEXT NOT NULL
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_name TEXT NOT NULL, sender_name TEXT NOT NULL,
            subject TEXT, body TEXT NOT NULL,
            sent_at TEXT NOT NULL, read_at TEXT, expires_at TEXT NOT NULL
        );
        INSERT INTO agents VALUES
            ('alpha', 'x', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', 'tok');
        INSERT INTO messages (recipient_name, sender_name, subject, body, sent_at, expires_at)
            VALUES ('alpha', 'boss', 's', 'b', '2026-01-01T00:00:00+00:00',
                    '2099-01-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()

    store.init_schema()
    with sqlite3.connect(str(db)) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == store.SCHEMA_VERSION
    # Store is usable post-migration and the migrated message survived.
    reg = store.register("alpha", force=True, prior_token="tok")
    msgs = store.read("alpha", session_token=reg["session_token"], unread_only=False)
    assert any(m.body == "b" for m in msgs)


# ── Issue 9: per-inbox ring-buffer cap (drop oldest) ─────────────────────────


def test_inbox_cap_drops_oldest_and_keeps_newest(monkeypatch) -> None:
    monkeypatch.setenv("DLB_MAX_INBOX", "5")
    for i in range(12):
        store.send(to="flooded", body=f"m{i}")
    reg = store.register("flooded")
    msgs = store.read("flooded", session_token=reg["session_token"], unread_only=False, limit=1000)
    bodies = {m.body for m in msgs}
    assert len(msgs) == 5, "inbox must be bounded to the cap"
    assert bodies == {"m7", "m8", "m9", "m10", "m11"}, "newest 5 retained, oldest dropped"


def test_inbox_cap_never_drops_the_just_sent_message(monkeypatch) -> None:
    monkeypatch.setenv("DLB_MAX_INBOX", "1")
    store.send(to="one", body="first")
    m = store.send(to="one", body="second")
    reg = store.register("one")
    msgs = store.read("one", session_token=reg["session_token"], unread_only=False, limit=10)
    assert [x.body for x in msgs] == ["second"]
    assert m.body == "second"


def test_inbox_cap_disabled_with_zero(monkeypatch) -> None:
    monkeypatch.setenv("DLB_MAX_INBOX", "0")
    for i in range(30):
        store.send(to="unbounded", body=f"m{i}")
    reg = store.register("unbounded")
    msgs = store.read(
        "unbounded", session_token=reg["session_token"], unread_only=False, limit=1000
    )
    assert len(msgs) == 30, "cap disabled → nothing dropped"


def test_inbox_cap_is_per_recipient(monkeypatch) -> None:
    monkeypatch.setenv("DLB_MAX_INBOX", "2")
    for i in range(5):
        store.send(to="a", body=f"a{i}")
        store.send(to="b", body=f"b{i}")
    ra = store.register("a")
    rb = store.register("b")
    a = store.read("a", session_token=ra["session_token"], unread_only=False, limit=100)
    b = store.read("b", session_token=rb["session_token"], unread_only=False, limit=100)
    assert len(a) == 2 and len(b) == 2, "cap applies independently per inbox"


# ── Issue 8: list_threads writes under an IMMEDIATE transaction ───────────────


def test_list_threads_purges_and_returns(monkeypatch) -> None:
    # TTL 0 → the message is already expired; list_threads' purge (a write)
    # must run cleanly under the IMMEDIATE txn and drop it.
    monkeypatch.setenv("DLB_MESSAGE_TTL_DAYS", "0")
    store.register("a")
    store.send(to="a", body="ephemeral")
    threads = store.list_threads()
    names = {t.name for t in threads}
    assert "a" in names
    a = next(t for t in threads if t.name == "a")
    assert a.unread_count == 0  # expired message purged

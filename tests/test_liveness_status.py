"""#3 agent liveness — self-reported status surfaced in list_threads."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from dlb_mcp import store
from dlb_mcp.store import AuthError, DLBError


def _summary(name: str):
    return next(s for s in store.list_threads() if s.name == name)


def test_register_defaults_status_to_working():
    store.register("alpha")
    assert _summary("alpha").status == "working"


def test_set_status_updates_and_surfaces_in_list_threads():
    reg = store.register("alpha")
    store.set_status("alpha", reg["session_token"], "blocked", detail="waiting on PR #42")
    s = _summary("alpha")
    assert s.status == "blocked"
    assert s.status_detail == "waiting on PR #42"


def test_set_status_bumps_last_seen_heartbeat():
    reg = store.register("alpha")
    before = _summary("alpha").last_seen
    time.sleep(0.01)
    store.set_status("alpha", reg["session_token"], "idle")
    assert _summary("alpha").last_seen >= before


def test_set_status_requires_valid_token():
    store.register("alpha")
    with pytest.raises(AuthError):
        store.set_status("alpha", "wrong-token", "done")


def test_set_status_unregistered_name_raises():
    with pytest.raises(DLBError):
        store.set_status("ghost", "whatever", "done")


def test_set_status_rejects_empty_status():
    reg = store.register("alpha")
    with pytest.raises(DLBError):
        store.set_status("alpha", reg["session_token"], "")


def test_reregister_does_not_clobber_a_set_status():
    reg = store.register("alpha")
    store.set_status("alpha", reg["session_token"], "done")
    # Re-register (recovery / resume) must not silently reset a deliberate status.
    store.register("alpha", working_on="resumed", force=True, prior_token=reg["session_token"])
    assert _summary("alpha").status == "done"


def test_v4_db_migrates_to_v5_with_status_columns(isolated_store: Path):
    """Hand-build a v4 agents table (no status cols), migrate, verify v5."""
    conn = sqlite3.connect(str(isolated_store))
    conn.executescript(
        """
        CREATE TABLE agents (
            name TEXT PRIMARY KEY, working_on TEXT, registered_at_ms INTEGER NOT NULL,
            last_seen_ms INTEGER NOT NULL, session_token TEXT NOT NULL
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, recipient_name TEXT NOT NULL,
            sender_name TEXT NOT NULL, body TEXT NOT NULL, sent_at_ms INTEGER NOT NULL,
            expires_at_ms INTEGER NOT NULL
        );
        PRAGMA user_version = 4;
        """
    )
    conn.close()

    store.init_schema()

    conn = sqlite3.connect(str(isolated_store))
    cols = {r[1] for r in conn.execute("PRAGMA table_info(agents)")}
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert "status" in cols and "status_detail" in cols
    assert version == 5

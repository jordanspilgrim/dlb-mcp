"""Stage 1 regressions — atomic, locked, self-healing init_schema.

Covers the red-team migration/concurrency findings:
  #2  concurrent init_schema no longer throws raw sqlite errors
  #3  a crashed fresh-init (modern tables + user_version 0) self-heals, not bricks
  #11 a store newer than the code raises a clear DLBError
  #12 a corrupted v1 table with duplicate names migrates instead of stranding
"""

from __future__ import annotations

import multiprocessing as mp
import sqlite3
from pathlib import Path

import pytest

from dlb_mcp import store
from dlb_mcp.store import DLBError


def _build_v5_store(path: Path, n_agents: int = 3) -> None:
    """Hand-build a v5-shaped store (pre session_id / token_hash) with rows."""
    c = sqlite3.connect(str(path))
    # Real DLB stores are always WAL; set it here so the concurrency test
    # exercises migration-lock contention, not one-time journal-mode conversion.
    c.execute("PRAGMA journal_mode = WAL")
    c.executescript(
        """
        CREATE TABLE agents (
            name TEXT PRIMARY KEY, working_on TEXT,
            registered_at_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL,
            session_token TEXT NOT NULL, status TEXT, status_detail TEXT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_name TEXT NOT NULL, sender_name TEXT NOT NULL,
            subject TEXT, body TEXT NOT NULL,
            sent_at_ms INTEGER NOT NULL, read_at_ms INTEGER, expires_at_ms INTEGER NOT NULL,
            msg_type TEXT, in_reply_to INTEGER, status TEXT, status_note TEXT,
            status_updated_at_ms INTEGER, headline TEXT
        );
        CREATE INDEX idx_messages_recipient
            ON messages(recipient_name, read_at_ms, sent_at_ms DESC);
        PRAGMA user_version = 5;
        """
    )
    for i in range(n_agents):
        c.execute(
            "INSERT INTO agents (name, working_on, registered_at_ms, last_seen_ms, "
            "session_token, status) VALUES (?, ?, 1, 1, ?, 'working')",
            (f"agent{i}", "x", f"tok{i}"),
        )
    c.commit()
    c.close()


def _mig_worker(store_path: str, idx: int, barrier, q) -> None:
    import os

    os.environ["DLB_STORE"] = store_path
    from dlb_mcp import store as s

    try:
        barrier.wait(timeout=20)
        # Any op triggers init_schema; do a unique-name write and a read to
        # exercise both the migration lock and post-migration write contention.
        s.register(f"racer{idx}")
        s.list_threads()
        q.put(("ok", None))
    except Exception as e:  # noqa: BLE001 — we want to report ANY escape
        q.put(("err", f"{type(e).__name__}: {e}"))


def test_concurrent_init_schema_has_no_unhandled_errors(tmp_path: Path) -> None:
    """8 processes hit a pre-v7 store at a barrier; none may throw a raw
    OperationalError, and the store ends fully migrated. (Was ~70% failure.)"""
    dbp = tmp_path / "store.sqlite3"
    _build_v5_store(dbp)
    ctx = mp.get_context("spawn")
    n = 8
    barrier = ctx.Barrier(n)
    q: mp.Queue = ctx.Queue()
    procs = [ctx.Process(target=_mig_worker, args=(str(dbp), i, barrier, q)) for i in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(30)
    results = [q.get(timeout=5) for _ in range(n)]
    errs = [m for (s, m) in results if s == "err"]
    assert not errs, f"concurrent migration raised: {errs}"
    with sqlite3.connect(str(dbp)) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == store.SCHEMA_VERSION


def test_crashed_fresh_init_self_heals(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Modern (v7) tables stamped user_version=0 — the state a crash between
    _create_current_schema and the version stamp used to leave. Must self-heal
    to the current version and be usable, NOT brick as a mis-detected v1 DB."""
    dbp = tmp_path / "store.sqlite3"
    monkeypatch.setenv("DLB_STORE", str(dbp))
    store.init_schema()  # build a clean current-schema store
    with sqlite3.connect(str(dbp)) as c:
        c.execute("PRAGMA user_version = 0")  # simulate the crash gap
    store.init_schema()  # must not raise / brick
    with sqlite3.connect(str(dbp)) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == store.SCHEMA_VERSION
    # And the store is usable.
    reg = store.register("alpha")
    assert store.read("alpha", session_token=reg["session_token"]) == []


def test_store_newer_than_code_raises_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dbp = tmp_path / "store.sqlite3"
    monkeypatch.setenv("DLB_STORE", str(dbp))
    store.init_schema()
    with sqlite3.connect(str(dbp)) as c:
        c.execute(f"PRAGMA user_version = {store.SCHEMA_VERSION + 1}")
    with pytest.raises(DLBError, match="newer than this dlb-mcp"):
        store.init_schema()


def test_v1_duplicate_names_migrate_without_bricking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupted v1 agents table lacking the name PRIMARY KEY, with duplicate
    names, must migrate (de-duplicated) instead of aborting and stranding."""
    dbp = tmp_path / "store.sqlite3"
    monkeypatch.setenv("DLB_STORE", str(dbp))
    c = sqlite3.connect(str(dbp))
    c.executescript(
        """
        CREATE TABLE agents (
            name TEXT, working_on TEXT, registered_at TEXT NOT NULL,
            last_seen TEXT NOT NULL, session_token TEXT NOT NULL
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, recipient_name TEXT NOT NULL,
            sender_name TEXT NOT NULL, subject TEXT, body TEXT NOT NULL,
            sent_at TEXT NOT NULL, read_at TEXT, expires_at TEXT NOT NULL
        );
        INSERT INTO agents VALUES
            ('dup','a','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00','t1');
        INSERT INTO agents VALUES
            ('dup','b','2026-02-01T00:00:00+00:00','2026-02-01T00:00:00+00:00','t2');
        """
    )
    c.commit()
    c.close()
    store.init_schema()  # must not raise IntegrityError
    with sqlite3.connect(str(dbp)) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == store.SCHEMA_VERSION
        assert c.execute("SELECT count(*) FROM agents WHERE name='dup'").fetchone()[0] == 1

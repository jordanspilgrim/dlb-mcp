"""Shared test fixtures. Each test gets an isolated SQLite store."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolated_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point DLB at a per-test sqlite file. Auto-used by every test."""
    p = tmp_path / "store.sqlite3"
    monkeypatch.setenv("DLB_STORE", str(p))
    # Also clear any cached env so store_path() re-reads
    monkeypatch.delenv("DLB_MESSAGE_TTL_DAYS", raising=False)
    # The suite often runs inside a Claude Code session, whose real session
    # id would leak into store.resolve_session_id(). Tests set ids explicitly.
    monkeypatch.delenv("DLB_SESSION_ID", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    return p

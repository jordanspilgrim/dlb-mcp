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
    return p

"""Shared assertion for "this store is fully migrated".

Since v9 the store carries two versions: PRAGMA user_version holds the minimum
reader version (so older builds keep working during a rolling upgrade), and
dlb_meta.schema_version holds the schema actually applied. A migrated store
must have both.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from dlb_mcp import store


def assert_store_current(db: Path | str) -> None:
    with sqlite3.connect(str(db)) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == store.MIN_READER_VERSION
        row = c.execute("SELECT value FROM dlb_meta WHERE key = 'schema_version'").fetchone()
        assert row is not None, "dlb_meta.schema_version missing after migration"
        assert int(row[0]) == store.SCHEMA_VERSION

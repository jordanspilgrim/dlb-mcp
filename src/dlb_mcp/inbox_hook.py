"""``dlb-inbox-check``: Claude Code SessionStart / UserPromptSubmit hook.

Tells the agent, at every prompt boundary, which registered names have unread
work waiting, so a session that is not running dlb-monitor still finds its
mail on its next turn.

Replaces the old hand-written ``~/.claude/hooks/dlb-inbox-check.sh``, which
had two bugs that made it silently report nothing:

  • it queried ``read_at``, a column that does not exist (it is ``read_at_ms``);
  • it ran the sqlite3 CLI read-only against a WAL store, which fails.

Counts match ``list_threads``' ``unread_count``: read receipts, cancelled or
superseded messages, and messages past their deadline are not work, so they
are not counted.

Usage: ``dlb-inbox-check <EventName>`` (default UserPromptSubmit). Reads the
store strictly read-only (never creates, migrates or writes it). Always exits
0 and prints nothing when the store is missing or nothing is unread: a hook
must never break a prompt.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time

from . import store

DEFAULT_EVENT = "UserPromptSubmit"


def unread_counts() -> list[tuple[str, int]]:
    """(recipient, unread work count) pairs, sorted by name. [] on any error."""
    path = store.store_path()
    if not path.is_file():
        return []
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error:
        return []
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
        if "read_at_ms" not in cols:
            return []
        where = ["read_at_ms IS NULL"]
        params: list[object] = []
        if "msg_type" in cols:
            where.append("(msg_type IS NULL OR msg_type != 'receipt')")
        if "cancelled_at_ms" in cols:
            where.append("cancelled_at_ms IS NULL")
        if "deadline_ms" in cols:
            where.append("(deadline_ms IS NULL OR deadline_ms > ?)")
            params.append(int(time.time() * 1000))
        if "expires_at_ms" in cols:
            where.append("(expires_at_ms IS NULL OR expires_at_ms > ?)")
            params.append(int(time.time() * 1000))
        rows = conn.execute(
            "SELECT recipient_name, COUNT(*) FROM messages WHERE "
            + " AND ".join(where)
            + " GROUP BY recipient_name ORDER BY recipient_name",
            params,
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    return [(str(name), int(n)) for name, n in rows]


def render(event: str, counts: list[tuple[str, int]]) -> str | None:
    """The hook's JSON output, or None when there is nothing to report."""
    if not counts:
        return None
    listing = ", ".join(f"{name} ({n})" for name, n in counts)
    context = (
        f"📬 DLB inbox status: {listing} unread. If your registered name is in this "
        "list, call mcp__dlb__read with your session_token to fetch the messages "
        "BEFORE responding to the user. Surface message senders + bodies to the "
        "user explicitly."
    )
    return json.dumps(
        {"hookSpecificOutput": {"hookEventName": event, "additionalContext": context}},
        ensure_ascii=False,
    )


def main() -> None:
    event = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else DEFAULT_EVENT
    try:
        out = render(event, unread_counts())
    except Exception:
        return
    if out:
        print(out)

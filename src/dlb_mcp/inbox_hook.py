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

It also warns when a name THIS session registered has no live dlb-monitor
(no heartbeat within the wake window). The hook payload's ``session_id`` is
the value the MCP config passes to the server as ``DLB_SESSION_ID``, so the
hook finds this session's names without being told them. That catches the
two silent failures: the agent never launched the monitor, or compaction
killed the Monitor task.

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


def unmonitored_names(session_id: str | None) -> list[str]:
    """Names registered by ``session_id`` with no fresh monitor heartbeat.

    Sorted. [] when the session id is unknown, the store predates heartbeats
    (schema < v9), or on any error.
    """
    if not session_id:
        return []
    path = store.store_path()
    if not path.is_file():
        return []
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error:
        return []
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(agents)")}
        if not {"session_id", "monitor_heartbeat_ms"} <= cols:
            return []
        cutoff = int(time.time() * 1000) - store.wake_window_seconds() * 1000
        rows = conn.execute(
            "SELECT name FROM agents WHERE session_id = ? "
            "AND (monitor_heartbeat_ms IS NULL OR monitor_heartbeat_ms < ?) "
            "ORDER BY name",
            (session_id, cutoff),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    return [str(r[0]) for r in rows]


def render(
    event: str,
    counts: list[tuple[str, int]],
    unmonitored: list[str] | None = None,
) -> str | None:
    """The hook's JSON output, or None when there is nothing to report."""
    parts: list[str] = []
    if counts:
        listing = ", ".join(f"{name} ({n})" for name, n in counts)
        parts.append(
            f"📬 DLB inbox status: {listing} unread. If your registered name is in "
            "this list, call mcp__dlb__read with your session_token to fetch the "
            "messages BEFORE responding to the user. Surface message senders + "
            "bodies to the user explicitly."
        )
    for name in unmonitored or []:
        parts.append(
            f"⚠ DLB: '{name}' is registered from this session but no dlb-monitor is "
            "running for it, so mail sent while you are idle will not wake you. "
            "Launch it now: Monitor({command: "
            f'"~/.claude/hooks/dlb-monitor-supervised.sh --name {name}", '
            f'description: "DLB inbox: {name}", persistent: true}}). '
            "If you are done with this name, unregister it instead."
        )
    if not parts:
        return None
    context = "\n\n".join(parts)
    return json.dumps(
        {"hookSpecificOutput": {"hookEventName": event, "additionalContext": context}},
        ensure_ascii=False,
    )


def _payload_session_id() -> str | None:
    """``session_id`` from the hook's stdin JSON payload, if any."""
    try:
        if sys.stdin is None or sys.stdin.isatty():
            return None
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return None
    sid = payload.get("session_id") if isinstance(payload, dict) else None
    return sid if isinstance(sid, str) and sid else None


def main() -> None:
    event = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else DEFAULT_EVENT
    session_id = _payload_session_id()
    try:
        out = render(event, unread_counts(), unmonitored_names(session_id))
    except Exception:
        return
    if out:
        print(out)

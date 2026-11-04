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

When the hook payload carries a ``session_id``, the unread list is scoped to
names THIS session registered (``agents.session_id``), so one session is not
told about every other session's mail. Without a session id it falls back to
the global list. Either way the list is capped (``MAX_LISTED`` names, each at
most ``MAX_NAME_CHARS``) so a large fleet cannot flood the prompt.

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
import shlex
import sqlite3
import sys
import time

from . import store

DEFAULT_EVENT = "UserPromptSubmit"
MAX_LISTED = 20
MAX_NAME_CHARS = 64


def unread_counts(session_id: str | None = None) -> list[tuple[str, int]]:
    """(recipient, unread work count) pairs, sorted by name. [] on any error.

    With ``session_id``, only names registered by that session are counted
    (when the store records ``agents.session_id``); otherwise every recipient.
    """
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
        # Only registered names: a dead-letter recipient (send accepts any
        # name) has no session that could read it, so reminding about it on
        # every prompt is noise.
        where.append("recipient_name IN (SELECT name FROM agents)")
        if session_id:
            agent_cols = {r[1] for r in conn.execute("PRAGMA table_info(agents)")}
            if "session_id" in agent_cols:
                where.append("recipient_name IN (SELECT name FROM agents WHERE session_id = ?)")
                params.append(session_id)
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


def _clip(name: str) -> str:
    """A name shortened for display (never used inside a command)."""
    return name if len(name) <= MAX_NAME_CHARS else name[: MAX_NAME_CHARS - 1] + "…"


def _more(total: int) -> str:
    extra = total - MAX_LISTED
    return f", +{extra} more" if extra > 0 else ""


def render(
    event: str,
    counts: list[tuple[str, int]],
    unmonitored: list[str] | None = None,
) -> str | None:
    """The hook's JSON output, or None when there is nothing to report."""
    parts: list[str] = []
    if counts:
        listing = ", ".join(f"{_clip(name)} ({n})" for name, n in counts[:MAX_LISTED]) + _more(
            len(counts)
        )
        parts.append(
            f"📬 DLB inbox status: {listing} unread. If your registered name is in "
            "this list, call mcp__dlb__read with your session_token to fetch the "
            "messages BEFORE responding to the user. Surface message senders + "
            "bodies to the user explicitly."
        )
    unmonitored = unmonitored or []
    for name in unmonitored[:MAX_LISTED]:
        # The name is free text chosen by whoever registered it: shell-quote it
        # inside the command and JSON-encode both strings so it cannot break
        # out of either.
        command = json.dumps(
            "~/.claude/hooks/dlb-monitor-supervised.sh --name " + shlex.quote(name),
            ensure_ascii=False,
        )
        description = json.dumps("DLB inbox: " + _clip(name), ensure_ascii=False)
        parts.append(
            f"⚠ DLB: {json.dumps(_clip(name), ensure_ascii=False)} is registered "
            "from this session but no dlb-monitor is running for it, so mail sent "
            "while you are idle will not wake you. Launch it now: Monitor({command: "
            f"{command}, description: {description}, persistent: true}}). "
            "If you are done with this name, unregister it instead."
        )
    if len(unmonitored) > MAX_LISTED:
        parts.append(
            f"⚠ DLB: {len(unmonitored) - MAX_LISTED} more names from this session "
            "have no dlb-monitor."
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
        out = render(event, unread_counts(session_id), unmonitored_names(session_id))
    except Exception:
        return
    if out:
        print(out)

"""FastMCP server exposing DLB's six tools over stdio.

This module is a thin façade — all real logic lives in store.py. The tools
here exist only to:
    1. Receive MCP arguments and dispatch to store functions
    2. Convert store dataclasses to plain dicts for JSON-RPC serialization
    3. Translate store exceptions into MCP-friendly error responses
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from mcp.server.fastmcp import FastMCP

from . import store

mcp = FastMCP(
    name="dlb",
    instructions=(
        "DLB — Dead Letter Box. Six tools for inter-agent messaging:\n"
        "  register, list_threads, send, read, ack, unregister.\n"
        "Send is open (no auth). Read/ack/unregister on a registered name "
        "require its session_token.\n"
        "Sends to nonexistent recipients are queued and delivered on later registration."
    ),
)


# ── Serialization helpers ────────────────────────────────────────────────────


def _agent_summary_dict(s: store.AgentSummary) -> dict[str, Any]:
    return {
        "name": s.name,
        "working_on": s.working_on,
        "last_seen": s.last_seen.isoformat(),
        "unread_count": s.unread_count,
        "stale": s.stale,
    }


def _message_dict(m: store.Message) -> dict[str, Any]:
    return {
        "id": m.id,
        "recipient_name": m.recipient_name,
        "sender_name": m.sender_name,
        "subject": m.subject,
        "body": m.body,
        "sent_at": m.sent_at.isoformat(),
        "read_at": m.read_at.isoformat() if m.read_at else None,
        "expires_at": m.expires_at.isoformat(),
    }


# ── Tools ────────────────────────────────────────────────────────────────────


@mcp.tool()
def register(
    name: str,
    working_on: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Declare or refresh an agent identity.

    Returns the agent record plus a `session_token` you must keep — it's
    required for `read`, `ack`, and `unregister` on this name.

    Conflict behavior: if the name is already registered to an active
    session and you don't pass `force=True`, raises an error with up to
    three suggested alternatives (e.g., `alpha-2`, `alpha-3`).

    Args:
        name: Stable identifier you'll be addressed by. Accepted as-given
            (no rename, no auto-suffix). Examples: "alpha", "ThreadBeta",
            "worker-1".
        working_on: Free-text description of what this agent is doing.
            Shown to other agents via `list_threads`. Optional.
        force: If True, evict the prior session holding this name and
            take it over. Use with care — silently kicks the previous owner.
    """
    return store.register(name, working_on=working_on, force=force)


@mcp.tool()
def list_threads(active_within_hours: int = 24) -> list[dict[str, Any]]:
    """List all known agents with their unread counts and staleness flags.

    No auth required. Use this to discover who's around before sending.
    Agents whose `last_seen` is older than `active_within_hours` are marked
    `stale=True` but still returned (so you can see "alpha was here last week").

    Args:
        active_within_hours: How many hours back to consider 'active' for
            the staleness flag. Default 24.
    """
    items = store.list_threads(active_within=timedelta(hours=active_within_hours))
    return [_agent_summary_dict(s) for s in items]


@mcp.tool()
def send(
    to: str,
    body: str,
    subject: str | None = None,
    from_: str | None = None,
) -> dict[str, Any]:
    """Drop a message into `to`'s inbox.

    ALWAYS succeeds, even if `to` is not registered yet. This is DLB's
    namesake feature — messages queue under the name, and will be in the
    recipient's inbox when they register (or whenever someone reads under
    that name).

    No auth required. The sender is identified by the `from_` field you
    pass; if omitted, it's the literal string "anonymous". DLB does not
    try to auto-identify the caller.

    Args:
        to: Recipient name.
        body: Message body (free text / Markdown).
        subject: Optional short subject line.
        from_: Sender label. Defaults to "anonymous".
    """
    msg = store.send(to=to, body=body, subject=subject, from_=from_)
    return _message_dict(msg)


@mcp.tool()
def read(
    name: str,
    session_token: str | None = None,
    unread_only: bool = True,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Read messages in `name`'s inbox. Auto-marks them read.

    Auth rules:
    - If `name` is registered, `session_token` must match.
    - If `name` is NOT registered, anyone can read (no owner to protect).
      You can use this to peek at the dead-letter queue before claiming
      a name.

    Args:
        name: The inbox to read.
        session_token: Required if `name` is registered; ignored otherwise.
        unread_only: If True (default), only returns messages with read_at
            still NULL at the time of this call.
        limit: Max messages to return (1-1000). Default 20.
    """
    msgs = store.read(
        name=name,
        session_token=session_token,
        unread_only=unread_only,
        limit=limit,
    )
    return [_message_dict(m) for m in msgs]


@mcp.tool()
def ack(message_id: int, session_token: str) -> dict[str, Any]:
    """Explicitly acknowledge a message ("I saw this and acted on it").

    Optional — `read` already marks messages as read. Use ack to record a
    stronger signal. v1 treats ack as "definitely read" (sets read_at if
    not already set). The read/ack distinction is reserved for future use.

    Args:
        message_id: The message ID from `send` or `read`.
        session_token: Token for the message's recipient. Required.
    """
    ok = store.ack(message_id, session_token)
    return {"ok": ok, "message_id": message_id}


@mcp.tool()
def unregister(name: str, session_token: str) -> dict[str, Any]:
    """Release a name. Messages addressed to it are PRESERVED.

    Re-registering the same name (by anyone) restores access to the
    waiting messages. Use this when an agent session is exiting and
    won't be back, OR when you want to hand the name to someone else
    cleanly (instead of letting them `force` it).

    Args:
        name: The name to release.
        session_token: Must match the current holder.
    """
    ok = store.unregister(name, session_token)
    return {"ok": ok, "name": name}


# ── Stdio entry point ────────────────────────────────────────────────────────


def serve_stdio() -> None:
    """Run the MCP server over stdio. Used by Claude Code / Cursor / Codex."""
    mcp.run("stdio")

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
        "DLB — Dead Letter Box. Ten tools for inter-agent messaging + task lifecycle:\n"
        "  register, recover_token, set_status, list_threads, send, read, ack,\n"
        "  unregister, update_status, get_task_status.\n"
        "Liveness: call set_status(name, token, 'working'|'idle'|'blocked'|'done') "
        "as you work so list_threads shows real status, not just a last_seen guess.\n"
        "Lost your session_token to context compaction? Call recover_token(name) "
        "to get it back — do NOT re-register (that mints a new token and trips "
        "the takeover gate on your own live name).\n"
        "Send is open (no auth); pass session_token on send to bind from_ to "
        "your registered name (otherwise from_ is unverified free text). "
        "Task-shaped messages: pass msg_type='task' + optional in_reply_to when "
        "sending a task; the recipient's protocol requires an immediate reply "
        "acknowledging receipt (see ~/.claude/CLAUDE.md DLB Inbox Protocol).\n"
        "Read/ack/unregister/update_status on a registered name require its "
        "session_token. get_task_status is auth-free (sender-side probe).\n"
        "Sends to nonexistent recipients are queued and delivered on later "
        "registration.\n"
        "Trust boundary: session_token gates the tool API, not the underlying "
        "SQLite file. DLB is coordination between cooperating agents under the "
        "same OS user — not confidentiality."
    ),
)


# ── Serialization helpers ────────────────────────────────────────────────────


def _agent_summary_dict(
    s: store.AgentSummary, working_on_chars: int | None = 140
) -> dict[str, Any]:
    working_on = s.working_on
    if working_on and working_on_chars is not None and len(working_on) > working_on_chars:
        working_on = working_on[: working_on_chars - 1].rstrip() + "…"
    d: dict[str, Any] = {
        "name": s.name,
        "working_on": working_on,
        "last_seen": s.last_seen.isoformat(),
        "unread_count": s.unread_count,
        "stale": s.stale,
    }
    if s.status is not None:
        d["status"] = s.status
    if s.status_detail is not None:
        d["status_detail"] = s.status_detail
    return d


def _message_dict(m: store.Message) -> dict[str, Any]:
    """Serialize a Message, OMITTING optional lifecycle fields when null.

    Every message historically carried five v3 lifecycle keys
    (msg_type/in_reply_to/status/status_note/status_updated_at) plus read_at,
    even when all were null — dead weight on every read/send payload. We now
    include an optional key only when it has a value, so a plain (non-task)
    message serializes to just the six core fields. Callers should treat a
    missing key as null (the prior explicit-null semantics).
    """
    d: dict[str, Any] = {
        "id": m.id,
        "recipient_name": m.recipient_name,
        "sender_name": m.sender_name,
        "subject": m.subject,
        "body": m.body,
        "sent_at": m.sent_at.isoformat(),
        "expires_at": m.expires_at.isoformat(),
    }
    if m.headline is not None:
        d["headline"] = m.headline
    if m.read_at:
        d["read_at"] = m.read_at.isoformat()
    if m.msg_type is not None:
        d["msg_type"] = m.msg_type
    if m.in_reply_to is not None:
        d["in_reply_to"] = m.in_reply_to
    if m.status is not None:
        d["status"] = m.status
    if m.status_note is not None:
        d["status_note"] = m.status_note
    if m.status_updated_at is not None:
        d["status_updated_at"] = m.status_updated_at.isoformat()
    return d


# ── Tools ────────────────────────────────────────────────────────────────────


@mcp.tool()
def register(
    name: str,
    working_on: str | None = None,
    force: bool = False,
    prior_token: str | None = None,
) -> dict[str, Any]:
    """Declare or refresh an agent identity.

    Returns the agent record plus a `session_token` you must keep — it's
    required for `read`, `ack`, and `unregister` on this name.

    Conflict behavior:
    - Name unused → register normally.
    - Name held, force=False → NameConflict with up to three suggestions
      (e.g. `alpha-2`, `alpha-3`).
    - Name held, force=True:
        - If `prior_token` matches the current holder → take it immediately.
        - Else if the holder has been silent longer than
          DLB_TAKEOVER_AFTER_SECONDS (default 24h) → take it.
        - Else → TakeoverDenied (preserves the "reclaim a dead session"
          use case while blocking casual hijack of a live session).

    Args:
        name: Stable identifier you'll be addressed by. Accepted as-given
            (no rename, no auto-suffix). Examples: "alpha", "ThreadBeta",
            "worker-1".
        working_on: Free-text description of what this agent is doing.
            Shown to other agents via `list_threads`. Optional.
        force: If True, attempt to evict the prior session. Stale-gated —
            see conflict behavior above.
        prior_token: The current holder's session_token, if you have it.
            Lets a legitimate handoff bypass the stale-gate on force=True.
    """
    return store.register(name, working_on=working_on, force=force, prior_token=prior_token)


@mcp.tool()
def recover_token(name: str) -> dict[str, Any] | None:
    """Re-obtain the session_token for a name you already registered.

    Use this when you've LOST your token — the common cause is context
    compaction wiping it from your working memory mid-session. It returns the
    live token so you can resume reading your inbox. Do NOT re-register to
    recover: register mints a brand-new token and, because your old session is
    still the live holder of the name, trips the takeover gate.

    No auth. This is consistent with DLB's trust model — the token already sits
    in the SQLite store readable by any same-OS-user process; this is just the
    sanctioned path (the raw-SQLite read is what your safety layer blocks).
    Returns None if `name` was never registered.

    Args:
        name: The name you previously registered and want the token for.
    """
    return store.recover_token(name)


@mcp.tool()
def set_status(
    name: str,
    session_token: str,
    status: str,
    detail: str | None = None,
) -> dict[str, Any]:
    """Report your current liveness status so coordinators aren't guessing.

    Convention: status ∈ {"working", "idle", "blocked", "done"} (any string is
    accepted). `detail` is optional free text — e.g. what you're blocked on
    (status="blocked", detail="waiting on PR #42 review"). Requires your
    session_token. Also refreshes last_seen, so reporting status doubles as a
    heartbeat: a coordinator reading list_threads can tell a quiet-but-alive
    agent (recent last_seen, status "working"/"idle") from a stopped one.

    Set "done" when you finish and "blocked" the moment you stall — far more
    reliable than the last_seen staleness proxy.

    Args:
        name: Your registered name.
        session_token: Token for `name`.
        status: Your current status (working/idle/blocked/done by convention).
        detail: Optional free-text detail (what you're blocked on, an ETA, …).
    """
    return store.set_status(name=name, session_token=session_token, status=status, detail=detail)


@mcp.tool()
def list_threads(
    active_within_hours: int = 24,
    include_stale: bool = False,
    only_unread: bool = False,
    working_on_chars: int = 140,
) -> list[dict[str, Any]]:
    """List known agents with their unread counts and staleness flags.

    No auth required. Use this to discover who's around before sending, or to
    check who has mail waiting.

    Token-efficiency defaults (v0.3.1+): to keep the payload small on a large
    fleet, this now returns ONLY active (non-stale) agents by default, and
    truncates each `working_on` to ~140 chars. On a store with hundreds of
    historical agents the old behavior (return everything, full text) produced
    thousands of tokens per call. Widen explicitly when you need to:

    Args:
        active_within_hours: How many hours back counts as 'active' for the
            stale flag. Default 24.
        include_stale: If True, also return agents whose `last_seen` is older
            than the window (the old default). Default False — stale agents
            are omitted entirely, not just flagged.
        only_unread: If True, return only agents with unread_count > 0
            (answers "who has mail?" with a minimal payload). Default False.
        working_on_chars: Truncate each `working_on` to this many chars
            (append …). Pass 0 to omit `working_on` entirely for the leanest
            roster; a large value to get full text. Default 140.
    """
    items = store.list_threads(active_within=timedelta(hours=active_within_hours))
    if not include_stale:
        items = [s for s in items if not s.stale]
    if only_unread:
        items = [s for s in items if s.unread_count > 0]
    omit_working_on = working_on_chars <= 0
    trunc = None if omit_working_on else working_on_chars
    out: list[dict[str, Any]] = []
    for s in items:
        d = _agent_summary_dict(s, working_on_chars=trunc)
        if omit_working_on:
            d.pop("working_on", None)
        out.append(d)
    return out


@mcp.tool()
def send(
    to: str,
    body: str | None = None,
    subject: str | None = None,
    from_: str | None = None,
    session_token: str | None = None,
    msg_type: str | None = None,
    in_reply_to: int | None = None,
    headline: str | None = None,
    content: str | None = None,
) -> dict[str, Any]:
    """Drop a message into `to`'s inbox.

    ALWAYS succeeds (subject to size cap), even if `to` is not registered
    yet. Messages queue under the name and surface when someone reads it.

    Provenance:
    - Without session_token: from_ is whatever the caller passed (default
      "anonymous"). Free text — unverified. The sender label could be a lie.
    - With session_token: token is looked up. If from_ is omitted, it
      becomes the token's registered name. If from_ is supplied, it MUST
      match the token's name or AuthError is raised. A message claiming to
      be from a registered name X is then either authenticated or anonymous
      — never spoofed.

    Size cap: body must be <= DLB_MAX_BODY_BYTES (default 256 KiB,
    UTF-8 encoded). Oversized bodies raise DLBError.

    Lifecycle hints (v0.3.0+):
    - msg_type: advisory tag. Convention: `"task"` signals to the recipient
      that this message expects action + acknowledgment. The recipient's
      protocol (see the DLB Inbox Protocol in ~/.claude/CLAUDE.md) says
      they should reply immediately confirming receipt and stating a plan.
    - in_reply_to: id of the message this one is replying to (typically a
      status update on a task-typed message). Enables threading.

    Args:
        to: Recipient name.
        body: Message body (free text / Markdown). UTF-8 bytes capped.
        subject: Optional short subject line.
        from_: Sender label. Defaults to "anonymous". Must match the
            session_token's name when both are provided.
        session_token: Optional. When supplied, binds from_ to your
            registered name (and validates it if from_ is also given).
        msg_type: Optional advisory tag (e.g. "task") signaling recipient
            behavior. See the DLB Inbox Protocol.
        in_reply_to: Optional id of the message this is replying to
            (for threading task-acknowledgment replies to the original task).
        headline: Optional machine-parseable one-liner surfaced UNTRUNCATED in
            monitor/list previews — read a sender's status without opening the
            message or holding a token. Use this instead of encoding status in
            the subject.

    Read-receipts (v0.3.2+): when the recipient reads a message you sent with
    msg_type='task' (and you sent it under a registered name), DLB drops a
    lightweight msg_type='receipt' back into YOUR inbox so you learn it was
    seen without polling. Disable globally with env DLB_READ_RECEIPTS=0.

    Body alias (v0.5.1+): `content` is accepted as an alias for `body`. LLM
    callers frequently pass the message text under `content`; rather than fail
    with a confusing "body Field required" validation error, we coalesce the
    two here. `body` wins if both are given.
    """
    if body is None:
        body = content
    if body is None:
        raise store.DLBError("send requires the message text in 'body' (or its alias 'content').")
    msg = store.send(
        to=to,
        body=body,
        subject=subject,
        from_=from_,
        session_token=session_token,
        msg_type=msg_type,
        in_reply_to=in_reply_to,
        headline=headline,
    )
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


@mcp.tool()
def update_status(
    message_id: int,
    status: str,
    session_token: str,
    note: str | None = None,
) -> dict[str, Any]:
    """Update the lifecycle status of a message you received (v0.3.0+).

    Called by the RECIPIENT to signal progress on a task back to the sender.
    Convention (not enforced by DLB): status ∈ {"queued", "accepted",
    "running", "done", "blocked"}. Any string is accepted so future
    conventions can extend without a schema migration.

    Side effect: sets read_at on the message if it was still unread —
    updating status implies you've read it, so this replaces `ack` for
    the common case.

    Sender-side counterpart: `get_task_status(message_id)` returns the
    current status without needing the recipient's session_token.

    Args:
        message_id: The id returned by send() (task) or read() (task received).
        status: Free-string status. Convention: queued/accepted/running/done/blocked.
        session_token: Recipient's token. Required.
        note: Optional short free-text detail (e.g. "20m ETA", "hit missing
            fixture", "PR #42").
    """
    msg = store.update_status(
        message_id=message_id,
        status=status,
        session_token=session_token,
        note=note,
    )
    return _message_dict(msg)


@mcp.tool()
def get_task_status(message_id: int) -> dict[str, Any] | None:
    """Query the lifecycle status of a message without reading its body (v0.3.0+).

    NO AUTH — DLB's trust model is coordination between cooperating agents
    under the same OS user. This is the sender-side counterpart to
    `update_status`: it lets a sender check "did my task get picked up? is
    it running? is it done?" without holding the recipient's session_token.

    Returns None if the message id doesn't exist. Otherwise returns a
    lightweight status dict — NOT the message body. Use `read` if you
    need the content.

    Args:
        message_id: The id you received from send() when you dispatched a task.
    """
    return store.get_task_status(message_id)


# ── Stdio entry point ────────────────────────────────────────────────────────


def serve_stdio() -> None:
    """Run the MCP server over stdio. Used by Claude Code / Cursor / Codex."""
    mcp.run("stdio")

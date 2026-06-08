"""SQLite-backed store for DLB.

One file, two tables, WAL mode. Every MCP tool call opens a fresh connection,
runs one transaction, closes. No long-running server, no lock file — SQLite
WAL handles concurrent readers + serialized writers for free.

Schema:
    agents(name PK, working_on, registered_at, last_seen, session_token)
    messages(id PK, recipient_name, sender_name, subject, body,
             sent_at, read_at, expires_at)

TTL is enforced lazily: expired messages are deleted on the next read of
their inbox, not by a background job. Keeps the "no daemon" promise.
"""

from __future__ import annotations

import os
import secrets
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

# ── Configuration ────────────────────────────────────────────────────────────

DEFAULT_STORE_PATH = Path.home() / ".dlb" / "store.sqlite3"
DEFAULT_TTL_DAYS = 7


def store_path() -> Path:
    return Path(os.environ.get("DLB_STORE", str(DEFAULT_STORE_PATH))).expanduser()


def ttl_days() -> int:
    try:
        return int(os.environ.get("DLB_MESSAGE_TTL_DAYS", str(DEFAULT_TTL_DAYS)))
    except ValueError:
        return DEFAULT_TTL_DAYS


# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class Agent:
    name: str
    working_on: str | None
    registered_at: datetime
    last_seen: datetime
    # session_token is intentionally omitted from list views; it's only
    # returned to the caller on register.


@dataclass
class AgentSummary:
    name: str
    working_on: str | None
    last_seen: datetime
    unread_count: int
    stale: bool  # True if last_seen older than active_within window


@dataclass
class Message:
    id: int
    recipient_name: str
    sender_name: str
    subject: str | None
    body: str
    sent_at: datetime
    read_at: datetime | None
    expires_at: datetime


# ── Connection management ────────────────────────────────────────────────────


def _ensure_store_dir() -> None:
    """Create ~/.dlb/ with 700 perms if it doesn't exist."""
    p = store_path().parent
    p.mkdir(parents=True, exist_ok=True)
    # Tighten perms on the directory. We don't enforce on Windows.
    with suppress(OSError, NotImplementedError):
        p.chmod(0o700)


@contextmanager
def _connect() -> Iterator[sqlite3.Connection]:
    """Open one connection, run one transaction, close.

    WAL mode is set on every connection (cheap; sqlite remembers it). The
    busy_timeout lets us tolerate brief contention between sibling processes
    without immediately erroring out.
    """
    _ensure_store_dir()
    conn = sqlite3.connect(
        str(store_path()),
        isolation_level=None,  # autocommit; we open explicit txns below
        detect_types=sqlite3.PARSE_DECLTYPES,
        timeout=5.0,
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        conn.close()


def init_schema() -> None:
    """Create tables if missing. Idempotent; safe to call on every connect."""
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS agents (
                name           TEXT PRIMARY KEY,
                working_on     TEXT,
                registered_at  TEXT NOT NULL,
                last_seen      TEXT NOT NULL,
                session_token  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                recipient_name  TEXT NOT NULL,
                sender_name     TEXT NOT NULL,
                subject         TEXT,
                body            TEXT NOT NULL,
                sent_at         TEXT NOT NULL,
                read_at         TEXT,
                expires_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_recipient
                ON messages(recipient_name, read_at, sent_at DESC);
            """
        )


# ── Time helpers ─────────────────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _parse(s: str | None) -> datetime | None:
    if s is None:
        return None
    return datetime.fromisoformat(s)


# ── Internal helpers ─────────────────────────────────────────────────────────


def _row_to_agent(r: sqlite3.Row) -> Agent:
    return Agent(
        name=r["name"],
        working_on=r["working_on"],
        registered_at=_parse(r["registered_at"]),  # type: ignore[arg-type]
        last_seen=_parse(r["last_seen"]),  # type: ignore[arg-type]
    )


def _row_to_message(r: sqlite3.Row) -> Message:
    return Message(
        id=r["id"],
        recipient_name=r["recipient_name"],
        sender_name=r["sender_name"],
        subject=r["subject"],
        body=r["body"],
        sent_at=_parse(r["sent_at"]),  # type: ignore[arg-type]
        read_at=_parse(r["read_at"]),
        expires_at=_parse(r["expires_at"]),  # type: ignore[arg-type]
    )


def _purge_expired(conn: sqlite3.Connection, *, recipient: str | None = None) -> int:
    """Delete messages whose expires_at is in the past.

    If recipient is given, only that inbox is scanned (cheap per-read cleanup).
    Otherwise scans the whole table (called from list_threads).
    """
    now_iso = _iso(_now())
    if recipient is not None:
        cur = conn.execute(
            "DELETE FROM messages WHERE recipient_name = ? AND expires_at < ?",
            (recipient, now_iso),
        )
    else:
        cur = conn.execute("DELETE FROM messages WHERE expires_at < ?", (now_iso,))
    return cur.rowcount


def _suggest_alternatives(conn: sqlite3.Connection, base: str, n: int = 3) -> list[str]:
    """Generate `base-2`, `base-3`, ... skipping any already taken."""
    taken = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM agents WHERE name LIKE ? || '-%'", (base,)
        ).fetchall()
    }
    taken.add(base)
    out: list[str] = []
    i = 2
    while len(out) < n and i < 100:
        candidate = f"{base}-{i}"
        if candidate not in taken:
            out.append(candidate)
        i += 1
    return out


# ── Errors ───────────────────────────────────────────────────────────────────


class DLBError(Exception):
    """Base for all DLB-raised errors."""


class NameConflict(DLBError):
    """Name is taken by another active session."""

    def __init__(self, name: str, suggestions: list[str]) -> None:
        self.name = name
        self.suggestions = suggestions
        msg = (
            f"Name '{name}' is taken by an active session. "
            f"Suggested alternatives: {', '.join(suggestions) if suggestions else '(none)'}. "
            f"Pass force=true to evict the prior session and take this name."
        )
        super().__init__(msg)


class AuthError(DLBError):
    """Session token missing or wrong for this name."""


# ── Public API: the six operations the MCP tools wrap ────────────────────────


def register(name: str, working_on: str | None = None, *, force: bool = False) -> dict:
    """Declare/refresh an identity. Returns the new session_token.

    Conflict policy:
    - If the name doesn't exist: create it.
    - If it exists and force=True: rotate the session_token (evict prior owner).
    - If it exists and force=False: raise NameConflict with suggestions.

    There is no "refresh my own existing session" shortcut — once you have a
    token, just use it; only call register again if you want a new token.
    """
    init_schema()
    if not name or not isinstance(name, str):
        raise DLBError("name must be a non-empty string")

    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT name FROM agents WHERE name = ?", (name,)
            ).fetchone()

            if existing and not force:
                suggestions = _suggest_alternatives(conn, name)
                conn.execute("ROLLBACK")
                raise NameConflict(name, suggestions)

            now = _iso(_now())
            token = secrets.token_urlsafe(32)

            if existing:
                conn.execute(
                    "UPDATE agents SET working_on = ?, last_seen = ?, session_token = ? "
                    "WHERE name = ?",
                    (working_on, now, token, name),
                )
            else:
                conn.execute(
                    "INSERT INTO agents (name, working_on, registered_at, last_seen, "
                    "session_token) VALUES (?, ?, ?, ?, ?)",
                    (name, working_on, now, now, token),
                )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    return {
        "name": name,
        "working_on": working_on,
        "registered_at": now,
        "last_seen": now,
        "session_token": token,
    }


def list_threads(active_within: timedelta = timedelta(hours=24)) -> list[AgentSummary]:
    """List all known agents, with unread counts and staleness flags.

    No auth required. Also opportunistically purges any expired messages
    across the whole store (cheap, since we're scanning anyway).
    """
    init_schema()
    with _connect() as conn:
        conn.execute("BEGIN")
        try:
            _purge_expired(conn)

            # join agents → unread message counts in one query
            cutoff = _iso(_now() - active_within)
            rows = conn.execute(
                """
                SELECT a.name,
                       a.working_on,
                       a.last_seen,
                       COALESCE(m.unread_count, 0) AS unread_count
                FROM agents a
                LEFT JOIN (
                    SELECT recipient_name, COUNT(*) AS unread_count
                    FROM messages
                    WHERE read_at IS NULL
                    GROUP BY recipient_name
                ) m ON m.recipient_name = a.name
                ORDER BY a.last_seen DESC
                """
            ).fetchall()

            out: list[AgentSummary] = []
            for r in rows:
                last_seen = _parse(r["last_seen"])
                out.append(
                    AgentSummary(
                        name=r["name"],
                        working_on=r["working_on"],
                        last_seen=last_seen,  # type: ignore[arg-type]
                        unread_count=r["unread_count"],
                        stale=r["last_seen"] < cutoff,
                    )
                )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    return out


def send(
    to: str,
    body: str,
    subject: str | None = None,
    from_: str | None = None,
) -> Message:
    """Drop a message in `to`'s inbox.

    Dead-letter semantics: ALWAYS succeeds, even if `to` is not registered.
    Once someone later registers as that name, the messages will be in their
    inbox waiting (and `read` on an unregistered name is also allowed, so
    you can peek without claiming the name).

    `from_` defaults to the literal string "anonymous" if not given. (We
    don't auto-discover the caller's identity — that would require a session
    token, which would violate the "send is auth-free" promise.)
    """
    init_schema()
    if not to or not isinstance(to, str):
        raise DLBError("to must be a non-empty string")
    if not body or not isinstance(body, str):
        raise DLBError("body must be a non-empty string")

    sender = from_ or "anonymous"
    sent = _now()
    expires = sent + timedelta(days=ttl_days())

    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO messages (recipient_name, sender_name, subject, body, "
            "sent_at, read_at, expires_at) VALUES (?, ?, ?, ?, ?, NULL, ?)",
            (to, sender, subject, body, _iso(sent), _iso(expires)),
        )
        msg_id = cur.lastrowid

    return Message(
        id=msg_id,  # type: ignore[arg-type]
        recipient_name=to,
        sender_name=sender,
        subject=subject,
        body=body,
        sent_at=sent,
        read_at=None,
        expires_at=expires,
    )


def read(
    name: str,
    session_token: str | None = None,
    *,
    unread_only: bool = True,
    limit: int = 20,
) -> list[Message]:
    """Read messages for `name`. Auto-marks them read.

    Auth rules:
    - If `name` IS registered: session_token must match. Otherwise AuthError.
    - If `name` IS NOT registered: anyone can read (no owner to protect).
      session_token is ignored in that case.

    Also opportunistically purges expired messages in this inbox before
    returning.
    """
    init_schema()
    if not name or not isinstance(name, str):
        raise DLBError("name must be a non-empty string")
    if limit < 1 or limit > 1000:
        raise DLBError("limit must be between 1 and 1000")

    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            agent_row = conn.execute(
                "SELECT name, session_token FROM agents WHERE name = ?", (name,)
            ).fetchone()

            if agent_row is not None:
                # Registered name → must auth
                if session_token != agent_row["session_token"]:
                    conn.execute("ROLLBACK")
                    raise AuthError(
                        f"Invalid or missing session_token for registered name '{name}'."
                    )
                # Refresh last_seen on successful auth (it's a real activity)
                conn.execute(
                    "UPDATE agents SET last_seen = ? WHERE name = ?",
                    (_iso(_now()), name),
                )
            # else: unregistered name, no auth check — anyone may read

            _purge_expired(conn, recipient=name)

            where_unread = " AND read_at IS NULL" if unread_only else ""
            rows = conn.execute(
                f"SELECT * FROM messages WHERE recipient_name = ?{where_unread} "
                "ORDER BY sent_at DESC LIMIT ?",
                (name, limit),
            ).fetchall()

            # Mark them read in the same transaction
            ids = [r["id"] for r in rows if r["read_at"] is None]
            if ids:
                now_iso = _iso(_now())
                placeholders = ",".join("?" * len(ids))
                conn.execute(
                    f"UPDATE messages SET read_at = ? WHERE id IN ({placeholders})",
                    (now_iso, *ids),
                )

            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    # Re-shape with the read_at we just set, for the returned objects.
    out: list[Message] = []
    for r in rows:
        m = _row_to_message(r)
        if m.read_at is None and m.id in ids:
            m.read_at = _now()
        out.append(m)
    return out


def ack(message_id: int, session_token: str) -> bool:
    """Mark a message as acknowledged (separate from read).

    Currently we treat ack as 'definitely read' — we set read_at if it's
    still NULL. The distinction between read and ack is for future use
    (e.g., 'I saw this but haven't acted on it' vs 'done'); v1 just records
    that ack happened by ensuring read_at is set.

    Auth: the caller must hold a valid session_token for the message's
    recipient. (If the recipient is unregistered, ack is a no-op — there's
    no owner to take responsibility.)
    """
    init_schema()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            msg = conn.execute(
                "SELECT id, recipient_name, read_at FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            if msg is None:
                conn.execute("ROLLBACK")
                return False

            agent = conn.execute(
                "SELECT session_token FROM agents WHERE name = ?",
                (msg["recipient_name"],),
            ).fetchone()
            if agent is None:
                # Unregistered recipient — no one to authenticate as
                conn.execute("ROLLBACK")
                raise AuthError(
                    f"Cannot ack message for unregistered recipient '{msg['recipient_name']}'."
                )
            if session_token != agent["session_token"]:
                conn.execute("ROLLBACK")
                raise AuthError("Invalid session_token for this message's recipient.")

            if msg["read_at"] is None:
                conn.execute(
                    "UPDATE messages SET read_at = ? WHERE id = ?",
                    (_iso(_now()), message_id),
                )
            conn.execute("COMMIT")
            return True
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise


def unregister(name: str, session_token: str) -> bool:
    """Release a name.

    Messages addressed to that name are PRESERVED — re-registration (by
    anyone) gives access to them again. This is intentional: the "name
    holder" abstraction is over agent presence, not over message ownership.
    """
    init_schema()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            agent = conn.execute(
                "SELECT session_token FROM agents WHERE name = ?", (name,)
            ).fetchone()
            if agent is None:
                conn.execute("ROLLBACK")
                return False
            if session_token != agent["session_token"]:
                conn.execute("ROLLBACK")
                raise AuthError(f"Invalid session_token for '{name}'.")

            conn.execute("DELETE FROM agents WHERE name = ?", (name,))
            conn.execute("COMMIT")
            return True
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

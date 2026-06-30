"""SQLite-backed store for DLB.

One file, two tables, WAL mode. Every MCP tool call opens a fresh connection,
runs one transaction, closes. No long-running server, no lock file — SQLite
WAL handles concurrent readers + serialized writers for free.

Schema (v2):
    agents(name PK, working_on, registered_at_ms, last_seen_ms, session_token)
    messages(id PK, recipient_name, sender_name, subject, body,
             sent_at_ms, read_at_ms, expires_at_ms)

Schema version is tracked via PRAGMA user_version. v1 used TEXT ISO timestamps
which compared correctly only by lexicographic accident of the chosen format;
v2 uses INTEGER epoch-ms which compares numerically and cannot silently break
under format drift. Migration from v1 is automatic on first connect.

TTL is enforced lazily: expired messages are deleted on the next read of
their inbox, not by a background job. Keeps the "no daemon" promise.

Trust boundary note: session_token gates the DLB tool API, not the underlying
SQLite file. Any process running as the same OS user can read ~/.dlb/store.sqlite3
directly. DLB is COORDINATION between cooperating agents, not confidentiality
against adversarial ones.
"""

from __future__ import annotations

import os
import secrets
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

# ── Configuration ────────────────────────────────────────────────────────────

DEFAULT_STORE_PATH = Path.home() / ".dlb" / "store.sqlite3"
DEFAULT_TTL_DAYS = 7
DEFAULT_MAX_BODY_BYTES = 256 * 1024  # 256 KiB
DEFAULT_TAKEOVER_AFTER_SECONDS = 24 * 60 * 60  # 24h: matches list_threads' default stale window

SCHEMA_VERSION = 2


def store_path() -> Path:
    return Path(os.environ.get("DLB_STORE", str(DEFAULT_STORE_PATH))).expanduser()


def ttl_days() -> int:
    try:
        return int(os.environ.get("DLB_MESSAGE_TTL_DAYS", str(DEFAULT_TTL_DAYS)))
    except ValueError:
        return DEFAULT_TTL_DAYS


def max_body_bytes() -> int:
    try:
        v = int(os.environ.get("DLB_MAX_BODY_BYTES", str(DEFAULT_MAX_BODY_BYTES)))
        return v if v > 0 else DEFAULT_MAX_BODY_BYTES
    except ValueError:
        return DEFAULT_MAX_BODY_BYTES


def takeover_after_seconds() -> int:
    try:
        v = int(os.environ.get("DLB_TAKEOVER_AFTER_SECONDS", str(DEFAULT_TAKEOVER_AFTER_SECONDS)))
        return v if v >= 0 else DEFAULT_TAKEOVER_AFTER_SECONDS
    except ValueError:
        return DEFAULT_TAKEOVER_AFTER_SECONDS


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


def _ensure_store_file_perms() -> None:
    """chmod 600 the SQLite file once it exists. Best-effort.

    The directory is already 0o700 but the file itself can be opened from
    other processes if its perms are looser. Tightening matches the
    "single OS user, cooperating agents" trust model.
    """
    p = store_path()
    if p.exists():
        with suppress(OSError, NotImplementedError):
            p.chmod(0o600)


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
    # File only exists after the first write. Tighten on the way out so a
    # caller doesn't race the perms with another reader's first open.
    _ensure_store_file_perms()


def init_schema() -> None:
    """Create tables if missing; migrate v1→v2 if needed. Idempotent."""
    with _connect() as conn:
        current_version = conn.execute("PRAGMA user_version").fetchone()[0]

        if current_version >= SCHEMA_VERSION:
            return  # already at current schema

        if current_version == 0:
            # Fresh DB OR pre-versioning v1 DB. Probe for legacy v1 tables.
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='agents'"
            ).fetchone()
            if row is None:
                # Truly fresh — create v2 schema directly.
                _create_v2_schema(conn)
            else:
                # Legacy v1 DB without a user_version stamp. Migrate.
                _migrate_v1_to_v2(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif current_version == 1:
            _migrate_v1_to_v2(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def _create_v2_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS agents (
            name              TEXT PRIMARY KEY,
            working_on        TEXT,
            registered_at_ms  INTEGER NOT NULL,
            last_seen_ms      INTEGER NOT NULL,
            session_token     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_name  TEXT NOT NULL,
            sender_name     TEXT NOT NULL,
            subject         TEXT,
            body            TEXT NOT NULL,
            sent_at_ms      INTEGER NOT NULL,
            read_at_ms      INTEGER,
            expires_at_ms   INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_messages_recipient
            ON messages(recipient_name, read_at_ms, sent_at_ms DESC);
        """
    )


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """One-shot in-place migration: add INTEGER ms columns, backfill from
    the existing TEXT ISO timestamps, then leave the old columns as inert
    legacy. Idempotent under retry (ADD COLUMN is guarded by a probe)."""
    existing_agent_cols = {r["name"] for r in conn.execute("PRAGMA table_info(agents)")}
    existing_msg_cols = {r["name"] for r in conn.execute("PRAGMA table_info(messages)")}

    if "registered_at_ms" not in existing_agent_cols:
        conn.execute("ALTER TABLE agents ADD COLUMN registered_at_ms INTEGER")
    if "last_seen_ms" not in existing_agent_cols:
        conn.execute("ALTER TABLE agents ADD COLUMN last_seen_ms INTEGER")
    if "sent_at_ms" not in existing_msg_cols:
        conn.execute("ALTER TABLE messages ADD COLUMN sent_at_ms INTEGER")
    if "read_at_ms" not in existing_msg_cols:
        conn.execute("ALTER TABLE messages ADD COLUMN read_at_ms INTEGER")
    if "expires_at_ms" not in existing_msg_cols:
        conn.execute("ALTER TABLE messages ADD COLUMN expires_at_ms INTEGER")

    # Backfill agents
    for r in conn.execute(
        "SELECT name, registered_at, last_seen FROM agents WHERE "
        "registered_at_ms IS NULL OR last_seen_ms IS NULL"
    ).fetchall():
        reg_ms = _iso_to_ms(r["registered_at"]) or _now_ms()
        seen_ms = _iso_to_ms(r["last_seen"]) or reg_ms
        conn.execute(
            "UPDATE agents SET registered_at_ms = ?, last_seen_ms = ? WHERE name = ?",
            (reg_ms, seen_ms, r["name"]),
        )

    # Backfill messages
    for r in conn.execute(
        "SELECT id, sent_at, read_at, expires_at FROM messages WHERE "
        "sent_at_ms IS NULL OR expires_at_ms IS NULL"
    ).fetchall():
        sent_ms = _iso_to_ms(r["sent_at"]) or _now_ms()
        read_ms = _iso_to_ms(r["read_at"]) if r["read_at"] else None
        # If expires_at is missing/unparseable, default to "expired" so the
        # next lazy purge cleans it up — safer than keeping orphan rows
        # forever.
        exp_ms = _iso_to_ms(r["expires_at"]) or 0
        conn.execute(
            "UPDATE messages SET sent_at_ms = ?, read_at_ms = ?, expires_at_ms = ? WHERE id = ?",
            (sent_ms, read_ms, exp_ms, r["id"]),
        )

    # Replace the old index (which referenced the TEXT columns) with one
    # over the new INTEGER columns. SQLite happily keeps both — but the
    # old one is wasted maintenance, so drop it.
    with suppress(sqlite3.OperationalError):
        conn.execute("DROP INDEX IF EXISTS idx_messages_recipient")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_recipient "
        "ON messages(recipient_name, read_at_ms, sent_at_ms DESC)"
    )


# ── Time helpers ─────────────────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now(UTC)


def _now_ms() -> int:
    """Current time as integer milliseconds since the Unix epoch.

    Integer ms is what the schema stores. Numeric comparison guarantees
    ordering invariants regardless of timezone-formatting quirks (the v1
    bug class).
    """
    return int(time.time() * 1000)


def _ms_to_dt(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _iso_to_ms(s: str | None) -> int | None:
    """Parse a v1 ISO timestamp string into integer ms. None on parse error."""
    if s is None:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except (TypeError, ValueError):
        return None


# ── Internal helpers ─────────────────────────────────────────────────────────


def _row_to_message(r: sqlite3.Row) -> Message:
    return Message(
        id=r["id"],
        recipient_name=r["recipient_name"],
        sender_name=r["sender_name"],
        subject=r["subject"],
        body=r["body"],
        sent_at=_ms_to_dt(r["sent_at_ms"]),  # type: ignore[arg-type]
        read_at=_ms_to_dt(r["read_at_ms"]),
        expires_at=_ms_to_dt(r["expires_at_ms"]),  # type: ignore[arg-type]
    )


def _purge_expired(conn: sqlite3.Connection, *, recipient: str | None = None) -> int:
    """Delete messages whose expires_at_ms has been reached.

    Uses `<=` so a TTL of 0 days behaves correctly at ms granularity
    (under `<`, send+read in the same millisecond would keep the message).
    Equality means "lifetime fully elapsed", which is what callers expect.
    """
    now_ms = _now_ms()
    if recipient is not None:
        cur = conn.execute(
            "DELETE FROM messages WHERE recipient_name = ? AND expires_at_ms <= ?",
            (recipient, now_ms),
        )
    else:
        cur = conn.execute("DELETE FROM messages WHERE expires_at_ms <= ?", (now_ms,))
    return cur.rowcount


def _suggest_alternatives(conn: sqlite3.Connection, base: str, n: int = 3) -> list[str]:
    """Generate `base-2`, `base-3`, ... skipping any already taken.

    The LIKE pattern uses an explicit ESCAPE character ('\\') and the base
    has its own LIKE metacharacters (%/_/\\) escaped before substitution.
    Without this, a name containing % or _ would over-match (e.g. base
    'a%' would match 'a-2', 'abc-2', etc. as taken when they aren't).
    """
    escaped = base.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    taken = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM agents WHERE name LIKE ? || '-%' ESCAPE '\\'",
            (escaped,),
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
            f"Pass force=true (with prior_token, or after the holder goes stale) to take it."
        )
        super().__init__(msg)


class TakeoverDenied(DLBError):
    """force=True was requested, but the prior holder is still active and no
    matching prior_token was supplied. Distinct from NameConflict so callers
    can offer the right remediation ("wait for staleness or supply token")."""

    def __init__(self, name: str, last_seen_age_seconds: float) -> None:
        self.name = name
        self.last_seen_age_seconds = last_seen_age_seconds
        msg = (
            f"Cannot force-take '{name}': prior holder is still active "
            f"(last_seen {int(last_seen_age_seconds)}s ago; takeover allowed "
            f"after DLB_TAKEOVER_AFTER_SECONDS={takeover_after_seconds()}s). "
            f"Pass prior_token to evict immediately, or wait for staleness."
        )
        super().__init__(msg)


class AuthError(DLBError):
    """Session token missing or wrong for this name."""


# ── Public API: the six operations the MCP tools wrap ────────────────────────


def register(
    name: str,
    working_on: str | None = None,
    *,
    force: bool = False,
    prior_token: str | None = None,
) -> dict:
    """Declare/refresh an identity. Returns the new session_token.

    Conflict policy:
    - If the name doesn't exist: create it.
    - If it exists and force=False: raise NameConflict with suggestions.
    - If it exists and force=True:
        - If prior_token matches the current holder: take it immediately.
        - Else if the holder has been silent longer than
          DLB_TAKEOVER_AFTER_SECONDS (default 24h): take it.
        - Else: raise TakeoverDenied.

    The stale-gate exists because force=True used to be unauthenticated name
    takeover (just hijack-a-name). The legitimate use is reclaiming a name
    whose owner died without unregistering; stale-gating preserves that
    while blocking casual hijack of a live session.
    """
    init_schema()
    if not name or not isinstance(name, str):
        raise DLBError("name must be a non-empty string")

    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT name, last_seen_ms, session_token FROM agents WHERE name = ?",
                (name,),
            ).fetchone()

            if existing and not force:
                suggestions = _suggest_alternatives(conn, name)
                conn.execute("ROLLBACK")
                raise NameConflict(name, suggestions)

            if existing and force:
                # Gate: either prior_token must match OR holder must be stale.
                token_matches = prior_token is not None and prior_token == existing["session_token"]
                if not token_matches:
                    age_seconds = (_now_ms() - int(existing["last_seen_ms"])) / 1000
                    if age_seconds < takeover_after_seconds():
                        conn.execute("ROLLBACK")
                        raise TakeoverDenied(name, age_seconds)

            now_ms = _now_ms()
            token = secrets.token_urlsafe(32)

            if existing:
                conn.execute(
                    "UPDATE agents SET working_on = ?, last_seen_ms = ?, "
                    "session_token = ? WHERE name = ?",
                    (working_on, now_ms, token, name),
                )
            else:
                conn.execute(
                    "INSERT INTO agents (name, working_on, registered_at_ms, "
                    "last_seen_ms, session_token) VALUES (?, ?, ?, ?, ?)",
                    (name, working_on, now_ms, now_ms, token),
                )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    now_iso = _ms_to_dt(now_ms).isoformat()  # type: ignore[union-attr]
    return {
        "name": name,
        "working_on": working_on,
        "registered_at": now_iso,
        "last_seen": now_iso,
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

            cutoff_ms = _now_ms() - int(active_within.total_seconds() * 1000)
            rows = conn.execute(
                """
                SELECT a.name,
                       a.working_on,
                       a.last_seen_ms,
                       COALESCE(m.unread_count, 0) AS unread_count
                FROM agents a
                LEFT JOIN (
                    SELECT recipient_name, COUNT(*) AS unread_count
                    FROM messages
                    WHERE read_at_ms IS NULL
                    GROUP BY recipient_name
                ) m ON m.recipient_name = a.name
                ORDER BY a.last_seen_ms DESC
                """
            ).fetchall()

            out: list[AgentSummary] = []
            for r in rows:
                last_seen_ms = int(r["last_seen_ms"])
                out.append(
                    AgentSummary(
                        name=r["name"],
                        working_on=r["working_on"],
                        last_seen=_ms_to_dt(last_seen_ms),  # type: ignore[arg-type]
                        unread_count=r["unread_count"],
                        stale=last_seen_ms < cutoff_ms,
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
    *,
    session_token: str | None = None,
) -> Message:
    """Drop a message in `to`'s inbox.

    Dead-letter semantics: ALWAYS succeeds once validation passes, even if
    `to` is not registered. Messages queue under the name and surface when
    someone reads it.

    Provenance:
    - Without session_token: `from_` is the literal string the caller passed
      (defaulting to "anonymous"). No verification — the sender label is
      free text and could be a lie.
    - With session_token: the token is looked up; if `from_` is omitted, it
      becomes the token's registered name; if `from_` is supplied, it MUST
      equal the token's name or AuthError is raised. This means: a message
      that claims to be from a registered name X is either authenticated
      (token verified) or anonymous (sender_name set to whatever string,
      callers can distinguish).

    Size cap:
    - body bytes (UTF-8 encoded) must be <= DLB_MAX_BODY_BYTES (default 256KiB).
      Rejects with DLBError on overflow.
    """
    init_schema()
    if not to or not isinstance(to, str):
        raise DLBError("to must be a non-empty string")
    if not body or not isinstance(body, str):
        raise DLBError("body must be a non-empty string")

    body_bytes = len(body.encode("utf-8"))
    cap = max_body_bytes()
    if body_bytes > cap:
        raise DLBError(f"body too large: {body_bytes} bytes exceeds DLB_MAX_BODY_BYTES={cap}")

    sender = from_ if from_ is not None else "anonymous"
    if session_token is not None:
        with _connect() as conn:
            row = conn.execute(
                "SELECT name FROM agents WHERE session_token = ?",
                (session_token,),
            ).fetchone()
            if row is None:
                raise AuthError("Invalid session_token for send.")
            authenticated_name = row["name"]
            if from_ is not None and from_ != authenticated_name:
                raise AuthError(
                    f"from_={from_!r} does not match session_token's name "
                    f"({authenticated_name!r}). Use from_=None to auto-set, "
                    f"or pass from_={authenticated_name!r}."
                )
            sender = authenticated_name

    sent_ms = _now_ms()
    expires_ms = sent_ms + ttl_days() * 24 * 60 * 60 * 1000

    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO messages (recipient_name, sender_name, subject, body, "
            "sent_at_ms, read_at_ms, expires_at_ms) VALUES (?, ?, ?, ?, ?, NULL, ?)",
            (to, sender, subject, body, sent_ms, expires_ms),
        )
        msg_id = cur.lastrowid

    return Message(
        id=msg_id,  # type: ignore[arg-type]
        recipient_name=to,
        sender_name=sender,
        subject=subject,
        body=body,
        sent_at=_ms_to_dt(sent_ms),  # type: ignore[arg-type]
        read_at=None,
        expires_at=_ms_to_dt(expires_ms),  # type: ignore[arg-type]
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
                if session_token != agent_row["session_token"]:
                    conn.execute("ROLLBACK")
                    raise AuthError(
                        f"Invalid or missing session_token for registered name '{name}'."
                    )
                # Refresh last_seen on successful auth.
                conn.execute(
                    "UPDATE agents SET last_seen_ms = ? WHERE name = ?",
                    (_now_ms(), name),
                )

            _purge_expired(conn, recipient=name)

            where_unread = " AND read_at_ms IS NULL" if unread_only else ""
            rows = conn.execute(
                f"SELECT * FROM messages WHERE recipient_name = ?{where_unread} "
                "ORDER BY sent_at_ms DESC LIMIT ?",
                (name, limit),
            ).fetchall()

            # Mark them read in the same transaction. Capture the exact ms
            # value we write so the returned objects' read_at matches the DB
            # to the millisecond (avoids a "stored vs. returned" drift bug).
            ids = [r["id"] for r in rows if r["read_at_ms"] is None]
            mark_read_ms: int | None = None
            if ids:
                mark_read_ms = _now_ms()
                placeholders = ",".join("?" * len(ids))
                conn.execute(
                    f"UPDATE messages SET read_at_ms = ? WHERE id IN ({placeholders})",
                    (mark_read_ms, *ids),
                )

            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    out: list[Message] = []
    for r in rows:
        m = _row_to_message(r)
        if m.read_at is None and m.id in ids and mark_read_ms is not None:
            m.read_at = _ms_to_dt(mark_read_ms)
        out.append(m)
    return out


def ack(message_id: int, session_token: str) -> bool:
    """Mark a message as acknowledged (separate from read).

    Currently we treat ack as 'definitely read' — we set read_at if it's
    still NULL. The distinction between read and ack is for future use.

    Auth: the caller must hold a valid session_token for the message's
    recipient. (If the recipient is unregistered, ack is a no-op — there's
    no owner to take responsibility.)
    """
    init_schema()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            msg = conn.execute(
                "SELECT id, recipient_name, read_at_ms FROM messages WHERE id = ?",
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
                conn.execute("ROLLBACK")
                raise AuthError(
                    f"Cannot ack message for unregistered recipient '{msg['recipient_name']}'."
                )
            if session_token != agent["session_token"]:
                conn.execute("ROLLBACK")
                raise AuthError("Invalid session_token for this message's recipient.")

            if msg["read_at_ms"] is None:
                conn.execute(
                    "UPDATE messages SET read_at_ms = ? WHERE id = ?",
                    (_now_ms(), message_id),
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
    anyone) gives access to them again. The "name holder" abstraction is
    over agent presence, not over message ownership.
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

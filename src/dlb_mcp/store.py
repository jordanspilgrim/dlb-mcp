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

import hashlib
import hmac
import os
import re
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

SCHEMA_VERSION = 5


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


def read_receipts_enabled() -> bool:
    """Whether reading a TASK message auto-sends a read-receipt to its sender.
    Default on; set DLB_READ_RECEIPTS=0 (or false/no) to disable globally."""
    return os.environ.get("DLB_READ_RECEIPTS", "1").strip().lower() not in {"0", "false", "no", "off"}


# ── Deterministic identity (rec #1B) ─────────────────────────────────────────
# A session_token that is a pure function of (per-user secret, name) cannot be
# "lost": after a restart or context compaction, re-registering the name — or
# calling recover_token(name) — yields the SAME token, with no stored value to
# look up. The secret is 32 random bytes on disk (chmod 600), so tokens stay
# unguessable WITHOUT it, but any cooperating same-OS-user session (or, later,
# any account holding the shared secret — the cross-account identity layer)
# derives the identical token. Trade-off (owner-approved 2026-07-15): because
# the token is invariant, a force-takeover no longer MINTS a different token or
# invalidates the old one — acceptable under DLB's cooperative same-OS-user
# model, where takeover reclaims a dead name rather than evicting a hostile one.


def _secret_path() -> Path:
    """Secret lives beside the store, so an isolated store dir → isolated secret
    (keeps tests hermetic and lets a fleet scope its identity to one store)."""
    return store_path().parent / "secret"


def _get_or_create_secret() -> bytes:
    """Read (or first-time create) the 32-byte per-store secret. Best-effort
    chmod 600. Not cached — the store path can change between calls in tests."""
    p = _secret_path()
    try:
        data = p.read_bytes()
        if len(data) >= 32:
            return data
    except OSError:
        pass
    _ensure_store_dir()
    secret = secrets.token_bytes(32)
    with suppress(OSError):
        p.write_bytes(secret)
        p.chmod(0o600)
    return secret


def deterministic_token(name: str) -> str:
    """token = HMAC-SHA256(secret, name). Stable across restart; unguessable
    without the secret; distinct per name."""
    return hmac.new(_get_or_create_secret(), name.encode("utf-8"), hashlib.sha256).hexdigest()


def _write_token_sidecar(name: str, token: str) -> None:
    """Persist name→token under <store_dir>/tokens/<name> (chmod 600) so a
    SessionStart hook can rediscover which names were registered on this machine
    and remind the agent how to recover. Best-effort — never blocks register."""
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)[:200] or "_"
    d = store_path().parent / "tokens"
    with suppress(OSError):
        d.mkdir(parents=True, exist_ok=True)
        d.chmod(0o700)
        f = d / safe
        f.write_text(f"{name}\n{token}\n")
        f.chmod(0o600)


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
    # v5 liveness (rec #3): agent-self-reported status beats the last_seen proxy.
    status: str | None = None  # convention: working | idle | blocked | done
    status_detail: str | None = None  # free text, e.g. "on: PR #42 review" when blocked


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
    # v3 lifecycle fields (all optional/None on legacy rows)
    msg_type: str | None = None  # advisory tag from sender; convention: "task"
    in_reply_to: int | None = None  # references messages.id — sender-set
    status: str | None = None  # recipient-set; convention: queued/accepted/running/done/blocked
    status_note: str | None = None  # recipient-set free-text detail on status
    status_updated_at: datetime | None = None
    # v4 field: sender-set machine-parseable one-liner, surfaced UNTRUNCATED in
    # monitor/list previews (kills the "encode the answer in the subject" hack).
    headline: str | None = None


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
    """Create tables if missing; migrate through v1→v2→v3 as needed. Idempotent."""
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
                # Truly fresh — create current schema directly (already v5).
                _create_current_schema(conn)
            else:
                # Legacy v1 DB without a user_version stamp. Migrate all the way up.
                _migrate_v1_to_v2(conn)
                _migrate_v2_to_v3(conn)
                _migrate_v3_to_v4(conn)
                _migrate_v4_to_v5(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif current_version == 1:
            _migrate_v1_to_v2(conn)
            _migrate_v2_to_v3(conn)
            _migrate_v3_to_v4(conn)
            _migrate_v4_to_v5(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif current_version == 2:
            _migrate_v2_to_v3(conn)
            _migrate_v3_to_v4(conn)
            _migrate_v4_to_v5(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif current_version == 3:
            _migrate_v3_to_v4(conn)
            _migrate_v4_to_v5(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif current_version == 4:
            _migrate_v4_to_v5(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def _create_current_schema(conn: sqlite3.Connection) -> None:
    """Create v3 (current) tables + index. Uses individual execute() calls
    (NOT executescript) because executescript disregards isolation_level
    and commits mid-script — which would close a caller's outer transaction
    when this is invoked from inside `_migrate_v1_to_v2`."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agents (
            name              TEXT PRIMARY KEY,
            working_on        TEXT,
            registered_at_ms  INTEGER NOT NULL,
            last_seen_ms      INTEGER NOT NULL,
            session_token     TEXT NOT NULL,
            status            TEXT,
            status_detail     TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_name        TEXT NOT NULL,
            sender_name           TEXT NOT NULL,
            subject               TEXT,
            body                  TEXT NOT NULL,
            sent_at_ms            INTEGER NOT NULL,
            read_at_ms            INTEGER,
            expires_at_ms         INTEGER NOT NULL,
            msg_type              TEXT,
            in_reply_to           INTEGER,
            status                TEXT,
            status_note           TEXT,
            status_updated_at_ms  INTEGER,
            headline              TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_recipient "
        "ON messages(recipient_name, read_at_ms, sent_at_ms DESC)"
    )


# Kept for backward-compat naming — some tests may reference _create_v2_schema
_create_v2_schema = _create_current_schema


def _migrate_v3_to_v4(conn: sqlite3.Connection) -> None:
    """Additive v3 → v4: add the nullable `headline` column to messages.
    Idempotent (guarded by a table_info probe)."""
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(messages)")}
    if "headline" not in existing:
        conn.execute("ALTER TABLE messages ADD COLUMN headline TEXT")


def _migrate_v4_to_v5(conn: sqlite3.Connection) -> None:
    """Additive v4 → v5: add nullable `status` + `status_detail` to agents for
    self-reported liveness (rec #3). Idempotent (guarded by a table_info probe)."""
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(agents)")}
    for col in ("status", "status_detail"):
        if col not in existing:
            conn.execute(f"ALTER TABLE agents ADD COLUMN {col} TEXT")


def _migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
    """Additive v2 → v3 migration: add 5 nullable columns to messages for
    the task-lifecycle convention. All columns nullable → no rebuild
    needed (unlike v1→v2 which had NOT NULL landmines).

    Idempotent: guards each ADD COLUMN with a table_info probe.
    """
    existing_msg_cols = {r["name"] for r in conn.execute("PRAGMA table_info(messages)")}
    for col_name, col_type in [
        ("msg_type", "TEXT"),
        ("in_reply_to", "INTEGER"),
        ("status", "TEXT"),
        ("status_note", "TEXT"),
        ("status_updated_at_ms", "INTEGER"),
    ]:
        if col_name not in existing_msg_cols:
            conn.execute(f"ALTER TABLE messages ADD COLUMN {col_name} {col_type}")


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """Rebuild agents + messages tables into pure v2 shape.

    v1 stored timestamps as TEXT ISO strings; v2 stores INTEGER epoch-ms.
    An earlier draft of this migration used ADD COLUMN + backfill and left
    the legacy TEXT columns in place — but v2 INSERT statements don't
    populate them, and the legacy columns declare NOT NULL, so every send
    or register on a migrated DB failed with "NOT NULL constraint failed".

    The correct migration is a table rebuild: rename → create clean → copy
    with conversion → drop backup. SQLite's ALTER TABLE cannot relax a
    NOT NULL constraint, so this is the only path. Wrapped in an explicit
    IMMEDIATE transaction so a crash mid-migration leaves the DB in a
    consistent state (either fully v1 or fully v2, never a broken hybrid).

    Idempotent: if a prior partial migration left backup tables around,
    they are dropped up front before the rename.
    """
    # Belt-and-suspenders: clear any leftover backup tables from a
    # previously-interrupted migration attempt. DROP IF EXISTS is a no-op
    # when the tables aren't there.
    conn.execute("DROP TABLE IF EXISTS agents_v1_backup")
    conn.execute("DROP TABLE IF EXISTS messages_v1_backup")

    conn.execute("BEGIN IMMEDIATE")
    try:
        # 1. Move the v1 tables out of the way
        conn.execute("ALTER TABLE agents RENAME TO agents_v1_backup")
        conn.execute("ALTER TABLE messages RENAME TO messages_v1_backup")

        # 2. Create the fresh v2 tables (identical shape to _create_v2_schema)
        _create_v2_schema(conn)

        # 3. Copy agents with timestamp conversion. If an ISO parse fails,
        #    default to _now_ms() (better than losing the row; the agent
        #    can re-register to fix its timestamps).
        for r in conn.execute(
            "SELECT name, working_on, registered_at, last_seen, session_token FROM agents_v1_backup"
        ).fetchall():
            reg_ms = _iso_to_ms(r["registered_at"]) or _now_ms()
            seen_ms = _iso_to_ms(r["last_seen"]) or reg_ms
            conn.execute(
                "INSERT INTO agents (name, working_on, registered_at_ms, "
                "last_seen_ms, session_token) VALUES (?, ?, ?, ?, ?)",
                (r["name"], r["working_on"], reg_ms, seen_ms, r["session_token"]),
            )

        # 4. Copy messages. Unparseable expires_at → 0 so the next lazy
        #    purge cleans up the orphan; unparseable sent_at → _now_ms()
        #    (better to have the message with an approximate timestamp
        #    than to lose it).
        for r in conn.execute(
            "SELECT id, recipient_name, sender_name, subject, body, "
            "sent_at, read_at, expires_at FROM messages_v1_backup ORDER BY id ASC"
        ).fetchall():
            sent_ms = _iso_to_ms(r["sent_at"]) or _now_ms()
            read_ms = _iso_to_ms(r["read_at"]) if r["read_at"] else None
            exp_ms = _iso_to_ms(r["expires_at"]) or 0
            conn.execute(
                "INSERT INTO messages (id, recipient_name, sender_name, subject, "
                "body, sent_at_ms, read_at_ms, expires_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    r["id"],
                    r["recipient_name"],
                    r["sender_name"],
                    r["subject"],
                    r["body"],
                    sent_ms,
                    read_ms,
                    exp_ms,
                ),
            )

        # 5. Drop the backup tables and any legacy index
        conn.execute("DROP TABLE agents_v1_backup")
        conn.execute("DROP TABLE messages_v1_backup")

        conn.execute("COMMIT")
    except Exception:
        with suppress(sqlite3.OperationalError):
            conn.execute("ROLLBACK")
        raise


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
    # Use dict-style .keys() check so we tolerate rows from pre-v3 test DBs
    # or partial column sets in edge cases (SELECT specific cols).
    keys = r.keys() if hasattr(r, "keys") else []
    return Message(
        id=r["id"],
        recipient_name=r["recipient_name"],
        sender_name=r["sender_name"],
        subject=r["subject"],
        body=r["body"],
        sent_at=_ms_to_dt(r["sent_at_ms"]),  # type: ignore[arg-type]
        read_at=_ms_to_dt(r["read_at_ms"]),
        expires_at=_ms_to_dt(r["expires_at_ms"]),  # type: ignore[arg-type]
        msg_type=r["msg_type"] if "msg_type" in keys else None,
        in_reply_to=r["in_reply_to"] if "in_reply_to" in keys else None,
        status=r["status"] if "status" in keys else None,
        status_note=r["status_note"] if "status_note" in keys else None,
        status_updated_at=(
            _ms_to_dt(r["status_updated_at_ms"]) if "status_updated_at_ms" in keys else None
        ),
        headline=r["headline"] if "headline" in keys else None,
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
            token = deterministic_token(name)

            if existing:
                conn.execute(
                    "UPDATE agents SET working_on = ?, last_seen_ms = ?, "
                    "session_token = ? WHERE name = ?",
                    (working_on, now_ms, token, name),
                )
            else:
                conn.execute(
                    "INSERT INTO agents (name, working_on, registered_at_ms, "
                    "last_seen_ms, session_token, status) VALUES (?, ?, ?, ?, ?, ?)",
                    (name, working_on, now_ms, now_ms, token, "working"),
                )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    _write_token_sidecar(name, token)

    now_iso = _ms_to_dt(now_ms).isoformat()  # type: ignore[union-attr]
    return {
        "name": name,
        "working_on": working_on,
        "registered_at": now_iso,
        "last_seen": now_iso,
        "session_token": token,
    }


def set_status(name: str, session_token: str, status: str, detail: str | None = None) -> dict:
    """Set an agent's self-reported liveness status (rec #3).

    Convention (not enforced): status ∈ {working, idle, blocked, done}; any
    string is accepted. `detail` is optional free text (e.g. what you're blocked
    on). Requires the name's session_token. Also bumps last_seen — reporting
    status is itself a heartbeat, so `list_threads` can distinguish a
    quiet-but-alive agent from a stopped one. Returns the updated summary.
    """
    init_schema()
    if not status or not isinstance(status, str):
        raise DLBError("status must be a non-empty string")
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT session_token FROM agents WHERE name = ?", (name,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise DLBError(f"Cannot set status: name '{name}' is not registered.")
            if session_token != row["session_token"]:
                conn.execute("ROLLBACK")
                raise AuthError(f"Invalid session_token for '{name}'.")
            now_ms = _now_ms()
            conn.execute(
                "UPDATE agents SET status = ?, status_detail = ?, last_seen_ms = ? WHERE name = ?",
                (status, detail, now_ms, name),
            )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    return {
        "name": name,
        "status": status,
        "status_detail": detail,
        "last_seen": _ms_to_dt(now_ms).isoformat(),  # type: ignore[union-attr]
    }


def recover_token(name: str) -> dict | None:
    """Return the live session_token for a registered name — compaction recovery.

    Fixes the #1 documented dark-agent cause: an agent loses its session_token
    when context compacts, then cannot read its own inbox. Re-registering does
    NOT help — it mints a NEW token and trips the takeover gate on the agent's
    own still-live name. This exposes the token directly.

    Trust model: this returns a token to any caller, which is consistent with
    DLB's stated boundary — "session_token gates the tool API, not the
    underlying SQLite file; cooperating agents under one OS user, not
    confidentiality." Any same-OS process can already read this token straight
    from the SQLite file; recover_token is the sanctioned path (the raw-SQLite
    read is what the agent safety layer blocks). Returns None if `name` is not
    registered. Pure read — no last_seen bump; the agent's next send/read does
    that.
    """
    init_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT name, working_on, session_token, last_seen_ms FROM agents WHERE name = ?",
            (name,),
        ).fetchone()
    if row is None:
        return None
    return {
        "name": row["name"],
        "working_on": row["working_on"],
        "session_token": row["session_token"],
        "last_seen": _ms_to_dt(int(row["last_seen_ms"])).isoformat(),  # type: ignore[union-attr]
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
                       a.status,
                       a.status_detail,
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

            keys = rows[0].keys() if rows else []
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
                        status=r["status"] if "status" in keys else None,
                        status_detail=r["status_detail"] if "status_detail" in keys else None,
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
    msg_type: str | None = None,
    in_reply_to: int | None = None,
    headline: str | None = None,
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

    Lifecycle hints (v0.3.0+):
    - msg_type: advisory tag. Convention: `"task"` signals to the recipient
      that this message expects action + acknowledgment. DLB does not enforce
      any type-based behavior — enforcement is at the client (CLAUDE.md
      protocol level).
    - in_reply_to: id of a message this one is replying to. Enables the
      sender to correlate task acknowledgments and status updates back to
      the original task. Soft reference — not a foreign key; a stale id
      is stored as-is.
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

    sent_ms = _now_ms()
    expires_ms = sent_ms + ttl_days() * 24 * 60 * 60 * 1000

    # Single connection + explicit BEGIN IMMEDIATE so the token lookup and
    # the insert are one atomic unit. Prior implementation used two separate
    # _connect() contexts — a benign TOCTOU: the token could be unregistered
    # between lookup and insert, worst case binding sender_name to a name
    # whose holder had just departed. No safety hole, but airtight is cheap
    # when the fix is one connection.
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            sender = from_ if from_ is not None else "anonymous"
            if session_token is not None:
                row = conn.execute(
                    "SELECT name FROM agents WHERE session_token = ?",
                    (session_token,),
                ).fetchone()
                if row is None:
                    conn.execute("ROLLBACK")
                    raise AuthError("Invalid session_token for send.")
                authenticated_name = row["name"]
                if from_ is not None and from_ != authenticated_name:
                    conn.execute("ROLLBACK")
                    raise AuthError(
                        f"from_={from_!r} does not match session_token's name "
                        f"({authenticated_name!r}). Use from_=None to auto-set, "
                        f"or pass from_={authenticated_name!r}."
                    )
                sender = authenticated_name

            cur = conn.execute(
                "INSERT INTO messages (recipient_name, sender_name, subject, body, "
                "sent_at_ms, read_at_ms, expires_at_ms, msg_type, in_reply_to, headline) "
                "VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)",
                (to, sender, subject, body, sent_ms, expires_ms, msg_type, in_reply_to, headline),
            )
            msg_id = cur.lastrowid
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    return Message(
        id=msg_id,  # type: ignore[arg-type]
        recipient_name=to,
        sender_name=sender,
        subject=subject,
        body=body,
        sent_at=_ms_to_dt(sent_ms),  # type: ignore[arg-type]
        read_at=None,
        expires_at=_ms_to_dt(expires_ms),  # type: ignore[arg-type]
        msg_type=msg_type,
        in_reply_to=in_reply_to,
        headline=headline,
    )


def update_status(
    message_id: int,
    status: str,
    session_token: str,
    note: str | None = None,
) -> Message:
    """Update the lifecycle status of a message (v0.3.0+).

    Called by the RECIPIENT of a message to signal progress back to the
    sender. Convention (not enforced): status ∈ {"queued", "accepted",
    "running", "done", "blocked"}. DLB stores whatever string you pass;
    schema does not constrain the value so future conventions can extend
    without a migration.

    Auth: session_token must match the message's recipient (they're the
    one running the task). If the recipient is unregistered, status
    updates are rejected — there's no owner to authenticate as.

    Side effect: sets read_at_ms if not already set (a status update
    implies you've read the message). Deprecates the read/ack distinction
    tentatively reserved in v1 — update_status IS the future of ack.

    Returns the updated Message so the caller can echo it back to the
    sender if desired.
    """
    init_schema()
    if not status or not isinstance(status, str):
        raise DLBError("status must be a non-empty string")

    now_ms = _now_ms()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            msg_row = conn.execute(
                "SELECT recipient_name, read_at_ms FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            if msg_row is None:
                conn.execute("ROLLBACK")
                raise DLBError(f"No message with id={message_id}")

            agent = conn.execute(
                "SELECT session_token FROM agents WHERE name = ?",
                (msg_row["recipient_name"],),
            ).fetchone()
            if agent is None:
                conn.execute("ROLLBACK")
                raise AuthError(
                    f"Cannot update_status for unregistered recipient "
                    f"'{msg_row['recipient_name']}'."
                )
            if session_token != agent["session_token"]:
                conn.execute("ROLLBACK")
                raise AuthError("Invalid session_token for this message's recipient.")

            # Set read_at_ms if it was still NULL (status update implies read)
            new_read_at_ms = msg_row["read_at_ms"] or now_ms
            conn.execute(
                "UPDATE messages SET status = ?, status_note = ?, "
                "status_updated_at_ms = ?, read_at_ms = ? WHERE id = ?",
                (status, note, now_ms, new_read_at_ms, message_id),
            )

            # Return the fully-hydrated row
            row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    return _row_to_message(row)


def get_task_status(message_id: int) -> dict | None:
    """Return the lifecycle status of a message without reading it (v0.3.0+).

    No auth required — DLB's trust model is coordination between cooperating
    agents under the same OS user, and knowing "did my task get picked up?"
    is exactly the kind of query a sender needs to make against a recipient's
    inbox without holding the recipient's token.

    Returns None if the message id doesn't exist. Otherwise a dict with
    the id, msg_type, status, status_note, status_updated_at (ISO),
    read_at (ISO if read, else None), and in_reply_to fields. Body/subject
    are NOT returned — this is a lightweight status probe, not a content
    read.
    """
    init_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT id, msg_type, in_reply_to, status, status_note, "
            "status_updated_at_ms, read_at_ms, recipient_name, sender_name "
            "FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
    if row is None:
        return None
    read_at = _ms_to_dt(row["read_at_ms"])
    status_updated_at = _ms_to_dt(row["status_updated_at_ms"])
    return {
        "id": row["id"],
        "recipient_name": row["recipient_name"],
        "sender_name": row["sender_name"],
        "msg_type": row["msg_type"],
        "in_reply_to": row["in_reply_to"],
        "status": row["status"],
        "status_note": row["status_note"],
        "status_updated_at": status_updated_at.isoformat() if status_updated_at else None,
        "read_at": read_at.isoformat() if read_at else None,
    }


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

            # #4 read-receipts: for each TASK message we just marked read, drop a
            # lightweight receipt back to its sender so they learn it was seen
            # without polling. Storm/loop guards: task-type only; a real
            # registered recipient is reading (not a dead-letter peek); skip
            # anonymous senders and self-sends; and receipts are msg_type=
            # 'receipt' (never 'task') so they never generate further receipts.
            if ids and mark_read_ms is not None and agent_row is not None and read_receipts_enabled():
                read_iso = _ms_to_dt(mark_read_ms).isoformat()  # type: ignore[union-attr]
                exp_ms = mark_read_ms + ttl_days() * 24 * 60 * 60 * 1000
                id_set = set(ids)
                for r in rows:
                    if r["id"] not in id_set or (r["msg_type"] or "") != "task":
                        continue
                    origin = r["sender_name"]
                    if origin in (None, "anonymous", name):
                        continue
                    # Only receipt a sender that actually holds an inbox — a
                    # receipt to an unregistered/spoofed name is dead-letter noise.
                    if conn.execute("SELECT 1 FROM agents WHERE name = ?", (origin,)).fetchone() is None:
                        continue
                    label = r["headline"] or r["subject"] or (r["body"] or "")[:60]
                    conn.execute(
                        "INSERT INTO messages (recipient_name, sender_name, subject, body, "
                        "sent_at_ms, read_at_ms, expires_at_ms, msg_type, in_reply_to, headline) "
                        "VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)",
                        (
                            origin,
                            name,
                            "✓ read",
                            f"✓ '{label}' read by {name} at {read_iso}",
                            mark_read_ms,
                            exp_ms,
                            "receipt",
                            r["id"],
                            f"✓ read by {name}: {label}",
                        ),
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

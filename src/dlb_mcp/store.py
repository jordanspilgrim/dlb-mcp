"""SQLite-backed store for DLB.

One file, two tables, WAL mode. Every MCP tool call opens a fresh connection,
runs one transaction, closes. No long-running server, no lock file: SQLite
WAL handles concurrent readers + serialized writers for free.

Schema (v2):
    agents(name PK, working_on, registered_at_ms, last_seen_ms, token_hash,
           status, status_detail, session_id, reclaim_hash)  # v7: hashed at rest
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

Scope of the token, precisely: the MCP tool API never hands one session another
session's token (recover_token is gated by per-process ownership / session-id),
and NO plaintext token is stored at rest: the DB holds only sha256(token) and
the sidecar holds a single-use, rotating RECLAIM SECRET (not the token). The live
token lives only in the memory of the process that minted it.

What remains out of scope is RAW access (reading files / importing the store).
But even that no longer yields a usable token: the most a raw-access peer can do
is read the sidecar's reclaim secret and reclaim a name ONCE, which rotates the
secret, so it locks out and alerts the legitimate owner (detectable, not silent).
So `sender_name` is still an attribution HINT rather than a cryptographically
authenticated fact, but forging it now costs a single-use, detectable takeover.
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
# Short metadata fields (name, to, from_, subject, headline, msg_type). The body
# has its own, much larger cap; everything else is a one-liner-ish string and an
# unbounded value here is pure abuse surface; `headline` in particular is
# surfaced UNTRUNCATED into monitor previews, so a giant one floods LLM context.
DEFAULT_MAX_FIELD_BYTES = 8 * 1024  # 8 KiB
# Ring-buffer bound per recipient inbox: on send, if an inbox exceeds this many
# messages, the OLDEST are dropped to make room (send still always succeeds).
# Bounds unbounded row growth from an unauthenticated send flood; 1000 is deep
# headroom over any real coordination inbox. Set DLB_MAX_INBOX=0 to disable.
DEFAULT_MAX_INBOX_MESSAGES = 1000
DEFAULT_TAKEOVER_AFTER_SECONDS = 24 * 60 * 60  # 24h: matches list_threads' default stale window

SCHEMA_VERSION = 9  # v9: cancel/supersede/deadline, monitor heartbeat, receipt opt-out, claims
DEFAULT_WAKE_WINDOW_SECONDS = 120  # a monitor heartbeat newer than this = wakeable
DEFAULT_CLAIM_TTL_SECONDS = 15 * 60
MAX_CLAIM_TTL_SECONDS = 7 * 24 * 60 * 60


def store_path() -> Path:
    return Path(os.environ.get("DLB_STORE", str(DEFAULT_STORE_PATH))).expanduser()


# SQLite stores INTEGERs as signed 64-bit; a value at/over this overflows the
# bind and raised a raw OverflowError from send()/etc. before it was clamped.
_INT64_MAX = 2**63 - 1
# Cap TTL so `sent_ms + ttl*ms_per_day` stays a valid Python datetime (year
# <= 9999); the return path formats expires_at via datetime.isoformat(), which
# raises well before int64 overflow. 365,000 days ≈ 1000 years is effectively
# "never expires" while keeping every derived timestamp representable.
_MAX_TTL_DAYS = 365_000


def ttl_days() -> int:
    try:
        v = int(os.environ.get("DLB_MESSAGE_TTL_DAYS", str(DEFAULT_TTL_DAYS)))
    except ValueError:
        return DEFAULT_TTL_DAYS
    return min(v, _MAX_TTL_DAYS)  # clamp the upper bound; negatives expire-in-past


def _check_message_id(value: int, label: str = "message_id") -> None:
    """Reject a message id outside SQLite's signed-64-bit range (a JSON int can
    be arbitrarily large and would otherwise crash the bind with OverflowError)."""
    if not isinstance(value, int) or isinstance(value, bool) or not (0 <= value <= _INT64_MAX):
        raise DLBError(f"{label} must be an integer in [0, 2**63-1]")


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


def max_field_bytes() -> int:
    try:
        v = int(os.environ.get("DLB_MAX_FIELD_BYTES", str(DEFAULT_MAX_FIELD_BYTES)))
        return v if v > 0 else DEFAULT_MAX_FIELD_BYTES
    except ValueError:
        return DEFAULT_MAX_FIELD_BYTES


def max_inbox_messages() -> int:
    """Per-recipient inbox cap. 0 (or negative) disables the ring buffer."""
    try:
        return int(os.environ.get("DLB_MAX_INBOX", str(DEFAULT_MAX_INBOX_MESSAGES)))
    except ValueError:
        return DEFAULT_MAX_INBOX_MESSAGES


def wake_window_seconds() -> int:
    """How fresh a dlb-monitor heartbeat must be for an agent to count as wakeable."""
    try:
        v = int(os.environ.get("DLB_WAKE_WINDOW_SECONDS", str(DEFAULT_WAKE_WINDOW_SECONDS)))
        return v if v > 0 else DEFAULT_WAKE_WINDOW_SECONDS
    except ValueError:
        return DEFAULT_WAKE_WINDOW_SECONDS


def current_session_id() -> str | None:
    """The harness session id for THIS dlb-mcp process (Design 1), or None.

    Read server-side from DLB_SESSION_ID; the deployment wires it from whatever
    stable per-session id the harness exposes (e.g. CLAUDE_CODE_SESSION_ID) via
    the MCP config `env` block or a launch wrapper. Because the server reads it
    from its OWN environment, the caller (agent) never handles it: compaction
    can't lose it, and a peer can't forge it by passing an argument.

    MUST be unique per session for the recovery boundary to hold. A stable
    harness session id is; two sessions sharing one value would share identity.
    Empty/unset → None, which never matches (recovery falls back to the
    per-process owned-set + stale-gate).
    """
    return os.environ.get("DLB_SESSION_ID") or None


# Control characters that must never appear in identity/metadata fields. Newlines
# and carriage returns are the dangerous ones: an unsanitized value containing a
# newline can forge a second line in a dlb-monitor notification (each stdout line
# is a distinct wake event), letting an unauthenticated sender inject a fully
# fabricated event with an arbitrary sender label. We reject the whole C0 control
# range (plus DEL) at the write boundary so nothing pathological is ever stored;
# the monitor ALSO sanitizes at its sink (defense in depth). Ordinary unicode
# names ("alpha", "ThreadBeta", "worker-1", "ré视") are unaffected.
# C0 controls + DEL + C1 controls (incl. NEL U+0085) + LINE/PARAGRAPH SEPARATOR.
# The extra range beyond C0 matters because Python's str.splitlines() (and many
# terminals/log viewers) treat NEL/U+2028/U+2029 as line boundaries, so a value
# containing one could otherwise forge a second dlb-monitor event line.
_CONTROL_CHARS_RE = re.compile("[\x00-\x1f\x7f-\x9f\u2028\u2029]")


def _check_field(value: str | None, label: str) -> None:
    """Validate a short metadata field: size-capped and free of control chars.

    No-op for None. Raises DLBError on overflow or on any embedded control
    character (see _CONTROL_CHARS_RE). Keeps identity/preview fields safe to
    surface into notifications and hook output without further escaping.
    """
    if value is None:
        return
    if _CONTROL_CHARS_RE.search(value):
        raise DLBError(f"{label} must not contain control characters (newlines, tabs, etc.)")
    n = len(value.encode("utf-8"))
    cap = max_field_bytes()
    if n > cap:
        raise DLBError(f"{label} too large: {n} bytes exceeds DLB_MAX_FIELD_BYTES={cap}")


def _auth_ok(presented: str | None, stored_hash: str | None) -> bool:
    """True iff `presented` hashes to `stored_hash` (constant-time).

    The DB stores only sha256(token), so authentication compares HASHES, never
    plaintext; a read of the DB yields no usable token. Returns False if either
    side is missing, so "no token supplied" and "wrong token" are indistinguish-
    able. compare_digest avoids the early-exit timing signal of ``==``.
    """
    if not presented or not stored_hash:
        return False
    return hmac.compare_digest(token_hash(presented), stored_hash)


def read_receipts_enabled() -> bool:
    """Whether reading a TASK message auto-sends a read-receipt to its sender.
    Default on; set DLB_READ_RECEIPTS=0 (or false/no) to disable globally."""
    return os.environ.get("DLB_READ_RECEIPTS", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


# ── Token identity (random, hashed at rest) ──────────────────────────────────
# Tokens are high-entropy random strings, NOT derived from any on-disk secret.
# The DB stores only sha256(token) (the agents.token_hash column), so reading the
# SQLite file (or a direct ``import dlb_mcp.store``) yields only a hash, never a
# usable token. The live token exists solely in the memory of the process that
# minted it (the server-side owned-token cache) and in the client that called
# register. A force-takeover mints a FRESH token, invalidating the prior
# holder's, restoring real eviction (the earlier deterministic scheme could not
# rotate, since the token was a pure function of the name).


def mint_token() -> str:
    """A fresh, unguessable session token (256 bits of CSPRNG entropy)."""
    return secrets.token_hex(32)


def token_hash(token: str) -> str:
    """sha256 hex of a token, what the DB stores at rest. A random 256-bit token
    has no low-entropy preimage to brute-force, so a plain fast hash (no salt or
    KDF) is sufficient; the point is only to keep plaintext off disk."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def tokens_dir() -> Path:
    """Directory holding the per-name token sidecars: <store_dir>/tokens/."""
    return store_path().parent / "tokens"


def _sidecar_filename(name: str) -> str:
    """Injective, filesystem-safe sidecar filename: sha256(name) hex.

    A lossy sanitizer (the old `re.sub(...)`) mapped distinct names (e.g.
    'a/b', 'a_b', 'a b') onto ONE file, so registering one name would clobber
    another's reclaim credential. Hashing is collision-free and cannot escape
    the tokens dir. The human-readable name still lives on line 1 of the file's
    contents (read by the SessionStart hook), so nothing is lost for display."""
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


def sidecar_path(name: str) -> Path:
    """Path to `name`'s reclaim sidecar. The file (chmod 600) holds two lines:
    the name, then the current RECLAIM SECRET (not the token). It is the instant
    path for a FULL restart: the returning owner reads the secret and calls
    register(force=True, prior_token=<secret>), which bypasses the stale-gate.

    The secret is single-use and rotates on every register/reclaim, so a stolen
    sidecar works at most once and is detectable (a later attempt with the same
    secret fails). No plaintext TOKEN is ever written here. Reading the file is
    file-layer access, out of scope for the tool-API boundary."""
    return tokens_dir() / _sidecar_filename(name)


def _write_reclaim_sidecar(name: str, reclaim_secret: str) -> None:
    """Persist name→reclaim_secret under <store_dir>/tokens/<sha256(name)> (chmod
    600) so the SessionStart hook can rediscover registered names and a returning
    owner can reclaim. Best-effort, never blocks register. Holds the reclaim
    secret, NOT the token (the token is never written to disk)."""
    d = tokens_dir()
    with suppress(OSError):
        d.mkdir(parents=True, exist_ok=True)
        d.chmod(0o700)
        f = d / _sidecar_filename(name)
        f.write_text(f"{name}\n{reclaim_secret}\n")
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
    # v9 wake: True only while a dlb-monitor for this name is heartbeating. An
    # agent can be fresh (recent last_seen) and still NOT wakeable: it sees mail
    # only at its next prompt boundary.
    wakeable: bool = False
    monitor_heartbeat: datetime | None = None
    # v9: receipts are counted apart from unread_count so they don't read as work.
    unread_receipts: int = 0


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
    in_reply_to: int | None = None  # references messages.id, sender-set
    status: str | None = None  # recipient-set; convention: queued/accepted/running/done/blocked
    status_note: str | None = None  # recipient-set free-text detail on status
    status_updated_at: datetime | None = None
    # v4 field: sender-set machine-parseable one-liner, surfaced UNTRUNCATED in
    # monitor/list previews (kills the "encode the answer in the subject" hack).
    headline: str | None = None
    # v9 cancel / supersede / deadline (all sender-set)
    cancelled_at: datetime | None = None
    cancel_reason: str | None = None
    superseded_by: int | None = None
    deadline: datetime | None = None

    @property
    def lifecycle(self) -> str | None:
        """'superseded' | 'cancelled' | 'expired' when the message is no longer
        actionable, else None."""
        if self.superseded_by is not None:
            return "superseded"
        if self.cancelled_at is not None:
            return "cancelled"
        # Same clock as the read filter (deadline_ms <= _now_ms()), so the two
        # can never disagree about whether a message has expired.
        if self.deadline is not None and int(self.deadline.timestamp() * 1000) <= _now_ms():
            return "expired"
        return None


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
    """Bring the store to the current schema, atomically and race-safely.

    The entire check-and-migrate runs in ONE `BEGIN IMMEDIATE` transaction with
    double-checked locking: N concurrent processes serialize on the write lock;
    a loser re-reads `user_version` under the lock, sees the winner already
    migrated, and returns without running any DDL. Table creation, all
    migrations, and the `user_version` stamp commit together, so a crash can
    never leave a half-migrated or mis-versioned store. Idempotent.

    Forward-compat: a store whose `user_version` is NEWER than this build (an
    older dlb-mcp meeting a store a newer one upgraded) raises a clear DLBError
    instead of letting raw `no such column` errors leak from every operation.
    """
    with _connect() as conn:
        current = conn.execute("PRAGMA user_version").fetchone()[0]
        if current == SCHEMA_VERSION:
            return
        if current > SCHEMA_VERSION:
            raise DLBError(
                f"DLB store schema v{current} is newer than this dlb-mcp "
                f"(supports v{SCHEMA_VERSION}). Upgrade dlb-mcp to match."
            )
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Re-read under the lock: another process may have migrated while we
            # waited for the write lock. This closes the check-then-act window
            # that let concurrent callers double-run migrations and throw.
            current = conn.execute("PRAGMA user_version").fetchone()[0]
            if current == SCHEMA_VERSION:
                conn.execute("COMMIT")
                return
            if current > SCHEMA_VERSION:
                # A NEWER dlb-mcp upgraded the store while we waited for the lock.
                # Repeat the forward-compat guard here (not just before the lock)
                # so we raise the clear error instead of returning "success" and
                # then leaking raw errors against an unsupported newer schema.
                conn.execute("ROLLBACK")
                raise DLBError(
                    f"DLB store schema v{current} is newer than this dlb-mcp "
                    f"(supports v{SCHEMA_VERSION}). Upgrade dlb-mcp to match."
                )
            _apply_migrations(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Run every migration needed to reach the current schema, from ANY prior
    state, inside the caller's (already-open) transaction.

    Every step is individually idempotent (guarded by column/table probes), so
    the full tail is safe to run from any starting shape, which also self-heals
    a store whose tables are already modern but whose user_version is stale
    (e.g. after a crashed fresh-init). Legacy v1 (TEXT `registered_at`) is the
    only non-additive starting point and is detected by that column; everything
    from v2 up is reached by the additive/rebuild steps below.
    """
    has_agents = (
        conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agents'").fetchone()
        is not None
    )
    if not has_agents:
        _create_current_schema(conn)  # truly fresh DB
        return
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(agents)")}
    if "registered_at" in cols:  # v1 TEXT-timestamp shape (rebuild required)
        _migrate_v1_to_v2(conn)
    _migrate_v2_to_v3(conn)
    _migrate_v3_to_v4(conn)
    _migrate_v4_to_v5(conn)
    _migrate_v5_to_v6(conn)
    _migrate_v6_to_v7(conn)
    _migrate_v7_to_v8(conn)
    _migrate_v8_to_v9(conn)


def _create_current_schema(conn: sqlite3.Connection) -> None:
    """Create the CURRENT (v9) tables + indexes. Individual execute() calls (NOT
    executescript) so it can run inside a caller's transaction.

    v7 agents store `token_hash`/`reclaim_hash` (no plaintext token at rest).
    v8 messages carry an `authenticated` flag (was the send session-token-backed)
    and an expires index for cheap TTL purges. Fresh DBs get this shape directly;
    upgrades reach it via migrations.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agents (
            name              TEXT PRIMARY KEY,
            working_on        TEXT,
            registered_at_ms  INTEGER NOT NULL,
            last_seen_ms      INTEGER NOT NULL,
            token_hash        TEXT NOT NULL,
            status            TEXT,
            status_detail     TEXT,
            session_id        TEXT,
            reclaim_hash      TEXT,
            monitor_heartbeat_ms  INTEGER,
            receipts_opt_out      INTEGER
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
            headline              TEXT,
            authenticated         INTEGER,
            cancelled_at_ms       INTEGER,
            cancel_reason         TEXT,
            superseded_by         INTEGER,
            deadline_ms           INTEGER
        )
        """
    )
    _create_claims_table(conn)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_recipient "
        "ON messages(recipient_name, read_at_ms, sent_at_ms DESC)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages(expires_at_ms)")


def _create_v2_schema(conn: sqlite3.Connection) -> None:
    """Create the LITERAL v2 tables (agents.session_token, no later columns).

    Used ONLY by the v1→v2 rebuild, which must land on the exact v2 shape and
    then evolve through the additive v3→v7 migrations. It is decoupled from
    _create_current_schema on purpose: once v7 dropped the session_token column,
    aliasing the two would make the v1→v2 INSERT (which writes session_token)
    fail against a current-shape table."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agents (
            name              TEXT PRIMARY KEY,
            working_on        TEXT,
            registered_at_ms  INTEGER NOT NULL,
            last_seen_ms      INTEGER NOT NULL,
            session_token     TEXT NOT NULL
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
            expires_at_ms         INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_recipient "
        "ON messages(recipient_name, read_at_ms, sent_at_ms DESC)"
    )


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


def _migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
    """Additive v5 → v6: add nullable `session_id` to agents (Design 1). Binds a
    name to the harness session that registered it so a NEW process in the SAME
    session (e.g. an MCP-server crash-respawn while the app stays open) can
    recover the token without waiting out the stale-gate. Idempotent."""
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(agents)")}
    if "session_id" not in existing:
        conn.execute("ALTER TABLE agents ADD COLUMN session_id TEXT")


def _migrate_v6_to_v7(conn: sqlite3.Connection) -> None:
    """v6 → v7: stop storing the plaintext token. Replace agents.session_token
    (NOT NULL) with token_hash = sha256(token), and add nullable reclaim_hash.

    SQLite can't drop/rename a NOT NULL column in place, so this is a table
    rebuild (rename → create v7 → copy with hash → drop backup). Runs inside the
    caller's transaction (init_schema owns the single BEGIN IMMEDIATE and stamps
    user_version); this function manages no transaction of its own. Idempotent:
    a table already carrying token_hash is left untouched.
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(agents)")}
    if "token_hash" in cols:
        return  # already v7-shaped

    conn.execute("DROP TABLE IF EXISTS agents_v6_backup")
    conn.execute("ALTER TABLE agents RENAME TO agents_v6_backup")
    # Create ONLY the v7 agents table here (not _create_current_schema, which
    # also re-touches messages/index and would trip on a legacy messages shape).
    conn.execute(
        """
        CREATE TABLE agents (
            name              TEXT PRIMARY KEY,
            working_on        TEXT,
            registered_at_ms  INTEGER NOT NULL,
            last_seen_ms      INTEGER NOT NULL,
            token_hash        TEXT NOT NULL,
            status            TEXT,
            status_detail     TEXT,
            session_id        TEXT,
            reclaim_hash      TEXT
        )
        """
    )
    for r in conn.execute(
        "SELECT name, working_on, registered_at_ms, last_seen_ms, "
        "session_token, status, status_detail, session_id FROM agents_v6_backup"
    ).fetchall():
        conn.execute(
            "INSERT INTO agents (name, working_on, registered_at_ms, "
            "last_seen_ms, token_hash, status, status_detail, session_id, "
            "reclaim_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            (
                r["name"],
                r["working_on"],
                r["registered_at_ms"],
                r["last_seen_ms"],
                token_hash(r["session_token"]),
                r["status"],
                r["status_detail"],
                r["session_id"],
            ),
        )
    conn.execute("DROP TABLE agents_v6_backup")


def _migrate_v7_to_v8(conn: sqlite3.Connection) -> None:
    """Additive v7 → v8: add nullable `messages.authenticated` (1 when the send
    was backed by a valid session_token; used to gate read-receipt generation)
    and an index on `expires_at_ms` so the lazy TTL purge is a range-delete, not
    a full scan. Idempotent (guarded)."""
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(messages)")}
    if "authenticated" not in existing:
        conn.execute("ALTER TABLE messages ADD COLUMN authenticated INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages(expires_at_ms)")


def _create_claims_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS claims (
            key            TEXT PRIMARY KEY,
            holder         TEXT NOT NULL,
            claimed_at_ms  INTEGER NOT NULL,
            expires_at_ms  INTEGER NOT NULL,
            note           TEXT
        )
        """
    )


def _migrate_v8_to_v9(conn: sqlite3.Connection) -> None:
    """Additive v8 -> v9. All nullable, so no rebuild. Idempotent (guarded).

    agents:   monitor_heartbeat_ms (a running dlb-monitor proves the agent can be
              woken mid-idle), receipts_opt_out (sender declines read receipts).
    messages: cancelled_at_ms / cancel_reason / superseded_by (sender withdraws or
              replaces a task), deadline_ms (task stops being actionable after
              this, but the row is kept so the sender can still see the outcome).
    claims:   atomic advisory locks with a TTL, so two agents can't both take
              the same piece of work.
    """
    agent_cols = {r["name"] for r in conn.execute("PRAGMA table_info(agents)")}
    if "monitor_heartbeat_ms" not in agent_cols:
        conn.execute("ALTER TABLE agents ADD COLUMN monitor_heartbeat_ms INTEGER")
    if "receipts_opt_out" not in agent_cols:
        conn.execute("ALTER TABLE agents ADD COLUMN receipts_opt_out INTEGER")
    msg_cols = {r["name"] for r in conn.execute("PRAGMA table_info(messages)")}
    for col, typ in (
        ("cancelled_at_ms", "INTEGER"),
        ("cancel_reason", "TEXT"),
        ("superseded_by", "INTEGER"),
        ("deadline_ms", "INTEGER"),
    ):
        if col not in msg_cols:
            conn.execute(f"ALTER TABLE messages ADD COLUMN {col} {typ}")
    _create_claims_table(conn)


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
    the legacy TEXT columns in place, but v2 INSERT statements don't
    populate them, and the legacy columns declare NOT NULL, so every send
    or register on a migrated DB failed with "NOT NULL constraint failed".

    The correct migration is a table rebuild: rename → create clean → copy
    with conversion → drop backup. SQLite's ALTER TABLE cannot relax a
    NOT NULL constraint, so this is the only path. Runs inside the caller's
    transaction (init_schema owns the single BEGIN IMMEDIATE and stamps
    user_version atomically); this function manages no transaction of its own.

    Idempotent: leftover backup tables from an interrupted run are dropped up
    front. Robust to a corrupted v1 table that lacks the name PRIMARY KEY:
    duplicate names are de-duplicated via ON CONFLICT rather than aborting the
    whole migration (which would strand the store).
    """
    # Belt-and-suspenders: clear any leftover backup tables from a
    # previously-interrupted migration attempt. DROP IF EXISTS is a no-op
    # when the tables aren't there.
    conn.execute("DROP TABLE IF EXISTS agents_v1_backup")
    conn.execute("DROP TABLE IF EXISTS messages_v1_backup")

    # 1. Move the v1 tables out of the way
    conn.execute("ALTER TABLE agents RENAME TO agents_v1_backup")
    conn.execute("ALTER TABLE messages RENAME TO messages_v1_backup")

    # 2. Create the fresh v2 tables (identical shape to _create_v2_schema)
    _create_v2_schema(conn)

    # 3. Copy agents with timestamp conversion. If an ISO parse fails, default
    #    to _now_ms() (better than losing the row). ON CONFLICT keeps the most
    #    recently-seen row so a duplicate name in a corrupted v1 table can't
    #    abort the migration.
    for r in conn.execute(
        "SELECT name, working_on, registered_at, last_seen, session_token FROM agents_v1_backup"
    ).fetchall():
        reg_ms = _iso_to_ms(r["registered_at"]) or _now_ms()
        seen_ms = _iso_to_ms(r["last_seen"]) or reg_ms
        conn.execute(
            "INSERT INTO agents (name, working_on, registered_at_ms, "
            "last_seen_ms, session_token) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET "
            "working_on = excluded.working_on, "
            "session_token = excluded.session_token, "
            "registered_at_ms = MIN(agents.registered_at_ms, excluded.registered_at_ms), "
            "last_seen_ms = MAX(agents.last_seen_ms, excluded.last_seen_ms)",
            (r["name"], r["working_on"], reg_ms, seen_ms, r["session_token"]),
        )

    # 4. Copy messages. Unparseable expires_at → 0 so the next lazy purge cleans
    #    up the orphan; unparseable sent_at → _now_ms().
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

    # 5. Drop the backup tables
    conn.execute("DROP TABLE agents_v1_backup")
    conn.execute("DROP TABLE messages_v1_backup")


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
        cancelled_at=_ms_to_dt(r["cancelled_at_ms"]) if "cancelled_at_ms" in keys else None,
        cancel_reason=r["cancel_reason"] if "cancel_reason" in keys else None,
        superseded_by=r["superseded_by"] if "superseded_by" in keys else None,
        deadline=_ms_to_dt(r["deadline_ms"]) if "deadline_ms" in keys else None,
    )


# SQL fragment: a message that is still actionable (not cancelled, superseded,
# or past its deadline). Takes one bound parameter: now_ms.
_ACTIVE_SQL = "cancelled_at_ms IS NULL AND (deadline_ms IS NULL OR deadline_ms > ?)"


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


def _enforce_inbox_cap(conn: sqlite3.Connection, recipient: str) -> int:
    """Trim `recipient`'s inbox to the newest max_inbox_messages() rows.

    Ring-buffer semantics: keeps the highest-id (newest) N messages and deletes
    everything older. Called inside send()'s transaction after the INSERT so the
    just-arrived message is always retained. A no-op when the cap is disabled
    (<= 0) or the inbox is under the limit. Returns the number of rows dropped.

    Trade-off (owner-chosen): under a sustained flood this can drop a real
    queued-but-unread message. That is preferable to unbounded row growth in
    DLB's cooperative model; raise DLB_MAX_INBOX or set it to 0 to opt out.
    """
    cap = max_inbox_messages()
    if cap <= 0:
        return 0
    cur = conn.execute(
        "DELETE FROM messages WHERE recipient_name = ? AND id NOT IN ("
        "SELECT id FROM messages WHERE recipient_name = ? ORDER BY id DESC LIMIT ?)",
        (recipient, recipient, cap),
    )
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
    # A name flows into sender_name on authenticated sends, into the monitor
    # notification line, and into the SessionStart hook output; reject control
    # chars (newline-injection) and cap the size, same as any metadata field.
    _check_field(name, "name")
    _check_field(working_on, "working_on")

    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT name, last_seen_ms, token_hash, reclaim_hash FROM agents WHERE name = ?",
                (name,),
            ).fetchone()

            if existing and not force:
                suggestions = _suggest_alternatives(conn, name)
                conn.execute("ROLLBACK")
                raise NameConflict(name, suggestions)

            if existing and force:
                # Gate: prior_token may be the current TOKEN (a handoff) OR the
                # rotating RECLAIM SECRET from the sidecar (a full-restart reclaim);
                # either bypasses the stale-gate. Otherwise the holder must be
                # stale. Both credentials are single-use here: this register
                # rotates them below, so a stolen sidecar works at most once and a
                # later attempt with the same secret fails (tamper-evident).
                proof_ok = _auth_ok(prior_token, existing["token_hash"]) or _auth_ok(
                    prior_token, existing["reclaim_hash"]
                )
                if not proof_ok:
                    age_seconds = (_now_ms() - int(existing["last_seen_ms"])) / 1000
                    if age_seconds < takeover_after_seconds():
                        conn.execute("ROLLBACK")
                        raise TakeoverDenied(name, age_seconds)

            now_ms = _now_ms()
            token = mint_token()  # random; only sha256(token) is stored
            reclaim_secret = mint_token()  # rotating single-use reclaim credential
            # Bind this registration to the current harness session (Design 1).
            # None when DLB_SESSION_ID is unset → session-id recovery inactive.
            sid = current_session_id()

            if existing:
                conn.execute(
                    "UPDATE agents SET working_on = ?, last_seen_ms = ?, "
                    "token_hash = ?, reclaim_hash = ?, session_id = ? WHERE name = ?",
                    (working_on, now_ms, token_hash(token), token_hash(reclaim_secret), sid, name),
                )
            else:
                conn.execute(
                    "INSERT INTO agents (name, working_on, registered_at_ms, "
                    "last_seen_ms, token_hash, status, session_id, reclaim_hash) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        name,
                        working_on,
                        now_ms,
                        now_ms,
                        token_hash(token),
                        "working",
                        sid,
                        token_hash(reclaim_secret),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise

    # The sidecar holds the RECLAIM SECRET (not the token); a returning owner
    # reads it and calls register(force=True, prior_token=<secret>).
    _write_reclaim_sidecar(name, reclaim_secret)

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
    on). Requires the name's session_token. Also bumps last_seen; reporting
    status is itself a heartbeat, so `list_threads` can distinguish a
    quiet-but-alive agent from a stopped one. Returns the updated summary.
    """
    init_schema()
    if not status or not isinstance(status, str):
        raise DLBError("status must be a non-empty string")
    _check_field(status, "status")
    _check_field(detail, "detail")
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute("SELECT token_hash FROM agents WHERE name = ?", (name,)).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise DLBError(f"Cannot set status: name '{name}' is not registered.")
            if not _auth_ok(session_token, row["token_hash"]):
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


def agent_meta(name: str) -> dict | None:
    """Registration metadata for `name`, or None if unregistered.

    Deliberately does NOT return a token: the DB stores only sha256(token), so
    there is no plaintext token to hand back. The token lives solely in the
    memory of the process that minted it (the server's owned-token cache). The
    server uses this to check existence + the bound session_id (Design 1) and
    the reclaim_hash (the rotating credential) when deciding whether to recover.
    """
    init_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT name, working_on, last_seen_ms, session_id, token_hash, reclaim_hash "
            "FROM agents WHERE name = ?",
            (name,),
        ).fetchone()
    if row is None:
        return None
    return {
        "name": row["name"],
        "working_on": row["working_on"],
        "last_seen": _ms_to_dt(int(row["last_seen_ms"])).isoformat(),  # type: ignore[union-attr]
        "last_seen_ms": int(row["last_seen_ms"]),
        "session_id": row["session_id"],
        "token_hash": row["token_hash"],
        "reclaim_hash": row["reclaim_hash"],
    }


def rotate_token(name: str, new_token: str, new_reclaim_secret: str) -> bool:
    """Rotate `name`'s token AND reclaim secret (store only their hashes), bind
    to the current harness session, bump last_seen, and refresh the sidecar with
    the new reclaim secret. Returns False if the name is not registered.

    Used by the server to hand a fresh token to a legitimate returning owner
    (Design 1 crash-respawn) without a plaintext token ever touching disk. The
    old hashes are overwritten → the old token and old reclaim secret both stop
    working (rotation is what makes the reclaim credential single-use).
    """
    init_schema()
    now_ms = _now_ms()
    sid = current_session_id()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute("SELECT name FROM agents WHERE name = ?", (name,)).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return False
            conn.execute(
                "UPDATE agents SET token_hash = ?, reclaim_hash = ?, "
                "session_id = ?, last_seen_ms = ? WHERE name = ?",
                (token_hash(new_token), token_hash(new_reclaim_secret), sid, now_ms, name),
            )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    _write_reclaim_sidecar(name, new_reclaim_secret)
    return True


def bound_session_id(name: str) -> str | None:
    """The harness session id bound to `name` at its last register (Design 1),
    or None if the name is unregistered or was registered without DLB_SESSION_ID
    set. Used by server.recover_token to let a new process in the SAME session
    reclaim the token without the stale-gate."""
    init_schema()
    with _connect() as conn:
        row = conn.execute("SELECT session_id FROM agents WHERE name = ?", (name,)).fetchone()
    return row["session_id"] if row is not None else None


def list_threads(active_within: timedelta = timedelta(hours=24)) -> list[AgentSummary]:
    """List all known agents, with unread counts and staleness flags.

    No auth required. Also opportunistically purges any expired messages
    across the whole store (cheap, since we're scanning anyway).
    """
    init_schema()
    with _connect() as conn:
        # IMMEDIATE, not deferred: this transaction writes (via _purge_expired's
        # DELETE). A deferred BEGIN that upgrades to a write lock mid-transaction
        # can raise SQLITE_BUSY under sibling contention instead of waiting on
        # busy_timeout; acquiring the write lock up front matches every other
        # writer here and lets busy_timeout do its job.
        conn.execute("BEGIN IMMEDIATE")
        try:
            _purge_expired(conn)

            now_ms = _now_ms()
            cutoff_ms = now_ms - int(active_within.total_seconds() * 1000)
            wake_cutoff_ms = now_ms - wake_window_seconds() * 1000
            # unread_count is ACTIONABLE mail only: receipts are counted apart,
            # and cancelled / superseded / past-deadline messages not at all, so
            # a coordinator never mistakes noise or withdrawn work for a backlog.
            rows = conn.execute(
                f"""
                SELECT a.name,
                       a.working_on,
                       a.last_seen_ms,
                       a.status,
                       a.status_detail,
                       a.monitor_heartbeat_ms,
                       COALESCE(m.unread_count, 0) AS unread_count,
                       COALESCE(m.unread_receipts, 0) AS unread_receipts
                FROM agents a
                LEFT JOIN (
                    SELECT recipient_name,
                           SUM(CASE WHEN COALESCE(msg_type, '') != 'receipt'
                                     AND {_ACTIVE_SQL} THEN 1 ELSE 0 END) AS unread_count,
                           SUM(CASE WHEN msg_type = 'receipt' THEN 1 ELSE 0 END)
                               AS unread_receipts
                    FROM messages
                    WHERE read_at_ms IS NULL
                    GROUP BY recipient_name
                ) m ON m.recipient_name = a.name
                ORDER BY a.last_seen_ms DESC
                """,
                (now_ms,),
            ).fetchall()

            keys = rows[0].keys() if rows else []
            out: list[AgentSummary] = []
            for r in rows:
                last_seen_ms = int(r["last_seen_ms"])
                hb_ms = r["monitor_heartbeat_ms"]
                out.append(
                    AgentSummary(
                        name=r["name"],
                        working_on=r["working_on"],
                        last_seen=_ms_to_dt(last_seen_ms),  # type: ignore[arg-type]
                        unread_count=r["unread_count"],
                        stale=last_seen_ms < cutoff_ms,
                        status=r["status"] if "status" in keys else None,
                        status_detail=r["status_detail"] if "status_detail" in keys else None,
                        wakeable=hb_ms is not None and hb_ms >= wake_cutoff_ms,
                        monitor_heartbeat=_ms_to_dt(hb_ms),
                        unread_receipts=r["unread_receipts"],
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
    supersedes: int | None = None,
    expires_in_seconds: int | None = None,
) -> Message:
    """Drop a message in `to`'s inbox.

    Dead-letter semantics: ALWAYS succeeds once validation passes, even if
    `to` is not registered. Messages queue under the name and surface when
    someone reads it.

    Provenance (read this carefully; it is weaker than it looks):
    - Without session_token: `from_` is the literal string the caller passed
      (defaulting to "anonymous"). No verification: the sender label is
      free text and could be a lie.
    - With session_token: the token is looked up; if `from_` is omitted, it
      becomes the token's registered name; if `from_` is supplied, it MUST
      equal the token's name or AuthError is raised.
    - What this does NOT give you: unforgeable provenance. No token is stored at
      rest, so a peer can't simply read one, but a same-user peer with RAW file
      access could read the sidecar's single-use reclaim secret and hijack X
      once (which rotates the secret → detectable). `sender_name` is therefore an
      attribution HINT, not proof of origin. Acceptable under DLB's cooperative
      same-OS-user model but never a trust boundary between distinct agents.

    Size caps (all reject with DLBError on overflow):
    - body bytes (UTF-8) must be <= DLB_MAX_BODY_BYTES (default 256KiB).
    - to / from_ / subject / headline / msg_type each <= DLB_MAX_FIELD_BYTES
      (default 8KiB) and must contain no control characters; they are surfaced
      into notifications and hook output where a newline could forge an event.

    Inbox cap: send ALWAYS succeeds, but the recipient's inbox is a ring buffer
    of at most DLB_MAX_INBOX messages (default 1000). On overflow the OLDEST
    messages are dropped to bound row growth from a send flood; under sustained
    flooding this can drop a real queued message. Set DLB_MAX_INBOX=0 to disable.

    Lifecycle hints (v0.3.0+):
    - msg_type: advisory tag. Convention: `"task"` signals to the recipient
      that this message expects action + acknowledgment. DLB does not enforce
      any type-based behavior; enforcement is at the client (CLAUDE.md
      protocol level).
    - in_reply_to: id of a message this one is replying to. Enables the
      sender to correlate task acknowledgments and status updates back to
      the original task. Soft reference, not a foreign key; a stale id
      is stored as-is.

    Withdrawal (v9):
    - supersedes: id of an earlier message THIS sender sent (authenticated)
      that the new one replaces. Requires session_token. The old message is
      marked superseded and hidden from the recipient's default read; if they
      had already read it, a msg_type='cancel' notice tells them to stop.
    - expires_in_seconds: a deadline. Past it the message stops counting as
      unread work and is hidden from the default read (it is still retained
      until the TTL purge, and visible with include_inactive=True).
    """
    init_schema()
    if supersedes is not None:
        _check_message_id(supersedes, "supersedes")
        if session_token is None:
            raise AuthError("supersedes requires session_token (only the original sender may).")
    if expires_in_seconds is not None and (
        isinstance(expires_in_seconds, bool)
        or not isinstance(expires_in_seconds, int)
        or expires_in_seconds <= 0
    ):
        raise DLBError("expires_in_seconds must be a positive integer")
    if not to or not isinstance(to, str):
        raise DLBError("to must be a non-empty string")
    if not body or not isinstance(body, str):
        raise DLBError("body must be a non-empty string")

    body_bytes = len(body.encode("utf-8"))
    cap = max_body_bytes()
    if body_bytes > cap:
        raise DLBError(f"body too large: {body_bytes} bytes exceeds DLB_MAX_BODY_BYTES={cap}")

    # Bound + sanitize every short metadata field. Only `body` had a cap before;
    # subject/headline/from_/to/msg_type were unbounded (context-flood surface)
    # and unsanitized (control chars → forged monitor events). `to` is validated
    # for control chars only via _check_field; its non-emptiness is above.
    _check_field(to, "to")
    _check_field(from_, "from_")
    _check_field(subject, "subject")
    _check_field(headline, "headline")
    _check_field(msg_type, "msg_type")
    if in_reply_to is not None:
        _check_message_id(in_reply_to, "in_reply_to")

    sent_ms = _now_ms()
    expires_ms = sent_ms + ttl_days() * 24 * 60 * 60 * 1000
    deadline_ms = sent_ms + expires_in_seconds * 1000 if expires_in_seconds else None

    # Single connection + explicit BEGIN IMMEDIATE so the token lookup and
    # the insert are one atomic unit. Prior implementation used two separate
    # _connect() contexts, a benign TOCTOU: the token could be unregistered
    # between lookup and insert, worst case binding sender_name to a name
    # whose holder had just departed. No safety hole, but airtight is cheap
    # when the fix is one connection.
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            sender = from_ if from_ is not None else "anonymous"
            authenticated = 0  # 1 only when a valid session_token backed the send
            if session_token is not None:
                row = conn.execute(
                    "SELECT name FROM agents WHERE token_hash = ?",
                    (token_hash(session_token),),
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
                authenticated = 1

            if supersedes is not None:
                old = _load_withdrawable(conn, supersedes, sender)

            cur = conn.execute(
                "INSERT INTO messages (recipient_name, sender_name, subject, body, "
                "sent_at_ms, read_at_ms, expires_at_ms, msg_type, in_reply_to, headline, "
                "authenticated, deadline_ms) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)",
                (
                    to,
                    sender,
                    subject,
                    body,
                    sent_ms,
                    expires_ms,
                    msg_type,
                    in_reply_to,
                    headline,
                    authenticated,
                    deadline_ms,
                ),
            )
            msg_id = cur.lastrowid
            if supersedes is not None:
                _withdraw(
                    conn,
                    old,
                    reason=f"superseded by #{msg_id}",
                    superseded_by=msg_id,
                    now_ms=sent_ms,
                )
            # Ring-buffer the recipient's inbox (drop oldest beyond the cap).
            # Runs inside this txn so the message just inserted is never the one
            # dropped. No-op when DLB_MAX_INBOX <= 0.
            _enforce_inbox_cap(conn, to)
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
        deadline=_ms_to_dt(deadline_ms),
    )


def _load_withdrawable(conn: sqlite3.Connection, message_id: int, caller: str) -> sqlite3.Row:
    """Fetch a message the caller may cancel or supersede, or raise.

    Only the ORIGINAL sender of an AUTHENTICATED message may withdraw it: an
    unauthenticated message's sender label is free text, so nobody can prove
    they sent it, and letting a recipient withdraw its own inbound work would
    let a stalled agent silently drop tasks.
    """
    row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
    if row is None:
        raise DLBError(f"message #{message_id} not found (never sent, or purged)")
    if not row["authenticated"] or row["sender_name"] != caller:
        raise AuthError(
            f"message #{message_id} can only be withdrawn by its authenticated sender "
            f"({row['sender_name']!r}, authenticated={bool(row['authenticated'])})."
        )
    return row


def _withdraw(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    reason: str | None,
    superseded_by: int | None,
    now_ms: int,
) -> bool:
    """Mark `row` cancelled (and optionally superseded). Returns True if this
    call changed it; False if it was already withdrawn (idempotent).

    If the recipient had ALREADY read the message, hiding it is not enough:
    they may be working on it. Drop a msg_type='cancel' notice so they stop.
    """
    if row["cancelled_at_ms"] is not None:
        return False
    conn.execute(
        "UPDATE messages SET cancelled_at_ms = ?, cancel_reason = ?, superseded_by = ? "
        "WHERE id = ?",
        (now_ms, reason, superseded_by, row["id"]),
    )
    if row["read_at_ms"] is not None:
        verb = f"superseded by #{superseded_by}" if superseded_by else "cancelled"
        conn.execute(
            "INSERT INTO messages (recipient_name, sender_name, subject, body, sent_at_ms, "
            "read_at_ms, expires_at_ms, msg_type, in_reply_to, headline, authenticated) "
            "VALUES (?, ?, ?, ?, ?, NULL, ?, 'cancel', ?, ?, 1)",
            (
                row["recipient_name"],
                row["sender_name"],
                f"✗ #{row['id']} {verb}",
                f"Stop work on message #{row['id']}: {verb}."
                + (f" Reason: {reason}" if reason and not superseded_by else ""),
                now_ms,
                now_ms + ttl_days() * 24 * 60 * 60 * 1000,
                row["id"],
                f"cancel #{row['id']}",
            ),
        )
        _enforce_inbox_cap(conn, row["recipient_name"])
    return True


def _token_name(conn: sqlite3.Connection, session_token: str) -> str:
    row = conn.execute(
        "SELECT name FROM agents WHERE token_hash = ?", (token_hash(session_token),)
    ).fetchone()
    if row is None:
        raise AuthError("Invalid session_token.")
    return row["name"]


def cancel_task(message_id: int, session_token: str, reason: str | None = None) -> dict:
    """Withdraw a message you sent. Idempotent.

    Returns {"cancelled": True, "already": bool, "notified": bool}: `notified`
    is True when the recipient had already read it and got a cancel notice.
    """
    init_schema()
    _check_message_id(message_id)
    _check_field(reason, "reason")
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            caller = _token_name(conn, session_token)
            row = _load_withdrawable(conn, message_id, caller)
            changed = _withdraw(conn, row, reason=reason, superseded_by=None, now_ms=_now_ms())
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    return {
        "cancelled": True,
        "already": not changed,
        "notified": changed and row["read_at_ms"] is not None,
    }


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
    updates are rejected; there's no owner to authenticate as.

    Side effect: sets read_at_ms if not already set (a status update
    implies you've read the message). Deprecates the read/ack distinction
    tentatively reserved in v1; update_status IS the future of ack.

    Returns the updated Message so the caller can echo it back to the
    sender if desired.
    """
    init_schema()
    _check_message_id(message_id)
    if not status or not isinstance(status, str):
        raise DLBError("status must be a non-empty string")
    _check_field(status, "status")
    _check_field(note, "note")

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
                "SELECT token_hash FROM agents WHERE name = ?",
                (msg_row["recipient_name"],),
            ).fetchone()
            if agent is None:
                conn.execute("ROLLBACK")
                raise AuthError(
                    f"Cannot update_status for unregistered recipient "
                    f"'{msg_row['recipient_name']}'."
                )
            if not _auth_ok(session_token, agent["token_hash"]):
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

    No auth required: DLB's trust model is coordination between cooperating
    agents under the same OS user, and knowing "did my task get picked up?"
    is exactly the kind of query a sender needs to make against a recipient's
    inbox without holding the recipient's token.

    Returns None if the message id doesn't exist. Otherwise a dict with
    the id, msg_type, status, status_note, status_updated_at (ISO),
    read_at (ISO if read, else None), and in_reply_to fields. Body/subject
    are NOT returned; this is a lightweight status probe, not a content
    read.
    """
    init_schema()
    _check_message_id(message_id)
    with _connect() as conn:
        row = conn.execute(
            "SELECT id, msg_type, in_reply_to, status, status_note, "
            "status_updated_at_ms, read_at_ms "
            "FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
    if row is None:
        return None
    read_at = _ms_to_dt(row["read_at_ms"])
    status_updated_at = _ms_to_dt(row["status_updated_at_ms"])
    # A lightweight, auth-free status probe. It deliberately does NOT return
    # recipient_name/sender_name (the docstring never promised them): keeping the
    # auth-free surface to pure lifecycle status shrinks the enumeration-harvest
    # surface (ids are sequential). Use read() for message content/parties.
    return {
        "id": row["id"],
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
    order: str = "newest",
    exclude_types: list[str] | None = None,
    include_inactive: bool = False,
) -> list[Message]:
    """Read messages for `name`. Marks them read ONLY for the registered owner.

    Selection (v9):
    - order: "newest" (default, back-compat) or "oldest". Use "oldest" to work
      an inbox FIFO, so a superseding follow-up is never handled before the
      task it replaces.
    - exclude_types: msg_types to leave out, e.g. ["receipt"]. Excluded rows
      are NOT returned and NOT marked read; they stay waiting.
    - include_inactive: by default, cancelled, superseded and past-deadline
      messages are hidden (they are not work any more). True shows them, with
      `lifecycle` set on each so the reader can tell why.

    Auth rules:
    - If `name` IS registered: session_token must match. Otherwise AuthError.
      Messages returned are marked read (and TASK messages generate receipts).
    - If `name` IS NOT registered: anyone can read (no owner to protect), but
      this is a NON-DESTRUCTIVE PEEK; messages are returned WITHOUT being
      marked read. That preserves DLB's core dead-letter guarantee: a message
      queued for a not-yet-registered name is still unread (and still surfaced
      by the default unread_only read + the SessionStart reminder) when the
      intended owner finally registers. Previously any caller could peek an
      unclaimed inbox and silently mark its queued mail read, so the real owner
      saw nothing on arrival.

    Also opportunistically purges expired messages in this inbox before
    returning.
    """
    init_schema()
    if not name or not isinstance(name, str):
        raise DLBError("name must be a non-empty string")
    if limit < 1 or limit > 1000:
        raise DLBError("limit must be between 1 and 1000")
    if order not in ("newest", "oldest"):
        raise DLBError("order must be 'newest' or 'oldest'")
    if exclude_types is not None:
        if not isinstance(exclude_types, list) or len(exclude_types) > 50:
            raise DLBError("exclude_types must be a list of at most 50 msg_type strings")
        for t in exclude_types:
            if not isinstance(t, str):
                raise DLBError("exclude_types entries must be strings")
            _check_field(t, "exclude_types entry")

    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            agent_row = conn.execute(
                "SELECT name, token_hash FROM agents WHERE name = ?", (name,)
            ).fetchone()
            is_owner = agent_row is not None

            if is_owner:
                if not _auth_ok(session_token, agent_row["token_hash"]):
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

            where = ""
            params: list = [name]
            if unread_only:
                where += " AND read_at_ms IS NULL"
            if not include_inactive:
                where += f" AND {_ACTIVE_SQL}"
                params.append(_now_ms())
            if exclude_types:
                where += (
                    f" AND COALESCE(msg_type, '') NOT IN ({','.join('?' * len(exclude_types))})"
                )
                params.extend(exclude_types)
            direction = "ASC" if order == "oldest" else "DESC"
            params.append(limit)
            rows = conn.execute(
                f"SELECT * FROM messages WHERE recipient_name = ?{where} "
                f"ORDER BY sent_at_ms {direction}, id {direction} LIMIT ?",
                params,
            ).fetchall()

            # Mark read ONLY when the authenticated owner is reading. An
            # unregistered-name peek is deliberately non-destructive (see
            # docstring); it leaves read_at NULL so the mail is still waiting
            # for whoever eventually claims the name. Capture the exact ms value
            # we write so the returned objects' read_at matches the DB to the
            # millisecond (avoids a "stored vs. returned" drift bug).
            ids = [r["id"] for r in rows if r["read_at_ms"] is None]
            mark_read_ms: int | None = None
            if ids and is_owner:
                mark_read_ms = _now_ms()
                placeholders = ",".join("?" * len(ids))
                conn.execute(
                    f"UPDATE messages SET read_at_ms = ? WHERE id IN ({placeholders})",
                    (mark_read_ms, *ids),
                )

            # #4 read-receipts: for each TASK message we just marked read, drop a
            # lightweight receipt back to its sender so they learn it was seen
            # without polling. Guards: task-type only; a real registered recipient
            # is reading (not a dead-letter peek); skip anonymous/self-sends; the
            # ORIGINAL send must have been AUTHENTICATED (session-token-backed);
            # otherwise `sender_name` is a spoofable free-text `from_` and we'd
            # author false-attribution receipts into an uninvolved agent's inbox;
            # and receipts are msg_type='receipt' (never 'task') so they never
            # generate further receipts. The receipt INSERTs also obey the inbox
            # ring buffer (enforced per recipient below); the send() path is not
            # the only writer, so the cap must be applied here too.
            if (
                ids
                and mark_read_ms is not None
                and agent_row is not None
                and read_receipts_enabled()
            ):
                read_iso = _ms_to_dt(mark_read_ms).isoformat()  # type: ignore[union-attr]
                exp_ms = mark_read_ms + ttl_days() * 24 * 60 * 60 * 1000
                id_set = set(ids)
                receipt_recipients: set[str] = set()
                for r in rows:
                    if (
                        r["id"] not in id_set
                        or (r["msg_type"] or "") != "task"
                        or not r["authenticated"]
                    ):
                        continue
                    origin = r["sender_name"]
                    if origin in (None, "anonymous", name):
                        continue
                    # Only receipt a sender that actually holds an inbox; a
                    # receipt to an unregistered/spoofed name is dead-letter noise.
                    # A sender that opted out (set_receipts) gets none at all.
                    origin_row = conn.execute(
                        "SELECT receipts_opt_out FROM agents WHERE name = ?", (origin,)
                    ).fetchone()
                    if origin_row is None or origin_row["receipts_opt_out"]:
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
                    receipt_recipients.add(origin)
                # Keep the ring-buffer invariant on every inbox we just wrote to.
                for origin in receipt_recipients:
                    _enforce_inbox_cap(conn, origin)

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

    Currently we treat ack as 'definitely read'; we set read_at if it's
    still NULL. The distinction between read and ack is for future use.

    Auth: the caller must hold a valid session_token for the message's
    recipient. (If the recipient is unregistered, ack is a no-op; there's
    no owner to take responsibility.)
    """
    init_schema()
    _check_message_id(message_id)
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
                "SELECT token_hash FROM agents WHERE name = ?",
                (msg["recipient_name"],),
            ).fetchone()
            if agent is None:
                conn.execute("ROLLBACK")
                raise AuthError(
                    f"Cannot ack message for unregistered recipient '{msg['recipient_name']}'."
                )
            if not _auth_ok(session_token, agent["token_hash"]):
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

    Messages addressed to that name are PRESERVED; re-registration (by
    anyone) gives access to them again. The "name holder" abstraction is
    over agent presence, not over message ownership.
    """
    init_schema()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            agent = conn.execute("SELECT token_hash FROM agents WHERE name = ?", (name,)).fetchone()
            if agent is None:
                conn.execute("ROLLBACK")
                return False
            if not _auth_ok(session_token, agent["token_hash"]):
                conn.execute("ROLLBACK")
                raise AuthError(f"Invalid session_token for '{name}'.")

            conn.execute("DELETE FROM agents WHERE name = ?", (name,))
            # A released name holds nothing: drop its claims so the work it was
            # guarding is immediately claimable instead of waiting out the TTL.
            conn.execute("DELETE FROM claims WHERE holder = ?", (name,))
            conn.execute("COMMIT")
            return True
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise


def _require_auth(conn: sqlite3.Connection, name: str, session_token: str, action: str) -> None:
    """Raise unless `name` is registered and `session_token` is its token."""
    row = conn.execute("SELECT token_hash FROM agents WHERE name = ?", (name,)).fetchone()
    if row is None:
        raise DLBError(f"Cannot {action}: name '{name}' is not registered.")
    if not _auth_ok(session_token, row["token_hash"]):
        raise AuthError(f"Invalid session_token for '{name}'.")


def record_monitor_heartbeat(name: str) -> bool:
    """Mark that a live wake source (dlb-monitor) is watching `name`'s inbox.

    Called by dlb-monitor on its poll loop. Deliberately separate from
    last_seen: last_seen says the agent DID something recently, the heartbeat
    says something WILL wake it when mail arrives. list_threads turns this into
    `wakeable`, and send() reports it back to the sender.

    No token: the monitor is a separate process that only watches the store.
    Same trust boundary as the rest of DLB (cooperating agents, one OS user).
    Returns False if `name` is not registered (nothing to mark).
    """
    init_schema()
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE agents SET monitor_heartbeat_ms = ? WHERE name = ?", (_now_ms(), name)
        )
        return cur.rowcount > 0


def set_receipts(name: str, session_token: str, enabled: bool) -> dict:
    """Opt `name` in or out of auto-generated read receipts for tasks it sends.

    Receipts default on (see read_receipts_enabled for the global switch). An
    agent that tracks its tasks with get_task_status, or that coordinates many
    workers, can turn them off so its inbox only holds real replies.
    """
    init_schema()
    if not isinstance(enabled, bool):
        raise DLBError("enabled must be a boolean")
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _require_auth(conn, name, session_token, "set receipts")
            conn.execute(
                "UPDATE agents SET receipts_opt_out = ? WHERE name = ?",
                (0 if enabled else 1, name),
            )
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    return {"name": name, "receipts": enabled}


def _check_claim_key(key: str) -> None:
    if not key or not isinstance(key, str):
        raise DLBError("key must be a non-empty string")
    _check_field(key, "key")


def _claim_dict(row: sqlite3.Row) -> dict:
    return {
        "key": row["key"],
        "holder": row["holder"],
        "claimed_at": _ms_to_dt(row["claimed_at_ms"]).isoformat(),  # type: ignore[union-attr]
        "expires_at": _ms_to_dt(row["expires_at_ms"]).isoformat(),  # type: ignore[union-attr]
        "note": row["note"],
    }


def claim(
    key: str,
    name: str,
    session_token: str,
    ttl_seconds: int = DEFAULT_CLAIM_TTL_SECONDS,
    note: str | None = None,
) -> dict:
    """Atomically claim `key` (a task id, a file, a PR: any string) for `name`.

    One BEGIN IMMEDIATE transaction does the check and the write, so two agents
    racing for the same key cannot both win: exactly one gets claimed=True, the
    other gets claimed=False plus the winner's name. This replaces the
    "list_threads, see nobody on it, announce I'm taking it" pattern, which
    lets two agents start the same work.

    - Calling again while you hold it renews the TTL (heartbeat it on long work).
    - An expired claim is free: the next caller takes it over
      (`took_over_from` names the previous holder).
    - unregister() releases every claim the name holds.
    """
    init_schema()
    _check_claim_key(key)
    _check_field(note, "note")
    if (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, int)
        or not 1 <= ttl_seconds <= MAX_CLAIM_TTL_SECONDS
    ):
        raise DLBError(f"ttl_seconds must be an integer between 1 and {MAX_CLAIM_TTL_SECONDS}")
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _require_auth(conn, name, session_token, "claim")
            now_ms = _now_ms()
            row = conn.execute("SELECT * FROM claims WHERE key = ?", (key,)).fetchone()
            if row is not None and row["holder"] != name and row["expires_at_ms"] > now_ms:
                conn.execute("COMMIT")
                return {"claimed": False, **_claim_dict(row)}
            renewed = row is not None and row["holder"] == name
            took_over_from = row["holder"] if row is not None and not renewed else None
            claimed_at_ms = row["claimed_at_ms"] if renewed else now_ms
            conn.execute(
                "INSERT INTO claims (key, holder, claimed_at_ms, expires_at_ms, note) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET holder = excluded.holder, "
                "claimed_at_ms = excluded.claimed_at_ms, expires_at_ms = excluded.expires_at_ms, "
                "note = excluded.note",
                (key, name, claimed_at_ms, now_ms + ttl_seconds * 1000, note),
            )
            new_row = conn.execute("SELECT * FROM claims WHERE key = ?", (key,)).fetchone()
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    return {
        "claimed": True,
        "renewed": renewed,
        "took_over_from": took_over_from,
        **_claim_dict(new_row),
    }


def release(key: str, name: str, session_token: str) -> dict:
    """Release a claim `name` holds. Releasing a key you don't hold is a no-op
    (released=False) that reports the current holder, if any."""
    init_schema()
    _check_claim_key(key)
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _require_auth(conn, name, session_token, "release")
            cur = conn.execute("DELETE FROM claims WHERE key = ? AND holder = ?", (key, name))
            released = cur.rowcount > 0
            row = conn.execute("SELECT holder FROM claims WHERE key = ?", (key,)).fetchone()
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    return {"key": key, "released": released, "holder": row["holder"] if row else None}


def list_claims(prefix: str | None = None) -> list[dict]:
    """Live (unexpired) claims, oldest first, optionally limited to keys that
    start with `prefix`. No auth: who holds what is coordination metadata."""
    init_schema()
    _check_field(prefix, "prefix")
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_ms = _now_ms()
            conn.execute("DELETE FROM claims WHERE expires_at_ms <= ?", (now_ms,))
            if prefix:
                rows = conn.execute(
                    "SELECT * FROM claims WHERE substr(key, 1, ?) = ? ORDER BY claimed_at_ms",
                    (len(prefix), prefix),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM claims ORDER BY claimed_at_ms").fetchall()
            conn.execute("COMMIT")
        except Exception:
            with suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    return [_claim_dict(r) for r in rows]


def dispatch_assessment(name: str) -> dict:
    """Will `name` actually see a message soon? Computed at send time so the
    SENDER finds out immediately, instead of waiting on a task nobody is
    awake to pick up.

    Returns registered, wakeable (a dlb-monitor heartbeat inside the wake
    window), status, the ages of last_seen and the heartbeat in seconds, and a
    human `warning` (None when delivery looks fine).
    """
    init_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT last_seen_ms, status, status_detail, monitor_heartbeat_ms "
            "FROM agents WHERE name = ?",
            (name,),
        ).fetchone()
    if row is None:
        return {
            "registered": False,
            "wakeable": False,
            "status": None,
            "last_seen_age_seconds": None,
            "monitor_heartbeat_age_seconds": None,
            "warning": (
                f"'{name}' is not registered: the message is queued, but nobody sees it "
                "until a session registers that name."
            ),
        }
    now_ms = _now_ms()
    hb_ms = row["monitor_heartbeat_ms"]
    hb_age = (now_ms - hb_ms) // 1000 if hb_ms is not None else None
    wakeable = hb_age is not None and hb_age <= wake_window_seconds()
    status = row["status"]
    warnings = []
    if not wakeable:
        warnings.append(
            f"'{name}' has no live dlb-monitor, so this will not wake an idle session; "
            "it is seen on the recipient's next prompt."
        )
    if status in ("done", "blocked"):
        detail = f" ({row['status_detail']})" if row["status_detail"] else ""
        warnings.append(f"'{name}' last reported status '{status}'{detail}.")
    return {
        "registered": True,
        "wakeable": wakeable,
        "status": status,
        "last_seen_age_seconds": (now_ms - int(row["last_seen_ms"])) // 1000,
        "monitor_heartbeat_age_seconds": hb_age,
        "warning": " ".join(warnings) or None,
    }

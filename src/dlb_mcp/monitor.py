"""dlb-monitor — push-like wake source for the Claude Code Monitor tool.

Polls the DLB SQLite store and emits one stdout line per NEW message
(newest first within a poll tick). Each line is in turn delivered to
the LLM as a notification by the `Monitor` tool, effectively turning
the request-response MCP into a push channel without any new server,
daemon, or socket.

Usage from inside Claude Code:

    Monitor({
      command: "dlb-monitor --name alpha",
      description: "DLB inbox: alpha",
      persistent: true
    })

Output format (one event per line, line-buffered so each is delivered
immediately):

    2026-06-30T21:30:14Z bravo: "ping — can you look at the reskin route?"

The watermark is taken from MAX(messages.id) for the target recipient
at startup, so pre-existing mail is intentionally not re-surfaced (that
is the SessionStart hook's responsibility — re-surfacing here would
double-notify on every relaunch).

Filters:
  --include-senders bob,carol   only notify on messages from these names
  --exclude-senders bot,system  drop messages from these names
                                 (--include and --exclude are mutually
                                 exclusive)

Process model: pure poller. Exits on Ctrl-C (Monitor stops it cleanly
on session end). All errors are logged to stderr and the loop continues
— a single SQLite hiccup must not kill the wake source.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from . import store

DEFAULT_INTERVAL_SECONDS = 2.0
DEFAULT_BODY_PREVIEW_LEN = 80
# How long to back off after an unexpected (non-sqlite, non-KeyboardInterrupt)
# error in the poll loop. We can't distinguish "transient I/O hiccup" from
# "broken environment", so the backoff is a balance — long enough to avoid a
# tight crashloop, short enough that the user notices things are wrong within
# a reasonable window.
LOOP_ERROR_BACKOFF_SECONDS = 5.0


# Any C0 control char (incl. newline, CR, tab) or DEL. Each stdout line the
# monitor prints is a distinct Monitor wake event, so a control char in ANY
# interpolated field — most dangerously the sender label — could forge extra
# events or corrupt the line. store.py now rejects these at write time, but the
# monitor reads raw SQLite (and may see legacy pre-fix rows), so it sanitizes
# again at the sink. Defense in depth: never trust the stored value here.
_CONTROL_CHARS_RE = re.compile("[\x00-\x1f\x7f-\x9f\u2028\u2029]")


def _sanitize_line(s: str) -> str:
    """Collapse control characters to single spaces so a value can't break out
    of its one-line event slot."""
    return _CONTROL_CHARS_RE.sub(" ", s)


def _format_event(
    sent_at_ms: int,
    sender: str,
    subject: str | None,
    body: str,
    preview_len: int = DEFAULT_BODY_PREVIEW_LEN,
    headline: str | None = None,
) -> str:
    """One-line, line-buffered event format.

    The leading ISO timestamp + space is a deliberate readability concession
    — Claude Code's Monitor shows the line verbatim in the notification,
    and a date prefix makes it obvious when the wake fired.

    If the sender set a `headline`, it is shown UNTRUNCATED (that is the whole
    point of the field — a machine-parseable status you can read without
    opening the message). Otherwise we fall back to a truncated subject/body
    snippet.

    Every interpolated field is control-char-sanitized (_sanitize_line) so a
    newline in `sender`, `headline`, `subject`, or `body` cannot forge a second
    event line.
    """
    ts = datetime.fromtimestamp(sent_at_ms / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    sender = _sanitize_line(sender).strip()
    if headline:
        snippet = _sanitize_line(headline).strip()
    else:
        snippet = _sanitize_line(subject if subject else body).strip()
        if len(snippet) > preview_len:
            snippet = snippet[: preview_len - 1] + "…"
    return f'{ts} {sender}: "{snippet}"'


def _baseline_max_id(db_path: Path, recipient: str) -> int:
    """Highest message.id currently in the recipient's inbox. Returns 0 on
    empty/missing DB so the first real message lands above it."""
    if not db_path.exists():
        return 0
    try:
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        try:
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM messages WHERE recipient_name = ?",
                (recipient,),
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()
    except sqlite3.Error as e:
        print(f"dlb-monitor: warning: baseline query failed: {e}", file=sys.stderr)
        return 0


def _fetch_new(
    db_path: Path,
    recipient: str,
    since_id: int,
) -> list[tuple[int, int, str, str | None, str, str | None, str | None]]:
    """Return (id, sent_at_ms, sender_name, subject, body, headline, msg_type) for any
    message in `recipient`'s inbox with id > since_id, oldest first so events
    fire in arrival order. Empty list on error (logged, loop continues).

    `headline` is selected tolerantly — a pre-v4 store without the column
    falls back to None rather than erroring. `msg_type` (present since v0.3.0)
    lets the caller suppress auto-generated receipt acks."""
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        try:
            has_headline = any(
                r[1] == "headline" for r in conn.execute("PRAGMA table_info(messages)")
            )
            col = "headline" if has_headline else "NULL AS headline"
            has_msg_type = any(
                r[1] == "msg_type" for r in conn.execute("PRAGMA table_info(messages)")
            )
            mtcol = "msg_type" if has_msg_type else "NULL AS msg_type"
            rows = conn.execute(
                f"SELECT id, sent_at_ms, sender_name, subject, body, {col}, {mtcol} FROM messages "
                "WHERE recipient_name = ? AND id > ? ORDER BY id ASC",
                (recipient, since_id),
            ).fetchall()
            return [(int(r[0]), int(r[1]), r[2], r[3], r[4], r[5], r[6]) for r in rows]
        finally:
            conn.close()
    except sqlite3.Error as e:
        print(f"dlb-monitor: warning: fetch failed: {e}", file=sys.stderr)
        return []


def _poll_iteration(
    db_path: Path,
    name: str,
    seen_max_id: int,
    include_senders: set[str] | None,
    exclude_senders: set[str] | None,
    emit_receipts: bool,
) -> int:
    """One polling tick. Returns the updated watermark.

    Extracted from run() so the loop body is unit-testable in isolation
    and the outer loop can wrap it with crash-resilience without ballooning
    in size.
    """
    new_msgs = _fetch_new(db_path, name, seen_max_id)
    for msg_id, sent_ms, sender, subject, body, headline, msg_type in new_msgs:
        filtered = (
            (include_senders is not None and sender not in include_senders)
            or (exclude_senders is not None and sender in exclude_senders)
            # Auto-generated read-receipts are machine acks — don't wake a turn
            # for them by default. They still sit in the inbox to be read.
            or (msg_type == "receipt" and not emit_receipts)
        )
        if not filtered:
            line = _format_event(sent_ms, sender, subject, body, headline=headline)
            print(line, flush=True)
        # Always advance watermark — filtered messages still happened.
        seen_max_id = max(seen_max_id, msg_id)
    return seen_max_id


def run(
    name: str,
    interval_seconds: float,
    include_senders: set[str] | None,
    exclude_senders: set[str] | None,
    emit_receipts: bool = False,
    stop_event: threading.Event | None = None,
) -> int:
    """Main poll loop. Returns exit code (0 on clean Ctrl-C / stop).

    Resilience contract: the loop MUST NOT die on a transient error.
    `_fetch_new` already swallows `sqlite3.Error` and returns []. This
    outer try/except catches anything else — OSError on stdout, encoding
    bugs, memory blips, third-party-side weirdness — logs it to stderr,
    backs off, and resumes. The only paths out are KeyboardInterrupt
    (Ctrl-C → exit 0) and a real process-kill (SIGKILL → uncatchable; the
    Monitor tool reports the exit code so the LLM can re-launch).

    `stop_event` is an optional cooperative shutdown signal. When supplied,
    the loop returns 0 as soon as it is set and its inter-tick wait becomes
    interruptible (`Event.wait` instead of `time.sleep`), so a caller that
    embeds the monitor in a thread can stop AND join it deterministically
    instead of leaking an unkillable daemon. `main()` (the CLI path) passes
    no event, so the default behavior — infinite loop until Ctrl-C/SIGKILL —
    is unchanged.
    """
    db_path = store.store_path()
    # init_schema runs migration on a legacy v1 DB so we can read *_ms
    # safely. Safe to call here — it's idempotent.
    try:
        store.init_schema()
    except Exception as e:
        print(f"dlb-monitor: warning: init_schema failed: {e}", file=sys.stderr)

    seen_max_id = _baseline_max_id(db_path, name)
    # Brief diagnostic to stderr so users can see "I started, here's my baseline".
    # stderr does NOT become a Monitor notification; only stdout does.
    print(
        f"dlb-monitor: watching '{name}' in {db_path} (baseline id={seen_max_id}, "
        f"interval={interval_seconds}s)",
        file=sys.stderr,
    )

    def _wait(seconds: float) -> bool:
        """Sleep between ticks. With a stop_event, wait interruptibly and
        report whether a stop was requested; otherwise fall through to the
        plain time.sleep the CLI has always used."""
        if stop_event is not None:
            return stop_event.wait(seconds)
        time.sleep(seconds)
        return False

    try:
        while stop_event is None or not stop_event.is_set():
            try:
                seen_max_id = _poll_iteration(
                    db_path,
                    name,
                    seen_max_id,
                    include_senders,
                    exclude_senders,
                    emit_receipts,
                )
                if _wait(interval_seconds):
                    break
            except KeyboardInterrupt:
                raise  # forwarded to the outer handler for clean exit
            except Exception as e:
                # Hard guarantee: a non-fatal error in one iteration must not
                # take down the wake source. Log loud enough that the user
                # notices (this stderr line goes to the Monitor task's
                # output file but NOT to the conversation as a notification —
                # which is the right asymmetry: we want the loop to keep
                # running silently in normal failure, not to spam every
                # transient hiccup into the LLM's context).
                print(
                    f"dlb-monitor: warning: poll iteration failed "
                    f"({type(e).__name__}: {e}); backing off "
                    f"{LOOP_ERROR_BACKOFF_SECONDS}s and continuing",
                    file=sys.stderr,
                )
                if _wait(LOOP_ERROR_BACKOFF_SECONDS):
                    break
        # Fell out of the loop because stop_event was set (cooperative stop).
        print("dlb-monitor: stopping (stop requested)", file=sys.stderr)
        return 0
    except KeyboardInterrupt:
        print("dlb-monitor: stopping (KeyboardInterrupt)", file=sys.stderr)
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dlb-monitor",
        description=(
            "Emit one stdout line per new DLB message for `--name`. "
            "Designed to be wrapped by Claude Code's Monitor tool to "
            "convert DLB's polling model into a push notification."
        ),
    )
    parser.add_argument("--name", required=True, help="DLB inbox name to watch.")
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("DLB_MONITOR_INTERVAL", DEFAULT_INTERVAL_SECONDS)),
        help=f"Poll interval in seconds (default {DEFAULT_INTERVAL_SECONDS}, "
        "overridable via DLB_MONITOR_INTERVAL).",
    )
    parser.add_argument(
        "--include-senders",
        default=None,
        help=(
            "Comma-separated allowlist of sender names. Mutually exclusive with --exclude-senders."
        ),
    )
    parser.add_argument(
        "--exclude-senders",
        default=None,
        help=(
            "Comma-separated denylist of sender names. Mutually exclusive with --include-senders."
        ),
    )
    parser.add_argument(
        "--emit-receipts",
        action="store_true",
        help=(
            "Emit an event for auto-generated read-receipt messages too. By "
            "default receipts are suppressed so they don't wake a turn (they "
            "still sit in the inbox to be read)."
        ),
    )
    args = parser.parse_args(argv)

    if args.include_senders and args.exclude_senders:
        parser.error("--include-senders and --exclude-senders are mutually exclusive")

    include = (
        {s.strip() for s in args.include_senders.split(",") if s.strip()}
        if args.include_senders
        else None
    )
    exclude = (
        {s.strip() for s in args.exclude_senders.split(",") if s.strip()}
        if args.exclude_senders
        else None
    )

    if args.interval <= 0:
        parser.error("--interval must be positive")

    return run(
        name=args.name,
        interval_seconds=args.interval,
        include_senders=include,
        exclude_senders=exclude,
        emit_receipts=args.emit_receipts,
    )


if __name__ == "__main__":
    sys.exit(main())

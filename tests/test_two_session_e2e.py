"""End-to-end: two concurrent MCP sessions sharing one SQLite store,
plus a `dlb-monitor` subprocess proving the push-via-Monitor wake path.

This is the automated equivalent of the manual "open two Claude Code
terminals and see them coordinate" smoke test. Runs in ~5 seconds; wired
into the standard `pytest` invocation so every CI run confirms the
whole stack still works together.

What's covered here that isolated unit tests can't cover:
    * Real subprocess-to-subprocess coordination via the shared SQLite
      file (WAL mode, no daemon, cross-process visibility).
    * The MCP transport (stdio JSON-RPC + FastMCP tool dispatch).
    * The session_token-bound `from_` on `send`, proves an
      authenticated write in one process is trusted by a read in another.
    * `dlb-monitor` actually emits stdout lines when new mail arrives.
      Without this test, the only proof that Monitor-wrapped monitor
      works is manual inspection.

What's NOT covered (out of scope):
    * The Claude Code `Monitor` tool wrapper itself (that's Claude's code,
      not ours; we only need to prove `dlb-monitor` produces stdout events).
    * PTY-based launcher wake, belongs in dlb-launcher's test suite.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

# ── MCP session subprocess helpers ───────────────────────────────────────────


def _send_rpc(proc: subprocess.Popen, messages: list[dict]) -> list[dict]:
    """Line-delimited JSON-RPC. Blocking on responses for each id."""
    assert proc.stdin is not None
    for m in messages:
        proc.stdin.write(json.dumps(m) + "\n")
        proc.stdin.flush()

    expected = {m["id"] for m in messages if "id" in m}
    responses: list[dict] = []
    deadline = time.time() + 10.0
    assert proc.stdout is not None
    while expected and time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("id") in expected:
            responses.append(obj)
            expected.discard(obj["id"])
    return responses


def _call(server: subprocess.Popen, call_id: int, tool: str, args: dict) -> dict:
    """One tools/call round-trip. Returns the parsed structured result."""
    resp = _send_rpc(
        server,
        [
            {
                "jsonrpc": "2.0",
                "id": call_id,
                "method": "tools/call",
                "params": {"name": tool, "arguments": args},
            }
        ],
    )
    assert resp, f"no response to {tool}"
    r = resp[0]
    if "error" in r:
        return {"_error": r["error"]}
    result = r.get("result", {})
    if "structuredContent" in result:
        return result["structuredContent"]
    content = result.get("content", [])
    if content and content[0].get("type") == "text":
        try:
            return json.loads(content[0]["text"])
        except (json.JSONDecodeError, TypeError):
            return {"_text": content[0]["text"]}
    return result


@contextlib.contextmanager
def _mcp_session(env: dict[str, str]) -> Iterator[subprocess.Popen]:
    """Spawn one MCP server subprocess, complete the initialize handshake,
    yield the process. Torn down cleanly on exit."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "dlb_mcp"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1,
    )
    try:
        # MCP initialize dance
        init_resp = _send_rpc(
            proc,
            [
                {
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "pytest-e2e", "version": "0.1"},
                    },
                }
            ],
        )
        assert init_resp, "server did not respond to initialize"
        assert proc.stdin is not None
        proc.stdin.write(
            json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"
        )
        proc.stdin.flush()

        yield proc
    finally:
        with contextlib.suppress(Exception):
            if proc.stdin:
                proc.stdin.close()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _unwrap_list(v: object) -> list:
    """FastMCP wraps list returns as {'result': [...]} in structuredContent."""
    if isinstance(v, dict) and "result" in v:
        return v["result"]  # type: ignore[return-value]
    assert isinstance(v, list), f"expected list, got {type(v).__name__}: {v!r}"
    return v


# ── Test 1: two-session message exchange ────────────────────────────────────


def test_two_sessions_exchange_authenticated_messages(tmp_path: Path) -> None:
    """The core two-thread coordination case. Alpha and Bravo run in
    separate MCP server processes, share one SQLite store, exchange
    messages both ways with token-bound `from_`. This IS the use case
    the whole DLB + launcher effort was built for."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(tmp_path / "shared.sqlite3")

    with _mcp_session(env) as alpha_srv, _mcp_session(env) as bravo_srv:
        alpha = _call(alpha_srv, 1, "register", {"name": "alpha"})
        bravo = _call(bravo_srv, 1, "register", {"name": "bravo"})
        assert alpha["name"] == "alpha"
        assert bravo["name"] == "bravo"
        alpha_tok = alpha["session_token"]
        bravo_tok = bravo["session_token"]

        # Alpha → Bravo, authenticated → sender_name bound to "alpha"
        sent1 = _call(
            alpha_srv,
            2,
            "send",
            {
                "to": "bravo",
                "body": "ping from alpha",
                "session_token": alpha_tok,
            },
        )
        assert sent1["recipient_name"] == "bravo"
        assert sent1["sender_name"] == "alpha", "token-bound send should override anonymous default"

        # Bravo reads
        bravo_inbox = _unwrap_list(
            _call(bravo_srv, 2, "read", {"name": "bravo", "session_token": bravo_tok})
        )
        assert len(bravo_inbox) == 1
        assert bravo_inbox[0]["body"] == "ping from alpha"
        assert bravo_inbox[0]["sender_name"] == "alpha"

        # Bravo replies to Alpha, also authenticated
        _call(
            bravo_srv,
            3,
            "send",
            {
                "to": "alpha",
                "body": "pong from bravo",
                "session_token": bravo_tok,
            },
        )

        # Alpha reads the reply
        alpha_inbox = _unwrap_list(
            _call(alpha_srv, 3, "read", {"name": "alpha", "session_token": alpha_tok})
        )
        assert len(alpha_inbox) == 1
        assert alpha_inbox[0]["body"] == "pong from bravo"
        assert alpha_inbox[0]["sender_name"] == "bravo"

        # Cross-visibility check: list_threads from either side sees both agents
        threads = _unwrap_list(_call(alpha_srv, 4, "list_threads", {}))
        names = {t["name"] for t in threads}
        assert names == {"alpha", "bravo"}


def test_send_from_third_party_reaches_registered_inbox(tmp_path: Path) -> None:
    """Anonymous/unauthenticated send from a third session still lands in
    the target inbox. Covers the dead-letter path across processes."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(tmp_path / "shared.sqlite3")

    with _mcp_session(env) as alpha_srv, _mcp_session(env) as sender_srv:
        alpha = _call(alpha_srv, 1, "register", {"name": "alpha"})
        # Anonymous send from a session that doesn't hold alpha's token
        _call(sender_srv, 1, "send", {"to": "alpha", "body": "hi", "from_": "eve"})
        inbox = _unwrap_list(
            _call(
                alpha_srv,
                2,
                "read",
                {"name": "alpha", "session_token": alpha["session_token"]},
            )
        )
        assert len(inbox) == 1
        assert inbox[0]["body"] == "hi"
        # Unauthenticated from_ is a free-text label, preserved as-is
        assert inbox[0]["sender_name"] == "eve"


# ── Test 2: dlb-monitor emits wake events ───────────────────────────────────


def _read_line_with_timeout(stream, timeout_s: float) -> str | None:
    """Read one line from a stream with a wall-clock timeout. Returns None
    on timeout. Uses select on the underlying fd (POSIX only; the whole
    file is Unix-only anyway due to PTY-adjacent tests)."""
    import select

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        remaining = deadline - time.time()
        r, _, _ = select.select([stream], [], [], min(0.2, remaining))
        if stream in r:
            line = stream.readline()
            if line:
                return line
            return None  # EOF
    return None


def test_dlb_monitor_emits_wake_line_on_new_message(tmp_path: Path) -> None:
    """Push-via-Monitor smoke: register a name in one session, spawn
    dlb-monitor watching that name, drop a message in from another
    session, verify dlb-monitor's stdout contains the wake line.

    This is the piece the Claude Code Monitor tool relies on: each
    stdout line becomes an LLM notification. If this test passes, the
    contract with Monitor holds."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(tmp_path / "shared.sqlite3")

    # 1. Register alpha (so the DB has a v2 schema before dlb-monitor runs)
    with _mcp_session(env) as srv:
        alpha = _call(srv, 1, "register", {"name": "alpha"})
        assert alpha["session_token"]

    # 2. Spawn dlb-monitor watching alpha with a fast poll interval
    monitor = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "dlb_mcp.monitor",
            "--name",
            "alpha",
            "--interval",
            "0.2",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1,
    )
    try:
        # Give the monitor a moment to take its baseline snapshot
        time.sleep(0.5)

        # 3. From a fresh session, send a message to alpha
        with _mcp_session(env) as sender_srv:
            _call(
                sender_srv,
                1,
                "send",
                {"to": "alpha", "body": "wake up alpha", "from_": "bravo"},
            )

        # 4. Read from monitor's stdout, must see the wake line within
        #    a few seconds. Poll interval is 200ms; typical latency <500ms.
        assert monitor.stdout is not None
        line = _read_line_with_timeout(monitor.stdout, timeout_s=5.0)
        assert line is not None, "dlb-monitor produced no stdout; wake did not fire"
        # Sender + body preview appear in the format we defined
        assert "bravo" in line, f"expected sender 'bravo' in wake line, got: {line!r}"
        assert "wake up alpha" in line, (
            f"expected body preview 'wake up alpha' in wake line, got: {line!r}"
        )
    finally:
        monitor.terminate()
        try:
            monitor.wait(timeout=3)
        except subprocess.TimeoutExpired:
            monitor.kill()


def test_dlb_monitor_ignores_pre_existing_messages(tmp_path: Path) -> None:
    """Baseline contract: messages present at launcher startup are NOT
    re-notified. Without this, every relaunch would spam the LLM with
    already-read mail."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(tmp_path / "shared.sqlite3")

    # 1. Register alpha AND pre-populate the inbox before starting monitor
    with _mcp_session(env) as srv:
        alpha = _call(srv, 1, "register", {"name": "alpha"})
        assert alpha["session_token"]
        _call(srv, 2, "send", {"to": "alpha", "body": "pre-existing message"})

    # 2. Start dlb-monitor; it must NOT emit anything for the pre-existing
    monitor = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "dlb_mcp.monitor",
            "--name",
            "alpha",
            "--interval",
            "0.2",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1,
    )
    try:
        assert monitor.stdout is not None
        # Wait longer than several poll intervals, should still be silent
        line = _read_line_with_timeout(monitor.stdout, timeout_s=1.5)
        assert line is None, (
            f"dlb-monitor emitted a wake for pre-existing mail (should ignore): {line!r}"
        )
    finally:
        monitor.terminate()
        try:
            monitor.wait(timeout=3)
        except subprocess.TimeoutExpired:
            monitor.kill()


# ── Test 3: two-session + monitor together (the whole use case) ──────────────


def test_end_to_end_two_sessions_with_monitor_wake(tmp_path: Path) -> None:
    """The full picture: two MCP sessions, one has a Monitor watching its
    own inbox, mail sent by the other triggers the wake. This is the
    documented reason DLB + dlb-monitor exists; validating it end-to-end
    prevents regressions from silently breaking the primary use case."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(tmp_path / "shared.sqlite3")

    # Register both agents in one shot so the monitor has a stable name
    with _mcp_session(env) as alpha_srv:
        alpha = _call(alpha_srv, 1, "register", {"name": "alpha"})
        alpha_tok = alpha["session_token"]

    # Spawn Alpha's monitor
    monitor = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "dlb_mcp.monitor",
            "--name",
            "alpha",
            "--interval",
            "0.2",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1,
    )
    try:
        time.sleep(0.5)

        # Bravo registers + sends to alpha
        with _mcp_session(env) as bravo_srv:
            bravo = _call(bravo_srv, 1, "register", {"name": "bravo"})
            _call(
                bravo_srv,
                2,
                "send",
                {
                    "to": "alpha",
                    "body": "collab request",
                    "session_token": bravo["session_token"],
                },
            )

        # Alpha's monitor should wake
        assert monitor.stdout is not None
        line = _read_line_with_timeout(monitor.stdout, timeout_s=5.0)
        assert line is not None, "monitor did not wake for cross-session send"
        assert "bravo" in line
        assert "collab request" in line

        # And Alpha's actual read (which is what the real LLM would do on
        # wake) sees the message with authenticated sender
        with _mcp_session(env) as alpha_srv:
            inbox = _unwrap_list(
                _call(
                    alpha_srv,
                    2,
                    "read",
                    {"name": "alpha", "session_token": alpha_tok},
                )
            )
            assert len(inbox) == 1
            assert inbox[0]["sender_name"] == "bravo"
    finally:
        monitor.terminate()
        try:
            monitor.wait(timeout=3)
        except subprocess.TimeoutExpired:
            monitor.kill()


# Skip whole module on non-POSIX (subprocess semantics + select on the
# monitor stdout are Unix-specific; Windows has different quirks).
if sys.platform == "win32":
    pytest.skip("E2E subprocess tests are POSIX-only", allow_module_level=True)

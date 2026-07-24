"""End-to-end integration test: spawn the MCP server via stdio,
drive it with raw JSON-RPC, verify all 6 tools work over the wire.

This test exercises the same code path Claude Code uses, proves the
server actually negotiates MCP, registers its tools, and responds to
tool calls without us hand-waving the transport.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest


def _send_rpc(proc: subprocess.Popen, messages: list[dict]) -> list[dict]:
    """Write JSON-RPC messages line-delimited; read responses for each id."""
    for m in messages:
        assert proc.stdin is not None
        proc.stdin.write(json.dumps(m) + "\n")
        proc.stdin.flush()

    expected_ids = {m["id"] for m in messages if "id" in m}
    responses: list[dict] = []
    deadline = time.time() + 10.0
    assert proc.stdout is not None
    while expected_ids and time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("id") in expected_ids:
            responses.append(obj)
            expected_ids.discard(obj["id"])
    return responses


@pytest.fixture()
def server(tmp_path: Path) -> subprocess.Popen:
    """Spawn dlb-mcp serve-stdio as a subprocess, isolated SQLite store."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(tmp_path / "store.sqlite3")
    proc = subprocess.Popen(
        [sys.executable, "-m", "dlb_mcp"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1,
    )
    # MCP handshake
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
                    "clientInfo": {"name": "pytest", "version": "0.1"},
                },
            }
        ],
    )
    assert init_resp, "server did not respond to initialize"
    # Send initialized notification
    assert proc.stdin is not None
    proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
    proc.stdin.flush()
    yield proc
    proc.stdin.close()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _call(server: subprocess.Popen, call_id: int, tool: str, args: dict) -> dict:
    """Call one tool, return the parsed result body."""
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
    # FastMCP returns content + structuredContent; prefer structured
    result = r.get("result", {})
    if "structuredContent" in result:
        return result["structuredContent"]
    # Fallback: parse the text content as JSON
    content = result.get("content", [])
    if content and content[0].get("type") == "text":
        text = content[0]["text"]
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return {"_text": text}
    return result


def test_full_happy_path(server: subprocess.Popen) -> None:
    """Tool catalog → register → send → read → ack → unregister."""
    # 1. List tools: the full DLB tool catalog (9 as of v0.3.1).
    tools_resp = _send_rpc(server, [{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}])
    assert tools_resp
    tools = tools_resp[0]["result"]["tools"]
    names = {t["name"] for t in tools}
    assert names == {
        "register",
        "recover_token",
        "set_status",
        "list_threads",
        "send",
        "read",
        "ack",
        "unregister",
        "update_status",
        "get_task_status",
    }, f"unexpected tool set: {names}"

    # 2. Register alpha
    reg = _call(server, 2, "register", {"name": "alpha", "working_on": "testing"})
    assert reg["name"] == "alpha"
    token = reg["session_token"]
    assert token

    # 3. Send a message to alpha from anonymous
    sent = _call(server, 3, "send", {"to": "alpha", "body": "hello alpha"})
    assert sent["recipient_name"] == "alpha"
    assert sent["sender_name"] == "anonymous"
    msg_id = sent["id"]

    # 4. Read alpha's inbox with the token, should find the message
    inbox = _call(
        server,
        4,
        "read",
        {"name": "alpha", "session_token": token, "unread_only": True},
    )
    # FastMCP wraps list returns; structured form is a dict containing "result"
    if isinstance(inbox, dict) and "result" in inbox:
        inbox = inbox["result"]
    assert isinstance(inbox, list)
    assert len(inbox) == 1
    assert inbox[0]["body"] == "hello alpha"

    # 5. Ack the message
    ack = _call(server, 5, "ack", {"message_id": msg_id, "session_token": token})
    assert ack["ok"] is True

    # 6. Unregister alpha
    out = _call(server, 6, "unregister", {"name": "alpha", "session_token": token})
    assert out["ok"] is True


def test_send_to_nonexistent_succeeds(server: subprocess.Popen) -> None:
    """Differentiator: send to unknown name persists; read finds it."""
    sent = _call(server, 1, "send", {"to": "ghost", "body": "anyone there?"})
    assert sent["recipient_name"] == "ghost"
    assert sent["id"] > 0

    # Read unregistered name without a token, must work
    inbox = _call(server, 2, "read", {"name": "ghost"})
    if isinstance(inbox, dict) and "result" in inbox:
        inbox = inbox["result"]
    assert len(inbox) == 1
    assert inbox[0]["body"] == "anyone there?"

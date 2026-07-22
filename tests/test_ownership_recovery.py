"""Design 2 — per-process (per-session) ownership gate on recover_token.

Closes Issue 1's tool-API leak: recover_token used to hand ANY name's token to
ANY caller. Now the dlb-mcp server process only returns a token for a name that
THIS process registered. Because the process survives context compaction (but
not a full restart), this gives:

    same session, post-compaction  → recover works
    a different session            → refused
    a brand-new process (restart)  → refused; reclaim via register(force=True)

Two layers of test:
  * unit — direct calls to the server tool functions, resetting the process
    ownership set between cases to simulate distinct sessions (fast).
  * e2e  — two REAL dlb-mcp processes over stdio JSON-RPC sharing one store,
    proving the boundary end to end across actual OS processes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from dlb_mcp import server
from dlb_mcp.store import AuthError

# ── Unit layer ───────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_ownership() -> Iterator[None]:
    """Each test starts with an empty owned-set (the process is shared across
    the whole pytest run; a real session gets a fresh process)."""
    server._reset_owned_names_for_tests()
    yield
    server._reset_owned_names_for_tests()


def test_owner_recovers_its_own_token() -> None:
    reg = server.register("alpha", working_on="x")
    rec = server.recover_token("alpha")
    assert rec is not None
    assert rec["session_token"] == reg["session_token"]


def test_recovery_refused_for_a_name_this_session_did_not_register() -> None:
    # Session A registers alpha (store row + ownership).
    server.register("alpha")
    # Simulate a DIFFERENT session: same shared store, but a fresh process has
    # an empty owned-set.
    server._reset_owned_names_for_tests()
    with pytest.raises(AuthError, match="did not register"):
        server.recover_token("alpha")


def test_unknown_name_still_returns_none() -> None:
    # Never registered anywhere → None (unchanged contract), not an error.
    assert server.recover_token("ghost") is None


def test_force_reclaim_regains_ownership(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_TAKEOVER_AFTER_SECONDS", "0")  # instantly stale
    server.register("alpha")
    server._reset_owned_names_for_tests()  # new process: no ownership
    with pytest.raises(AuthError):
        server.recover_token("alpha")
    # Legitimately reclaim the (now-stale) name; ownership is regained.
    server.register("alpha", force=True)
    rec = server.recover_token("alpha")
    assert rec is not None and rec["session_token"]


def test_unregister_drops_ownership() -> None:
    reg = server.register("alpha")
    assert server.recover_token("alpha") is not None
    server.unregister("alpha", reg["session_token"])
    # Name is gone from the store → None; and ownership was dropped.
    assert server.recover_token("alpha") is None
    assert not server._owns("alpha")


# ── E2E layer: two real processes over stdio ─────────────────────────────────


def _send_rpc(proc: subprocess.Popen, messages: list[dict]) -> list[dict]:
    assert proc.stdin is not None
    for m in messages:
        proc.stdin.write(json.dumps(m) + "\n")
        proc.stdin.flush()
    expected = {m["id"] for m in messages if "id" in m}
    out: list[dict] = []
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
            out.append(obj)
            expected.discard(obj["id"])
    return out


@contextmanager
def _session(store_path: Path) -> Iterator[subprocess.Popen]:
    """Spawn one dlb-mcp process (= one session) bound to a shared store."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(store_path)
    proc = subprocess.Popen(
        [sys.executable, "-m", "dlb_mcp"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1,
    )
    _send_rpc(
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
    assert proc.stdin is not None
    proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
    proc.stdin.flush()
    try:
        yield proc
    finally:
        proc.stdin.close()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _call(proc: subprocess.Popen, call_id: int, tool: str, args: dict) -> dict:
    resp = _send_rpc(
        proc,
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
    if "error" in r:  # JSON-RPC protocol error
        return {"_error": r["error"]}
    result = r.get("result", {})
    if result.get("isError"):  # tool raised (FastMCP surfaces as isError result)
        content = result.get("content", [])
        text = content[0]["text"] if content and content[0].get("type") == "text" else ""
        return {"_isError": True, "_text": text}
    sc = result.get("structuredContent")
    if sc is not None:
        # FastMCP wraps a non-object/Optional return (recover_token is dict|None)
        # under a single "result" key; peel it so callers see the payload.
        if isinstance(sc, dict) and set(sc.keys()) == {"result"}:
            inner = sc["result"]
            return inner if isinstance(inner, dict) else {"_value": inner}
        return sc
    content = result.get("content", [])
    if content and content[0].get("type") == "text":
        try:
            return json.loads(content[0]["text"])
        except (json.JSONDecodeError, TypeError):
            return {"_text": content[0]["text"]}
    return result


def test_e2e_owner_recovers_but_foreign_session_is_refused(tmp_path: Path) -> None:
    """The decisive cross-process proof: two real dlb-mcp processes on one
    store. Session A registers `alpha` and can recover its token; session B
    (a distinct process) cannot recover `alpha`'s token."""
    shared = tmp_path / "shared.sqlite3"

    with _session(shared) as a, _session(shared) as b:
        reg = _call(a, 1, "register", {"name": "alpha", "working_on": "owns it"})
        token = reg["session_token"]
        assert token

        # Same session recovers its own token (survives compaction in real use).
        rec_a = _call(a, 2, "recover_token", {"name": "alpha"})
        assert rec_a.get("session_token") == token

        # A DIFFERENT process must NOT be able to recover alpha's token.
        rec_b = _call(b, 3, "recover_token", {"name": "alpha"})
        assert "_error" in rec_b or rec_b.get("_isError"), (
            f"foreign session recovered a token it should not have: {rec_b}"
        )

        # And session B genuinely cannot read alpha's inbox with a guessed/empty
        # token — the boundary holds end to end.
        _call(a, 4, "send", {"to": "alpha", "body": "for the real owner"})
        read_b = _call(b, 5, "read", {"name": "alpha", "session_token": "not-the-token"})
        assert "_error" in read_b or read_b.get("_isError"), (
            "foreign session read alpha's inbox with a bad token"
        )

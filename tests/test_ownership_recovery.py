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

from dlb_mcp import server, store
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
def _session(store_path: Path, session_id: str | None = None) -> Iterator[subprocess.Popen]:
    """Spawn one dlb-mcp process (= one session) bound to a shared store.

    Pass session_id to set DLB_SESSION_ID (Design 1) — two processes sharing a
    value simulate a crash-respawn within one harness session."""
    env = os.environ.copy()
    env["DLB_STORE"] = str(store_path)
    env.pop("DLB_SESSION_ID", None)
    if session_id is not None:
        env["DLB_SESSION_ID"] = session_id
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


# ── Design 1: harness-session-id recovery (survives an MCP-server respawn) ────


def test_new_process_same_session_id_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_SESSION_ID", "S1")
    reg = server.register("alpha")
    server._reset_owned_names_for_tests()  # new process: empty owned-set...
    # ...but same harness session (DLB_SESSION_ID still S1) → recover via match.
    # With hashed-at-rest tokens this MINTS a fresh token (the DB has only a hash
    # and this new process never held the original).
    rec = server.recover_token("alpha")
    assert rec is not None
    assert rec["session_token"] and rec["session_token"] != reg["session_token"]  # rotated
    assert server._owns("alpha"), "ownership should be adopted on session-id match"
    # The recovered token actually works.
    assert store.read("alpha", session_token=rec["session_token"]) == []


def test_new_process_different_session_id_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_SESSION_ID", "S1")
    server.register("alpha")
    server._reset_owned_names_for_tests()
    monkeypatch.setenv("DLB_SESSION_ID", "S2")  # a different session
    with pytest.raises(AuthError, match="did not register"):
        server.recover_token("alpha")


def test_no_session_id_uses_ownership_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DLB_SESSION_ID", raising=False)
    server.register("alpha")  # bound session_id is None
    server._reset_owned_names_for_tests()
    with pytest.raises(AuthError):
        server.recover_token("alpha")  # no owned-set, no session id → refused


def test_none_bound_id_never_matches_a_set_caller_id(monkeypatch: pytest.MonkeyPatch) -> None:
    # Guard against None==None style false matches: registered without an id,
    # a later caller WITH an id must not be able to recover.
    monkeypatch.delenv("DLB_SESSION_ID", raising=False)
    server.register("alpha")  # bound None
    server._reset_owned_names_for_tests()
    monkeypatch.setenv("DLB_SESSION_ID", "S1")
    with pytest.raises(AuthError):
        server.recover_token("alpha")


def test_bound_session_id_is_persisted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_SESSION_ID", "S1")
    server.register("alpha")
    assert store.bound_session_id("alpha") == "S1"


def test_e2e_crash_respawn_same_session_recovers(tmp_path: Path) -> None:
    """Design 1 over the wire: process A registers `alpha` under session S1 and
    EXITS (simulating an MCP-server crash). A brand-new process with the SAME
    DLB_SESSION_ID recovers the token with no stale-gate — proving recovery
    rests on the persisted session id, not the (now-empty) in-memory owned-set.
    A process with a DIFFERENT id is refused."""
    shared = tmp_path / "shared.sqlite3"

    with _session(shared, session_id="S1") as a:
        reg = _call(a, 1, "register", {"name": "alpha"})
        token = reg["session_token"]
        assert token
    # `a` has now exited — its process (and owned-set) is gone.

    with _session(shared, session_id="S1") as respawn:
        rec = _call(respawn, 2, "recover_token", {"name": "alpha"})
        new_token = rec.get("session_token")
        # Recovery mints a fresh token (hashed-at-rest); it must be present, work,
        # and differ from the original (which the dead process held).
        assert new_token and new_token != token, "same-session respawn must recover a fresh token"
        got = _call(respawn, 3, "read", {"name": "alpha", "session_token": new_token})
        # read of an empty inbox → [] (wrapped as {"_value": []} by the helper);
        # the point is it AUTHENTICATED (no error), proving the fresh token works.
        assert not (isinstance(got, dict) and ("_error" in got or got.get("_isError"))), (
            f"recovered token should authenticate reads: {got}"
        )

    with _session(shared, session_id="S2") as foreign:
        rej = _call(foreign, 4, "recover_token", {"name": "alpha"})
        assert "_error" in rej or rej.get("_isError"), "different session must be refused"


# ── Persisted-token reclaim after a FULL restart (bypasses the stale-gate) ────


def test_persisted_token_reclaim_after_full_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    """The instant path for the case neither Design 1 nor 2 covers: a full
    restart where the session id rotated AND the prior holder is not yet stale.
    The returning owner reads its token from the sidecar and reclaims via
    prior_token — which matches, so the stale-gate is bypassed."""
    monkeypatch.setenv("DLB_TAKEOVER_AFTER_SECONDS", "1800")  # prior holder stays "live"
    monkeypatch.setenv("DLB_SESSION_ID", "S1")
    reg = server.register("alpha")
    token = reg["session_token"]

    # Full restart: new process (empty owned-set) AND a rotated session id.
    server._reset_owned_names_for_tests()
    monkeypatch.setenv("DLB_SESSION_ID", "S2")

    # Neither auto path works: recover_token refused, and plain force is denied
    # because the prior holder is not stale yet.
    with pytest.raises(AuthError):
        server.recover_token("alpha")
    with pytest.raises(store.TakeoverDenied):
        server.register("alpha", force=True)

    # Persisted-token reclaim: read the token from the sidecar (line 2) and
    # present it. It matches → instant reclaim, no stale-gate wait. The reclaim
    # ROTATES the token (random-token eviction), so the returned token is fresh.
    persisted = store.sidecar_path("alpha").read_text().splitlines()[1]
    assert persisted == token
    reclaimed = server.register("alpha", force=True, prior_token=persisted)
    assert reclaimed["session_token"] != token  # rotated
    # Ownership regained → recover_token works again in this new session.
    assert server._owns("alpha")
    assert server.recover_token("alpha") is not None


def test_recover_hook_documents_both_recovery_paths(capsys: pytest.CaptureFixture[str]) -> None:
    from dlb_mcp import recover_hook

    server.register("alpha")
    recover_hook.main()
    out = capsys.readouterr().out
    assert "recover_token" in out  # compaction path
    assert "force=true" in out and "prior_token" in out  # full-restart reclaim path
    assert "- alpha" in out

"""The upgrade proxy: swap the worker without a restart, keep identity, stay safe.

The proxy runs in this process on os pipes (so tests can fake "a newer version
is installed"); every worker is a real `dlb-mcp worker` subprocess on the
per-test temp store from conftest. Nothing here touches ~/.dlb.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from dlb_mcp import __version__, proxy, store
from dlb_mcp._control import PRIVATE_ID_PREFIX

NEW_VERSION = "9.9.9"


class Client:
    """A fake MCP client driving an in-process Proxy over pipes."""

    def __init__(self, **kwargs: Any) -> None:
        cin_r, cin_w = os.pipe()
        cout_r, cout_w = os.pipe()
        self._w = os.fdopen(cin_w, "wb")
        self._out_w = os.fdopen(cout_w, "wb")
        self._r = os.fdopen(cout_r, "rb")
        self.proxy = proxy.Proxy(os.fdopen(cin_r, "rb"), self._out_w, **kwargs)
        self._thread = threading.Thread(target=self.proxy.run, daemon=True)
        self._thread.start()
        self._q: queue.Queue[dict[str, Any]] = queue.Queue()
        self.notifications: list[dict[str, Any]] = []
        self.raw: list[bytes] = []
        threading.Thread(target=self._read, daemon=True).start()
        self._next = 0

    def _read(self) -> None:
        for line in self._r:
            self.raw.append(line)
            self._q.put(json.loads(line))

    def send(self, obj: dict[str, Any]) -> None:
        self._w.write((json.dumps(obj) + "\n").encode())
        self._w.flush()

    def wait_for(self, msg_id: Any, timeout: float = 20.0) -> dict[str, Any]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = self._q.get(timeout=0.2)
            except queue.Empty:
                continue
            if "method" in msg:
                self.notifications.append(msg)
            elif msg.get("id") == msg_id:
                return msg
        raise AssertionError(f"no response to id {msg_id!r}")

    def request(self, method: str, params: dict[str, Any], msg_id: Any = None) -> dict[str, Any]:
        if msg_id is None:
            self._next += 1
            msg_id = self._next
        self.send({"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params})
        return self.wait_for(msg_id)

    def initialize(self) -> dict[str, Any]:
        resp = self.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "pytest-proxy", "version": "0.1"},
            },
        )
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return resp

    def call(self, tool: str, args: dict[str, Any]) -> Any:
        resp = self.request("tools/call", {"name": tool, "arguments": args})
        assert "error" not in resp, resp
        result = resp["result"]
        if "structuredContent" in result:
            sc = result["structuredContent"]
            return sc.get("result", sc) if set(sc) == {"result"} else sc
        return json.loads(result["content"][0]["text"])

    def close(self) -> None:
        self._w.close()
        self._thread.join(15)
        self._out_w.close()

    @property
    def worker_pid(self) -> int:
        assert self.proxy.worker is not None
        return self.proxy.worker.proc.pid


class FakeInstall:
    """Stands in for _version.installed_version: what is 'on disk' right now."""

    def __init__(self) -> None:
        self.version: str | None = __version__

    def __call__(self) -> str | None:
        return self.version


@pytest.fixture
def install() -> FakeInstall:
    return FakeInstall()


@pytest.fixture
def client(install: FakeInstall, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DLB_SESSION_ID", "sess-proxy-test")
    c = Client(installed_version=install, check_interval=0.0, start_timeout=10.0)
    c.initialize()
    yield c
    c.close()


def test_initialize_advertises_tool_list_changes(install: FakeInstall) -> None:
    c = Client(installed_version=install, check_interval=0.0)
    try:
        resp = c.initialize()
        assert resp["result"]["capabilities"]["tools"]["listChanged"] is True
    finally:
        c.close()


def test_swap_keeps_identity_and_switches_worker(client: Client, install: FakeInstall) -> None:
    reg = client.call("register", {"name": "alpha"})
    token = reg["session_token"]
    old_pid = client.worker_pid

    install.version = NEW_VERSION
    rec = client.call("recover_token", {"name": "alpha"})

    assert client.proxy.swaps == 1
    assert client.worker_pid != old_pid
    # Same token: the owned map crossed over, it was not re-minted.
    assert rec["session_token"] == token
    assert any(n["method"] == "notifications/tools/list_changed" for n in client.notifications)
    # The token still authenticates against the store.
    assert client.call("read", {"name": "alpha", "session_token": token}) == []


def test_no_restart_notice_after_a_successful_swap(client: Client, install: FakeInstall) -> None:
    install.version = NEW_VERSION
    threads = client.call("list_threads", {})
    assert all(t.get("name") != "__dlb_server__" for t in threads)


def test_swap_keeps_session_id(client: Client, install: FakeInstall) -> None:
    install.version = NEW_VERSION
    client.call("list_threads", {})
    assert client.proxy.swaps == 1
    client.call("register", {"name": "beta"})
    # The 0.7.4 regression guard: the new worker resolves the same session id.
    assert store.bound_session_id("beta") == "sess-proxy-test"


def test_send_in_flight_during_swap_is_delivered_once(client: Client, install: FakeInstall) -> None:
    calls = {"n": 0}

    def flips_on_second_check() -> str | None:
        calls["n"] += 1
        return __version__ if calls["n"] == 1 else NEW_VERSION

    client.proxy.installed_version = flips_on_second_check
    client.send(
        {
            "jsonrpc": "2.0",
            "id": 500,
            "method": "tools/call",
            "params": {"name": "send", "arguments": {"to": "inbox", "body": "once"}},
        }
    )
    client.send(
        {
            "jsonrpc": "2.0",
            "id": 501,
            "method": "tools/call",
            "params": {"name": "list_claims", "arguments": {}},
        }
    )
    first = client.wait_for(500)
    second = client.wait_for(501)
    assert "error" not in first and "error" not in second
    assert client.proxy.swaps == 1
    assert [m.body for m in store.read("inbox", unread_only=False)] == ["once"]


def test_broken_new_worker_keeps_old_one_serving(client: Client, install: FakeInstall) -> None:
    old_pid = client.worker_pid
    client.proxy.start_timeout = 2.0
    # Never says hello on the control pipe, like a half-installed upgrade.
    client.proxy.worker_cmd = [sys.executable, "-c", "import time; time.sleep(30)"]
    install.version = NEW_VERSION

    threads = client.call("list_threads", {})

    assert client.proxy.swaps == 0
    assert client.worker_pid == old_pid
    sentinel = [t for t in threads if t.get("name") == "__dlb_server__"]
    assert sentinel and "could not switch" in sentinel[0]["stale_server"]


def test_protocol_mismatch_keeps_old_worker_and_says_restart(
    client: Client, install: FakeInstall, tmp_path: Path
) -> None:
    fake = tmp_path / "v2_worker.py"
    fake.write_text(
        textwrap.dedent("""
        import json, os, time
        r, w = (int(x) for x in os.environ["DLB_CONTROL_FDS"].split(","))
        os.fdopen(r, "rb").readline()
        out = os.fdopen(w, "wb")
        out.write(json.dumps({"op": "hello", "v": 2, "version": "9.9.9"}).encode() + b"\\n")
        out.flush()
        time.sleep(30)
    """)
    )
    old_pid = client.worker_pid
    client.proxy.worker_cmd = [sys.executable, str(fake)]
    install.version = NEW_VERSION

    threads = client.call("list_threads", {})

    assert client.worker_pid == old_pid
    sentinel = [t for t in threads if t.get("name") == "__dlb_server__"]
    assert sentinel and "handoff protocol 2" in sentinel[0]["stale_server"]
    assert "Restart the session" in sentinel[0]["stale_server"]
    # It tries once per installed version, not on every call.
    client.call("list_threads", {})
    assert client.worker_pid == old_pid


def test_worker_crash_respawns_with_owned_tokens(client: Client) -> None:
    token = client.call("register", {"name": "gamma"})["session_token"]
    old = client.proxy.worker
    assert old is not None
    old.proc.kill()
    old.proc.wait()
    deadline = time.time() + 5
    while client.proxy.worker is old and time.time() < deadline:
        time.sleep(0.05)

    rec = client.call("recover_token", {"name": "gamma"})

    assert client.worker_pid != old.proc.pid
    assert rec["session_token"] == token


def test_client_eof_stops_proxy_and_worker(install: FakeInstall) -> None:
    c = Client(installed_version=install, check_interval=0.0)
    c.initialize()
    worker = c.proxy.worker
    assert worker is not None
    c.close()
    assert not c._thread.is_alive()
    assert worker.proc.poll() is not None


# ── Security properties ────────────────────────────────────────────────────────


def test_tokens_never_reach_worker_env_or_argv(
    client: Client, install: FakeInstall, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = client.call("register", {"name": "delta"})["session_token"]
    seen: list[tuple[list[str], dict[str, str]]] = []
    real_popen = subprocess.Popen

    def spy(cmd: list[str], **kw: Any) -> subprocess.Popen:
        seen.append((list(cmd), dict(kw.get("env") or {})))
        return real_popen(cmd, **kw)

    monkeypatch.setattr(proxy.subprocess, "Popen", spy)
    install.version = NEW_VERSION
    rec = client.call("recover_token", {"name": "delta"})

    assert rec["session_token"] == token  # it did cross over...
    assert seen, "the swap should have started a worker"
    for cmd, env in seen:  # ...but not through argv or env
        assert all(token not in a for a in cmd)
        assert all(token not in v for v in env.values())
        assert set(env.get("DLB_CONTROL_FDS", "")) <= set("0123456789,")


def test_reserved_ids_are_refused(client: Client) -> None:
    resp = client.request("tools/list", {}, msg_id=f"{PRIVATE_ID_PREFIX}1")
    assert resp["error"]["code"] == -32600


def test_control_ops_over_mcp_do_not_leak_tokens(client: Client) -> None:
    token = client.call("register", {"name": "eps"})["session_token"]
    for method in ("dump", "op", "stale"):
        resp = client.request(method, {"op": "dump"})
        assert "error" in resp
    client.send({"op": "dump"})  # not even JSON-RPC
    client.call("list_claims", {})  # flush: anything it produced has arrived
    # The register response is the only frame that may carry the token.
    assert sum(token.encode() in line for line in client.raw) == 1


def test_planted_package_and_path_are_not_run(tmp_path: Path) -> None:
    # A repo the session works in could contain a `dlb_mcp/` package, and PATH
    # could hold a fake `dlb-mcp`. Neither may stand in for the worker.
    marker = tmp_path / "planted-ran"
    plant = tmp_path / "dlb_mcp"
    plant.mkdir()
    (plant / "__init__.py").write_text(f"open({str(marker)!r}, 'w').write('x')\n")
    (plant / "__main__.py").write_text(f"open({str(marker)!r}, 'w').write('x')\n")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    fake = bindir / "dlb-mcp"
    fake.write_text(f"#!/bin/sh\necho x > {marker}\n")
    fake.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = f"{bindir}{os.pathsep}{env.get('PATH', '')}"
    # Launch the proxy the way its console script does (no cwd on sys.path).
    proc = subprocess.Popen(
        [sys.executable, "-I", "-c", "from dlb_mcp.__main__ import main; main()"],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    assert proc.stdin is not None and proc.stdout is not None
    init = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "t", "version": "0"},
        },
    }
    proc.stdin.write((json.dumps(init) + "\n").encode())
    proc.stdin.flush()
    resp = json.loads(proc.stdout.readline())
    proc.stdin.close()
    proc.wait(15)

    assert resp["id"] == 1 and "result" in resp
    assert not marker.exists()

"""Thin stdio proxy: upgrade the DLB server without restarting the session.

Claude Code starts `dlb-mcp` once per session and talks to it over stdin and
stdout for the session's whole life. A plain server keeps the code it imported
at startup, so an upgrade only reached a session after a restart. Now
`dlb-mcp` is this proxy. It forwards MCP traffic, unchanged, to a worker
subprocess that runs the real server (`dlb-mcp worker`). When a newer dlb-mcp
is installed, the proxy swaps in a worker on the new code before the next tool
call, and the client never notices.

Swap, in order:
  1. Stop forwarding; the triggering call waits.
  2. Wait for the old worker to answer every call already sent to it, so no
     call is lost or sent twice.
  3. Ask the old worker for its owned tokens (server._owned_tokens, what
     recover_token uses to know this session registered a name).
  4. Start the new worker, hand it the tokens, replay the client's
     `initialize`, and probe it with `tools/list`.
  5. Only if all of that worked: retire the old worker and switch. Otherwise
     keep the old worker and have it tell the session a restart is needed.
  6. Send `notifications/tools/list_changed`, then forward the waiting call.

Security:
  - Tokens move only over private pipe pairs the proxy creates for each worker,
    passed by fd number (server.start_control_channel). Never through the MCP
    stream the model drives, env values, argv or disk. Nothing a client sends
    is ever written to a control pipe.
  - The worker command is fixed at startup: this interpreter, in isolated mode
    (-I), so neither PATH nor a `dlb_mcp` directory in the session's working
    directory can stand in for it.
  - Each worker gets this process's environment unchanged, so every worker
    resolves the same session id (store.resolve_session_id).
  - Trust note: code written into the install now reaches live sessions at
    their next tool call instead of at their next restart. That is the same
    same-user boundary the monitor's exit-75 respawn already relies on.

The proxy logs to stderr only; stdout carries MCP frames and nothing else.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from typing import Any, BinaryIO

from . import _version
from ._control import CONTROL_FDS_ENV, CONTROL_PROTOCOL, PRIVATE_ID_PREFIX

# How often (at most) the proxy checks the disk for a newer install.
CHECK_INTERVAL_S = 5.0
# How long a swap waits for the old worker to finish its in-flight calls.
DRAIN_TIMEOUT_S = 30.0
# How long a new worker gets to say hello, answer initialize and the probe.
START_TIMEOUT_S = 15.0
# How long a retired worker gets to exit on stdin EOF before it is killed.
RETIRE_GRACE_S = 5.0


def default_worker_cmd() -> list[str]:
    return [sys.executable, "-I", "-m", "dlb_mcp", "worker"]


def _log(msg: str) -> None:
    print(f"[dlb-proxy] {msg}", file=sys.stderr, flush=True)


def _key(msg_id: Any) -> str:
    """Hashable key for a JSON-RPC id (ints and strings must not collide)."""
    return json.dumps(msg_id, sort_keys=True)


class WorkerError(Exception):
    pass


class Worker:
    """One `dlb-mcp worker` subprocess with its stdio and control pipes."""

    def __init__(
        self,
        cmd: list[str],
        env: dict[str, str],
        owned: dict[str, str],
        on_line: Callable[[Worker, bytes], None],
        on_control: Callable[[Worker, dict[str, Any]], None],
        on_exit: Callable[[Worker], None],
    ) -> None:
        # Proxy -> worker and worker -> proxy control pipes. os.pipe fds are not
        # inheritable; pass_fds makes only the worker's two ends visible to it.
        to_r, to_w = os.pipe()
        from_r, from_w = os.pipe()
        child_env = dict(env)
        child_env[CONTROL_FDS_ENV] = f"{to_r},{from_w}"
        try:
            self.proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                env=child_env,
                pass_fds=(to_r, from_w),
            )
        finally:
            os.close(to_r)
            os.close(from_w)
        self.ctl_out = os.fdopen(to_w, "wb")
        self.ctl_in = os.fdopen(from_r, "rb")
        self._stdin_lock = threading.Lock()
        self._ctl_lock = threading.Lock()
        self._next_id = 0
        self._private: dict[str, list[Any]] = {}  # key -> [Event, response]
        self._dump: list[Any] = [threading.Event(), None]
        self.hello = threading.Event()
        self.version: str | None = None
        self.protocol: Any = None
        self.retired = False
        self._on_line = on_line
        self._on_control = on_control
        self._on_exit = on_exit

        self._send_control({"v": CONTROL_PROTOCOL, "owned": owned, "notice": None})
        threading.Thread(target=self._read_stdout, daemon=True).start()
        threading.Thread(target=self._read_control, daemon=True).start()

    # -- pipes -------------------------------------------------------------

    def _send_control(self, obj: dict[str, Any]) -> bool:
        with self._ctl_lock:
            try:
                self.ctl_out.write((json.dumps(obj) + "\n").encode())
                self.ctl_out.flush()
                return True
            except (OSError, ValueError):
                return False

    def send(self, raw: bytes) -> bool:
        """Write one MCP frame (a full line) to the worker's stdin."""
        assert self.proc.stdin is not None
        with self._stdin_lock:
            try:
                self.proc.stdin.write(raw if raw.endswith(b"\n") else raw + b"\n")
                self.proc.stdin.flush()
                return True
            except (OSError, ValueError):
                return False

    def _read_stdout(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            if self._take_private(line):
                continue
            self._on_line(self, line)
        self._on_exit(self)

    def _read_control(self) -> None:
        for raw in self.ctl_in:
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            op = msg.get("op")
            if op == "hello":
                self.protocol = msg.get("v")
                v = msg.get("version")
                self.version = v if isinstance(v, str) else None
                self.hello.set()
            elif op == "dump":
                self._dump[1] = msg.get("owned")
                self._dump[0].set()
            else:
                self._on_control(self, msg)

    # -- private requests ----------------------------------------------------

    def _take_private(self, line: bytes) -> bool:
        try:
            msg = json.loads(line)
        except ValueError:
            return False
        if not isinstance(msg, dict) or "method" in msg:
            return False
        msg_id = msg.get("id")
        if not (isinstance(msg_id, str) and msg_id.startswith(PRIVATE_ID_PREFIX)):
            return False
        slot = self._private.pop(_key(msg_id), None)
        if slot is not None:
            slot[1] = msg
            slot[0].set()
        return True

    def request(self, msg: dict[str, Any], timeout: float) -> dict[str, Any]:
        """Send a proxy-internal request and wait for its response."""
        self._next_id += 1
        msg_id = f"{PRIVATE_ID_PREFIX}{self._next_id}"
        slot: list[Any] = [threading.Event(), None]
        self._private[_key(msg_id)] = slot
        if not self.send(json.dumps({**msg, "id": msg_id}).encode()):
            raise WorkerError("worker stdin closed")
        if not slot[0].wait(timeout):
            raise WorkerError(f"no answer to {msg.get('method')} in {timeout:.0f}s")
        resp = slot[1]
        if "error" in resp:
            raise WorkerError(f"{msg.get('method')} failed: {resp['error']}")
        return resp

    def notify(self, msg: dict[str, Any]) -> None:
        self.send(json.dumps(msg).encode())

    def dump_owned(self, timeout: float) -> dict[str, str] | None:
        self._dump[0].clear()
        if not self._send_control({"op": "dump"}) or not self._dump[0].wait(timeout):
            return None
        owned = self._dump[1]
        return owned if isinstance(owned, dict) else None

    def set_notice(self, notice: str | None) -> None:
        self._send_control({"op": "stale", "notice": notice})

    # -- lifecycle -----------------------------------------------------------

    def alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self, grace: float = RETIRE_GRACE_S) -> None:
        """Close stdin (the worker exits on EOF); kill it if it lingers."""
        self.retired = True
        assert self.proc.stdin is not None
        with self._stdin_lock, contextlib.suppress(OSError):
            self.proc.stdin.close()
        for f in (self.ctl_out,):
            with contextlib.suppress(OSError):
                f.close()
        try:
            self.proc.wait(grace)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()

    def retire_async(self) -> None:
        threading.Thread(target=self.stop, daemon=True).start()


class Proxy:
    """Forwards one MCP client's stdio to a replaceable worker."""

    def __init__(
        self,
        client_in: BinaryIO,
        client_out: BinaryIO,
        *,
        worker_cmd: list[str] | None = None,
        env: dict[str, str] | None = None,
        installed_version: Callable[[], str | None] = _version.installed_version,
        check_interval: float = CHECK_INTERVAL_S,
        drain_timeout: float = DRAIN_TIMEOUT_S,
        start_timeout: float = START_TIMEOUT_S,
    ) -> None:
        self.client_in = client_in
        self.client_out = client_out
        self.worker_cmd = worker_cmd or default_worker_cmd()
        self.env = dict(os.environ if env is None else env)
        self.installed_version = installed_version
        self.check_interval = check_interval
        self.drain_timeout = drain_timeout
        self.start_timeout = start_timeout

        self.cond = threading.Condition()
        self.out_lock = threading.Lock()
        self.worker: Worker | None = None
        self.pending: set[str] = set()  # client request ids sent to self.worker
        self.owned: dict[str, str] = {}  # last snapshot from the current worker
        self.init_msg: dict[str, Any] | None = None
        self.init_key: str | None = None
        self.initialized_msg: dict[str, Any] | None = None
        self.notice: str | None = None  # set when a swap could not happen
        self.tried_version: str | None = None  # installed version last swapped for
        self.last_check = 0.0
        self.swaps = 0  # for tests and logs

    # -- client side ---------------------------------------------------------

    def write_client(self, raw: bytes) -> None:
        with self.out_lock:
            self.client_out.write(raw if raw.endswith(b"\n") else raw + b"\n")
            self.client_out.flush()

    def reply_error(self, msg_id: Any, message: str, code: int = -32603) -> None:
        err = {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}
        self.write_client(json.dumps(err).encode())

    def run(self) -> None:
        try:
            self.worker = self.start_worker({}, replay=False)
        except WorkerError as e:
            _log(f"worker did not start: {e}")
        for line in self.client_in:
            if line.strip():
                self.handle_client(line)
        self.shutdown()

    def handle_client(self, line: bytes) -> None:
        try:
            msg = json.loads(line)
        except ValueError:
            msg = None
        if not isinstance(msg, dict):
            # Not a single JSON-RPC object (bad JSON, or a batch): the worker
            # answers it as the plain server would. Nothing here is tracked.
            self.forward(line, None)
            return
        method = msg.get("method")
        has_id = "id" in msg
        if method is not None and has_id:
            msg_id = msg["id"]
            if isinstance(msg_id, str) and msg_id.startswith(PRIVATE_ID_PREFIX):
                self.reply_error(
                    msg_id, f"request ids starting {PRIVATE_ID_PREFIX!r} are reserved", code=-32600
                )
                return
            if method == "initialize":
                self.init_msg = msg
                self.init_key = _key(msg_id)
            elif method == "tools/call":
                self.maybe_upgrade()
            self.forward(line, msg_id)
            return
        if method == "notifications/initialized":
            self.initialized_msg = msg
        self.forward(line, None)

    def forward(self, line: bytes, msg_id: Any) -> None:
        """Send a client frame to the current worker; msg_id marks a request."""
        worker = self.ensure_worker()
        if worker is None:
            if msg_id is not None:
                self.reply_error(msg_id, "dlb-mcp worker is not running; see stderr")
            return
        if msg_id is not None:
            with self.cond:
                self.pending.add(_key(msg_id))
        if not worker.send(line) and msg_id is not None:
            with self.cond:
                self.pending.discard(_key(msg_id))
            self.reply_error(msg_id, "dlb-mcp worker stopped before the call was sent")

    # -- worker side -----------------------------------------------------------

    def on_worker_line(self, worker: Worker, line: bytes) -> None:
        try:
            msg = json.loads(line)
        except ValueError:
            msg = None
        if isinstance(msg, dict) and "method" not in msg and "id" in msg:
            key = _key(msg["id"])
            with self.cond:
                self.pending.discard(key)
                self.cond.notify_all()
            if key == self.init_key and isinstance(msg.get("result"), dict):
                # The proxy sends tools/list_changed after a swap, so say so.
                caps = msg["result"].setdefault("capabilities", {})
                if isinstance(caps, dict):
                    tools = caps.get("tools")
                    if not isinstance(tools, dict):
                        tools = caps["tools"] = {}
                    tools["listChanged"] = True
                    line = json.dumps(msg).encode()
        self.write_client(line)

    def on_worker_control(self, worker: Worker, msg: dict[str, Any]) -> None:
        if msg.get("op") == "owned" and isinstance(msg.get("owned"), dict):
            with self.cond:
                if worker is self.worker:
                    self.owned = dict(msg["owned"])

    def on_worker_exit(self, worker: Worker) -> None:
        with self.cond:
            if worker is not self.worker or worker.retired:
                return
            self.worker = None
            lost = list(self.pending)
            self.pending.clear()
            self.cond.notify_all()
        code = worker.proc.wait()
        _log(f"worker exited code={code}; starting a new one on the next message")
        for key in lost:
            self.reply_error(json.loads(key), "dlb-mcp worker exited during the call")

    def ensure_worker(self) -> Worker | None:
        with self.cond:
            worker = self.worker
            owned = dict(self.owned)
        if worker is not None:
            return worker
        try:
            worker = self.start_worker(owned, replay=True)
        except WorkerError as e:
            _log(f"worker did not start: {e}")
            return None
        with self.cond:
            self.worker = worker
        if self.notice:
            worker.set_notice(self.notice)
        return worker

    def start_worker(self, owned: dict[str, str], *, replay: bool) -> Worker:
        """Start a worker, hand it `owned`, and bring it to a ready state."""
        worker = Worker(
            self.worker_cmd,
            self.env,
            owned,
            self.on_worker_line,
            self.on_worker_control,
            self.on_worker_exit,
        )
        try:
            if not worker.hello.wait(self.start_timeout):
                raise WorkerError(
                    "no hello on the control pipe (not a dlb-mcp worker, or it failed to start)"
                )
            if worker.protocol != CONTROL_PROTOCOL:
                raise WorkerError(
                    f"handoff protocol {worker.protocol!r}, this proxy speaks {CONTROL_PROTOCOL}"
                )
            if replay and self.init_msg is not None:
                worker.request(
                    {k: v for k, v in self.init_msg.items() if k != "id"}, self.start_timeout
                )
                if self.initialized_msg is not None:
                    worker.notify(self.initialized_msg)
                worker.request(
                    {"jsonrpc": "2.0", "method": "tools/list", "params": {}}, self.start_timeout
                )
        except WorkerError:
            worker.retired = True
            worker.stop(grace=1.0)
            raise
        return worker

    # -- upgrade ---------------------------------------------------------------

    def maybe_upgrade(self) -> None:
        now = time.monotonic()
        if now - self.last_check < self.check_interval:
            return
        self.last_check = now
        worker = self.worker
        installed = self.installed_version()
        if (
            worker is None
            or installed is None
            or worker.version is None
            or installed == worker.version
            or installed == self.tried_version
        ):
            return
        self.swap(worker, installed)

    def swap(self, old: Worker, installed: str) -> None:
        with self.cond:
            drained = self.cond.wait_for(lambda: not self.pending, self.drain_timeout)
        if not drained:
            _log(
                f"upgrade to {installed} postponed: calls still running after "
                f"{self.drain_timeout:.0f}s"
            )
            return
        self.tried_version = installed
        owned = old.dump_owned(self.start_timeout) if old.alive() else None
        if owned is None:
            with self.cond:
                owned = dict(self.owned)
        try:
            new = self.start_worker(owned, replay=True)
        except WorkerError as e:
            self.notice = (
                f"{old.version} -> {installed}: dlb-mcp {installed} is installed "
                f"but this session could not switch to it ({e}). "
                f"Restart the session to load it."
            )
            _log(self.notice)
            old.set_notice(self.notice)
            return
        with self.cond:
            self.worker = new
            self.owned = dict(owned)
        self.notice = None
        self.swaps += 1
        old.retired = True
        old.retire_async()
        _log(f"switched worker {old.version} -> {new.version}")
        self.write_client(
            json.dumps({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}).encode()
        )

    def shutdown(self) -> None:
        with self.cond:
            worker = self.worker
            self.worker = None
        if worker is not None:
            worker.stop()


def main() -> None:
    Proxy(sys.stdin.buffer, sys.stdout.buffer).run()

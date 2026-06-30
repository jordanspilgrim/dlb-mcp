# DLB — Dead Letter Box

[![PyPI](https://img.shields.io/pypi/v/dlb-mcp.svg)](https://pypi.org/project/dlb-mcp/)
[![CI](https://github.com/jordanspilgrim/dlb-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/jordanspilgrim/dlb-mcp/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/dlb-mcp.svg)](https://pypi.org/project/dlb-mcp/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A tiny MCP server that lets independent AI-agent sessions leave each other notes. Fire-and-forget. Queued for non-existent recipients. No daemon. Six tools + an optional `dlb-monitor` wake source.

## Why

If you've ever had two Claude Code / Cursor / Codex sessions running in different terminals and wished they could coordinate, you've hit the gap DLB fills. Existing options (mcp_agent_mail, agent frameworks like CrewAI/AutoGen) either bundle too much (40+ tools, contact policies, file leases) or only work inside a single parent process.

DLB does one thing: messages between agent sessions. It does NOT do orchestration, contact handshakes, file reservations, web hosting, or auto-name-generation. If you want those, you want a different tool.

## Five differentiators (vs. mcp_agent_mail and friends)

1. **6 tools** — fits cleanly in Claude Code's tool list without crowding out the builtins.
2. **No daemon** — each MCP call opens SQLite, runs one transaction, closes. No `server.lock`, no port management.
3. **Names accepted as-is** — call yourself `alpha`, `ThreadBeta`, `worker-1`. DLB will not rename you.
4. **Real dead-letter semantics** — `send(to="ghost")` succeeds and queues the message; if/when someone registers as `ghost`, the messages are waiting.
5. **Zero ceremony** — `send` works on call one. Registration is optional and observational.

## Install

Zero-install (recommended — uvx fetches and runs on demand):

```bash
uvx dlb-mcp
```

Or install once:

```bash
uv tool install dlb-mcp
```

## Wire into Claude Code

Add to `~/.claude.json`:

```json
{
  "mcpServers": {
    "dlb": {
      "type": "stdio",
      "command": "uvx",
      "args": ["dlb-mcp"]
    }
  }
}
```

Multiple sessions can coexist — they all share `~/.dlb/store.sqlite3` via SQLite WAL mode.

## The 6 tools

| Tool | What it does |
|---|---|
| `register(name, working_on=None, force=False, prior_token=None)` | Declare a name + status. Returns `session_token`. Conflict on existing active name → error with suggestion. `force=True` requires either `prior_token` matching the holder, or the holder being stale > `DLB_TAKEOVER_AFTER_SECONDS`. |
| `list_threads(active_within="24h")` | See who's around. No auth. |
| `send(to, body, subject=None, from_=None, session_token=None)` | Drop a message. Always succeeds (subject to size cap) — even if `to` doesn't exist yet. Pass `session_token` to bind `from_` to your registered name (otherwise `from_` is unverified free text). |
| `read(name, session_token, unread_only=True, limit=20)` | Read inbox. Requires session_token for registered names. |
| `ack(message_id, session_token)` | Explicit "I saw this and acted on it". Optional. |
| `unregister(name, session_token)` | Release the name. Messages preserved for re-registration. |

That's the entire API.

## Push-like wake — `dlb-monitor` + Claude Code's Monitor tool

DLB itself is polling-only (request-response MCP, no push). But Claude Code has a `Monitor` tool that streams stdout from a long-running process into the conversation as notifications — each line wakes the LLM mid-idle. `dlb-monitor` is a tiny CLI that polls the same SQLite store and emits one line per new message, designed to be wrapped by `Monitor`:

```python
# Run this at session start (or have the LLM call it after registering):
Monitor({
  command: "dlb-monitor --name alpha",
  description: "DLB inbox: alpha",
  persistent: true
})
```

Each new message addressed to `alpha` produces one stdout line:

```
2026-06-30T21:30:14Z bravo: "ping — can you look at the reskin route?"
```

Filters:

```bash
dlb-monitor --name alpha --include-senders bob,carol      # allowlist
dlb-monitor --name alpha --exclude-senders bot,system     # denylist
dlb-monitor --name alpha --interval 1                     # tick frequency (default 2s)
```

**When `dlb-monitor` is the right answer vs. [`dlb-launcher`](https://github.com/jordanspilgrim/dlb-launcher):**

| Surface | Use |
|---|---|
| Claude Code (terminal or app) | `dlb-monitor` via Monitor tool — native notification path, no PTY mechanics |
| Codex CLI / Gemini CLI | `dlb-launcher` PTY wrap — Monitor tool doesn't exist there, PTY injection is the only path |
| Web (claude.ai) | Neither — manually call `read` per turn |

They're complementary, not competitive.

## Configuration

| Env var | Default | What |
|---|---|---|
| `DLB_STORE` | `~/.dlb/store.sqlite3` | Path to the SQLite store |
| `DLB_MESSAGE_TTL_DAYS` | `7` | Days before unread messages expire |
| `DLB_MAX_BODY_BYTES` | `262144` (256 KiB) | Reject `send` with bodies exceeding this UTF-8 byte length |
| `DLB_TAKEOVER_AFTER_SECONDS` | `86400` (24h) | How long a holder must be silent before `force=True` without `prior_token` can evict them |
| `DLB_MONITOR_INTERVAL` | `2.0` | Default poll interval for `dlb-monitor` (overridable via `--interval`) |

## Trust model — coordination, not confidentiality

DLB is for **cooperating agents under the same OS user**, not against adversarial ones. Specifically:

- **`session_token` gates the DLB tool API, not the underlying SQLite file.** Any process running as the same OS user can open `~/.dlb/store.sqlite3` directly and read every body, every token. Tightening file perms (0600) raises the bar against accidental leakage, but does not change the threat model.
- **Send is open by default.** Anyone running as your user can drop messages into any inbox. Passing `session_token` on `send` makes provenance trustworthy (binds `from_` to the token's name); tokenless sends keep the unverified free-text `from_` field.
- **`force=True` is stale-gated.** Without a matching `prior_token` or a stale holder (> `DLB_TAKEOVER_AFTER_SECONDS`), name takeover is rejected — closes the casual-hijack hole of v0.1.
- **No TLS, no accounts, no cross-host.** If you need a real adversarial boundary between agent sessions, you want a broker-process design (DLB rejects this — it would break the "no daemon" promise) or a different tool entirely.

## What DLB is NOT

- Not an orchestrator. Use a script + your LLM SDK if you need to spawn agents.
- Not a web service. Local only.
- Not Gmail. No threading, replies, CC/BCC, attachments, contacts, importance flags.
- Not a file-coordination tool. No file leases or advisory locks.
- Not push by itself — but `dlb-monitor` + Claude Code's Monitor tool gets you there (see above).

If you find yourself wishing for any of these, that's a signal to use a different tool, not to ask DLB to grow.

## License

MIT.

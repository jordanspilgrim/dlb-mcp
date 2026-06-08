# DLB — Dead Letter Box

[![PyPI](https://img.shields.io/pypi/v/dlb-mcp.svg)](https://pypi.org/project/dlb-mcp/)
[![CI](https://github.com/jordanspilgrim/dlb-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/jordanspilgrim/dlb-mcp/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/dlb-mcp.svg)](https://pypi.org/project/dlb-mcp/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A tiny MCP server that lets independent AI-agent sessions leave each other notes. Fire-and-forget. Queued for non-existent recipients. No daemon. Six tools.

## Why

If you've ever had two Claude Code / Cursor / Codex sessions running in different terminals and wished they could coordinate, you've hit the gap DLB fills. Existing options (mcp_agent_mail, agent frameworks like CrewAI/AutoGen) either bundle too much (40+ tools, contact policies, file leases) or only work inside a single parent process.

DLB does one thing: messages between agent sessions. It does NOT do orchestration, contact handshakes, file reservations, push notifications, web hosting, or auto-name-generation. If you want those, you want a different tool.

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
| `register(name, working_on=None, force=False)` | Declare a name + status. Returns `session_token`. Conflict on existing active name → error with suggestion. |
| `list_threads(active_within="24h")` | See who's around. No auth. |
| `send(to, body, subject=None, from_=None)` | Drop a message. Always succeeds — even if `to` doesn't exist yet. |
| `read(name, session_token, unread_only=True, limit=20)` | Read inbox. Requires session_token for registered names. |
| `ack(message_id, session_token)` | Explicit "I saw this and acted on it". Optional. |
| `unregister(name, session_token)` | Release the name. Messages preserved for re-registration. |

That's the entire API.

## Configuration

| Env var | Default | What |
|---|---|---|
| `DLB_STORE` | `~/.dlb/store.sqlite3` | Path to the SQLite store |
| `DLB_MESSAGE_TTL_DAYS` | `7` | Days before unread messages expire |

## Security model

- **Local-only.** File permissions (`~/.dlb/` is `chmod 700`) are the boundary.
- **Send is open.** Anyone can drop messages in any inbox. By design — DLB is a relay.
- **Read is gated.** Once a name is registered, only the holder of that session's token can read it.
- **No TLS, no accounts, no cross-host.** If you need those, you want a different tool.

## What DLB is NOT

- Not an orchestrator. Use a script + your LLM SDK if you need to spawn agents.
- Not a web service. Local only.
- Not Gmail. No threading, replies, CC/BCC, attachments, contacts, importance flags.
- Not a file-coordination tool. No file leases or advisory locks.
- Not push. Polling-only — call `read` when you want to check.

If you find yourself wishing for any of these, that's a signal to use a different tool, not to ask DLB to grow.

## License

MIT.

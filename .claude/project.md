# Project descriptor — dlb-mcp

The per-project half of the shared Claude Code agent harness. The roster, `/kickoff`, `/standup`,
`/promote`, and the DLB/branch hooks resolve **globally** from `~/.claude` (installed by
`./scripts/harness-bootstrap.sh`) — only this file and `.claude/settings.json` live here.

```yaml
name: dlb-mcp
stack: "Python 3.11+ (3.11/3.12/3.13 in CI) · MCP server on mcp[cli]>=1.2.0 · stdlib sqlite3 store (WAL, hashed-at-rest tokens) · uv + hatchling build · pytest + pytest-asyncio (asyncio_mode=auto) · ruff (line-length 100; E,F,I,UP,B,SIM). Three console entrypoints: dlb-mcp (stdio server), dlb-monitor (push-like wake), dlb-session-recover (SessionStart hook). No UI, no web framework, no ORM."
base_ref: origin/main
worktree_isolation: true
check_cmd: "uv run ruff check . && uv run ruff format --check ."
test_cmd: "uv run pytest -q"
definition_of_done: "check_cmd clean + `uv run pytest -q` fully green (189 tests as of v0.6.5 — a drop in count is a silently skipped test, not a pass) + `uv run dlb-monitor --help` exits 0. NEVER run the server or any DLB tool against the real store at ~/.dlb — it is the LIVE coordination bus for concurrent agent sessions on this machine, and a stray send/register/unregister corrupts other people's in-flight work. Tests already isolate via tmp_path in tests/conftest.py; keep it that way."
deploy_targets: [pypi]  # tag `vX.Y.Z` on main → .github/workflows/publish.yml (OIDC trusted publishing, environment: pypi). Version is bumped in pyproject.toml; the tag is the release trigger. No servers, no rollback — a bad release is superseded, never deleted.
acceptance: "cli — run the entrypoint and assert stdout/exit code (e.g. `uv run dlb-monitor --help`), or drive the MCP tools over stdio against a throwaway store via DLB_STORE_PATH/tmp_path. Never against ~/.dlb."
product_context: "Dead Letter Box: a tiny MCP server that lets independent agent sessions leave each other notes. Ten tools (register/recover_token/set_status/list_threads/send/read/ack/unregister/update_status/get_task_status), no daemon, real dead-letter semantics — messages to an unregistered name queue and deliver on later registration. Users are agent fleets (Claude Code, Cursor, Codex) coordinating under one OS user; the trust model is cooperation, not confidentiality."
sources_of_truth: "README.md — gates any change to the tool surface (names, params, auth rules), install/config instructions, the store schema + migration/watermark behavior, or version history. A tool signature change that doesn't land in README.md is not done."
roster_profile: small
```

## Notes for every role

- **The store is the trust boundary, and it is deliberately thin.** `session_token` gates the tool
  API, not the SQLite file. Don't propose changes that imply confidentiality between agents under
  the same OS user — that's out of scope by design (README "Trust boundary").
- **Backward compatibility is load-bearing.** Published on PyPI and consumed by live fleets; the
  store carries a forward-compat guard that fails loudly if it's newer than the installed package.
  Schema changes need a migration and a test in `tests/test_migration_concurrency.py`.
- **`.claude/plans/` is gitignored** — plans and handoffs persist locally, never in the repo.

"""Entry point: ``dlb-mcp`` or ``python -m dlb_mcp``.

Subcommands:
  (none)   run the stdio MCP server behind the upgrade proxy (the default; what
           your MCP client invokes). DLB_PROXY=0 runs the server directly.
  worker   run the server itself; the proxy starts this (not meant for clients)
  setup    wire the recovery + inbox-check hooks into ~/.claude/settings.json
"""

from __future__ import annotations

import os
import sys

_HELP = (
    "dlb-mcp: Dead Letter Box MCP server (stdio transport).\n"
    "\n"
    "Usage:\n"
    "  dlb-mcp            Run the MCP server (speaks MCP over stdin/stdout).\n"
    "                     It runs behind a small proxy that switches to a newly\n"
    "                     installed dlb-mcp without a session restart.\n"
    "  dlb-mcp setup      Wire the SessionStart recovery hook and the\n"
    "                     SessionStart/UserPromptSubmit inbox-check hooks into\n"
    "                     ~/.claude/settings.json (idempotent).\n"
    "\n"
    "Env vars:\n"
    "  DLB_STORE              Path to SQLite store (default ~/.dlb/store.sqlite3)\n"
    "  DLB_MESSAGE_TTL_DAYS   Days before unread messages expire (default 7)\n"
    "  DLB_READ_RECEIPTS      0 to disable task read-receipts (default on)\n"
    "  DLB_MAX_BODY_BYTES     Max message body size (default 262144 = 256 KiB)\n"
    "  DLB_MAX_FIELD_BYTES    Max metadata field size (default 8192 = 8 KiB)\n"
    "  DLB_MAX_INBOX          Per-recipient inbox cap, drop-oldest (default 1000; 0=off)\n"
    "  DLB_WAKE_WINDOW_SECONDS  Max dlb-monitor heartbeat age that still counts\n"
    "                         as wakeable (default 120)\n"
    "  DLB_PROXY             0 to run the server without the upgrade proxy\n"
    "                         (upgrades then need a session restart)\n"
    "  DLB_SESSION_ID        Per-session id for recover_token identity (unset=off).\n"
    "                         Wire from the harness session id (e.g.\n"
    "                         CLAUDE_CODE_SESSION_ID); must be unique per session.\n"
)


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    if arg in ("--help", "-h", "help"):
        sys.stdout.write(_HELP)
        return
    if arg == "setup":
        from .setup import main as setup_main

        setup_main()
        return
    if arg == "worker" or os.environ.get("DLB_PROXY", "1") == "0":
        from .server import serve_stdio

        serve_stdio()
        return
    from .proxy import main as proxy_main

    proxy_main()


if __name__ == "__main__":
    main()

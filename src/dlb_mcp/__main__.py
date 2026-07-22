"""Entry point: ``dlb-mcp`` or ``python -m dlb_mcp``.

Subcommands:
  (none)   run the stdio MCP server (the default; what your MCP client invokes)
  setup    wire the SessionStart recovery hook into ~/.claude/settings.json
"""

from __future__ import annotations

import sys

from .server import serve_stdio

_HELP = (
    "dlb-mcp — Dead Letter Box MCP server (stdio transport).\n"
    "\n"
    "Usage:\n"
    "  dlb-mcp            Run the MCP server (speaks MCP over stdin/stdout).\n"
    "  dlb-mcp setup      Wire the SessionStart recovery hook into\n"
    "                     ~/.claude/settings.json (idempotent).\n"
    "\n"
    "Env vars:\n"
    "  DLB_STORE              Path to SQLite store (default ~/.dlb/store.sqlite3)\n"
    "  DLB_MESSAGE_TTL_DAYS   Days before unread messages expire (default 7)\n"
    "  DLB_READ_RECEIPTS      0 to disable task read-receipts (default on)\n"
    "  DLB_MAX_BODY_BYTES     Max message body size (default 262144 = 256 KiB)\n"
    "  DLB_MAX_FIELD_BYTES    Max metadata field size (default 8192 = 8 KiB)\n"
    "  DLB_MAX_INBOX          Per-recipient inbox cap, drop-oldest (default 1000; 0=off)\n"
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
    serve_stdio()


if __name__ == "__main__":
    main()

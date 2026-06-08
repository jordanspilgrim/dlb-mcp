"""Entry point: ``dlb-mcp`` or ``python -m dlb_mcp``.

Single subcommand for v1: run the stdio MCP server. We don't bother with a
real CLI parser — there's nothing else to do.
"""

from __future__ import annotations

import sys

from .server import serve_stdio


def main() -> None:
    # Accept --help / -h politely so users probing the binary aren't confused
    if len(sys.argv) > 1 and sys.argv[1] in ("--help", "-h"):
        sys.stdout.write(
            "dlb-mcp — Dead Letter Box MCP server (stdio transport).\n"
            "\n"
            "Usage: dlb-mcp\n"
            "\n"
            "Run this command as your MCP server entry; it speaks the MCP\n"
            "protocol over stdin/stdout. Not interactive.\n"
            "\n"
            "Env vars:\n"
            "  DLB_STORE              Path to SQLite store (default ~/.dlb/store.sqlite3)\n"
            "  DLB_MESSAGE_TTL_DAYS   Days before unread messages expire (default 7)\n"
        )
        return
    serve_stdio()


if __name__ == "__main__":
    main()

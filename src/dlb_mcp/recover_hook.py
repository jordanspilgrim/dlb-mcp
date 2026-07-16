"""``dlb-session-recover`` — Claude Code SessionStart hook entry point.

Prints the DLB names registered on this machine so an agent that lost its
session_token to a restart or context compaction knows to recover it. Tokens
are deterministic (v0.4.0+), so recovery is simply
``mcp__dlb__recover_token(name)`` — no stored value to retrieve, no re-register.

Clean no-op (prints nothing, exits 0) when no names have been registered yet,
so it is safe to wire unconditionally into every session start.

Packaged as a console script (``dlb-session-recover``) so it is on PATH on any
machine after ``uv tool install dlb-mcp`` — no repo checkout or absolute path
required. Wire it with ``dlb-mcp setup``.
"""

from __future__ import annotations

import sys

from . import store


def registered_names() -> list[str]:
    """Names with a token sidecar under <store_dir>/tokens/. Sorted; [] on any
    error or missing dir (the hook must never break a session start)."""
    d = store.store_path().parent / "tokens"
    if not d.is_dir():
        return []
    try:
        return sorted(p.name for p in d.iterdir() if p.is_file())
    except OSError:
        return []


def main() -> None:
    names = registered_names()
    if not names:
        return
    out = sys.stdout
    out.write(
        "📇 DLB: names registered on this machine. If one is YOURS and you lost "
        "your\nsession_token (restart/compaction), call "
        "mcp__dlb__recover_token(name) —\ntokens are deterministic, so no "
        "re-register is needed:\n"
    )
    for n in names:
        out.write(f"  - {n}\n")


if __name__ == "__main__":
    main()

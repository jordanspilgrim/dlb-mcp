"""``dlb-session-recover``: Claude Code SessionStart hook entry point.

Prints the DLB names registered on this machine so an agent that lost its
session_token knows how to get its identity back. There are two cases:

  • Same session, post-compaction: ``mcp__dlb__recover_token(name)`` returns
    the token (the dlb-mcp process, which owns the name, outlived compaction).
  • Full restart (quit+reopen, or --resume, which rotates the session):
    recover_token is refused because a new process does not own the name and
    the session id changed. Reclaim instantly by reading the persisted token
    from the sidecar and calling register(force=True, prior_token=<token>),
    which bypasses the stale-gate (the token matches). Reading the sidecar is
    file access; a peer using only the tool API cannot do it, so the boundary
    holds.

Clean no-op (prints nothing, exits 0) when no names have been registered yet,
so it is safe to wire unconditionally into every session start.

Packaged as a console script (``dlb-session-recover``) so it is on PATH on any
machine after ``uv tool install dlb-mcp``, no repo checkout or absolute path
required. Wire it with ``dlb-mcp setup``.
"""

from __future__ import annotations

import sys

from . import store


def registered_names() -> list[str]:
    """Names with a token sidecar under <store_dir>/tokens/. Sorted; [] on any
    error or missing dir (the hook must never break a session start).

    Sidecar FILENAMES are sha256(name) hashes (collision-free), so the human
    name is read from line 1 of each file's contents rather than the filename."""
    d = store.tokens_dir()
    if not d.is_dir():
        return []
    names: list[str] = []
    try:
        for p in d.iterdir():
            if not p.is_file():
                continue
            try:
                first = p.read_text().splitlines()[0]
            except (OSError, IndexError):
                continue
            if first:
                names.append(first)
    except OSError:
        return []
    return sorted(names)


def main() -> None:
    names = registered_names()
    if not names:
        return
    out = sys.stdout
    out.write(
        "📇 DLB: names registered on this machine. If one is YOURS:\n"
        "  • lost your token mid-session (compaction) → mcp__dlb__recover_token(name)\n"
        "  • after a full restart (recover_token refused) → read line 2 (the single-use\n"
        "    reclaim secret) of the file shown next to the name below, then call\n"
        "    register(name, force=true, prior_token=<that secret>) to reclaim instantly.\n"
    )
    # Sidecar filenames are sha256(name) hashes, so print each name's RESOLVED
    # path; the generic "tokens/<name>" no longer locates the file.
    for n in names:
        out.write(f"  - {n}  →  {store.sidecar_path(n)}\n")


if __name__ == "__main__":
    main()

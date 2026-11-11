"""Detect when the dlb-mcp code on disk is newer than the code in this process.

`uv tool install dlb-mcp` (or any upgrade) replaces files on disk, but a running
process keeps the code it imported at startup. Each Claude Code session runs its
own stdio server and monitor, so after an upgrade they keep serving the old code
until restarted. These helpers let a process notice that:

- the monitor exits with EXIT_VERSION_CHANGED so its supervisor respawns it on
  the new code at once;
- the server reports `stale_server` so the agent (and user) know a session
  restart is needed. The server never exits on its own: Claude Code does not
  respawn stdio servers, so exiting would take the tools away mid-session.
"""

from __future__ import annotations

import importlib
from importlib import metadata

from . import __version__ as LOADED_VERSION

DIST_NAME = "dlb-mcp"

# Distinct exit code (EX_TEMPFAIL) the supervised wrapper treats as
# "respawn now, no backoff". Kept clear of 0 (clean stop), 1/2 (errors,
# argparse) and 127 (not found).
EXIT_VERSION_CHANGED = 75

# Distinct exit code (EX_NOPERM) for "the store keeps refusing this monitor's
# heartbeat": the monitor cannot authenticate as the name, so respawning it
# would only fail the same way. The supervised wrapper treats it as fatal.
EXIT_HEARTBEAT_REFUSED = 77


def installed_version() -> str | None:
    """Return the version currently installed on disk, or None if unknown.

    importlib.metadata caches directory listings per sys.path entry, so a
    plain lookup can keep returning the version seen at startup.
    invalidate_caches() drops those caches so the lookup re-reads the
    dist-info directory that is there now.
    """
    try:
        importlib.invalidate_caches()
        return metadata.distribution(DIST_NAME).version
    except Exception:
        # Not installed as a distribution (e.g. run from a bare source tree),
        # or the dist-info is mid-replacement. Unknown is not "changed".
        return None


def stale_version() -> tuple[str, str] | None:
    """Return (loaded, installed) when they differ, else None."""
    installed = installed_version()
    if installed is None or installed == LOADED_VERSION:
        return None
    return (LOADED_VERSION, installed)


def stale_server_notice() -> str | None:
    """Human-readable notice for tool output, or None when current."""
    stale = stale_version()
    if stale is None:
        return None
    loaded, installed = stale
    return (
        f"{loaded} -> {installed}: this session's dlb-mcp server is running "
        f"{loaded} but {installed} is installed. Restart the session to load it."
    )

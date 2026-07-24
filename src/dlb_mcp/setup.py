"""``dlb-mcp setup``: wire the SessionStart recovery hook into Claude Code.

Idempotently adds a SessionStart hook running ``dlb-session-recover`` to the
user's ``~/.claude/settings.json`` (global / machine-wide). Design rules:

- **Idempotent:** if the hook is already present, do nothing.
- **Never clobber:** on unreadable / invalid-JSON / wrong-shape settings, abort
  with a clear message instead of overwriting. Back up before any write.
- **Preserve:** existing hooks (PreToolUse etc.) and other keys are untouched.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

HOOK_COMMAND = "dlb-session-recover"


def _settings_path() -> Path:
    return Path.home() / ".claude" / "settings.json"


def _has_hook(settings: dict) -> bool:
    hooks = settings.get("hooks")
    if not isinstance(hooks, dict):
        return False
    for group in hooks.get("SessionStart", []) or []:
        if not isinstance(group, dict):
            continue
        for h in group.get("hooks", []) or []:
            if isinstance(h, dict) and HOOK_COMMAND in (h.get("command") or ""):
                return True
    return False


def wire_session_hook(path: Path | None = None) -> tuple[bool, str]:
    """Add the SessionStart recovery hook idempotently.

    Returns (changed, message). `changed` is False for a no-op (already wired)
    or an abort (error); True when the file was written.
    """
    p = path or _settings_path()

    if p.exists():
        try:
            settings = json.loads(p.read_text())
        except OSError as e:
            return False, f"error: cannot read {p}: {e}. Add the hook by hand."
        except json.JSONDecodeError as e:
            return False, (
                f"error: {p} exists but is not valid JSON ({e}); refusing to "
                "modify it. Fix the JSON or add the hook by hand."
            )
        if not isinstance(settings, dict):
            return False, f"error: {p} is not a JSON object; not modifying it."
    else:
        settings = {}

    if _has_hook(settings):
        return False, f"ok: SessionStart hook already wired in {p}, nothing to do."

    hooks = settings.setdefault("hooks", {})
    if not isinstance(hooks, dict):
        return False, f"error: {p} has a non-object 'hooks' key; not modifying it."
    session_start = hooks.setdefault("SessionStart", [])
    if not isinstance(session_start, list):
        return False, f"error: {p} has a non-list 'hooks.SessionStart'; not modifying it."

    session_start.append({"hooks": [{"type": "command", "command": HOOK_COMMAND}]})

    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.exists():
            shutil.copy2(p, p.with_name(p.name + ".dlb-bak"))
        p.write_text(json.dumps(settings, indent=2) + "\n")
    except OSError as e:
        return False, f"error: failed to write {p}: {e}"

    return True, (
        f"ok: wired SessionStart hook ({HOOK_COMMAND}) into {p}. "
        "Restart your Claude Code session to activate."
    )


def main() -> None:
    _changed, message = wire_session_hook()
    print(message)

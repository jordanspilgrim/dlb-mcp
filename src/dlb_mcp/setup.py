"""``dlb-mcp setup``: wire DLB's Claude Code hooks.

Idempotently adds to the user's ``~/.claude/settings.json`` (global /
machine-wide):

- SessionStart: ``dlb-session-recover`` (token recovery reminder) and
  ``dlb-inbox-check SessionStart`` (unread mail on session start);
- UserPromptSubmit: ``dlb-inbox-check UserPromptSubmit`` (unread mail on every
  prompt, so a session without dlb-monitor still sees mail on its next turn).

Design rules:

- **Idempotent:** hooks already present are left alone; only missing ones are added.
- **Never clobber:** on unreadable / invalid-JSON / wrong-shape settings, abort
  with a clear message instead of overwriting. Back up before any write.
- **Preserve:** existing hooks (PreToolUse etc.) and other keys are untouched.
  The legacy ``dlb-inbox-check.sh`` is reported, never removed.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

HOOK_COMMAND = "dlb-session-recover"
INBOX_COMMAND = "dlb-inbox-check"

# (event, exact command) pairs `setup` guarantees are present.
REQUIRED_HOOKS: tuple[tuple[str, str], ...] = (
    ("SessionStart", HOOK_COMMAND),
    ("SessionStart", f"{INBOX_COMMAND} SessionStart"),
    ("UserPromptSubmit", f"{INBOX_COMMAND} UserPromptSubmit"),
)

# The hand-written hook this package replaces. It queried a column that does
# not exist (read_at instead of read_at_ms), so it never reported anything.
LEGACY_INBOX_HOOK = "dlb-inbox-check.sh"


def _settings_path() -> Path:
    return Path.home() / ".claude" / "settings.json"


def _commands(settings: dict, event: str) -> list[str]:
    hooks = settings.get("hooks")
    if not isinstance(hooks, dict):
        return []
    groups = hooks.get(event, []) or []
    if not isinstance(groups, list):
        return []
    out: list[str] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        for h in group.get("hooks", []) or []:
            if isinstance(h, dict) and isinstance(h.get("command"), str):
                out.append(h["command"].strip())
    return out


def _has_command(settings: dict, event: str, command: str) -> bool:
    """True if `command` is wired for `event`. Matches the command itself (an
    absolute path to the same console script counts), not a substring, so the
    legacy ``dlb-inbox-check.sh`` is never mistaken for the real hook."""
    want = command.split()
    for cmd in _commands(settings, event):
        have = cmd.split()
        if have and have[0].rsplit("/", 1)[-1] == want[0] and have[1:] == want[1:]:
            return True
    return False


def _has_hook(settings: dict) -> bool:
    """Back-compat: is the SessionStart recovery hook wired?"""
    return _has_command(settings, "SessionStart", HOOK_COMMAND)


def _missing(settings: dict) -> list[tuple[str, str]]:
    return [(e, c) for e, c in REQUIRED_HOOKS if not _has_command(settings, e, c)]


def _legacy_events(settings: dict) -> list[str]:
    return [
        event
        for event in ("SessionStart", "UserPromptSubmit")
        if any(LEGACY_INBOX_HOOK in c for c in _commands(settings, event))
    ]


def wire_session_hook(path: Path | None = None) -> tuple[bool, str]:
    """Add every hook in REQUIRED_HOOKS idempotently.

    Returns (changed, message). `changed` is False for a no-op (already wired)
    or an abort (error); True when the file was written.
    """
    p = path or _settings_path()

    if p.exists():
        try:
            settings = json.loads(p.read_text())
        except OSError as e:
            return False, f"error: cannot read {p}: {e}. Add the hooks by hand."
        except json.JSONDecodeError as e:
            return False, (
                f"error: {p} exists but is not valid JSON ({e}); refusing to "
                "modify it. Fix the JSON or add the hooks by hand."
            )
        if not isinstance(settings, dict):
            return False, f"error: {p} is not a JSON object; not modifying it."
    else:
        settings = {}

    legacy = _legacy_events(settings)
    legacy_note = (
        f" Note: {p} still wires the old {LEGACY_INBOX_HOOK} on "
        f"{', '.join(legacy)}. It never reported mail (it queries a column that "
        "does not exist); remove those entries, dlb-inbox-check replaces them."
        if legacy
        else ""
    )

    missing = _missing(settings)
    if not missing:
        return False, f"ok: DLB hooks already wired in {p}, nothing to do.{legacy_note}"

    hooks = settings.setdefault("hooks", {})
    if not isinstance(hooks, dict):
        return False, f"error: {p} has a non-object 'hooks' key; not modifying it."
    events = list(dict.fromkeys(e for e, _ in missing))
    for event in events:
        if not isinstance(hooks.setdefault(event, []), list):
            return False, f"error: {p} has a non-list 'hooks.{event}'; not modifying it."
    for event in events:
        # One new group per event, holding every command that event is missing.
        hooks[event].append(
            {"hooks": [{"type": "command", "command": c} for e, c in missing if e == event]}
        )

    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.exists():
            shutil.copy2(p, p.with_name(p.name + ".dlb-bak"))
        p.write_text(json.dumps(settings, indent=2) + "\n")
    except OSError as e:
        return False, f"error: failed to write {p}: {e}"

    wired = ", ".join(f"{e}: {c}" for e, c in missing)
    return True, (
        f"ok: wired DLB hooks ({wired}) into {p}. "
        f"Restart your Claude Code session to activate.{legacy_note}"
    )


def main() -> None:
    _changed, message = wire_session_hook()
    print(message)

#!/usr/bin/env bash
# DLB SessionStart recovery hook.
#
# On session start, list the DLB names registered on this machine so an agent
# that lost its session_token to a restart or context compaction can recover.
# Tokens are DETERMINISTIC (dlb-mcp v0.4.0+): recovery is simply
#   mcp__dlb__recover_token(name)   (or re-register — same token, no stored value).
#
# Wire it into Claude Code by adding a SessionStart hook to ~/.claude/settings.json:
#   { "hooks": { "SessionStart": [ { "hooks": [ { "type": "command",
#       "command": "bash /Users/gabriel/ClaudeCode/dlb-mcp/hooks/dlb-session-recover.sh" } ] } ] } }
# (or source it from an existing SessionStart hook such as dlb-inbox-check.sh).
set -euo pipefail

store="${DLB_STORE:-$HOME/.dlb/store.sqlite3}"
tokens_dir="$(dirname "$store")/tokens"
[ -d "$tokens_dir" ] || exit 0

names="$(ls -1 "$tokens_dir" 2>/dev/null | head -50 || true)"
[ -n "$names" ] || exit 0

echo "📇 DLB: names registered on this machine. If one is YOURS and you lost your"
echo "session_token (restart/compaction), call mcp__dlb__recover_token(name) —"
echo "tokens are deterministic, so no re-register is needed:"
printf '%s\n' "$names" | sed 's/^/  - /'

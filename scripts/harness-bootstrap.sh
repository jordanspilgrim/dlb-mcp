#!/usr/bin/env bash
# Ensure the shared Claude Code agent harness is installed into ~/.claude.
# Idempotent and safe to re-run. Requires read access to the harness repo.
# Override the location/URL via env if your checkout lives elsewhere.
set -euo pipefail
HARNESS_DIR="${HARNESS_DIR:-$HOME/ClaudeCode/harness}"
HARNESS_URL="${HARNESS_URL:-https://github.com/jordanspilgrim/harness.git}"
if [ -d "$HARNESS_DIR/.git" ]; then
  echo "harness: refreshing $HARNESS_DIR"
  git -C "$HARNESS_DIR" pull --ff-only || true
else
  echo "harness: cloning $HARNESS_URL -> $HARNESS_DIR"
  mkdir -p "$(dirname "$HARNESS_DIR")"
  git clone "$HARNESS_URL" "$HARNESS_DIR"
fi
"$HARNESS_DIR/install.sh"
echo "harness: installed. Run /harness-init (if no .claude/project.md yet) then /kickoff."

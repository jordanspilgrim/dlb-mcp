#!/usr/bin/env bash
# Reset the demo store so every recording take starts identical.
# Uses a scratch DB, never your real ~/.dlb/store.sqlite3.
set -euo pipefail

export DLB_STORE="${DLB_STORE:-/tmp/dlb-demo.sqlite3}"
rm -f "$DLB_STORE" "$DLB_STORE"-wal "$DLB_STORE"-shm
# Wipe the sidecar reclaim files for the demo names so re-runs are clean.
rm -rf "${HOME}/.dlb/reclaim" 2>/dev/null || true

echo "reset: DLB_STORE=$DLB_STORE"

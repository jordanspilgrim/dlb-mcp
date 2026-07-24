#!/usr/bin/env bash
# Reset the demo store so every recording take starts identical.
#
# Everything the demo touches lives under ONE scratch directory
# ($DLB_DEMO_DIR, default /tmp/dlb-demo): the SQLite store AND its reclaim
# sidecars (DLB writes those to <store_dir>/tokens/). We never go near your
# real ~/.dlb, so this can't touch your actual registered names.
set -euo pipefail

DLB_DEMO_DIR="${DLB_DEMO_DIR:-/tmp/dlb-demo}"
rm -rf "$DLB_DEMO_DIR"
mkdir -p "$DLB_DEMO_DIR"

echo "reset: DLB_STORE=$DLB_DEMO_DIR/store.sqlite3"

#!/usr/bin/env bash
# Remove a stale admin UDS from a previous run so the server can rebind.
set -euo pipefail
cd "$(dirname "$0")"
rm -f admin.sock
echo "stale admin.sock removed (if any)"

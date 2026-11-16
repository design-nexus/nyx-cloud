#!/usr/bin/env bash
# The daemon against a local rclone alias, on a private bus where no Secret Service or
# other service can start (tests/session.conf). No live accounts, no keyring.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
exec dbus-run-session --config-file="$here/session.conf" -- python3 "$here/integration.py"

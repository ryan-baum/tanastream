#!/bin/bash
# Installs tanastream as a macOS launchd user agent (RunAtLoad + KeepAlive, i.e. always running,
# auto-restarted). No hidden env-driven directory discovery and no bundled watchdog/guard job
# (a guard job is project-specific operational doctrine and doesn't belong in a public repo — see
# docs/OPERATIONS.md for what a guard/watchdog job would need to check on your own setup).
#
# Usage:
#   contrib/launchd/install.sh [install-dir]
#
# install-dir defaults to the repo root (two levels up from this script). It must contain the
# `tanastream` bin shim and your config must already be in place (see the main README).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${1:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
LAUNCH_DIR="$HOME/Library/LaunchAgents"
LOG_DIR="$HOME/Library/Logs"
UID_VALUE="$(id -u)"

if [ ! -x "$INSTALL_DIR/tanastream" ]; then
  echo "error: $INSTALL_DIR/tanastream not found or not executable (chmod +x it, or pass the right install-dir)" >&2
  exit 1
fi

mkdir -p "$LAUNCH_DIR" "$LOG_DIR"

sed \
  -e "s|__TANASTREAM_INSTALL_DIR__|$INSTALL_DIR|g" \
  -e "s|__HOME__|$HOME|g" \
  -e "s|__LOG_DIR__|$LOG_DIR|g" \
  "$SCRIPT_DIR/com.tanastream.plist.template" > "$LAUNCH_DIR/com.tanastream.plist"

# launchctl bootout is ASYNCHRONOUS — it returns before the job is actually unregistered. An
# immediate bootstrap right after can race it and fail with "Bootstrap failed: 5: Input/output
# error", leaving the daemon NOT running. Poll until the label is actually gone (bounded, so a
# genuinely stuck job doesn't hang this script forever).
launchctl bootout "gui/$UID_VALUE/com.tanastream" >/dev/null 2>&1 || true
for _ in $(seq 1 50); do
  launchctl print "gui/$UID_VALUE/com.tanastream" >/dev/null 2>&1 || break
  sleep 0.1
done

launchctl bootstrap "gui/$UID_VALUE" "$LAUNCH_DIR/com.tanastream.plist"
launchctl kickstart -k "gui/$UID_VALUE/com.tanastream"

launchctl print "gui/$UID_VALUE/com.tanastream" | sed -n '1,80p'

echo ""
echo "Installed. Plist: $LAUNCH_DIR/com.tanastream.plist"
echo "Logs: $LOG_DIR/tanastream.log (stdout) / $LOG_DIR/tanastream-error.log (stderr)"
echo "Stop:  launchctl bootout gui/$UID_VALUE/com.tanastream"

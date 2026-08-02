# Operations

Three ways to run the daemon, plus the dead-letter runbook and where logs/state live.

## Run modes

### 1. Foreground (simplest, good for a first run or debugging)

```bash
./tanastream daemon --verbose
```

Runs in your terminal, logs to stdout, stops on Ctrl-C. Good for confirming config is correct
before wiring up a supervisor.

### 2. macOS launchd (RunAtLoad + KeepAlive — always running, auto-restarted)

```bash
contrib/launchd/install.sh
```

Renders `contrib/launchd/com.tanastream.plist.template` (parameterized on your install path, home
directory, and log directory) into `~/Library/LaunchAgents/com.tanastream.plist`, then bootstraps
and kickstarts it. See `contrib/launchd/install.sh` for exactly what it does — it's a short,
readable script, not a black box.

```bash
launchctl print "gui/$(id -u)/com.tanastream"   # inspect
launchctl bootout "gui/$(id -u)/com.tanastream" # stop
```

### 3. systemd (Linux, user or system scope)

`contrib/systemd/tanastream.service` is a documented example unit — edit the two path lines, then:

```bash
mkdir -p ~/.config/systemd/user
cp contrib/systemd/tanastream.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tanastream
journalctl --user -u tanastream -f
```

There's no install script for systemd (distro/user-vs-system scope varies too much to script
generically) — the unit file is the documentation.

## Dead-letter runbook

A write that fails `maxAttempts` times (default 5) lands in the dead-letter state instead of
retrying forever, so one poison write never blocks the rest of the queue.

1. `tanastream dead-letter list` — see what's stuck, with `lastError`, `opType`, `payload`, `source`.
2. Read `lastError`. If it's a transient failure (network blip, Tana was closed) that's since
   resolved: `tanastream dead-letter retry <id>` — resets attempts to 0 and returns it to pending.
3. If the payload itself was wrong (bad node ID, missing required field): enqueue a corrected
   write with a **new** idempotency key. Leave the bad row dead — it's your audit trail.
4. If it's a KTD-9 rejection (`TANA_ID_REQUIRED` — a `tag`/`field` op was enqueued with a name
   instead of an ID) it never made it INTO the queue at all; there's nothing to retry. Fix the
   producer and re-enqueue with the correct ID.

## Logs and state

| What | Where |
|---|---|
| Spool database (SQLite outbox) | `~/.local/state/tanastream/spool.db` by default (`TANASTREAM_STATE_DIR` overrides) |
| launchd stdout/stderr | `~/Library/Logs/tanastream.log` / `tanastream-error.log` |
| systemd | `journalctl --user -u tanastream` |
| Foreground | your terminal |

## Pacing

The Local-route write pacing gate (`--local-min-interval-ms`, default 100ms) is on by default in
every run mode above — it's a `drainOnce()` option threaded through the CLI, not something the
supervisor config needs to know about. `0` disables it if you have a reason to (see the main
README's "Pacing" section for why the default exists before you do that).

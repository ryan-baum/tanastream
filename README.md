# tanastream

A durable, single-writer write queue for [Tana](https://tana.inc)'s Local API. Enqueue a write,
and it survives a crash, a closed Tana window, or a network blip — then applies exactly once,
verified by reading back what it wrote.

Standalone: no dependency on [supertag-cli](https://github.com/jcfischer/supertag-cli) or any
other tool. Bun + Tana's Local API (`:8262`) is the whole stack.

## Why a queue, if Tana's API is already safe to write to concurrently?

A 2026-08-01 stress test measured Tana's Local API directly: 300+ concurrent writes across REST,
`/mcp`, and three separate OS processes produced zero drops, duplicates, or corruption — **for the
operations tested: create/import, name/description update, done, read.** Move, trash, field set,
tag add/remove, and schema mutation were **not** tested concurrently and are **not** claimed safe
under concurrency here (see "What's not tested" below). So: corruption-avoidance was this tool's
original justification, and that specific premise is **falsified** for the tested operations — a
second concurrent writer will not corrupt your graph on its own. tanastream is kept anyway, on four
rationales the measurement didn't touch:

1. **Crash durability.** A batch of writes sent as a sequential loop with no persistence drops
   everything still pending the moment the process dies. tanastream's SQLite outbox
   (`synchronous=FULL`, WAL mode) is the only thing that makes a write-intent survive a crash.
2. **Queuing while Tana is closed.** The Local API only exists while Tana is running. Anything
   writing on a schedule — a cron job, a background agent, a scheduled import — needs somewhere to
   put a write when Tana isn't open, and a way to apply it once Tana reopens.
3. **Idempotency.** Tana's Local API has no server-assigned idempotency token. Retrying a failed
   write safely requires the client to prove, after the fact, whether the retry actually landed.
   tanastream does this with a visible `TanaStreamIdempotency - <key>` marker child plus a
   reconcile-before-retry pass — not a guess, a verified check.
4. **Pacing.** The same stress test found that a write burst measurably stalls every *other*
   reader hitting the same API — reads that normally run ~5,000/sec dropped to ~200/sec with
   individual reads stalling up to 2.4 seconds during a 50-write burst. If something else on your
   machine (the Tana UI itself, another script) reads from `:8262` while you're bulk-importing,
   client-side pacing is good citizenship, not paranoia.

**What's not tested:** move, trash, field set, tag add/remove, and schema mutation were not
exercised under concurrent load in the 2026-08-01 measurement. tanastream still routes ALL eight
op types through the single-writer queue — the untested ones are queue-governed on safety grounds,
not because a problem was found, but because none was ruled out either. If you run several
tanastream instances (or another Tana client) against the same graph concurrently, treat those five
op types as the open question.

## Install

```bash
git clone <this-repo> tanastream
cd tanastream
bun install
```

## Configure

tanastream looks for configuration in this order:

1. `TANASTREAM_ENDPOINT` / `TANASTREAM_TOKEN` environment variables
2. `~/.config/tanastream/config.json` (or `TANASTREAM_CONFIG` to point elsewhere)
3. `~/.config/supertag/config.json` — used as an explicit, logged fallback if you already have
   [supertag-cli](https://github.com/jcfischer/supertag-cli) configured

Copy `config.example.json` to `~/.config/tanastream/config.json` and fill in your Local API bearer
token (find it in Tana's own Local API settings) and a default target node:

```json
{
  "apiEndpoint": "http://127.0.0.1:8262",
  "apiToken": "YOUR_LOCAL_API_BEARER_TOKEN",
  "defaultTargetNode": "YOUR_INBOX_OR_TARGET_NODE_ID"
}
```

Run any command with no config found and tanastream prints the exact file path and example to
create — it never guesses or silently no-ops.

## Quickstart

```bash
./tanastream enqueue create --name "Hello from tanastream" --target INBOX --key hello-1
./tanastream drain --once
./tanastream status
```

That enqueues a node creation, drains the queue once (applying it via Tana's Local API), then shows
you the spool's state. Run `./tanastream help` for the full command reference and every op type
(`create | edit | tag | tag-create | field | trash | done | move`).

For always-on operation (a daemon that drains continuously), see
[`docs/OPERATIONS.md`](docs/OPERATIONS.md) — foreground, macOS launchd, and systemd are all
documented there.

## The idempotency marker

Every structured `create` gets a visible child node — `TanaStreamIdempotency - <key>` — by default.
It's what makes crash-replay safe: if the process dies mid-write and retries, tanastream reads the
target's children back, finds the marker, and skips re-creating the node instead of duplicating it.

If you don't want the marker node cluttering your graph, opt out per-write:

```bash
./tanastream enqueue create --name "No marker please" --target INBOX --key key-2 --no-marker
```

Opting out trades away crash-replay safety for that specific write — a crash between apply and
acknowledgment can duplicate it. Raw Tana Paste creates (`--payload-json` with a literal
`tanaPaste` field) never get a marker either, for the same reason: the marker's own append would be
inside content tanastream doesn't control the shape of.

## Pacing

The Local-route drain loop enforces a minimum interval between writes — 100ms by default (under
the ~12 writes/sec ceiling the stress test measured, so it costs close to zero throughput) — and
leaves headroom for other readers on the same API. Override with `--local-min-interval-ms` or the
`TANASTREAM_LOCAL_MIN_INTERVAL_MS` environment variable; `0` disables it.

## Tag and field IDs

`tag` and `field` operations need a tanastream tag ID (`--tag`) or field attribute ID
(`--attribute-id`) — never a name. This is a deliberate limitation of running standalone: the
original PAI-internal build could resolve names to IDs through supertag-cli's synced local index,
which doesn't exist in a standalone install. Find the ID you need via Tana's node context menu
("Copy ID") on the tag or field definition, or via `supertag-cli`'s own schema tools if you have it
installed.

## Companion tool

[supertag-cli](https://github.com/jcfischer/supertag-cli) is the read/index side of the same
problem space — it maintains a searchable local index of your Tana graph. tanastream is
write-only (see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)) and doesn't overlap with it; they
compose.

## Development

```bash
bun test                              # hermetic — no live Tana required
TANASTREAM_LIVE_SMOKE=1 bun test tests/live-smoke.test.ts   # one opt-in test against real Tana
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the outbox pattern, route selection, and
per-operation transport details.

## License

MIT — see [`LICENSE`](LICENSE).

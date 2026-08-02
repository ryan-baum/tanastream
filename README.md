# tanastream

A durable write queue for [Tana](https://tana.inc)'s Local API. You hand it a write; it survives a crash, a closed Tana window, or a network blip — then applies exactly once, and proves it by reading back what it wrote.

**Use it if any of these describe you:**

- You run scheduled or agentic writes into Tana — cron jobs, background agents, a voice-memo bridge — and Tana isn't always open when they fire.
- You can't afford to lose a write when a process dies mid-batch.
- You retry failed writes and need to know a retry won't create a duplicate.
- You bulk-import while anything else (including Tana itself) is reading the same API.

If you're one person typing into one interactive session, you don't need this. It's infrastructure for the moment your writes stop being attended.

Standalone: Bun + Tana's Local API (`:8262`) is the whole stack. No dependency on [supertag-cli](https://github.com/jcfischer/supertag-cli) or anything else.

## Why a queue, when the API is concurrency-safe?

Because the measurement that proved the API safe is the same one that showed what still breaks.

A direct stress test (2026-08-01, Tana Outliner 1.523.0 / Local API 1.0.0) ran 300+ concurrent writes across REST, `/mcp`, and three separate OS processes: zero drops, zero duplicates, zero corruption — for the operations tested (create/import, name/description update, done, read). Tana serializes writes server-side at ~12/sec regardless of client count. A second concurrent writer will not corrupt your graph on its own. Corruption-avoidance was this tool's original justification, and that premise is dead; you'll find no such claim here.

What the measurement didn't touch is everything *around* the write, and that's where the queue earns its keep:

1. **Crash durability.** A sequential await-loop with no persistence drops every pending write the moment the process dies. The SQLite outbox (`synchronous=FULL`, WAL mode) makes a write-intent survive.
2. **Tana being closed.** The Local API exists only while Tana is running. A scheduled writer needs somewhere to put a write at 4am and a way to apply it at 9.
3. **Idempotency.** Tana issues no idempotency token. A safe retry requires proving, after the fact, whether the previous attempt landed. tanastream proves it: a visible `TanaStreamIdempotency - <key>` marker child plus a reconcile-before-retry pass.
4. **Pacing.** The same test showed a write burst stalls every other reader on the API ~25× — reads drop from ~5,000/sec to ~200/sec, with individual reads hanging up to 2.4 seconds during a 50-write burst. The drain loop paces writes by default, so your bulk import doesn't freeze whatever else is reading.

**The open question:** move, trash, field set, tag add/remove, and schema mutation were never exercised under concurrent load. tanastream routes all eight op types through the queue anyway — those five are queue-governed because nothing ruled a problem out, and I'd rather ship "untested" as a label than as a surprise. If you run several writers against one graph, treat those five as the frontier.

## Install

```bash
git clone https://github.com/ryan-baum/tanastream.git
cd tanastream
bun install
```

## Configure

tanastream looks for configuration in this order:

1. `TANASTREAM_ENDPOINT` / `TANASTREAM_TOKEN` environment variables
2. `~/.config/tanastream/config.json` (or `TANASTREAM_CONFIG` to point elsewhere)
3. `~/.config/supertag/config.json` — an explicit, logged fallback if you already have [supertag-cli](https://github.com/jcfischer/supertag-cli) configured

Copy `config.example.json` to `~/.config/tanastream/config.json` and fill in your Local API bearer token (from Tana's Local API settings) and a default target node:

```json
{
  "apiEndpoint": "http://127.0.0.1:8262",
  "apiToken": "YOUR_LOCAL_API_BEARER_TOKEN",
  "defaultTargetNode": "YOUR_INBOX_OR_TARGET_NODE_ID"
}
```

Run any command with no config found and tanastream prints the exact file path and example to create. It never guesses and never silently no-ops.

## Quickstart

```bash
./tanastream enqueue create --name "Hello from tanastream" --target INBOX --key hello-1
./tanastream drain --once
./tanastream status
```

That enqueues a node creation, drains the queue once (applying it via Tana's Local API), then shows you the spool's state. `./tanastream help` has the full command reference and every op type (`create | edit | tag | tag-create | field | trash | done | move`).

For always-on operation — a daemon that drains continuously — see [`docs/OPERATIONS.md`](docs/OPERATIONS.md): foreground, macOS launchd, and systemd are all documented.

## The idempotency marker

Every structured `create` gets a visible child node — `TanaStreamIdempotency - <key>` — by default. That marker is what makes crash-replay safe: if the process dies mid-write and retries, tanastream reads the target's children back, finds the marker, and skips re-creating the node instead of duplicating it.

Don't want marker nodes in your graph? Opt out per-write:

```bash
./tanastream enqueue create --name "No marker please" --target INBOX --key key-2 --no-marker
```

Opting out trades away crash-replay safety for that write — a crash between apply and acknowledgment can duplicate it. Raw Tana Paste creates (`--payload-json` with a literal `tanaPaste` field) never get a marker either, since tanastream doesn't control the shape of content it would be appending into.

## Pacing

The Local-route drain loop enforces a minimum interval between writes — 100ms by default. Since the server caps out around 12 writes/sec anyway, the default costs you close to zero throughput and buys headroom for every other reader on the API. Override with `--local-min-interval-ms` or `TANASTREAM_LOCAL_MIN_INTERVAL_MS`; `0` disables it.

## Tag and field IDs

`tag` and `field` operations take a tag ID (`--tag`) or field attribute ID (`--attribute-id`) — never a name. A standalone install has no local name-resolution index, so name-to-ID lookup isn't on offer; guessing at ambiguous names would be worse than asking you for the ID once. Get the ID from Tana's node context menu ("Copy ID") on the tag or field definition, or from supertag-cli's schema tools if you have it installed.

## Companion tool

[supertag-cli](https://github.com/jcfischer/supertag-cli) is the read/index side of the same problem space: a searchable local index of your Tana graph. tanastream is write-only by design (see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)). They compose; neither needs the other.

## What's proven against a real Tana, and what isn't (yet)

Honest status: `create`, `move`, `trash`, and node read-back are proven against a real Tana Local API — they're what the concurrency measurement exercised live. The other four op types (`tag`, `tag-create`, `field`, `done`) go over Tana's `/mcp` JSON-RPC endpoint and have not yet been exercised against a live Tana instance by this codebase. Their implementation follows Tana's published tool schemas and the documented `/mcp` failure contract (`isError` decides success, never HTTP status), but the wire behavior for those four is unconfirmed. The hermetic suite (`bun test`) covers all eight against a mock server; the one live test (`tests/live-smoke.test.ts`) currently exercises `create` only. If something looks wrong specifically on those four ops, that's the first place to suspect — file an issue with the exact error.

## Development

```bash
bun test                              # hermetic — no live Tana required
TANASTREAM_LIVE_SMOKE=1 bun test tests/live-smoke.test.ts   # one opt-in test against real Tana
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the outbox pattern, route selection, and per-operation transport details.

## Authorship & provenance

This codebase was written by AI agents under human direction, and it's worth being plain about that. The implementation was built by Claude (Anthropic) — spec and orchestration by one Claude model, code by another — with independent pre-publish audits by two non-Anthropic models. Ryan Baum directed the work, made the design calls, reviewed the results, and ran the live verification, but did not hand-write the code: if you ask a deep question about a specific line, the honest answer is that an agent wrote it and a human accepted it. The full test suite, the audit findings, and this repo's commit history are the receipts. Issues and PRs welcome — they get the same human-plus-agent review loop.

## License

MIT — see [`LICENSE`](LICENSE).

# Architecture

## The outbox pattern

tanastream is a transactional-outbox implementation for Tana writes:

```
producer enqueues -> SQLite outbox (WAL, synchronous=FULL)
                   -> drain loop picks a route
                   -> apply via HTTP
                   -> verify by reading back what was just written
                   -> mark applied | retry | dead-letter
```

Every write starts as a durable row in SQLite before anything touches the network. A crash between
enqueue and apply loses nothing — `recoverInflight()` resets any row stuck mid-apply back to
`pending` on the next start, and the drain loop picks it up again. A write only leaves the
`pending`/`inflight` cycle once it's either verified `applied` or exhausted its retry budget into
`dead` (the dead-letter state — see the runbook in [`OPERATIONS.md`](OPERATIONS.md)).

The dedup key is `opType:idempotencyKey` (a producer-supplied key, or an auto-generated content
hash). Re-enqueueing the identical write is a safe no-op; re-enqueueing the same key with a
*different* payload or target is a loud `IDEMPOTENCY_CONFLICT` — never a silent overwrite.

## Route selection

Each pending write resolves to one of three outcomes on every drain tick:

- **Local route** — the Local API (`:8262`) is reachable. All 8 op types apply this way.
- **Input route** — the Local API is unreachable but the Input API is configured. Only `create`
  ops can use this route (the Input API has no equivalent for edit/tag/field/trash/done/move), and
  only structured creates — a raw Tana-Paste create is Local-only and held until Tana reopens,
  because routing it to Input would burn its retry budget on a guaranteed failure.
- **Held** — neither route is available. The write stays `pending` with a `holdReason` explaining
  why, and is retried on the next drain tick rather than failing.

Route selection happens fresh on every drain call — there's no route caching or stale-health risk.

## Dual wire-form transport (per operation)

All 8 op types apply over direct HTTP — no subprocess, no CLI shell-out. Two wire forms are used,
chosen per operation by which one Tana's Local API actually exposes:

| Op | Wire form | Endpoint / tool | Verification |
|---|---|---|---|
| `create` | REST | `POST /nodes/{id}/import` (Tana Paste) | Read-back: idempotency marker present, name/description/children match literally |
| `edit` | REST | `POST /nodes/{id}/update` | Read-back: name/description equal what was requested (whitespace-normalized) |
| `move` | REST | `POST /nodes/{id}/move` | Result of the move call itself |
| `trash` | REST | `POST /nodes/{id}/trash` | 2xx, or a 400 containing "already in trash" (idempotent-success exception) |
| `tag` | `/mcp` JSON-RPC | `tools/call` -> `tag` | `isError:false`; falls back to a plain re-read rather than asserting tag-membership rendering, which isn't confirmed to appear in read-back markdown |
| `tag-create` | `/mcp` JSON-RPC | `tools/call` -> `create_tag`, confirmed via `list_tags` | Polling readback (up to 20 attempts, 100ms apart) until the tag is visible by name |
| `field` | `/mcp` JSON-RPC | `tools/call` -> `set_field_content` / `set_field_option` | Same fallback rule as `tag` |
| `done` | `/mcp` JSON-RPC | `tools/call` -> `check_node` / `uncheck_node` | `isError:false` only — checkbox state isn't reliably rendered in read-back markdown |

**Why `/mcp` for four of the eight:** the Local API's REST surface doesn't expose tag/field/done
operations directly; Tana's `/mcp` endpoint (same process, same port, JSON-RPC 2.0) does. Every
`/mcp` call sends `Accept: application/json, text/event-stream` (its absence gets a
`-32000 Not Acceptable` error), and success is judged **solely** by the JSON-RPC response's
`result.isError` field — never by HTTP status. A 200-with-`isError:true` response is a real,
observed failure mode; treating any 2xx as success would silently mark a failed write as applied.

**Tag and field IDs, not names.** `tag`'s and `field`'s `/mcp` tools take `tagIds`/`attributeId` —
opaque IDs, not human-readable names. A standalone install has no local name-resolution index (the
kind a synced CLI tool might maintain), so tanastream requires the ID up front and rejects a
name-only payload loudly at enqueue time rather than failing opaquely against the tool schema later.

## Write-only, by design

tanastream has no read/search/query command, and no plans to add one. The one exception is
internal: read-back calls exist purely to verify what tanastream itself just wrote (or, for
`reconcile`, to check whether a previously-applied Input-route write has since become visible on
the Local route). None of that is exposed as a public read API.

The reasoning: a write queue that also reads blurs into being an unofficial Tana SDK, which is a
different product with a different design center (caching, query semantics, staleness). Keeping
the boundary sharp means tanastream's one job — write intent that survives everything short of
losing the disk — stays legible. If you need to read from Tana, that's
[supertag-cli](https://github.com/jcfischer/supertag-cli)'s job, or Tana's own APIs directly.

## The Tana-Paste injection hazard

Structured creates (`--name`, `--description`, `--child`) get assembled into
[Tana Paste](https://tana.inc/docs) — a plain-text outline format with several characters that
carry structural meaning: `::` starts a field, `%%...%%` is a block directive, `[[...]]` is a node
reference, `^` prefixes a node-ID reference, a leading `#` starts a supertag, a leading `- ` starts
a nested bullet, and a leading `N. ` starts an ordered-list item. Because Tana Paste is the *only*
literal-create format the Local API accepts (there's no structured-JSON create endpoint), any of
those characters appearing in what a producer intended as plain text gets silently reinterpreted —
a note titled `Status:: Done` doesn't create a node named "Status:: Done", it creates a field.

tanastream's enqueue-time validator rejects any of those control sequences in a structured create's
name/description/children, loudly, before the write ever reaches the network — converting a silent
misparse into an immediate, actionable error. Producers that intentionally want Tana Paste syntax
(building a search node, applying a tag, writing a field) opt in explicitly via a raw
`tanaPaste`/`rawTanaPaste` payload, which skips the denylist entirely — that's a deliberate escape
hatch, not a gap: the tool trusts an explicit opt-in over a heuristic.

## Everything else

The queue mechanics — dead-letter policy, the SQLite schema, event logging, drain-lock semantics —
are documented at the point of use in `src/*.ts`; the code is short (under 2,000 lines total) and
each file has a single, narrow job. Start at `src/queue.ts` (`drainOnce`) if you want to trace a
write end to end.

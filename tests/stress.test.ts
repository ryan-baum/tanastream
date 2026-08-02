#!/usr/bin/env bun

// Stress / adversarial probes added 2026-06-13 during the TanaStream stress-test pass.
// These extend (do not replace) the frozen pre-implementation matrix in tanastream.test.ts.
// Several probes target SILENT-correctness directions (false-accept / false-reject) that a
// green happy-path test would miss — per the Verify-the-verifier doctrine.

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { drainOnce, enqueueWrite } from "../src/queue";
import { buildTanaPaste, markerFor, markerMatches } from "../src/realBackend";
import { openSpool } from "../src/spool";
import { FakeBackend } from "./fakeBackend";
import type { WriteRow } from "../src/types";

function tempDir() {
  return mkdtempSync(join(tmpdir(), "tanastream-stress-"));
}

function withSpool<T>(fn: (ctx: { spool: ReturnType<typeof openSpool> }) => T | Promise<T>) {
  return async () => {
    const dir = tempDir();
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    try {
      return await fn({ spool });
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  };
}

function makeRow(overrides: Partial<WriteRow> & { dedupKey: string }): WriteRow {
  return {
    id: 1,
    idempotencyKey: overrides.dedupKey.split(":")[1] ?? overrides.dedupKey,
    opType: "create",
    payload: { name: "Node" },
    targetNodeId: "INBOX",
    priority: 0,
    source: "test",
    state: "pending",
    attempts: 0,
    maxAttempts: 5,
    lastError: null,
    holdReason: null,
    createdAt: 0,
    updatedAt: 0,
    inflightAt: null,
    appliedAt: null,
    evidence: null,
    route: null,
    needsReconcile: false,
    reconciledAt: null,
    ...overrides,
  };
}

describe("TanaStream synthetic stress", () => {
  // ISC-10: 100+ unique-key enqueue burst persists exactly N rows, no loss.
  test("S1 burst of 150 unique-key creates persists all rows", withSpool(async ({ spool }) => {
    for (let i = 0; i < 150; i += 1) {
      const r = await enqueueWrite(spool, {
        opType: "create",
        idempotencyKey: `burst-${i}`,
        payload: { name: `Burst ${i}` },
        targetNodeId: "INBOX",
        source: "stress",
      });
      expect(r.inserted).toBe(true);
    }
    expect(spool.status().pending).toBe(150);
  }));

  // ISC-15: a mutation held while Local API is closed applies on reopen (per-write route).
  test("S2 closed-app mutation holds then applies on reopen", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localSequence: [false, true] });
    const edit = await enqueueWrite(spool, {
      opType: "edit",
      idempotencyKey: "hold-edit",
      payload: { nodeId: "n1", name: "Later" },
      targetNodeId: "n1",
      source: "stress",
    });
    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "held" });
    expect(spool.status()).toMatchObject({ pending: 1, applied: 0 });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", writeId: edit.id, route: "local" });
  }));

  // ISC-21: marker-disabled creates have NO idempotency marker → at-least-once on replay (documented residual).
  test("S3 no-marker create yields null marker; mutations never carry a marker", () => {
    expect(markerFor(makeRow({ dedupKey: "create:default" }))).toBe("TanaStreamIdempotency - create:default");
    expect(markerFor(makeRow({ dedupKey: "create:nomark", payload: { name: "x", includeIdempotencyMarker: false } }))).toBeNull();
    expect(markerFor(makeRow({ dedupKey: "edit:x", opType: "edit", payload: { nodeId: "n", name: "y" } }))).toBeNull();
  });

  // Faithful Local API read-back: every rendered line carries a "<!-- node-id: X -->" suffix.
  // Tests MUST exercise THIS shape (not raw buildTanaPaste output), or they give false
  // confidence — the exact gap that let a false-reject regression ship to the live graph.
  function readbackMarkdown(parentName: string, dedupKey: string, parentId = "Pxxxxxxxxxxx", markerId = "Mxxxxxxxxxxx"): string {
    return `- ${parentName} <!-- node-id: ${parentId} -->\n  - TanaStreamIdempotency - ${dedupKey} <!-- node-id: ${markerId} -->\n`;
  }

  // ISC-23 (VERIFIER false-accept): marker matching must NOT match a prefix-collision node.
  // Key "1" must not reconcile against the node that carries key "10" — in REAL read-back form.
  test("S4 marker match rejects prefix-collision node in real read-back form (no silent lost write)", () => {
    const rowShort = makeRow({ dedupKey: "create:1", payload: { name: "Short" } });
    const longNodeMarkdown = readbackMarkdown("Long", "create:10");
    expect(markerMatches(longNodeMarkdown, rowShort)).toBe(false);
    // also reject the buildTanaPaste form of the long node
    expect(markerMatches(buildTanaPaste(makeRow({ dedupKey: "create:10", payload: { name: "Long" } })), rowShort)).toBe(false);
  });

  // ISC-23 companion (VERIFIER false-reject guard): the correct node still matches BOTH forms.
  test("S5 marker match accepts the genuinely matching node (real read-back AND paste form)", () => {
    const row = makeRow({ dedupKey: "create:1", payload: { name: "Short" } });
    expect(markerMatches(readbackMarkdown("Short", "create:1"), row)).toBe(true); // real Local API form
    expect(markerMatches(buildTanaPaste(row), row)).toBe(true); // paste form
  });

  // ISC-23 companion: realistic long dedupKey (timestamped session key) in real read-back form.
  test("S5b marker match works for a long timestamped dedupKey in real read-back form", () => {
    const key = "create:TanaStream-StressTest-20260613-113413-plain";
    const row = makeRow({ dedupKey: key, payload: { name: "TanaStream-StressTest-20260613-113413-plain" } });
    expect(markerMatches(readbackMarkdown(row.payload.name as string, key), row)).toBe(true);
  });

  // ISC-22: Unicode/emoji survive the Tana Paste builder unchanged.
  test("S6 unicode/emoji survive buildTanaPaste", () => {
    const row = makeRow({ dedupKey: "create:uni", payload: { name: "東京 नदी prueba 🌊" } });
    const paste = buildTanaPaste(row);
    expect(paste).toContain("東京 नदी prueba 🌊");
  });

  // Regression (2026-07-07): a raw Tana Paste create routed to Input always dead-lettered with
  // "name is required" — buildInputNode() has no tanaPaste branch. Local down + Input up must
  // now HOLD the raw-paste create for Local, never burn its attempt on a guaranteed-throw route.
  test("S7 raw tanaPaste create holds (not routed to Input) while Local is down", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "raw-paste-hold",
      payload: { tanaPaste: "%%tana%%\n- Raw paste node" },
      targetNodeId: "INBOX",
      source: "stress",
    });
    const result = await drainOnce(spool, backend, { nowMs: 1_000 });
    expect(result).toMatchObject({ kind: "held" });
    expect((result as { reason: string }).reason).toContain("raw Tana Paste");
    expect(spool.status()).toMatchObject({ pending: 1, applied: 0, dead: 0 });
  }));

  // S7 companion (false-reject guard): a STRUCTURED create (no tanaPaste) must still take the
  // Input route when Local is down — the fix must not over-hold ops that Input can actually serve.
  test("S8 structured create still routes to Input while Local is down (unaffected by S7 fix)", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    const created = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "structured-input-ok",
      payload: { name: "Structured node" },
      targetNodeId: "INBOX",
      source: "stress",
    });
    const result = await drainOnce(spool, backend, { nowMs: 1_000 });
    expect(result).toMatchObject({ kind: "applied", writeId: created.id, route: "input" });
  }));

  // S7 companion (sanity): once Local reopens, the held raw-paste create applies via Local.
  test("S9 raw tanaPaste create applies via Local once it reopens", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localSequence: [false, true] });
    const created = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "raw-paste-reopen",
      payload: { tanaPaste: "%%tana%%\n- Raw paste node" },
      targetNodeId: "INBOX",
      source: "stress",
    });
    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "held" });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", writeId: created.id, route: "local" });
  }));
});

// R-7 (SPEC.md §6): configurable min-interval pacing on the Local route (KTD-4), default 100ms,
// 0 disables. Mirrors the pre-existing Input-route interval mechanism (queue.ts pattern).

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { drainOnce, enqueueWrite } from "../src/queue";
import { openSpool } from "../src/spool";
import { FakeBackend } from "./fakeBackend";

function makeSpool() {
  const dir = mkdtempSync(join(tmpdir(), "tanastream-pacing-"));
  const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
  return { spool, cleanup: () => { spool.close(); rmSync(dir, { recursive: true, force: true }); } };
}

describe("R-7: Local-route pacing", () => {
  test("default 100ms: two consecutive local applies < 100ms apart rate-limit; >=100ms proceeds", async () => {
    const { spool, cleanup } = makeSpool();
    try {
      const backend = new FakeBackend({ localOpen: true });
      await enqueueWrite(spool, { opType: "done", idempotencyKey: "p1", payload: { nodeId: "n1" }, source: "t" });
      await enqueueWrite(spool, { opType: "done", idempotencyKey: "p2", payload: { nodeId: "n2" }, source: "t" });

      // First local apply at t=1000 — no prior local attempt recorded, so it proceeds.
      expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "applied", route: "local" });
      // Second write only 50ms later (< default 100ms) — rate-limited, NOT applied.
      expect(await drainOnce(spool, backend, { nowMs: 1_050 })).toMatchObject({ kind: "rate_limited", sleepMs: 50 });
      // At t=1100 (>= 100ms since the first apply) it proceeds.
      expect(await drainOnce(spool, backend, { nowMs: 1_100 })).toMatchObject({ kind: "applied", route: "local" });
    } finally {
      cleanup();
    }
  });

  test("localMinIntervalMs: 0 disables pacing entirely — back-to-back applies never rate-limit", async () => {
    const { spool, cleanup } = makeSpool();
    try {
      const backend = new FakeBackend({ localOpen: true });
      await enqueueWrite(spool, { opType: "done", idempotencyKey: "p3", payload: { nodeId: "n3" }, source: "t" });
      await enqueueWrite(spool, { opType: "done", idempotencyKey: "p4", payload: { nodeId: "n4" }, source: "t" });

      expect(await drainOnce(spool, backend, { nowMs: 1_000, localMinIntervalMs: 0 })).toMatchObject({ kind: "applied", route: "local" });
      // Same millisecond — would rate-limit at the 100ms default, but 0 disables the gate.
      expect(await drainOnce(spool, backend, { nowMs: 1_000, localMinIntervalMs: 0 })).toMatchObject({ kind: "applied", route: "local" });
    } finally {
      cleanup();
    }
  });

  test("a backwards clock cannot produce an over-interval sleep (elapsed clamped to >= 0)", async () => {
    const { spool, cleanup } = makeSpool();
    try {
      const backend = new FakeBackend({ localOpen: true });
      await enqueueWrite(spool, { opType: "done", idempotencyKey: "p5", payload: { nodeId: "n5" }, source: "t" });
      await enqueueWrite(spool, { opType: "done", idempotencyKey: "p6", payload: { nodeId: "n6" }, source: "t" });

      expect(await drainOnce(spool, backend, { nowMs: 5_000 })).toMatchObject({ kind: "applied", route: "local" });
      // Clock jumps BACKWARDS to 1_000 (4s before the last attempt). Elapsed clamps to 0, so
      // sleepMs must clamp to the full interval (100), never a huge/negative value.
      expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "rate_limited", sleepMs: 100 });
    } finally {
      cleanup();
    }
  });
});

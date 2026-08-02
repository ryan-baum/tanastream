// U-7 (SPEC.md §8): live smoke — the one test in this suite that talks to a REAL Tana instance.
// Gated behind TANASTREAM_LIVE_SMOKE=1 so `bun test` stays fully hermetic by default (R-9). MUST
// target INBOX and clean up after itself (trashes the node it creates). MUST skip, never fail,
// when :8262 is unreachable — Tana being closed is an expected, not exceptional, state.
//
// Run it explicitly with Tana open:
//   TANASTREAM_LIVE_SMOKE=1 bun test tests/live-smoke.test.ts

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { drainOnce, enqueueWrite } from "../src/queue";
import { RealTanaBackend } from "../src/realBackend";
import { openSpool } from "../src/spool";

const LIVE = process.env.TANASTREAM_LIVE_SMOKE === "1";

describe.skipIf(!LIVE)("Live smoke (TANASTREAM_LIVE_SMOKE=1, real Tana required)", () => {
  test("create round-trip against the real Local API — targets INBOX, verifies the marker, cleans up", async () => {
    const backend = new RealTanaBackend();
    const health = await backend.health();
    if (!health.localAvailable) {
      // Soft-skip: Tana being closed is expected, not a failure. Per SPEC.md §9's recovery
      // playbook, this is logged and the test passes trivially rather than red.
      console.warn("[live-smoke] Tana Local API unreachable at :8262 — skipping (Tana appears closed)");
      return;
    }

    const dir = mkdtempSync(join(tmpdir(), "tanastream-livesmoke-"));
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    let createdNodeId: string | undefined;
    try {
      const key = `smoke-${Date.now()}`;
      await enqueueWrite(spool, {
        opType: "create",
        idempotencyKey: key,
        payload: { name: `TanaStream live smoke ${key}` },
        targetNodeId: "INBOX",
        source: "tanastream-live-smoke-test",
      });

      const result = await drainOnce(spool, backend, { nowMs: Date.now() });
      expect(result.kind).toBe("applied");
      if (result.kind !== "applied") return; // narrows for TS; expect() above already failed the test

      const row = spool.getByDedupKey(`create:${key}`);
      expect(row?.state).toBe("applied");
      createdNodeId = typeof row?.evidence?.nodeId === "string" ? row.evidence.nodeId : undefined;
      expect(createdNodeId).toBeTruthy();
      expect(row?.evidence?.markerVerified).toBe(true);
    } finally {
      if (createdNodeId) {
        await backend.trashNode(createdNodeId).catch((err) => {
          console.warn(`[live-smoke] cleanup trash failed for ${createdNodeId} — trash it manually: ${String(err)}`);
        });
      }
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

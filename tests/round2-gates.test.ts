// Round 2 (Opus ultracode) — queue-gate findings:
//   F4  : the Input >5000-char oversized gate must count the appended idempotency-marker child,
//         else a ~4990-char create POSTs ~5030 chars to the Input API (gate promise violated).
//   F5  : the Input rate limiter must clamp elapsed >= 0 and sleepMs to [0, interval] so a
//         backwards clock cannot produce a negative-elapsed oversized sleep.
//   F6  : the idempotency hash must reflect the RESOLVED target (false-collapse guard: two
//         creates to different effective targets never share a key).

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { openSpool } from "../src/spool";
import { drainOnce } from "../src/queue";
import { FakeBackend } from "./fakeBackend";

function tempDir() {
  return mkdtempSync(join(tmpdir(), "tanastream-r2gates-"));
}
function inputBackend() {
  const b = new FakeBackend({ localOpen: false }); // local closed => create routes to Input
  b.inputOpen = true;
  return b;
}

describe("F4 — Input oversized gate counts the appended marker child", () => {
  test("F4a a create under 5000 WITHOUT the marker but over WITH it dead-letters (not sent to Input)", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      // base size (name + "[]") ~4987 < 5000; base + marker child > 5000
      spool.enqueue({ opType: "create", idempotencyKey: "big", payload: { name: "x".repeat(4985) }, targetNodeId: "INBOX", source: "t" });
      const result = await drainOnce(spool, inputBackend(), { nowMs: 1000 });
      expect(result.kind).toBe("dead");
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("F4b a comfortably-small create still routes to Input (no over-rejection)", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      spool.enqueue({ opType: "create", idempotencyKey: "small", payload: { name: "tiny" }, targetNodeId: "INBOX", source: "t" });
      const result = await drainOnce(spool, inputBackend(), { nowMs: 1000 });
      expect(result.kind).toBe("applied");
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("F5 — Input rate limiter clamps elapsed >= 0 and sleepMs <= interval", () => {
  test("F5a a backwards clock between two Input creates yields a bounded (non-negative-elapsed) sleep", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      spool.enqueue({ opType: "create", idempotencyKey: "i1", payload: { name: "a" }, targetNodeId: "INBOX", source: "t" });
      const backend = inputBackend();
      await drainOnce(spool, backend, { nowMs: 10_000 });
      spool.enqueue({ opType: "create", idempotencyKey: "i2", payload: { name: "b" }, targetNodeId: "INBOX", source: "t" });
      const r = await drainOnce(spool, backend, { nowMs: 9_000 }); // clock went backwards
      expect(r.kind).toBe("rate_limited");
      if (r.kind === "rate_limited") {
        expect(r.sleepMs).toBeGreaterThanOrEqual(0);
        expect(r.sleepMs).toBeLessThanOrEqual(1000);
      }
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("F5b a legitimate >1s gap is NOT rate-limited (no over-throttle)", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      spool.enqueue({ opType: "create", idempotencyKey: "j1", payload: { name: "a" }, targetNodeId: "INBOX", source: "t" });
      const backend = inputBackend();
      await drainOnce(spool, backend, { nowMs: 10_000 });
      spool.enqueue({ opType: "create", idempotencyKey: "j2", payload: { name: "b" }, targetNodeId: "INBOX", source: "t" });
      const r = await drainOnce(spool, backend, { nowMs: 11_500 });
      expect(r.kind).toBe("applied");
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("F6 — idempotency hash reflects the resolved target (false-collapse guard)", () => {
  test("F6a two unkeyed creates, same name, different top-level targets do NOT collapse", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      const a = spool.enqueue({ opType: "create", payload: { name: "Same" }, targetNodeId: "NODE_A", source: "t" });
      const b = spool.enqueue({ opType: "create", payload: { name: "Same" }, targetNodeId: "NODE_B", source: "t" });
      expect(a.idempotencyKey).not.toBe(b.idempotencyKey);
      expect(spool.status().pending).toBe(2);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("F6b two unkeyed creates, same name, target carried in payload, different targets do NOT collapse", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      const a = spool.enqueue({ opType: "create", payload: { name: "Same", targetNodeId: "NODE_A" }, source: "t" });
      const b = spool.enqueue({ opType: "create", payload: { name: "Same", targetNodeId: "NODE_B" }, source: "t" });
      expect(a.idempotencyKey).not.toBe(b.idempotencyKey);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("F6c the same unkeyed create dedups (stable key) on re-enqueue", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      const a = spool.enqueue({ opType: "create", payload: { name: "Same" }, targetNodeId: "NODE_A", source: "t" });
      const b = spool.enqueue({ opType: "create", payload: { name: "Same" }, targetNodeId: "NODE_A", source: "t" });
      expect(b.inserted).toBe(false);
      expect(a.idempotencyKey).toBe(b.idempotencyKey);
      expect(spool.status().pending).toBe(1);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

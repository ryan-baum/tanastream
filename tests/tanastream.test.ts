#!/usr/bin/env bun

import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { spawnSync } from "child_process";
import { drainOnce, enqueueWrite, reconcileAppliedInput, recoverInflight } from "../src/queue";
import { buildTanaPaste } from "../src/realBackend";
import { openSpool } from "../src/spool";
import { FakeBackend } from "./fakeBackend";

function tempDir() {
  return mkdtempSync(join(tmpdir(), "tanastream-test-"));
}

function withSpool<T>(fn: (ctx: { dir: string; dbPath: string; spool: ReturnType<typeof openSpool> }) => T | Promise<T>) {
  return async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      return await fn({ dir, dbPath, spool });
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  };
}

describe("TanaStream adversarial matrix", () => {
  test("M0 local create Tana Paste includes header and idempotency marker child", withSpool(async ({ spool }) => {
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "paste",
      payload: { name: "Paste shape" },
      targetNodeId: "INBOX",
      source: "test",
    });
    const row = spool.getByDedupKey("create:paste");
    if (!row) throw new Error("row missing");
    const paste = buildTanaPaste(row);
    expect(paste).toContain("%%tana%%\n- Paste shape");
    expect(paste).toContain("  - TanaStreamIdempotency - create:paste");
  }));

  test("M0b CLI create preserves repeatable child flags", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const cliPath = join(import.meta.dir, "..", "tanastream");
    try {
      const run = spawnSync(cliPath, [
        "enqueue",
        "create",
        "--db",
        dbPath,
        "--name",
        "CLI repeatable children",
        "--target",
        "INBOX",
        "--key",
        "cli-repeat-child",
        "--child",
        "First child",
        "--child",
        "Second child",
      ], { encoding: "utf8" });

      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(0);
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        const row = spool.getByDedupKey("create:cli-repeat-child");
        expect(row?.payload.children).toEqual(["First child", "Second child"]);
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("M1 durable SQLite spool uses WAL/full synchronous mode and records pending writes", withSpool(async ({ spool }) => {
    const inserted = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "m1",
      payload: { name: "Durable" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(inserted.inserted).toBe(true);
    expect(spool.pragma("journal_mode")).toBe("wal");
    expect(spool.pragma("synchronous")).toBe(2);
    expect(spool.status().pending).toBe(1);
  }));

  test("M2 duplicate enqueue dedups identical operation payload and rejects changed payload", withSpool(async ({ spool }) => {
    const first = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "dup",
      payload: { name: "One" },
      targetNodeId: "INBOX",
      source: "test",
    });
    const second = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "dup",
      payload: { name: "One" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(first.id).toBe(second.id);
    expect(second.inserted).toBe(false);
    expect(() => spool.enqueue({
      opType: "create",
      idempotencyKey: "dup",
      payload: { name: "One again" },
      targetNodeId: "INBOX",
      source: "test",
    })).toThrow(/IDEMPOTENCY_CONFLICT/);
    expect(spool.status().pending).toBe(1);
  }));

  test("M12 same idempotency key across different ops does not collapse intent", withSpool(async ({ spool }) => {
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "same-key",
      payload: { name: "Created" },
      targetNodeId: "INBOX",
      source: "test",
    });
    await enqueueWrite(spool, {
      opType: "edit",
      idempotencyKey: "same-key",
      payload: { nodeId: "node-1", name: "Edited" },
      targetNodeId: "node-1",
      source: "test",
    });

    expect(spool.status().pending).toBe(2);
  }));

  test("M5 closed Tana holds mutation but lets a later create route to Input API", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    await enqueueWrite(spool, {
      opType: "edit",
      idempotencyKey: "closed-edit",
      payload: { nodeId: "n1", name: "Nope" },
      targetNodeId: "n1",
      source: "test",
    });
    const create = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "closed-create",
      payload: { name: "Allowed while closed" },
      targetNodeId: "INBOX",
      source: "test",
    });

    const result = await drainOnce(spool, backend, { nowMs: 1_000 });
    expect(result.kind).toBe("applied");
    expect(result.writeId).toBe(create.id);
    expect(spool.status()).toMatchObject({ pending: 1, applied: 1, dead: 0 });
  }));

  test("M6 route is resolved per write when Tana closes mid-drain", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localSequence: [true, false, false] });
    const edit1 = await enqueueWrite(spool, {
      opType: "edit",
      idempotencyKey: "flip-edit-1",
      payload: { nodeId: "n1", name: "First" },
      targetNodeId: "n1",
      source: "test",
    });
    await enqueueWrite(spool, {
      opType: "edit",
      idempotencyKey: "flip-edit-2",
      payload: { nodeId: "n2", name: "Second" },
      targetNodeId: "n2",
      source: "test",
    });
    const create = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "flip-create",
      payload: { name: "Create still eligible" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "applied", writeId: edit1.id, route: "local" });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", writeId: create.id, route: "input" });
    expect(spool.status()).toMatchObject({ pending: 1, applied: 2 });
  }));

  test("M7 Input API route enforces one create per second", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "rate-1",
      payload: { name: "One" },
      targetNodeId: "INBOX",
      source: "test",
    });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "rate-2",
      payload: { name: "Two" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "applied", route: "input" });
    expect(await drainOnce(spool, backend, { nowMs: 1_500 })).toMatchObject({ kind: "rate_limited" });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", route: "input" });
    expect(backend.inputApplyTimes).toEqual([1_000, 2_000]);
  }));

  test("M7c failed Input attempts are rate-limited before retry", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    backend.transientFailures.set("create:input-fail", 1);
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "input-fail",
      payload: { name: "Fails once" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "retry" });
    expect(await drainOnce(spool, backend, { nowMs: 1_100 })).toMatchObject({ kind: "rate_limited", sleepMs: 900 });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", route: "input" });
  }));

  test("M7d closed-app create holds when Input API is unavailable", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    backend.inputOpen = false;
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "no-input",
      payload: { name: "Hold me" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "held", reason: "Input API unavailable; create held" });
    expect(spool.status()).toMatchObject({ pending: 1, applied: 0, dead: 0 });
  }));

  test("M15 atomic inflight claim prevents a second drainer from applying the same row", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: true });
    const row = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "atomic-claim",
      payload: { name: "Only once" },
      targetNodeId: "INBOX",
      source: "test",
    });
    expect(spool.markInflight(row.id, 1_000)).toBe(true);
    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "idle" });
    expect(backend.effects.get("create:atomic-claim")).toBeUndefined();
    recoverInflight(spool);
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", writeId: row.id });
    expect(backend.effects.get("create:atomic-claim")).toBe(1);
  }));

  test("M7b Input API applied rows persist reconcile-on-reopen state", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "input-reconcile",
      payload: { name: "Needs later reconcile" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "applied", route: "input" });
    expect(spool.status().unreconciledInput).toBe(1);

    backend.localOpen = true;
    expect(await reconcileAppliedInput(spool, backend, { nowMs: 2_000 })).toBe(1);
    expect(spool.status().unreconciledInput).toBe(0);
    const row = spool.getByDedupKey("create:input-reconcile");
    expect(row?.needsReconcile).toBe(false);
    expect(row?.reconciledAt).toBe(2_000);
    expect(row?.evidence?.reconcile).toBeTruthy();
  }));

  test("M8 closed-app create over 5,000 chars is rejected instead of sent to Input API", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: false });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "big",
      payload: { name: "x".repeat(5_001) },
      targetNodeId: "INBOX",
      source: "test",
    });
    const good = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "small",
      payload: { name: "small" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "dead" });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", writeId: good.id });
    expect(spool.status()).toMatchObject({ applied: 1, dead: 1 });
  }));

  test("M9 poison payload dead-letters and does not block the following good write", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: true });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "poison",
      payload: { name: "bad", poison: true },
      targetNodeId: "INBOX",
      source: "test",
      maxAttempts: 2,
    });
    const good = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "after-poison",
      payload: { name: "good" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "retry" });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "dead" });
    expect(await drainOnce(spool, backend, { nowMs: 3_000 })).toMatchObject({ kind: "applied", writeId: good.id });
  }));

  test("M4 ambiguous apply reconciles before retrying, preventing double apply", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: true });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "ambiguous",
      payload: { name: "Maybe applied", ambiguousAfterApply: true },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "retry" });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied", reconciled: true });
    expect(backend.effects.get("create:ambiguous")).toBe(1);
  }));

  test("M11 non-Latin and emoji node text survives enqueue and drain", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: true });
    await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "unicode",
      payload: { name: "東京 नदी prueba 🌊" },
      targetNodeId: "INBOX",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "applied" });
    expect(backend.names.get("create:unicode")).toBe("東京 नदी prueba 🌊");
  }));

  test("M13 flaky Local API timeout retries without marking false success", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: true });
    backend.transientFailures.set("edit:flaky", 1);
    await enqueueWrite(spool, {
      opType: "edit",
      idempotencyKey: "flaky",
      payload: { nodeId: "n1", name: "eventual" },
      targetNodeId: "n1",
      source: "test",
    });

    expect(await drainOnce(spool, backend, { nowMs: 1_000 })).toMatchObject({ kind: "retry" });
    expect(spool.status()).toMatchObject({ pending: 1, applied: 0 });
    expect(await drainOnce(spool, backend, { nowMs: 2_000 })).toMatchObject({ kind: "applied" });
  }));

  test("M14 ordering uses priority then id, not wall-clock timestamps", withSpool(async ({ spool }) => {
    const backend = new FakeBackend({ localOpen: true });
    const low = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "low",
      payload: { name: "low" },
      targetNodeId: "INBOX",
      source: "test",
      priority: 0,
    });
    const high = await enqueueWrite(spool, {
      opType: "create",
      idempotencyKey: "high",
      payload: { name: "high" },
      targetNodeId: "INBOX",
      source: "test",
      priority: 10,
    });

    expect(high.id).toBeGreaterThan(low.id);
    expect(await drainOnce(spool, backend, { nowMs: 999_999_999 })).toMatchObject({ kind: "applied", writeId: high.id });
    // U-4/R-7: the Local-route pacing gate (KTD-4) is orthogonal to this test's actual subject
    // (ordering by priority/id, not wall-clock) — the deliberate backwards clock jump to nowMs=1
    // would otherwise collide with the new default-on pacing gate (elapsed clamps to 0 < 100ms).
    // localMinIntervalMs: 0 neutralizes that unrelated feature without touching what's asserted.
    expect(await drainOnce(spool, backend, { nowMs: 1, localMinIntervalMs: 0 })).toMatchObject({ kind: "applied", writeId: low.id });
  }));

  test("M10 corrupt spool file is moved aside (timestamped, never deleted) and a clean spool starts", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    writeFileSync(dbPath, "not sqlite");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      expect(spool.recoveredCorruption).toBe(true);
      expect(spool.status()).toMatchObject({ pending: 0, applied: 0, dead: 0 });
      // Forge-audit fix (U-10, Finding 4): the backup name is now timestamped (spool.db.corrupt-<ISO>),
      // not a fixed ".corrupt" suffix, so a later corruption event can't clobber this one.
      const backups = readdirSync(dir).filter((f) => f.startsWith("spool.db.corrupt-"));
      expect(backups.length).toBe(1);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  // Forge-audit fix (U-10, Finding 4), both directions: (1) WAL/SHM are RENAMED alongside the
  // main db, never DELETED by our own code — a general property that matters most when corruption
  // is detected via PRAGMA integrity_check (the open itself succeeds, so the WAL can still hold
  // real committed content at that point); (2) a SECOND corruption event on a fresh spool must
  // not destroy the FIRST backup.
  //
  // NOTE (genuine finding from writing this test): for the "not a database" structural-failure
  // class specifically (as opposed to an integrity_check failure on an openable file), SQLite's
  // OWN `PRAGMA journal_mode = WAL` call truncates -wal to empty and rewrites -shm as a side
  // effect of the failed open attempt — BEFORE our corruption handler ever runs. So this
  // reproduction can't assert pre-seeded WAL *content* survives; it asserts the file is RENAMED
  // (not silently deleted) either way, which is the property our own code controls and Forge
  // flagged as missing.
  test("M10b corrupt recovery renames -wal/-shm as backups (never deletes), whatever their content", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    writeFileSync(dbPath, "not sqlite");
    writeFileSync(`${dbPath}-wal`, "wal contents (may be zeroed by SQLite's own failed-open attempt)");
    writeFileSync(`${dbPath}-shm`, "shm contents");
    const spool = openSpool({ dbPath, recoverCorrupt: true });
    try {
      const backups = readdirSync(dir);
      const walBackup = backups.find((f) => f.startsWith("spool.db.corrupt-") && f.endsWith("-wal"));
      const shmBackup = backups.find((f) => f.startsWith("spool.db.corrupt-") && f.endsWith("-shm"));
      expect(walBackup).toBeDefined();
      expect(shmBackup).toBeDefined();
      // Note: `${dbPath}-wal` legitimately exists again after this point — it's the FRESH spool's
      // own new WAL file (every WAL-mode SQLite db has one), not the old one; the backup's
      // existence under the timestamped name is what proves the OLD one was renamed, not deleted.
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("M10c a second corruption event does not destroy the first backup", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    writeFileSync(dbPath, "not sqlite (first corruption)");
    const spool1 = openSpool({ dbPath, recoverCorrupt: true });
    spool1.close();
    // Corrupt the FRESH spool again (a second, later corruption event).
    writeFileSync(dbPath, "not sqlite (second corruption)");
    const spool2 = openSpool({ dbPath, recoverCorrupt: true });
    try {
      const backups = readdirSync(dir).filter((f) => f.startsWith("spool.db.corrupt-") && !f.endsWith("-wal") && !f.endsWith("-shm"));
      // Two DISTINCT backup files — the second recovery must not have deleted the first.
      expect(backups.length).toBe(2);
    } finally {
      spool2.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("M3 actual kill after apply then restart reconciles exactly once", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const effectsPath = join(dir, "effects.json");
    const worker = join(import.meta.dir, "crash-worker.ts");
    try {
      const crashed = Bun.spawn([process.execPath, worker, dbPath, effectsPath, "crash"], {
        stdout: "pipe",
        stderr: "pipe",
      });
      await crashed.exited.catch(() => {});
      expect(crashed.exitCode).not.toBe(0);

      const restarted = Bun.spawn([process.execPath, worker, dbPath, effectsPath, "restart"], {
        stdout: "pipe",
        stderr: "pipe",
      });
      expect(await restarted.exited).toBe(0);

      const effects = JSON.parse(readFileSync(effectsPath, "utf-8")) as Record<string, number>;
      expect(effects["create:crash-key"]).toBe(1);

      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        recoverInflight(spool);
        expect(spool.status()).toMatchObject({ pending: 0, applied: 1, dead: 0 });
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

#!/usr/bin/env bun

// Hardening fixes from the 2026-06-13 stress-test pass: F2b (fail-closed corruption recovery),
// F3 (fail-closed CLI value-flag parsing), F2 (single-drainer mutual-exclusion lock).
// All deterministic — no timing races.

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { spawnSync } from "child_process";
import { openSpool, isCorruptionError } from "../src/spool";
import { acquireDrainLock } from "../src/lock";

function tempDir() {
  return mkdtempSync(join(tmpdir(), "tanastream-harden-"));
}
const CLI = join(import.meta.dir, "..", "tanastream");

describe("F2b — fail-closed corruption recovery", () => {
  // VERIFIER both directions: only genuine-corruption signatures may trigger move-aside.
  test("H1 isCorruptionError classifies genuine corruption true, transient/IO false", () => {
    for (const m of [
      "SQLite integrity check failed: ...",
      "file is not a database",
      "database disk image is malformed",
      "file is encrypted or is not a database",
    ]) expect(isCorruptionError(new Error(m))).toBe(true);

    for (const m of [
      "database is locked",
      "disk I/O error",
      "SQLITE_BUSY: database is locked",
      "unable to open database file",
      "some unknown transient error",
    ]) expect(isCorruptionError(new Error(m))).toBe(false);
  });

  // A valid spool with producer-acked rows must re-open WITHOUT recovery — never destroyed.
  test("H2 valid existing spool re-opens with rows intact, no recovery", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const a = openSpool({ dbPath, recoverCorrupt: true });
      a.enqueue({ opType: "create", idempotencyKey: "keep", payload: { name: "Durable" }, targetNodeId: "INBOX", source: "t" });
      a.close();
      const b = openSpool({ dbPath, recoverCorrupt: true });
      try {
        expect(b.recoveredCorruption).toBe(false);
        expect(b.status().pending).toBe(1); // row survived — not moved aside
      } finally {
        b.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("F3 — fail-closed CLI value-flag parsing", () => {
  function enqueue(dbPath: string, args: string[]) {
    return spawnSync(CLI, ["enqueue", "create", "--db", dbPath, ...args], { encoding: "utf8" });
  }

  test("H3 --child=-mid preserves a value starting with '-'", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = enqueue(dbPath, ["--name", "N", "--key", "k", "--child=-mid", "--child", "ok"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(0);
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        expect(spool.getByDedupKey("create:k")?.payload.children).toEqual(["-mid", "ok"]);
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("H4 --name=-5 kg accepts a '-'-leading name via the = form", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = enqueue(dbPath, ["--name=-5 kg", "--key", "k2"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(0);
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        expect(spool.getByDedupKey("create:k2")?.payload.name).toBe("-5 kg");
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("H5 a dropped '-'-leading value errors loudly instead of vanishing", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = enqueue(dbPath, ["--name", "N", "--key", "k3", "--child", "-mid"]);
      expect(run.status).not.toBe(0); // fail-closed: non-zero exit
      expect(run.stderr).toContain("expects a value");
      // and nothing was persisted
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        expect(spool.getByDedupKey("create:k3")).toBeNull();
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("F2 — single-drainer mutual-exclusion lock", () => {
  test("H6 acquire then release then re-acquire", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const a = acquireDrainLock(dbPath);
      expect(a).not.toBeNull();
      a!.release();
      const b = acquireDrainLock(dbPath);
      expect(b).not.toBeNull();
      b!.release();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("H7 refuses a second acquire while the first is still held (flock mutual exclusion)", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const a = acquireDrainLock(dbPath);
      expect(a).not.toBeNull();
      expect(acquireDrainLock(dbPath)).toBeNull(); // flock already held -> refuse (fail-closed)
      a!.release();
      const c = acquireDrainLock(dbPath);
      expect(c).not.toBeNull(); // released -> acquirable again
      c!.release();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("H8 a leftover lock file with no live holder is acquirable (no stale-lock dance)", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      writeFileSync(`${dbPath}.lock`, "leftover content from a dead process"); // no flock held
      const a = acquireDrainLock(dbPath);
      expect(a).not.toBeNull(); // flock not held -> freely acquirable
      a!.release();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

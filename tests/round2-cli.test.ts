// Round 2 (Opus ultracode) — NEW HIGH finding N1: inline `--flag=value` must not truncate the
// value at an internal '='. `split("=", 2)` keeps only the first two elements and DISCARDS the
// tail, so `--child=A=B` silently becomes "A" and `--key=a=b` poisons the dedup/marker/reconcile
// identity. Fully silent, zero exit code. Both-direction: full value preserved AND the simple
// no-'=' case still works (no over-correction).

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { spawnSync } from "child_process";
import { openSpool } from "../src/spool";

const CLI = join(import.meta.dir, "..", "tanastream");
function tempDir() {
  return mkdtempSync(join(tmpdir(), "tanastream-r2cli-"));
}
function enqueue(dbPath: string, args: string[]) {
  return spawnSync(CLI, ["enqueue", "create", "--db", dbPath, ...args], { encoding: "utf8" });
}

describe("N1 — inline --flag=value must not truncate at an internal '='", () => {
  test("N1a --child=A=B preserves the full value (no split-at-second-= truncation)", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = enqueue(dbPath, ["--name", "N", "--key", "k", "--child=A=B"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(0);
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        expect(spool.getByDedupKey("create:k")?.payload.children).toEqual(["A=B"]);
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("N1b --key=a=b=c keeps the whole idempotency key (dedup/marker identity not poisoned)", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = enqueue(dbPath, ["--name", "N", "--key=a=b=c"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(0);
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        // dedupKey is `create:<idempotencyKey>`; the full key must survive intact
        expect(spool.getByDedupKey("create:a=b=c")).not.toBeNull();
        expect(spool.getByDedupKey("create:a")).toBeNull();
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("N1c --description=k=v=x preserves the full description", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = enqueue(dbPath, ["--name", "N", "--key", "k2", "--description=k=v=x"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(0);
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        expect(spool.getByDedupKey("create:k2")?.payload.description).toBe("k=v=x");
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("N1d --child=Plain still preserves a value with no '=' (no over-correction)", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = enqueue(dbPath, ["--name", "N", "--key", "k3", "--child=Plain"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(0);
      const spool = openSpool({ dbPath, recoverCorrupt: true });
      try {
        expect(spool.getByDedupKey("create:k3")?.payload.children).toEqual(["Plain"]);
      } finally {
        spool.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

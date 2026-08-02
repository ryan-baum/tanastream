// flock-based single-drainer lock (replaces the prior `<pid>:<start-token>` lockfile scheme). The old
// scheme had a residual stale-reclaim TOCTOU — `reclaimIfStale` read the holder then `unlinkSync`d it
// non-atomically, so two reclaimers could both delete-and-recreate and admit two drainers. A 12-way race
// reproduced it (reliably, on 2026-06-15), and Cato confirmed it cross-vendor. flock binds exclusivity to
// the kernel and the open fd: it auto-releases on fd close OR process death, so there is no stale lock to
// reclaim and no read-then-unlink TOCTOU. The racer detects only SIMULTANEOUS holders (a real violation);
// sequential acquire-after-release is correct.

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { acquireDrainLock } from "../src/lock";

function tempDir() {
  return mkdtempSync(join(tmpdir(), "tanastream-r2lock-"));
}
const RACER = join(import.meta.dir, "lock-racer.ts");

describe("flock — auto-release on death; no stale-lock to reclaim", () => {
  test("L1 a leftover lock file with no live flock holder is freely acquirable (no stale-lock dance)", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      writeFileSync(`${dbPath}.lock`, "leftover content from a long-dead process");
      const a = acquireDrainLock(dbPath);
      expect(a).not.toBeNull(); // nobody holds the flock -> acquire succeeds, no reclaim required
      a?.release();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("L2 a SIGKILLed holder's lock auto-releases (the stale-lock + TOCTOU class is gone)", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const holder = Bun.spawn(["bun", RACER, dbPath, "10000"], { stdout: "pipe", stderr: "pipe" });
    try {
      const reader = holder.stdout.getReader();
      const dec = new TextDecoder();
      let got = "";
      while (!got.includes("ACQUIRED") && !got.includes("REFUSED")) {
        const { value, done } = await reader.read();
        if (done) break;
        got += dec.decode(value);
      }
      expect(got).toContain("ACQUIRED");
      expect(acquireDrainLock(dbPath)).toBeNull(); // refused while the holder is alive (fail-closed)
      holder.kill("SIGKILL"); // hard kill — no chance to run release()
      await holder.exited;
      const a = acquireDrainLock(dbPath); // kernel auto-released the flock on death -> acquirable
      expect(a, "lock must auto-release on holder death — no stale lock").not.toBeNull();
      a?.release();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("N3 — stale-reclaim must not admit two drainers (anti-clobber)", () => {
  test("L3 a live foreign holder is refused, not clobbered", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    const holder = Bun.spawn(["bun", RACER, dbPath, "1500"], { stdout: "pipe", stderr: "pipe" });
    try {
      const reader = holder.stdout.getReader();
      const dec = new TextDecoder();
      let got = "";
      while (!got.includes("ACQUIRED") && !got.includes("REFUSED")) {
        const { value, done } = await reader.read();
        if (done) break;
        got += dec.decode(value);
      }
      expect(got).toContain("ACQUIRED");
      expect(acquireDrainLock(dbPath)).toBeNull(); // must refuse the live holder, never clobber it
    } finally {
      await holder.exited;
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("L4 a 12-way race never admits two SIMULTANEOUS drainers (no double-hold)", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      writeFileSync(`${dbPath}.lock`, "leftover stale content from a dead process");
      const racers = Array.from({ length: 12 }, () =>
        Bun.spawn(["bun", RACER, dbPath, "120"], { stdout: "pipe", stderr: "pipe" }));
      const outs = await Promise.all(
        racers.map(async (r) => {
          await r.exited;
          return (await new Response(r.stdout).text()).trim();
        }),
      );
      const doubles = outs.filter((o) => o.includes("DOUBLE")).length;
      const acquired = outs.filter((o) => o.includes("ACQUIRED")).length;
      // flock NEVER admits two SIMULTANEOUS holders; sequential acquire-after-release is correct.
      expect(doubles, `outputs: ${JSON.stringify(outs)}`).toBe(0);
      expect(acquired).toBeGreaterThanOrEqual(1);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

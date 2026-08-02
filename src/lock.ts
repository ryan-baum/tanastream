import { closeSync, openSync } from "fs";
import { dlopen, FFIType } from "bun:ffi";

export interface DrainLock {
  release(): void;
}

/**
 * Single-drainer mutual exclusion via flock(2) — a kernel-enforced advisory lock bound to an open file
 * descriptor.
 *
 * The 24/7 launchd daemon and a manual `tanastream drain` MUST NOT both drain the same spool — a second
 * `recoverInflight` flips the first process's in-flight row back to pending and double-applies it to the
 * real graph. flock gives that exclusivity WITHOUT a pathname compare-and-swap: the lock is held for the
 * lifetime of the open fd and auto-released the instant the fd closes OR the holding process dies. So
 * there is NO stale lock to reclaim and NO read-then-unlink TOCTOU.
 *
 * (The prior `<pid>:<start-token>` lockfile scheme had a residual stale-reclaim race — `reclaimIfStale`
 * read the holder then `unlinkSync`d non-atomically, so two reclaimers could both delete-and-recreate
 * and admit two drainers. A 12-way race test reproduced it. flock removes the race class entirely by
 * binding exclusivity to the kernel, not to replaceable file contents — confirmed cross-vendor.)
 *
 * LOCK_EX | LOCK_NB returns immediately: 0 when we acquired it, -1/EWOULDBLOCK when a live holder owns it
 * (we refuse — fail-closed). Scoped per-db: a temp-db drain never blocks the production daemon.
 */
const LOCK_EX = 2;
const LOCK_NB = 4;
const LOCK_UN = 8;

const libc = dlopen(process.platform === "darwin" ? "libSystem.B.dylib" : "libc.so.6", {
  flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
});

export function acquireDrainLock(dbPath: string): DrainLock | null {
  const lockPath = `${dbPath}.lock`;
  let fd: number;
  try {
    // "a" = open-or-create WITHOUT truncating. The file CONTENT is irrelevant — exclusivity is the
    // kernel flock on this fd, not anything written to the file (a leftover lockfile from a dead
    // process carries no lock and is freely acquirable).
    fd = openSync(lockPath, "a");
  } catch {
    return null; // cannot open the lock file (e.g. the spool dir is gone) — refuse, fail-closed
  }
  const rc = libc.symbols.flock(fd, LOCK_EX | LOCK_NB);
  if (rc !== 0) {
    closeSync(fd); // a live holder owns the advisory lock — refuse (fail-closed)
    return null;
  }
  let released = false;
  return {
    release(): void {
      if (released) return;
      released = true;
      try {
        libc.symbols.flock(fd, LOCK_UN);
      } catch {
        /* best-effort; closing the fd releases the lock anyway */
      }
      try {
        closeSync(fd);
      } catch {
        /* already closed */
      }
    },
  };
}

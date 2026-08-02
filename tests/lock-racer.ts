#!/usr/bin/env bun
// Worker for the flock concurrency tests. Acquire the drain lock, then detect any CONCURRENT holder by
// creating an EXCLUSIVE marker file WHILE inside the critical section. If flock is sound, only one
// worker is ever in the section at a time, so the marker create (openSync "wx") always succeeds ->
// "ACQUIRED". If two workers ever hold the lock simultaneously, the second's "wx" create fails ->
// "DOUBLE" (the real bug — two drainers). Sequential acquisition (acquire, release, another acquires)
// is CORRECT and reports multiple ACQUIRED; only a DOUBLE indicates a mutual-exclusion violation.
import { acquireDrainLock } from "../src/lock";
import { closeSync, openSync, unlinkSync } from "fs";

const dbPath = process.argv[2];
const holdMs = Number(process.argv[3] ?? "300");
const lock = acquireDrainLock(dbPath);
if (!lock) {
  console.log("REFUSED");
  process.exit(0);
}
const marker = `${dbPath}.held`;
let created = false;
try {
  closeSync(openSync(marker, "wx")); // exclusive create — fails if a concurrent holder already has it
  created = true;
} catch {
  /* another holder is concurrently in the critical section — mutual exclusion was violated */
}
console.log(created ? "ACQUIRED" : "DOUBLE");
await Bun.sleep(holdMs);
if (created) {
  try {
    unlinkSync(marker);
  } catch {
    /* gone */
  }
}
lock.release();

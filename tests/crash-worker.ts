#!/usr/bin/env bun

import { openSpool } from "../src/spool";
import { drainOnce, enqueueWrite, recoverInflight } from "../src/queue";
import { FileEffectBackend } from "./fakeBackend";

const [, , dbPath, effectsPath, mode] = process.argv;

if (!dbPath || !effectsPath || !mode) {
  console.error("usage: crash-worker.ts <dbPath> <effectsPath> <mode>");
  process.exit(2);
}

const spool = openSpool({ dbPath, recoverCorrupt: true });
await enqueueWrite(spool, {
  opType: "create",
  idempotencyKey: "crash-key",
  payload: { name: "Crash replay node" },
  targetNodeId: "INBOX",
  source: "crash-worker",
});

recoverInflight(spool);
const backend = new FileEffectBackend(effectsPath, { crashKey: mode === "crash" ? "crash-key" : undefined });
// U-4/R-7: the Local-route pacing gate (KTD-4) persists its last-attempt timestamp in the spool
// DB (survives the SIGKILL by design — it's written before apply, same as noteInputAttempt). Both
// the "crash" and "restart" invocations use the same fixed nowMs=1_000 (this test isn't about
// pacing, it's about exactly-once reconcile across a real process kill), so without disabling
// pacing here the restart's drainOnce would see elapsed=0 and rate-limit instead of applying.
await drainOnce(spool, backend, { nowMs: 1_000, localMinIntervalMs: 0 });
spool.close();

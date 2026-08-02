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
await drainOnce(spool, backend, { nowMs: 1_000 });
spool.close();

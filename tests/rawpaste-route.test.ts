// Class-sweep companion to the 2026-08-03 rawTanaPaste retry-storm fix in the origin build:
// isRawPasteCreate() branched on `typeof payload.tanaPaste === "string"` when it meant "any raw
// opt-in" (validate.ts's isRaw). The verifier half of that class never existed here — localCreate
// already opts out via isRaw() and raw creates carry no marker — but the ROUTING half did: with
// the Local API down and Input up, a {name: "X #tag", rawTanaPaste: true} create routed to the
// Input API, which does NOT parse Tana Paste. The name lands as literal text (the tag the
// producer opted into raw syntax FOR is silently lost) and, with no marker, the input row can
// never be reconciled. Raw creates of either form are Local-only: hold, don't divert.
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { openSpool } from "../src/spool";
import { drainOnce, enqueueWrite } from "../src/queue";
import { FakeBackend } from "./fakeBackend";

describe("chooseRoute — rawTanaPaste:true creates are Local-only, like tanaPaste-string creates", () => {
  test("with Tana closed, a rawTanaPaste:true create is HELD, never sent to Input as literal text", async () => {
    const dir = mkdtempSync(join(tmpdir(), "tanastream-rawpaste-route-"));
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    try {
      const backend = new FakeBackend({ localOpen: false }); // Input up by default
      const enq = await enqueueWrite(spool, {
        opType: "create",
        idempotencyKey: "raw-flag-hold",
        payload: { name: "Weaving Strategy #project", rawTanaPaste: true },
        targetNodeId: "INBOX",
        source: "test",
      });
      const result = await drainOnce(spool, backend, { nowMs: 1_000 });
      expect(result.kind).toBe("held");
      expect(spool.getById(enq.id)?.holdReason).toBe("Local API unavailable; raw Tana Paste create requires the Local route");
      expect(backend.effects.size).toBe(0); // nothing applied via Input
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("regression: a STRUCTURED create (no raw opt-in) still falls back to Input when Local is down", async () => {
    const dir = mkdtempSync(join(tmpdir(), "tanastream-structured-route-"));
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    try {
      const backend = new FakeBackend({ localOpen: false });
      await enqueueWrite(spool, {
        opType: "create",
        idempotencyKey: "structured-fallback",
        payload: { name: "Plain literal create" },
        targetNodeId: "INBOX",
        source: "test",
      });
      const result = await drainOnce(spool, backend, { nowMs: 1_000 });
      expect(result).toMatchObject({ kind: "applied", route: "input" });
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

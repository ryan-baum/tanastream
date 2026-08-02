#!/usr/bin/env bun

// F1 fix — Tana-Paste injection guard. Frozen BEFORE the validator logic (TDD, verifier-class).
// Both directions: REJECT cases (false-accept probes) AND PASS cases (false-reject probes — the
// silent-rejection direction Ryan flagged: plain punctuation and Unicode MUST pass).
// reject-loud + rawTanaPaste opt-out, per the agreed design (no escaping).

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { spawnSync } from "child_process";
import { findTanaHazard, assertCreateSafe } from "../src/validate";
import { openSpool } from "../src/spool";
import { enqueueWrite } from "../src/queue";

const CLI = join(import.meta.dir, "..", "tanastream");
function tempDir() { return mkdtempSync(join(tmpdir(), "tanastream-inj-")); }

// ---- REJECT: content that Tana Paste would reinterpret (must be flagged) ----
const HAZARDS: Array<[string, string]> = [
  ["field injection ::", "Status:: Done"],
  ["field injection :: mid", "Author:: Karl Marx"],
  ["field injection :: code", "std::vector usage"],
  ["paste directive %%", "embedded %%tana%% header"],
  ["node reference [[", "see [[Other Node]]"],
  ["reference id ^", "node ^abc123"],
  ["leading supertag #", "#urgent thing"],
  ["whitespace supertag #", "meet #urgent person"],
  ["leading bullet - ", "- a sub item"],
  ["numbered list 1.", "1. first step"],
  ["numbered list 12.", "12. twelfth"],
];

// ---- PASS: plain punctuation + Unicode that must NOT be rejected (false-reject probes) ----
const SAFE: string[] = [
  "東京 नदी prueba 🌊",                  // unicode/emoji
  "ratio 16:9 widescreen",                // single colon
  "see p. 10 for details",                // period-space mid-text
  "subnet 10.0.0.1",                      // dotted but not "N. "
  "3.14 is pi",                           // decimal, no space after dot
  "50% off today",                        // single %
  "C# and F# notes",                      // # not whitespace-preceded
  "email me at a@b.com",                  // @ is not a Tana sigil
  "https://example.com/path?q=1",         // : and // but no ::
  "cost (with tax) & fees",               // parens, ampersand
  "-5 kg net",                            // leading '-' but NOT '- ' (no space)
  "Q1 2026 plan",                         // plain
  "TanaStream-StressTest-20260613-plain", // our own naming scheme
];

describe("F1 — findTanaHazard (verifier, both directions)", () => {
  for (const [label, text] of HAZARDS) {
    test(`REJECT: ${label}`, () => {
      expect(findTanaHazard(text)).not.toBeNull();
    });
  }
  for (const text of SAFE) {
    test(`PASS: ${JSON.stringify(text).slice(0, 40)}`, () => {
      expect(findTanaHazard(text)).toBeNull();
    });
  }
});

describe("F1 — assertCreateSafe", () => {
  test("throws on a hazardous name", () => {
    expect(() => assertCreateSafe({ name: "Status:: Done" })).toThrow();
  });
  test("throws on a hazardous child", () => {
    expect(() => assertCreateSafe({ name: "OK", children: ["fine", "Priority:: High"] })).toThrow();
  });
  test("throws on a hazardous description", () => {
    expect(() => assertCreateSafe({ name: "OK", description: "see [[X]]" })).toThrow();
  });
  test("passes clean structured content", () => {
    expect(() => assertCreateSafe({ name: "Meeting 16:9", description: "notes", children: ["a", "b 🌊"] })).not.toThrow();
  });
  test("rawTanaPaste opt-out bypasses validation", () => {
    expect(() => assertCreateSafe({ name: "Status:: Done", rawTanaPaste: true })).not.toThrow();
  });
  test("tanaPaste raw string bypasses validation", () => {
    expect(() => assertCreateSafe({ tanaPaste: "%%tana%%\n- Status:: Done" })).not.toThrow();
  });
});

describe("F1 — enqueue rejects hazardous creates loudly", () => {
  test("spool.enqueue throws on a :: name (fail-closed, nothing queued)", () => {
    const dir = tempDir();
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    try {
      expect(enqueueWrite(spool, { opType: "create", idempotencyKey: "bad", payload: { name: "TODO:: refactor" }, targetNodeId: "INBOX", source: "t" }))
        .rejects.toThrow();
      expect(spool.status().pending).toBe(0);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("non-create ops are not subject to Tana-Paste validation", async () => {
    const dir = tempDir();
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    try {
      // an edit setting a name with '::' is a Local-API supertag edit, not Tana Paste — must enqueue fine
      const r = await enqueueWrite(spool, { opType: "edit", idempotencyKey: "e", payload: { nodeId: "n1", name: "Status:: Done" }, targetNodeId: "n1", source: "t" });
      expect(r.inserted).toBe(true);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("CLI enqueue of a :: name exits non-zero with a clear message", () => {
    const dir = tempDir();
    const dbPath = join(dir, "spool.db");
    try {
      const run = spawnSync(CLI, ["enqueue", "create", "--db", dbPath, "--name", "Status:: Done", "--key", "k"], { encoding: "utf8" });
      expect(run.status).not.toBe(0);
      expect(`${run.stdout}${run.stderr}`.toLowerCase()).toContain("tana");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

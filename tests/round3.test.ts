// Round 3 (Opus ultracode) — fixes for the fresh blind re-audit's NEW confirmed CRITICAL/HIGH findings.
// Each is the silent verification-false-reject / denylist-bypass class (the BUG-2 25-orphan amplification
// family) or a fail-closed CLI contract. Both-direction: the hazard is rejected AND the clean case passes.
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { spawnSync } from "child_process";
import { assertCreateSafe, assertKeySafe } from "../src/validate";
import { markerFor } from "../src/realBackend";
import type { WriteRow } from "../src/types";

const CLI = join(import.meta.dir, "..", "tanastream");
const createRow = (payload: Record<string, unknown>, dedupKey = "create:k"): WriteRow =>
  ({ opType: "create", dedupKey, payload } as unknown as WriteRow);
const tmp = () => mkdtempSync(join(tmpdir(), "tanastream-r3-"));
const enqueue = (dbPath: string, args: string[]) =>
  spawnSync(CLI, ["enqueue", "create", "--db", dbPath, ...args], { encoding: "utf8" });

describe("R3 encoding/critical — raw tanaPaste create gets NO idempotency marker (no orphan amplification)", () => {
  test("markerFor returns null for a raw-tanaPaste create (verification won't require an uninjected marker)", () => {
    expect(markerFor(createRow({ tanaPaste: "%%tana%%\n- Raw Node" }))).toBeNull();
  });
  test("markerFor still returns a marker for a structured create (no over-correction)", () => {
    const m = markerFor(createRow({ name: "Structured" }));
    expect(m).not.toBeNull();
    expect(m).toContain("create:k");
  });
  test("markerFor still honors explicit no-marker opt-out", () => {
    expect(markerFor(createRow({ name: "X", includeIdempotencyMarker: false }))).toBeNull();
  });
});

describe("R3 encoding/high — injection denylist typed hole (non-string child name)", () => {
  test("array-wrapped hazard child name is rejected at enqueue (no typed-hole bypass)", () => {
    expect(() => assertCreateSafe({ name: "Clean Title", children: [{ name: ["Status:: Done"] }] })).toThrow(/TANA_PASTE_UNSAFE/);
  });
  test("number-coerced hazard child name is rejected", () => {
    expect(() => assertCreateSafe({ name: "Clean", children: [{ name: { toString: () => "a [[ref]]" } }] })).toThrow(/TANA_PASTE_UNSAFE/);
  });
  test("clean object child name still passes (no over-correction)", () => {
    expect(() => assertCreateSafe({ name: "Clean", children: [{ name: "fine note" }] })).not.toThrow();
  });
});

describe("R3 idempotency/high — assertKeySafe rejects collapse-unstable keys (both directions)", () => {
  test.each(["session  abc", "session\tabc", "session\nabc", "sessionabc ", " sessionabc", "a\r\nb"])(
    "rejects unstable key %j",
    (k) => expect(() => assertKeySafe(k)).toThrow(/TANA_KEY_UNSAFE/),
  );
  test.each(["session-abc", "a b c", "601f5abe9c2d", "op:123", "a=b=c"])(
    "accepts collapse-stable key %j",
    (k) => expect(() => assertKeySafe(k)).not.toThrow(),
  );
});

describe("R3 idempotency/high — enqueue rejects an unsafe explicit --key end-to-end", () => {
  test("double-space --key fails closed at enqueue", () => {
    const dir = tmp();
    try {
      const run = enqueue(join(dir, "s.db"), ["--name", "N", "--key=a  b"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(1);
      expect(run.stderr).toContain("TANA_KEY_UNSAFE");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  test("clean --key still enqueues (no over-correction)", () => {
    const dir = tmp();
    try {
      expect(enqueue(join(dir, "s.db"), ["--name", "N", "--key", "clean-key"]).status).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("R3 cli/high — repeated single-value flag fails closed (no silent default)", () => {
  test("--key given twice errors (does not fall back to a content hash)", () => {
    const dir = tmp();
    try {
      const run = enqueue(join(dir, "s.db"), ["--name", "N", "--key", "a", "--key", "b"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(1);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  test("--key given once still works (no over-correction)", () => {
    const dir = tmp();
    try {
      expect(enqueue(join(dir, "s.db"), ["--name", "N", "--key", "single"]).status).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  // Cato cross-vendor finding: the first fix missed the boolean-then-string repetition (`--key --key good`),
  // where the first occurrence parsed as a bare boolean and the second REPLACED it with a string.
  test("--key boolean-then-string (`--key --key good`) fails closed", () => {
    const dir = tmp();
    try {
      const run = enqueue(join(dir, "s.db"), ["--name", "N", "--key", "--key", "good"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(1);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  test("--target given twice fails closed", () => {
    const dir = tmp();
    try {
      const run = enqueue(join(dir, "s.db"), ["--name", "N", "--key", "k", "--target", "INBOX", "--target", "LIBRARY"]);
      expect(run.status, `${run.stdout}\n${run.stderr}`).toBe(1);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

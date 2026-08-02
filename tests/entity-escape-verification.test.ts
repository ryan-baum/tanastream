// Regression: 2026-08-02 five-duplicate incident (spool row 584). Tana's Local API HTML-escapes
// entities on read-back; the create verifier and the reconcile name gate both compared raw vs
// escaped, so a name containing "&" failed verification, missed reconcile, and re-created on every
// retry (5 real duplicates, then dead). Both failure directions are probed here per verifier-class
// discipline: the escaped-equal case must PASS, and a genuine mismatch must still FAIL.
import { describe, expect, test } from "bun:test";
import { decodeTanaEntities, readbackTextEqual } from "../src/realBackend";

describe("decodeTanaEntities", () => {
  test("decodes the escape set Tana emits", () => {
    expect(decodeTanaEntities("Downey &amp; Feldman")).toBe("Downey & Feldman");
    expect(decodeTanaEntities("a &lt;b&gt; &quot;c&quot; &#39;d&#39; &apos;e&apos;")).toBe(`a <b> "c" 'd' 'e'`);
  });

  test("leaves unknown entities and plain text untouched", () => {
    expect(decodeTanaEntities("R&D — no entity")).toBe("R&D — no entity");
    expect(decodeTanaEntities("&unknown;")).toBe("&unknown;");
  });
});

describe("readbackTextEqual (the comparator behind create-verify and reconcile)", () => {
  const raw = "Peer-reviewed handle: Rejection Sensitivity (Downey & Feldman)";
  const escaped = "Peer-reviewed handle: Rejection Sensitivity (Downey &amp; Feldman)";

  test("ACCEPT direction: escaped read-back equals raw payload (the 5-duplicate false mismatch)", () => {
    expect(readbackTextEqual(escaped, raw)).toBe(true);
    expect(readbackTextEqual(raw, escaped)).toBe(true); // either side may arrive escaped
  });

  test("ACCEPT direction: whitespace-collapse still applies alongside decoding", () => {
    expect(readbackTextEqual("A  &amp;   B", "A & B")).toBe(true);
  });

  test("REJECT direction: a genuinely different name still mismatches", () => {
    expect(readbackTextEqual("Downey &amp; Feldman", "Downey & Fieldman")).toBe(false);
    expect(readbackTextEqual("Something else entirely", raw)).toBe(false);
  });

  test("REJECT direction: truncated read-back is not equal (misparse must stay loud)", () => {
    expect(readbackTextEqual("Peer-reviewed handle: Rejection Sensitivity", raw)).toBe(false);
  });

  test("null handling: only null==null passes", () => {
    expect(readbackTextEqual(null, null)).toBe(true);
    expect(readbackTextEqual(null, raw)).toBe(false);
  });
});

describe("audit folds (Forge 2026-08-02)", () => {
  test("documented trade: literal '&amp;' text compares equal to '&' (accepted, safer than raw compare)", () => {
    expect(readbackTextEqual("A &amp; B", "A & B")).toBe(true);
  });

  test("CLI direct execution is no longer a silent no-op (import.meta.main guard)", async () => {
    const proc = Bun.spawn(["bun", new URL("../src/cli.ts", import.meta.url).pathname, "help"], { stdout: "pipe", stderr: "pipe" });
    const out = await new Response(proc.stdout).text();
    await proc.exited;
    expect(out.length).toBeGreaterThan(0); // pre-fix: main() never ran, output was empty
  });
});

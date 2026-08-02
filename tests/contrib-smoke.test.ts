// U-6 (SPEC.md §8): portability contrib smoke tests. Implementation-defined per the unit's own
// "Tests first" note: a shell-check style smoke wired into bun test via Bun.spawnSync, rather than
// a separate scripts/check.sh — keeps everything runnable via the single `bun test` entry point
// this repo already documents, no second test runner to remember.

import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

const REPO_ROOT = join(import.meta.dir, "..");

describe("R-14: portability contrib", () => {
  test("contrib/launchd/install.sh has valid bash syntax", () => {
    const script = join(REPO_ROOT, "contrib", "launchd", "install.sh");
    const result = Bun.spawnSync(["bash", "-n", script]);
    expect(result.exitCode, new TextDecoder().decode(result.stderr)).toBe(0);
  });

  test("the rendered plist (after install.sh's own sed substitution) is valid XML per plutil -lint", () => {
    const template = readFileSync(join(REPO_ROOT, "contrib", "launchd", "com.tanastream.plist.template"), "utf-8");
    const rendered = template
      .replaceAll("__TANASTREAM_INSTALL_DIR__", "/tmp/fake-install-dir")
      .replaceAll("__HOME__", "/tmp/fake-home")
      .replaceAll("__LOG_DIR__", "/tmp/fake-home/Library/Logs");
    // No unresolved placeholders should remain.
    expect(rendered).not.toMatch(/__[A-Z_]+__/);

    const dir = mkdtempSync(join(tmpdir(), "tanastream-plist-lint-"));
    const renderedPath = join(dir, "com.tanastream.plist");
    try {
      writeFileSync(renderedPath, rendered);
      const result = Bun.spawnSync(["plutil", "-lint", renderedPath]);
      expect(result.exitCode, new TextDecoder().decode(result.stdout) + new TextDecoder().decode(result.stderr)).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("contrib/systemd/tanastream.service has the required unit sections", () => {
    const unit = readFileSync(join(REPO_ROOT, "contrib", "systemd", "tanastream.service"), "utf-8");
    expect(unit).toContain("[Unit]");
    expect(unit).toContain("[Service]");
    expect(unit).toContain("[Install]");
    expect(unit).toContain("ExecStart=");
  });

  test("docs/OPERATIONS.md documents all three run modes", () => {
    const ops = readFileSync(join(REPO_ROOT, "docs", "OPERATIONS.md"), "utf-8");
    const matches = (ops.match(/systemd|launchd|foreground/gi) ?? []).length;
    expect(matches).toBeGreaterThanOrEqual(3);
  });

  test("no PAI coupling in any contrib/docs file", () => {
    const files = [
      join(REPO_ROOT, "contrib", "launchd", "install.sh"),
      join(REPO_ROOT, "contrib", "launchd", "com.tanastream.plist.template"),
      join(REPO_ROOT, "contrib", "systemd", "tanastream.service"),
      join(REPO_ROOT, "docs", "OPERATIONS.md"),
    ];
    for (const file of files) {
      const content = readFileSync(file, "utf-8");
      expect(content).not.toMatch(/PAI_DIR/);
      expect(content).not.toMatch(/\.claude\//);
      expect(content).not.toMatch(/com\.pai\./);
    }
  });
});

// Forge-audit fix (U-10, Finding 13): package.json wasn't publish-ready (private:true, no
// license/description/author/engines). Regression-guards those fields going forward.
describe("Finding 13: package.json is publish-ready", () => {
  test("no private:true, and license/description/author/engines are present", () => {
    const pkg = JSON.parse(readFileSync(join(REPO_ROOT, "package.json"), "utf-8")) as Record<string, unknown>;
    expect(pkg.private).not.toBe(true);
    expect(typeof pkg.license).toBe("string");
    expect(typeof pkg.description).toBe("string");
    expect(typeof pkg.author).toBe("string");
    expect(pkg.engines).toBeTruthy();
  });
});

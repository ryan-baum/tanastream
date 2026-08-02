// U-5 (SPEC.md §8): CLI + surface genericization — R-8 (write-only surface, no read-shaped
// command) and R-2 (zero PAI coupling in the CLI's own output/wording).

import { describe, expect, test } from "bun:test";
import { join } from "path";
import { spawnSync } from "child_process";

const CLI = join(import.meta.dir, "..", "tanastream");

function run(args: string[]) {
  return spawnSync(CLI, args, { encoding: "utf8" });
}

const READ_SHAPED_WORDS = ["read", "search", "query", "get", "list-nodes", "fetch"];

describe("R-8: write-only surface — no read/search/query command", () => {
  test("help lists only write-shaped commands", () => {
    const result = run(["help"]);
    expect(result.status).toBe(0);
    const commandLines = result.stdout
      .split("\n")
      .filter((line) => line.trim().startsWith("tanastream "))
      .map((line) => line.trim());
    expect(commandLines.length).toBeGreaterThan(0);
    // "dead-letter list" is listing the QUEUE's own dead-letter rows, not Tana content — it does
    // not count as a Tana-content read command. Every command's SECOND WORD (the subcommand) must
    // not be a bare content-read verb naming a Tana-graph read (read/search/query/fetch/get).
    for (const line of commandLines) {
      const parts = line.replace(/^tanastream\s+/, "").split(/\s+/);
      const verb = parts[0];
      expect(READ_SHAPED_WORDS).not.toContain(verb);
    }
  });

  test("no command is literally named read/search/query anywhere in the command inventory", () => {
    const result = run(["help"]);
    const usageBlock = result.stdout.split("Usage:")[1]?.split("Ops:")[0] ?? "";
    for (const word of ["read", "search", "query"]) {
      expect(usageBlock.toLowerCase()).not.toContain(`tanastream ${word}`);
    }
  });

  test("bare `help` with no args also works and exits 0", () => {
    const result = run([]);
    expect(result.status).toBe(0);
    expect(result.stdout).toContain("Usage:");
  });
});

describe("R-2 (CLI-scoped): help output carries no PAI coupling", () => {
  test("no PAI_DIR or .claude/ literal in help text", () => {
    // NOTE: help text legitimately prints the OPERATOR's own resolved home directory (via
    // ownConfigPath()) as part of telling them exactly where to put their config file — that's
    // correct, useful UX and varies per-user by design, not a coupling defect. R-2's actual
    // concern (hardcoded PAI/Ryan-specific SOURCE strings) is covered by the static `rg` probe
    // over the repo tree, not by asserting against this runtime-resolved, user-specific value.
    const result = run(["help"]);
    expect(result.stdout).not.toMatch(/PAI_DIR/);
    expect(result.stdout).not.toMatch(/\.claude\//);
  });

  test("help names the OWN config path (KTD-2), not a supertag-only path", () => {
    const result = run(["help"]);
    expect(result.stdout).toContain(".config/tanastream/config.json");
  });
});

describe("--version", () => {
  test("prints a semver-looking string and exits 0", () => {
    const result = run(["--version"]);
    expect(result.status).toBe(0);
    expect(result.stdout.trim()).toMatch(/^\d+\.\d+\.\d+/);
  });
});

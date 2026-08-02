// R-3 (SPEC.md §6): own config discovery — env vars / own config.json / supertag fallback / loud error.
// Both directions required: a temp $HOME with only tanastream config resolves; a temp $HOME with
// neither resolves NOTHING and throws a loud, helpful error naming the file to create.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { ConfigError, ownConfigPath, resolveConfig } from "../src/config";

const ENV_KEYS = ["HOME", "TANASTREAM_ENDPOINT", "TANASTREAM_TOKEN", "TANASTREAM_CONFIG"] as const;
let savedEnv: Record<string, string | undefined> = {};
let tempHome: string;

beforeEach(() => {
  savedEnv = Object.fromEntries(ENV_KEYS.map((k) => [k, process.env[k]]));
  tempHome = mkdtempSync(join(tmpdir(), "tanastream-cfg-home-"));
  process.env.HOME = tempHome;
  delete process.env.TANASTREAM_ENDPOINT;
  delete process.env.TANASTREAM_TOKEN;
  delete process.env.TANASTREAM_CONFIG;
});

afterEach(() => {
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
  rmSync(tempHome, { recursive: true, force: true });
});

describe("R-3: own config discovery", () => {
  test("temp $HOME with only tanastream config resolves endpoint+token", () => {
    const dir = join(tempHome, ".config", "tanastream");
    mkdirSync(dir, { recursive: true });
    writeFileSync(
      join(dir, "config.json"),
      JSON.stringify({
        apiEndpoint: "https://input.example/api",
        apiToken: "fake-token",
        defaultTargetNode: "FAKE_TARGET",
        localApi: { enabled: true, endpoint: "http://127.0.0.1:8262", bearerToken: "fake-bearer" },
      }),
    );

    const resolved = resolveConfig();
    expect(resolved.source).toBe("own-config");
    expect(resolved.localApi?.endpoint).toBe("http://127.0.0.1:8262");
    expect(resolved.localApi?.bearerToken).toBe("fake-bearer");
    expect(resolved.apiEndpoint).toBe("https://input.example/api");
    expect(resolved.apiToken).toBe("fake-token");

    // and it read from no supertag path at all — no .config/supertag dir was ever created
    expect(resolved.sourcePath).toBe(join(tempHome, ".config", "tanastream", "config.json"));
  });

  test("temp $HOME with no config at all throws a loud error naming the file to create", () => {
    let thrown: unknown;
    try {
      resolveConfig();
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(ConfigError);
    const message = (thrown as Error).message;
    expect(message).toContain(ownConfigPath());
    expect(message).toContain(".config/tanastream/config.json");
    expect(message).toContain("apiEndpoint");
  });

  test("TANASTREAM_ENDPOINT/TANASTREAM_TOKEN env vars take precedence over an own config file", () => {
    const dir = join(tempHome, ".config", "tanastream");
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "config.json"), JSON.stringify({ apiEndpoint: "https://file.example" }));
    process.env.TANASTREAM_ENDPOINT = "http://127.0.0.1:9999";
    process.env.TANASTREAM_TOKEN = "env-token";

    const resolved = resolveConfig();
    expect(resolved.source).toBe("env");
    expect(resolved.localApi?.endpoint).toBe("http://127.0.0.1:9999");
    expect(resolved.localApi?.bearerToken).toBe("env-token");
  });

  test("supertag config is used as an explicit, logged fallback when no tanastream config exists", () => {
    const dir = join(tempHome, ".config", "supertag");
    mkdirSync(dir, { recursive: true });
    writeFileSync(
      join(dir, "config.json"),
      JSON.stringify({
        apiEndpoint: "https://input.example/api",
        apiToken: "fallback-token",
        localApi: { enabled: true, endpoint: "http://127.0.0.1:8262", bearerToken: "fallback-bearer" },
      }),
    );

    const resolved = resolveConfig();
    expect(resolved.source).toBe("supertag-fallback");
    expect(resolved.localApi?.bearerToken).toBe("fallback-bearer");
  });

  test("an explicit configPath bypasses discovery entirely (existing RealTanaBackend contract)", () => {
    const dir = mkdtempSync(join(tmpdir(), "tanastream-cfg-explicit-"));
    const explicitPath = join(dir, "config.json");
    writeFileSync(explicitPath, JSON.stringify({ apiEndpoint: "https://explicit.example", apiToken: "t" }));
    try {
      const resolved = resolveConfig(explicitPath);
      expect(resolved.source).toBe("own-config");
      expect(resolved.sourcePath).toBe(explicitPath);
      expect(resolved.apiEndpoint).toBe("https://explicit.example");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

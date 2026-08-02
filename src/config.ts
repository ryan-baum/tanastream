import { existsSync, mkdirSync, readFileSync } from "fs";
import { homedir } from "os";
import { dirname, join } from "path";

// Bun's os.homedir() reads the OS user-directory record directly and does NOT
// honor a runtime HOME override on this platform (verified 2026-08-02: setting
// process.env.HOME then calling homedir() still returns the real account home).
// HOME is the canonical POSIX override regardless, so prefer it explicitly —
// this also makes config resolution testable against a temp $HOME.
function resolvedHomeDir(): string {
  return process.env.HOME || homedir();
}

/**
 * KTD-2 (SPEC.md): own config discovery. Precedence, evaluated in order:
 *   1. TANASTREAM_ENDPOINT / TANASTREAM_TOKEN env vars
 *   2. ~/.config/tanastream/config.json (or TANASTREAM_CONFIG override)
 *   3. ~/.config/supertag/config.json — explicit fallback, logged when used
 *   4. none found -> ConfigError naming the file to create
 *
 * State dir defaults to XDG (~/.local/state/tanastream), overridable via
 * TANASTREAM_STATE_DIR.
 */

export interface TanaStreamConfig {
  apiEndpoint?: string;
  apiToken?: string;
  defaultTargetNode?: string;
  localApi?: {
    enabled?: boolean;
    endpoint?: string;
    bearerToken?: string;
  };
}

export type ConfigSource = "env" | "own-config" | "supertag-fallback";

export interface ResolvedConfig extends TanaStreamConfig {
  source: ConfigSource;
  sourcePath?: string;
}

export const DEFAULT_LOCAL_ENDPOINT = "http://127.0.0.1:8262";

/**
 * KTD-4 (SPEC.md): pacing is first-class, default on. 100ms (~10 writes/s) sits comfortably under
 * the measured ~12/s server ceiling (2026-08-01 stress test), so default pacing costs ~0 throughput
 * while leaving read headroom (bursts stall other readers ~25x — see TanaWritePathDoctrine). `0`
 * disables it. Overridable via TANASTREAM_LOCAL_MIN_INTERVAL_MS or an explicit CLI/caller value.
 */
export const DEFAULT_LOCAL_MIN_INTERVAL_MS = 100;

export function resolveLocalMinIntervalMs(explicit?: number): number {
  if (explicit !== undefined) return explicit;
  const env = process.env.TANASTREAM_LOCAL_MIN_INTERVAL_MS;
  if (env === undefined) return DEFAULT_LOCAL_MIN_INTERVAL_MS;
  const parsed = Number(env);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : DEFAULT_LOCAL_MIN_INTERVAL_MS;
}

export function ownConfigPath(): string {
  return process.env.TANASTREAM_CONFIG || join(resolvedHomeDir(), ".config", "tanastream", "config.json");
}

export function supertagFallbackConfigPath(): string {
  return join(resolvedHomeDir(), ".config", "supertag", "config.json");
}

export function defaultStateDir(): string {
  return process.env.TANASTREAM_STATE_DIR || join(resolvedHomeDir(), ".local", "state", "tanastream");
}

export function defaultDbPath(): string {
  return process.env.TANASTREAM_DB || join(defaultStateDir(), "spool.db");
}

export function defaultEventLogPath(): string {
  return process.env.TANASTREAM_EVENT_LOG || join(defaultStateDir(), "events.jsonl");
}

export function ensureParent(path: string): void {
  mkdirSync(dirname(path), { recursive: true });
}

export class ConfigError extends Error {
  constructor(expectedPath: string) {
    super(
      [
        "tanastream is not configured.",
        "",
        `Create ${expectedPath} with:`,
        "{",
        `  "apiEndpoint": "${DEFAULT_LOCAL_ENDPOINT}",`,
        '  "apiToken": "YOUR_TOKEN",',
        '  "defaultTargetNode": "YOUR_TARGET_NODE_ID"',
        "}",
        "",
        "Or set TANASTREAM_ENDPOINT / TANASTREAM_TOKEN environment variables.",
        "See config.example.json in the repo root for the full shape.",
      ].join("\n"),
    );
    this.name = "ConfigError";
  }
}

function readConfigFile(path: string): TanaStreamConfig {
  return JSON.parse(readFileSync(path, "utf-8")) as TanaStreamConfig;
}

/**
 * Load a config file directly at an explicit path, bypassing discovery
 * precedence entirely (used by callers — e.g. tests, `--config` flag —
 * that already know exactly which file they want).
 */
export function loadConfigAt(path: string): TanaStreamConfig {
  if (!existsSync(path)) return {};
  return readConfigFile(path);
}

/**
 * Resolve config per the KTD-2 precedence. Throws ConfigError (naming the
 * file to create) when nothing is found.
 */
export function resolveConfig(explicitPath?: string): ResolvedConfig {
  if (explicitPath) {
    return { ...loadConfigAt(explicitPath), source: "own-config", sourcePath: explicitPath };
  }

  const envEndpoint = process.env.TANASTREAM_ENDPOINT;
  const envToken = process.env.TANASTREAM_TOKEN;
  if (envEndpoint || envToken) {
    return {
      localApi: { endpoint: envEndpoint || DEFAULT_LOCAL_ENDPOINT, bearerToken: envToken },
      source: "env",
    };
  }

  const ownPath = ownConfigPath();
  if (existsSync(ownPath)) {
    return { ...readConfigFile(ownPath), source: "own-config", sourcePath: ownPath };
  }

  const fallbackPath = supertagFallbackConfigPath();
  if (existsSync(fallbackPath)) {
    // eslint-disable-next-line no-console
    console.error(
      `[tanastream] no config at ${ownPath}; falling back to supertag config at ${fallbackPath} (see README "Configuration")`,
    );
    return { ...readConfigFile(fallbackPath), source: "supertag-fallback", sourcePath: fallbackPath };
  }

  throw new ConfigError(ownPath);
}

import { existsSync, mkdirSync } from "fs";
import { homedir } from "os";
import { dirname, join } from "path";

// TODO(U-2): this whole file is replaced by src/config.ts per KTD-2 (own config
// discovery). Interim rename below only removes the PAI-coupled env var name
// so no PAI string enters git history before the real replacement lands.
export function paiDir(): string {
  const configured = process.env.TODO_LEGACY_BASE_DIR;
  if (!configured) return join(homedir(), ".claude", "PAI");
  if (existsSync(join(configured, "TOOLS"))) return configured;
  if (existsSync(join(configured, "PAI", "TOOLS"))) return join(configured, "PAI");
  return configured.endsWith("/PAI") ? configured : join(configured, "PAI");
}

export function tanastreamDir(): string {
  return join(paiDir(), "TOOLS", "TanaStream");
}

export function defaultStateDir(): string {
  return process.env.TANASTREAM_STATE_DIR || join(paiDir(), "MEMORY", "STATE", "tanastream");
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

export function supertagPath(): string {
  return process.env.SUPERTAG_BIN || join(homedir(), "Tools", "supertag-cli", "supertag");
}

export function supertagConfigPath(): string {
  return process.env.SUPERTAG_CONFIG || join(homedir(), ".config", "supertag", "config.json");
}

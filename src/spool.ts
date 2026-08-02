import { Database } from "bun:sqlite";
import { existsSync, renameSync } from "fs";
import { createHash } from "crypto";
import { ensureParent, defaultDbPath } from "./config";
import { assertCreateSafe, assertFieldAttributeIdPresent, assertKeySafe, assertTagIdPresent } from "./validate";
import type { ApplyResult, ApplyRoute, EnqueueInput, EnqueueResult, OpType, QueueStatus, WriteRow, WriteState } from "./types";
import { OP_TYPES } from "./types";

interface StoredWriteRow {
  id: number;
  dedup_key: string;
  idempotency_key: string;
  op_type: OpType;
  payload_json: string;
  target_node_id: string | null;
  priority: number;
  source: string;
  state: WriteState;
  attempts: number;
  max_attempts: number;
  last_error: string | null;
  hold_reason: string | null;
  created_at: number;
  updated_at: number;
  inflight_at: number | null;
  applied_at: number | null;
  evidence_json: string | null;
  route: "local" | "input" | null;
  needs_reconcile: number;
  reconciled_at: number | null;
}

export interface OpenSpoolOptions {
  dbPath?: string;
  recoverCorrupt?: boolean;
}

export class TanaSpool {
  readonly dbPath: string;
  readonly db: Database;
  recoveredCorruption: boolean;

  constructor(dbPath: string, db: Database, recoveredCorruption: boolean) {
    this.dbPath = dbPath;
    this.db = db;
    this.recoveredCorruption = recoveredCorruption;
  }

  close(): void {
    this.db.close();
  }

  pragma(name: "journal_mode" | "synchronous"): string | number {
    const row = this.db.query(`PRAGMA ${name}`).get() as Record<string, string | number>;
    return row[Object.keys(row)[0]];
  }

  enqueue(input: EnqueueInput, nowMs = Date.now()): EnqueueResult {
    validateInput(input);
    // Reject Tana-Paste-unsafe create content loudly at the queue boundary (fail-closed),
    // before it can be silently misparsed on the Local API. Opt-in raw via rawTanaPaste/tanaPaste.
    if (input.opType === "create") {
      assertCreateSafe(input.payload);
      // Reject a collapse-unstable explicit idempotencyKey (the marker is built from it) before it can
      // orphan-duplicate the create via a read-back whitespace mismatch. Auto-hash keys are hex-safe.
      if (input.idempotencyKey) assertKeySafe(input.idempotencyKey);
    }
    // tag/field ops require an ID, never a name — reject loudly at enqueue (see validate.ts).
    if (input.opType === "tag") assertTagIdPresent(input.payload);
    if (input.opType === "field") assertFieldAttributeIdPresent(input.payload);
    const targetNodeId = input.targetNodeId || payloadTarget(input.payload);
    const idempotencyKey = input.idempotencyKey || hashInput(input, targetNodeId);
    const dedupKey = `${input.opType}:${idempotencyKey}`;
    const payloadJson = stableStringify(input.payload);
    const priority = input.priority ?? 0;
    const source = input.source || "unknown";
    const maxAttempts = input.maxAttempts ?? 5;

    const result = this.db
      .query(`
        INSERT INTO writes (
          dedup_key, idempotency_key, op_type, payload_json, target_node_id,
          priority, source, state, attempts, max_attempts, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
        ON CONFLICT(dedup_key) DO NOTHING
      `)
      .run(dedupKey, idempotencyKey, input.opType, payloadJson, targetNodeId, priority, source, maxAttempts, nowMs, nowMs);

    const row = this.getByDedupKey(dedupKey);
    if (!row) throw new Error("enqueue failed to read inserted row");
    if (result.changes === 0) {
      const samePayload = stableStringify(row.payload) === payloadJson;
      const sameTarget = row.targetNodeId === (targetNodeId ?? null);
      if (!samePayload || !sameTarget) {
        throw new Error(`IDEMPOTENCY_CONFLICT: ${dedupKey} already exists with different payload or target`);
      }
    }
    if (result.changes > 0) this.event(row.id, "enqueue", "write intent persisted", { dedupKey, opType: input.opType }, nowMs);
    return {
      id: row.id,
      dedupKey: row.dedupKey,
      idempotencyKey: row.idempotencyKey,
      inserted: result.changes > 0,
      state: row.state,
    };
  }

  pendingRows(limit = 100): WriteRow[] {
    const rows = this.db
      .query(`
        SELECT * FROM writes
        WHERE state = 'pending'
        ORDER BY priority DESC, id ASC
        LIMIT ?
      `)
      .all(limit) as StoredWriteRow[];
    return rows.map(decodeRow);
  }

  deadRows(limit = 100): WriteRow[] {
    const rows = this.db
      .query("SELECT * FROM writes WHERE state = 'dead' ORDER BY updated_at DESC, id ASC LIMIT ?")
      .all(limit) as StoredWriteRow[];
    return rows.map(decodeRow);
  }

  getById(id: number): WriteRow | null {
    const row = this.db.query("SELECT * FROM writes WHERE id = ?").get(id) as StoredWriteRow | null;
    return row ? decodeRow(row) : null;
  }

  getByDedupKey(dedupKey: string): WriteRow | null {
    const row = this.db.query("SELECT * FROM writes WHERE dedup_key = ?").get(dedupKey) as StoredWriteRow | null;
    return row ? decodeRow(row) : null;
  }

  markInflight(id: number, nowMs: number): boolean {
    const result = this.db
      .query("UPDATE writes SET state = 'inflight', inflight_at = ?, updated_at = ?, hold_reason = NULL WHERE id = ? AND state = 'pending'")
      .run(nowMs, nowMs, id);
    if (result.changes === 0) return false;
    this.event(id, "inflight", "write claimed by drain loop", {}, nowMs);
    return true;
  }

  markApplied(id: number, result: ApplyResult, nowMs: number): void {
    this.db
      .query(`
        UPDATE writes
        SET state = 'applied', applied_at = ?, updated_at = ?, evidence_json = ?,
            route = ?, needs_reconcile = ?, reconciled_at = ?,
            last_error = NULL, hold_reason = NULL
        WHERE id = ?
      `)
      .run(nowMs, nowMs, JSON.stringify(result.evidence), result.route, result.route === "input" ? 1 : 0, result.route === "input" ? null : nowMs, id);
    this.setMeta("last_drain_at", String(nowMs));
    if (result.route === "input") this.setMeta("last_input_at", String(nowMs));
    this.event(id, "applied", "write applied", result.evidence, nowMs);
  }

  markRetryOrDead(id: number, error: string, nowMs: number): { state: "pending" | "dead"; attempts: number } {
    const row = this.getById(id);
    if (!row) throw new Error(`write not found: ${id}`);
    const attempts = row.attempts + 1;
    const state: "pending" | "dead" = attempts >= row.maxAttempts ? "dead" : "pending";
    this.db
      .query(`
        UPDATE writes
        SET state = ?, attempts = ?, last_error = ?, updated_at = ?, inflight_at = NULL
        WHERE id = ?
      `)
      .run(state, attempts, error, nowMs, id);
    this.event(id, state === "dead" ? "dead" : "retry", error, { attempts }, nowMs);
    return { state, attempts };
  }

  markDead(id: number, error: string, nowMs: number): void {
    const row = this.getById(id);
    const attempts = row ? Math.max(row.attempts, row.maxAttempts) : 1;
    this.db
      .query(`
        UPDATE writes
        SET state = 'dead', attempts = ?, last_error = ?, updated_at = ?, inflight_at = NULL
        WHERE id = ?
      `)
      .run(attempts, error, nowMs, id);
    this.event(id, "dead", error, { attempts }, nowMs);
  }

  markHeld(id: number, reason: string, nowMs: number): void {
    this.db
      .query("UPDATE writes SET hold_reason = ?, updated_at = ? WHERE id = ? AND state = 'pending'")
      .run(reason, nowMs, id);
    this.event(id, "held", reason, {}, nowMs);
  }

  /**
   * Last-attempt tracking for the drain loop's per-route pacing gates. Route-keyed (Local and
   * Input each get their own meta key, so the two gates stay independent).
   */
  noteAttempt(route: ApplyRoute, nowMs: number): void {
    this.setMeta(`last_${route}_attempt_at`, String(nowMs));
  }

  lastAttemptAt(route: ApplyRoute): number | null {
    return numberMeta(this.getMeta(`last_${route}_attempt_at`));
  }

  /** Avoids computing the full status() aggregate (GROUP BY + counts + two more meta reads) just to read one meta field. */
  lastInputAppliedAt(): number | null {
    return numberMeta(this.getMeta("last_input_at"));
  }

  markReconciled(id: number, result: ApplyResult, nowMs: number): void {
    const row = this.getById(id);
    const previousEvidence = row?.evidence ?? {};
    const evidence = {
      ...previousEvidence,
      reconcile: result.evidence,
      reconciledTargetNodeId: result.targetNodeId,
    };
    this.db
      .query(`
        UPDATE writes
        SET needs_reconcile = 0,
            reconciled_at = ?,
            updated_at = ?,
            evidence_json = ?,
            last_error = NULL,
            hold_reason = NULL
        WHERE id = ? AND state = 'applied'
      `)
      .run(nowMs, nowMs, JSON.stringify(evidence), id);
    this.event(id, "reconciled", "input write reconciled after Local API reopen", result.evidence, nowMs);
  }

  inputRowsNeedingReconcile(limit = 100): WriteRow[] {
    const rows = this.db
      .query(`
        SELECT * FROM writes
        WHERE state = 'applied' AND route = 'input' AND needs_reconcile = 1
        ORDER BY applied_at ASC, id ASC
        LIMIT ?
      `)
      .all(limit) as StoredWriteRow[];
    return rows.map(decodeRow);
  }

  recoverInflight(nowMs = Date.now()): number {
    const rows = this.db.query("SELECT id FROM writes WHERE state = 'inflight'").all() as Array<{ id: number }>;
    this.db.query("UPDATE writes SET state = 'pending', inflight_at = NULL, updated_at = ? WHERE state = 'inflight'").run(nowMs);
    for (const row of rows) this.event(row.id, "recover", "inflight reset to pending after daemon start", {}, nowMs);
    return rows.length;
  }

  retryDead(id: number, nowMs = Date.now()): boolean {
    const result = this.db
      .query(`
        UPDATE writes
        SET state = 'pending', attempts = 0, last_error = NULL, hold_reason = NULL, updated_at = ?
        WHERE id = ? AND state = 'dead'
      `)
      .run(nowMs, id);
    if (result.changes > 0) this.event(id, "retry-dead", "dead-letter returned to pending", {}, nowMs);
    return result.changes > 0;
  }

  status(): QueueStatus {
    const counts = this.db
      .query("SELECT state, count(*) AS count FROM writes GROUP BY state")
      .all() as Array<{ state: WriteState; count: number }>;
    const byState = new Map(counts.map((row) => [row.state, row.count]));
    return {
      pending: byState.get("pending") ?? 0,
      inflight: byState.get("inflight") ?? 0,
      applied: byState.get("applied") ?? 0,
      dead: byState.get("dead") ?? 0,
      unreconciledInput: Number(
        (this.db
          .query("SELECT count(*) AS count FROM writes WHERE state = 'applied' AND route = 'input' AND needs_reconcile = 1")
          .get() as { count: number }).count,
      ),
      lastDrainAt: numberMeta(this.getMeta("last_drain_at")),
      lastInputAt: numberMeta(this.getMeta("last_input_at")),
      recoveredCorruption: this.recoveredCorruption,
    };
  }

  getMeta(key: string): string | null {
    const row = this.db.query("SELECT value FROM metadata WHERE key = ?").get(key) as { value: string } | null;
    return row?.value ?? null;
  }

  setMeta(key: string, value: string): void {
    this.db
      .query("INSERT INTO metadata (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
      .run(key, value);
  }

  event(writeId: number | null, eventType: string, message: string, data: Record<string, unknown>, nowMs: number): void {
    this.db
      .query("INSERT INTO events (write_id, event_type, message, data_json, created_at) VALUES (?, ?, ?, ?, ?)")
      .run(writeId, eventType, message, JSON.stringify(data), nowMs);
  }
}

export function openSpool(options: OpenSpoolOptions = {}): TanaSpool {
  const dbPath = options.dbPath || defaultDbPath();
  ensureParent(dbPath);
  let recoveredCorruption = false;
  let db: Database | undefined;
  try {
    db = new Database(dbPath, { create: true });
    initialize(db);
  } catch (error) {
    try {
      db?.close();
    } catch {
      /* already closed / never opened */
    }
    // Fail closed: only move the live spool aside on a GENUINE corruption signature.
    // A transient SQLITE_BUSY / IOERR (e.g. two processes opening a fresh spool at once)
    // must NEVER destroy producer-acked writes — surface it and halt instead. Recovery itself is
    // destructive enough (renames the live spool aside) that it must never fire just because SOME
    // caller happened to pass recoverCorrupt:true by default — see cli.ts's --recover-corrupt
    // flag, which is the only place `true` originates from in this codebase.
    if (!options.recoverCorrupt || !isCorruptionError(error)) throw error;
    const backupBase = moveCorruptFiles(dbPath);
    recoveredCorruption = true;
    db = new Database(dbPath, { create: true });
    initialize(db);
    const spool = new TanaSpool(dbPath, db, recoveredCorruption);
    spool.event(null, "corruption-recovered", "corrupt spool moved aside (db+wal+shm preserved) and clean spool initialized", { backupBase }, Date.now());
    return spool;
  }
  return new TanaSpool(dbPath, db, recoveredCorruption);
}

/**
 * True only for errors that positively indicate an unusable/corrupt database file —
 * NOT for transient locking/IO errors. Used to keep recovery fail-closed: ambiguous or
 * transient failures throw (halt) rather than move the live spool aside.
 */
export function isCorruptionError(error: unknown): boolean {
  const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
  return (
    message.includes("integrity check failed") ||
    message.includes("malformed") ||
    message.includes("not a database") ||
    message.includes("file is encrypted") ||
    message.includes("database disk image")
  );
}

function initialize(db: Database): void {
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = FULL");
  db.exec("PRAGMA foreign_keys = ON");
  db.exec("PRAGMA busy_timeout = 5000");
  const integrity = db.query("PRAGMA integrity_check").get() as Record<string, string>;
  if (Object.values(integrity)[0] !== "ok") throw new Error(`SQLite integrity check failed: ${Object.values(integrity)[0]}`);

  db.exec(`
    CREATE TABLE IF NOT EXISTS writes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dedup_key TEXT NOT NULL UNIQUE,
      idempotency_key TEXT NOT NULL,
      op_type TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      target_node_id TEXT,
      priority INTEGER NOT NULL DEFAULT 0,
      source TEXT NOT NULL DEFAULT 'unknown',
      state TEXT NOT NULL CHECK (state IN ('pending', 'inflight', 'applied', 'dead')),
      attempts INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 5,
      last_error TEXT,
      hold_reason TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      inflight_at INTEGER,
      applied_at INTEGER,
      evidence_json TEXT,
      route TEXT CHECK (route IN ('local', 'input') OR route IS NULL),
      needs_reconcile INTEGER NOT NULL DEFAULT 0,
      reconciled_at INTEGER
    );

    CREATE INDEX IF NOT EXISTS writes_state_priority_id
      ON writes (state, priority DESC, id ASC);

    CREATE TABLE IF NOT EXISTS events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      write_id INTEGER,
      event_type TEXT NOT NULL,
      message TEXT NOT NULL,
      data_json TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS events_write_id_id ON events (write_id, id);

    CREATE TABLE IF NOT EXISTS metadata (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);

  ensureColumn(db, "writes", "needs_reconcile", "needs_reconcile INTEGER NOT NULL DEFAULT 0");
  ensureColumn(db, "writes", "reconciled_at", "reconciled_at INTEGER");
}

function decodeRow(row: StoredWriteRow): WriteRow {
  return {
    id: row.id,
    dedupKey: row.dedup_key,
    idempotencyKey: row.idempotency_key,
    opType: row.op_type,
    payload: JSON.parse(row.payload_json) as Record<string, unknown>,
    targetNodeId: row.target_node_id,
    priority: row.priority,
    source: row.source,
    state: row.state,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    lastError: row.last_error,
    holdReason: row.hold_reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    inflightAt: row.inflight_at,
    appliedAt: row.applied_at,
    evidence: row.evidence_json ? (JSON.parse(row.evidence_json) as Record<string, unknown>) : null,
    route: row.route,
    needsReconcile: row.needs_reconcile === 1,
    reconciledAt: row.reconciled_at,
  };
}

function validateInput(input: EnqueueInput): void {
  if (!OP_TYPES.includes(input.opType)) throw new Error(`unsupported op type: ${input.opType}`);
  if (!input.payload || typeof input.payload !== "object" || Array.isArray(input.payload)) {
    throw new Error("payload must be a JSON object");
  }
  if (input.maxAttempts !== undefined && input.maxAttempts < 1) throw new Error("maxAttempts must be >= 1");
}

/** Also used by cli.ts (as the CLI's own --target fallback) — exported to avoid a byte-identical duplicate. */
export function payloadTarget(payload: Record<string, unknown>): string | undefined {
  if (typeof payload.targetNodeId === "string") return payload.targetNodeId;
  if (typeof payload.nodeId === "string") return payload.nodeId;
  return undefined;
}

function hashInput(input: EnqueueInput, effectiveTarget: string | undefined): string {
  // Hash the RESOLVED target (mirrors the stored target_node_id), not the raw top-level field —
  // otherwise an unkeyed write whose target lives in the payload hashes against `undefined` while
  // it is stored/applied against the resolved target. The CLI always sets a top-level target, so
  // CLI-originated keys are unchanged.
  return createHash("sha256")
    .update(stableStringify({ opType: input.opType, payload: input.payload, targetNodeId: effectiveTarget }))
    .digest("hex");
}

function stableStringify(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortValue);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = sortValue((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}

function ensureColumn(db: Database, table: string, column: string, definition: string): void {
  const columns = db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  if (!columns.some((row) => row.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${definition}`);
  }
}

function numberMeta(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Renames the main db AND its `-wal`/`-shm` files — never deletes them. In WAL mode, the `-wal`
 * file can hold recently COMMITTED transactions not yet checkpointed into the main file, so a
 * corrupt main-file header doesn't mean the WAL's contents are also bad; deleting it would
 * silently drop producer-acked writes that might otherwise be forensically recoverable. Each
 * call uses a fresh timestamped backup base, so repeated corruption events each keep their own
 * independent evidence rather than overwriting one another.
 */
function moveCorruptFiles(dbPath: string): string {
  // A random suffix (not just the millisecond timestamp) guards against two corruption events
  // landing in the same millisecond — a real possibility, and exactly the class of collision a
  // fixed or coarse-grained name would be vulnerable to.
  const unique = `${new Date().toISOString().replace(/[:.]/g, "-")}-${Math.random().toString(36).slice(2, 8)}`;
  const backupBase = `${dbPath}.corrupt-${unique}`;
  if (existsSync(dbPath)) renameSync(dbPath, backupBase);
  for (const suffix of ["-wal", "-shm"]) {
    const path = `${dbPath}${suffix}`;
    if (existsSync(path)) renameSync(path, `${backupBase}${suffix}`);
  }
  return backupBase;
}

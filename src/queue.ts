import type { ApplyResult, ApplyRoute, DrainResult, EnqueueInput, EnqueueResult, TanaBackend, WriteRow } from "./types";
import type { TanaSpool } from "./spool";
import { markerFor } from "./realBackend";
import { DEFAULT_LOCAL_MIN_INTERVAL_MS } from "./config";

export interface DrainOptions {
  nowMs?: number;
  batchLimit?: number;
  /** KTD-4 (R-7): min ms between Local-route write applies. Defaults to DEFAULT_LOCAL_MIN_INTERVAL_MS; 0 disables. */
  localMinIntervalMs?: number;
}

const INPUT_MIN_INTERVAL_MS = 1_000;
const INPUT_MAX_CHARS = 5_000;

/**
 * Shared clamp math for both route pacing gates (Input: fixed 1s; Local: configurable via
 * DrainOptions.localMinIntervalMs). Byte-identical arithmetic to what each gate had inline before
 * this extraction (simplify pass, U-10): elapsed clamps to >=0 so a backwards clock cannot produce
 * a negative-elapsed, over-interval sleep; the returned sleep clamps to [0, intervalMs].
 * Returns null when clear to proceed (interval disabled, no prior attempt, or interval elapsed).
 */
function nextRateLimitSleep(lastAttemptAt: number, intervalMs: number, nowMs: number): number | null {
  if (intervalMs <= 0 || lastAttemptAt <= 0) return null;
  const elapsed = Math.max(0, nowMs - lastAttemptAt);
  if (elapsed >= intervalMs) return null;
  return Math.min(intervalMs, intervalMs - elapsed);
}

export async function enqueueWrite(spool: TanaSpool, input: EnqueueInput): Promise<EnqueueResult> {
  return spool.enqueue(input);
}

export function recoverInflight(spool: TanaSpool): number {
  return spool.recoverInflight();
}

export async function reconcileAppliedInput(spool: TanaSpool, backend: TanaBackend, options: DrainOptions = {}): Promise<number> {
  const nowMs = options.nowMs ?? Date.now();
  const health = await backend.health();
  if (!health.localAvailable) return 0;
  let reconciledCount = 0;
  const rows = spool.inputRowsNeedingReconcile(options.batchLimit ?? 100);
  for (const row of rows) {
    // These rows are already marked 'applied' (via Input route) — an "unknown" outcome here just
    // means try again next tick; it never risks a duplicate apply the way drainOnce's use does.
    const reconciled = await safeReconcile(backend, row);
    if (reconciled.status !== "found") continue;
    spool.markReconciled(row.id, reconciled.result, nowMs);
    reconciledCount += 1;
  }
  return reconciledCount;
}

export async function drainOnce(spool: TanaSpool, backend: TanaBackend, options: DrainOptions = {}): Promise<DrainResult> {
  const nowMs = options.nowMs ?? Date.now();
  const health = await backend.health();
  const pendingRows = spool.pendingRows(options.batchLimit ?? 100);
  let held = 0;
  let firstHoldReason = "";

  for (const row of pendingRows) {
    const route = chooseRoute(row, health);
    if (!route) {
      held += 1;
      firstHoldReason ||= holdReason(row, health);
      spool.markHeld(row.id, firstHoldReason, nowMs);
      continue;
    }

    if (route === "input") {
      const size = inputPayloadSize(row);
      if (size > INPUT_MAX_CHARS) {
        const error = `Input API payload ${size} chars exceeds ${INPUT_MAX_CHARS}; dead-lettered by policy`;
        spool.markDead(row.id, error, nowMs);
        return { kind: "dead", writeId: row.id, attempts: row.maxAttempts, error };
      }

      const lastInputAt = Math.max(spool.lastInputAppliedAt() ?? 0, spool.lastAttemptAt("input") ?? 0);
      const inputSleepMs = nextRateLimitSleep(lastInputAt, INPUT_MIN_INTERVAL_MS, nowMs);
      if (inputSleepMs !== null) return { kind: "rate_limited", sleepMs: inputSleepMs };
    }

    if (route === "local") {
      // KTD-4/R-7: localMinIntervalMs: 0 disables the gate entirely (nextRateLimitSleep's own guard).
      const localMinIntervalMs = options.localMinIntervalMs ?? DEFAULT_LOCAL_MIN_INTERVAL_MS;
      const localSleepMs = nextRateLimitSleep(spool.lastAttemptAt("local") ?? 0, localMinIntervalMs, nowMs);
      if (localSleepMs !== null) return { kind: "rate_limited", sleepMs: localSleepMs };
    }

    const reconciled = await safeReconcile(backend, row);
    if (reconciled.status === "found") {
      spool.markApplied(row.id, reconciled.result, nowMs);
      return { kind: "applied", writeId: row.id, route: reconciled.result.route, reconciled: true };
    }
    if (reconciled.status === "unknown") {
      // Forge-audit fix (U-10, Finding 2): fail closed. We could not confirm whether this write
      // already landed on a prior attempt — proceeding to apply() now risks a real duplicate.
      // Hold instead (same per-row-held bookkeeping as an unavailable route) and let the next
      // drain tick's reconcile retry; it costs no attempt budget.
      held += 1;
      firstHoldReason ||= `reconcile could not confirm apply state: ${reconciled.error}`;
      spool.markHeld(row.id, firstHoldReason, nowMs);
      continue;
    }

    const claimed = spool.markInflight(row.id, nowMs);
    if (!claimed) return { kind: "stolen", writeId: row.id };
    try {
      if (route === "input") spool.noteAttempt("input", nowMs);
      if (route === "local") spool.noteAttempt("local", nowMs);
      const applied = await backend.apply(row, route, { nowMs });
      spool.markApplied(row.id, applied, nowMs);
      return { kind: "applied", writeId: row.id, route: applied.route };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const result = spool.markRetryOrDead(row.id, message, nowMs);
      if (result.state === "dead") {
        return { kind: "dead", writeId: row.id, attempts: result.attempts, error: message };
      }
      return { kind: "retry", writeId: row.id, attempts: result.attempts, error: message };
    }
  }

  if (held > 0) return { kind: "held", held, reason: firstHoldReason };
  return { kind: "idle" };
}

// The Input API has no Tana-Paste endpoint — buildInputNode() only knows structured
// name/description/children and throws "name is required" on a tanaPaste-only payload.
// Routing a raw-paste create to Input isn't a fallback, it's a guaranteed throw that burns
// the write's attempt budget for nothing. Such creates are Local-only; hold them instead.
function isRawPasteCreate(row: WriteRow): boolean {
  return row.opType === "create" && typeof row.payload.tanaPaste === "string";
}

function chooseRoute(row: WriteRow, health: { localAvailable: boolean; inputAvailable: boolean }): ApplyRoute | null {
  if (health.localAvailable) return "local";
  if (row.opType === "create" && health.inputAvailable && !isRawPasteCreate(row)) return "input";
  return null;
}

function holdReason(row: WriteRow, health: { localAvailable: boolean; inputAvailable: boolean }): string {
  if (isRawPasteCreate(row)) return "Local API unavailable; raw Tana Paste create requires the Local route";
  if (row.opType === "create" && !health.inputAvailable) return "Input API unavailable; create held";
  if (!health.localAvailable) return "Local API unavailable; mutation-shaped op held";
  return "No eligible route available";
}

/**
 * Forge-audit fix (U-10, Finding 2): the old contract (`T | null`, catching every exception into
 * null) could not distinguish "definitively not found" from "couldn't tell due to a transport
 * failure" — both collapsed to the same falsy value, and every caller treated "falsy" as license
 * to proceed toward apply(). A genuine transport failure now surfaces as `status: "unknown"`
 * instead, so callers can fail closed (hold, don't apply) rather than risking a duplicate.
 */
type ReconcileOutcome = { status: "found"; result: ApplyResult } | { status: "not-found" } | { status: "unknown"; error: string };

async function safeReconcile(backend: TanaBackend, row: WriteRow): Promise<ReconcileOutcome> {
  try {
    const result = await backend.reconcile(row);
    return result ? { status: "found", result } : { status: "not-found" };
  } catch (error) {
    return { status: "unknown", error: error instanceof Error ? error.message : String(error) };
  }
}

function inputPayloadSize(row: WriteRow): number {
  const name = typeof row.payload.name === "string" ? row.payload.name : "";
  const description = typeof row.payload.description === "string" ? row.payload.description : "";
  const children = Array.isArray(row.payload.children) ? [...row.payload.children] : [];
  // buildInputNode appends `{ name: marker }` as an extra child, so the >5000 gate must measure
  // the FULL outbound body including the marker — otherwise a just-under create POSTs over-limit.
  const marker = markerFor(row);
  if (marker) children.push({ name: marker });
  return name.length + description.length + JSON.stringify(children).length;
}

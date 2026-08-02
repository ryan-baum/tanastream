import type { ApplyRoute, DrainResult, EnqueueInput, EnqueueResult, TanaBackend, WriteRow } from "./types";
import type { TanaSpool } from "./spool";
import { markerFor } from "./realBackend";

export interface DrainOptions {
  nowMs?: number;
  batchLimit?: number;
}

const INPUT_MIN_INTERVAL_MS = 1_000;
const INPUT_MAX_CHARS = 5_000;

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
    const reconciled = await safeReconcile(backend, row);
    if (!reconciled) continue;
    spool.markReconciled(row.id, reconciled, nowMs);
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

      const lastInputAt = Math.max(spool.status().lastInputAt ?? 0, spool.lastInputAttemptAt() ?? 0);
      if (lastInputAt > 0) {
        // Clamp elapsed to >= 0 so a backwards clock cannot produce a negative-elapsed,
        // over-interval sleep; clamp sleepMs to [0, interval] so the wait is always bounded.
        const elapsed = Math.max(0, nowMs - lastInputAt);
        if (elapsed < INPUT_MIN_INTERVAL_MS) {
          return { kind: "rate_limited", sleepMs: Math.min(INPUT_MIN_INTERVAL_MS, INPUT_MIN_INTERVAL_MS - elapsed) };
        }
      }
    }

    const reconciled = await safeReconcile(backend, row);
    if (reconciled) {
      spool.markApplied(row.id, reconciled, nowMs);
      return { kind: "applied", writeId: row.id, route: reconciled.route, reconciled: true };
    }

    const claimed = spool.markInflight(row.id, nowMs);
    if (!claimed) return { kind: "stolen", writeId: row.id };
    try {
      if (route === "input") spool.noteInputAttempt(nowMs);
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

async function safeReconcile(backend: TanaBackend, row: WriteRow) {
  try {
    return await backend.reconcile(row);
  } catch {
    return null;
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

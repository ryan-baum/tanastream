export const OP_TYPES = ["create", "edit", "tag", "tag-create", "field", "trash", "done", "move"] as const;
export const STATES = ["pending", "inflight", "applied", "dead"] as const;

export type OpType = (typeof OP_TYPES)[number];
export type WriteState = (typeof STATES)[number];
export type ApplyRoute = "local" | "input";

export interface EnqueueInput {
  opType: OpType;
  idempotencyKey?: string;
  payload: Record<string, unknown>;
  targetNodeId?: string;
  priority?: number;
  source?: string;
  maxAttempts?: number;
}

export interface EnqueueResult {
  id: number;
  dedupKey: string;
  idempotencyKey: string;
  inserted: boolean;
  state: WriteState;
}

export interface WriteRow {
  id: number;
  dedupKey: string;
  idempotencyKey: string;
  opType: OpType;
  payload: Record<string, unknown>;
  targetNodeId: string | null;
  priority: number;
  source: string;
  state: WriteState;
  attempts: number;
  maxAttempts: number;
  lastError: string | null;
  holdReason: string | null;
  createdAt: number;
  updatedAt: number;
  inflightAt: number | null;
  appliedAt: number | null;
  evidence: Record<string, unknown> | null;
  route: ApplyRoute | null;
  needsReconcile: boolean;
  reconciledAt: number | null;
}

export interface QueueStatus {
  pending: number;
  inflight: number;
  applied: number;
  dead: number;
  unreconciledInput: number;
  lastDrainAt: number | null;
  lastInputAt: number | null;
  recoveredCorruption: boolean;
}

export interface BackendHealth {
  localAvailable: boolean;
  inputAvailable: boolean;
  detail?: Record<string, unknown>;
}

export interface ApplyResult {
  route: ApplyRoute;
  targetNodeId?: string;
  evidence: Record<string, unknown>;
}

export interface TanaBackend {
  health(): Promise<BackendHealth>;
  apply(row: WriteRow, route: ApplyRoute, context: { nowMs: number }): Promise<ApplyResult>;
  reconcile(row: WriteRow): Promise<ApplyResult | null>;
}

export type DrainResult =
  | { kind: "idle" }
  | { kind: "held"; held: number; reason: string }
  | { kind: "stolen"; writeId: number }
  | { kind: "rate_limited"; sleepMs: number }
  | { kind: "applied"; writeId: number; route: ApplyRoute; reconciled?: boolean }
  | { kind: "retry"; writeId: number; attempts: number; error: string }
  | { kind: "dead"; writeId: number; attempts: number; error: string };

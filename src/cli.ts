import { drainOnce, enqueueWrite, reconcileAppliedInput, recoverInflight } from "./queue";
import { RealTanaBackend } from "./realBackend";
import { openSpool } from "./spool";
import { acquireDrainLock } from "./lock";
import { defaultDbPath, resolveLocalMinIntervalMs } from "./config";
import type { EnqueueInput, OpType } from "./types";
import { OP_TYPES } from "./types";
import { readFileSync } from "fs";

interface ParsedArgs {
  command: string | null;
  positionals: string[];
  flags: Map<string, string | boolean | (string | boolean)[]>;
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const parsed = parseArgs(argv);
  const command = parsed.command;
  if (!command || command === "help" || parsed.flags.has("help") || parsed.flags.has("h")) {
    printHelp();
    return;
  }

  try {
    switch (command) {
      case "enqueue":
        await commandEnqueue(parsed);
        return;
      case "status":
        await commandStatus(parsed);
        return;
      case "drain":
        await commandDrain(parsed);
        return;
      case "reconcile":
        await commandReconcile(parsed);
        return;
      case "daemon":
        await commandDaemon(parsed);
        return;
      case "dead-letter":
        await commandDeadLetter(parsed);
        return;
      default:
        throw new Error(`unknown command: ${command}`);
    }
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

async function commandEnqueue(parsed: ParsedArgs): Promise<void> {
  const op = parsed.positionals[0] || stringFlag(parsed, "op");
  if (!isOpType(op)) throw new Error(`enqueue requires op: ${OP_TYPES.join(" | ")}`);
  const payload = payloadFromArgs(op, parsed);
  const input: EnqueueInput = {
    opType: op,
    idempotencyKey: stringFlag(parsed, "key") || stringFlag(parsed, "idempotency-key"),
    payload,
    targetNodeId: stringFlag(parsed, "target") || stringFlag(parsed, "target-node-id") || targetFromPayload(payload),
    priority: numberFlag(parsed, "priority") ?? 0,
    source: stringFlag(parsed, "source") || "tanastream-cli",
    maxAttempts: numberFlag(parsed, "max-attempts"),
  };
  const spool = openSpool({ dbPath: stringFlag(parsed, "db") || defaultDbPath(), recoverCorrupt: true });
  try {
    const result = await enqueueWrite(spool, input);
    printJson(result);
  } finally {
    spool.close();
  }
}

async function commandStatus(parsed: ParsedArgs): Promise<void> {
  const spool = openSpool({ dbPath: stringFlag(parsed, "db") || defaultDbPath(), recoverCorrupt: true });
  try {
    const status = spool.status();
    if (parsed.flags.has("no-health")) {
      printJson({ dbPath: spool.dbPath, ...status });
      return;
    }
    const backend = new RealTanaBackend();
    const health = await backend.health();
    printJson({ dbPath: spool.dbPath, ...status, health });
  } finally {
    spool.close();
  }
}

async function commandDrain(parsed: ParsedArgs): Promise<void> {
  const dbPath = stringFlag(parsed, "db") || defaultDbPath();
  const spool = openSpool({ dbPath, recoverCorrupt: true });
  const lock = acquireDrainLock(dbPath);
  if (!lock) {
    spool.close();
    throw new Error("another drainer (the daemon?) holds the drain lock for this spool; refusing to drain (fail-closed). Stop the daemon or target a different --db.");
  }
  try {
    recoverInflight(spool);
    const backend = new RealTanaBackend();
    const reconciled = await reconcileAppliedInput(spool, backend, { batchLimit: numberFlag(parsed, "reconcile-max") ?? 100 });
    const max = numberFlag(parsed, "max") ?? (parsed.flags.has("once") ? 1 : 100);
    const localMinIntervalMs = resolveLocalMinIntervalMs(numberFlag(parsed, "local-min-interval-ms"));
    const results = [];
    for (let i = 0; i < max; i += 1) {
      const result = await drainOnce(spool, backend, { localMinIntervalMs });
      results.push(result);
      if (result.kind === "idle" || result.kind === "held" || result.kind === "rate_limited") break;
    }
    printJson({ reconciled, drained: results.filter((r) => r.kind === "applied").length, results, status: spool.status() });
  } finally {
    lock.release();
    spool.close();
  }
}

async function commandDaemon(parsed: ParsedArgs): Promise<void> {
  const dbPath = stringFlag(parsed, "db") || defaultDbPath();
  const spool = openSpool({ dbPath, recoverCorrupt: true });
  const lock = acquireDrainLock(dbPath);
  if (!lock) {
    spool.close();
    throw new Error("another drainer holds the drain lock for this spool; refusing to start daemon (fail-closed).");
  }
  const backend = new RealTanaBackend();
  const idleSleepMs = numberFlag(parsed, "idle-ms") ?? 2_000;
  const heldSleepMs = numberFlag(parsed, "held-ms") ?? 10_000;
  const heartbeatMs = numberFlag(parsed, "heartbeat-ms") ?? 300_000;
  const localMinIntervalMs = resolveLocalMinIntervalMs(numberFlag(parsed, "local-min-interval-ms"));
  const verbose = parsed.flags.has("verbose");
  recoverInflight(spool);
  const shutdown = () => {
    lock.release();
    spool.close();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  // OPS-1: a verbose 24/7 daemon logged a full status JSON on every idle tick (~43k lines/day at a 2s
  // poll), growing the launchd StandardOutPath without bound. Log actionable ticks immediately, but
  // throttle idle/held/rate_limited to one heartbeat per heartbeatMs. The drain logic below is unchanged.
  let lastTickLogMs = 0;
  while (true) {
    await reconcileAppliedInput(spool, backend, { batchLimit: 100 });
    const result = await drainOnce(spool, backend, { localMinIntervalMs });
    if (verbose) {
      const actionable =
        result.kind === "applied" || result.kind === "retry" || result.kind === "dead" || result.kind === "stolen";
      const now = Date.now();
      if (actionable || now - lastTickLogMs >= heartbeatMs) {
        lastTickLogMs = now;
        printJson({ event: "drain", result, status: spool.status() });
      }
    }
    if (result.kind === "rate_limited") await sleep(result.sleepMs);
    else if (result.kind === "idle") await sleep(idleSleepMs);
    else if (result.kind === "held") await sleep(heldSleepMs);
  }
}

async function commandReconcile(parsed: ParsedArgs): Promise<void> {
  const spool = openSpool({ dbPath: stringFlag(parsed, "db") || defaultDbPath(), recoverCorrupt: true });
  try {
    const backend = new RealTanaBackend();
    const reconciled = await reconcileAppliedInput(spool, backend, { batchLimit: numberFlag(parsed, "max") ?? 100 });
    printJson({ reconciled, status: spool.status() });
  } finally {
    spool.close();
  }
}

async function commandDeadLetter(parsed: ParsedArgs): Promise<void> {
  const subcommand = parsed.positionals[0] || "list";
  const spool = openSpool({ dbPath: stringFlag(parsed, "db") || defaultDbPath(), recoverCorrupt: true });
  try {
    if (subcommand === "list") {
      printJson({ dead: spool.deadRows(numberFlag(parsed, "limit") ?? 100) });
      return;
    }
    if (subcommand === "retry") {
      const id = Number(parsed.positionals[1] || stringFlag(parsed, "id"));
      if (!Number.isInteger(id)) throw new Error("dead-letter retry requires a numeric id");
      printJson({ id, retried: spool.retryDead(id) });
      return;
    }
    throw new Error("dead-letter subcommand must be list or retry");
  } finally {
    spool.close();
  }
}

function payloadFromArgs(op: OpType, parsed: ParsedArgs): Record<string, unknown> {
  const payloadJson = stringFlag(parsed, "payload-json");
  if (payloadJson) return JSON.parse(payloadJson) as Record<string, unknown>;
  const payloadFile = stringFlag(parsed, "payload-file");
  if (payloadFile) return JSON.parse(readFileSync(payloadFile, "utf-8")) as Record<string, unknown>;

  switch (op) {
    case "create":
      return {
        name: requiredFlag(parsed, "name", "create requires --name or --payload-json"),
        description: stringFlag(parsed, "description"),
        children: listFlag(parsed, "child"),
        includeIdempotencyMarker: !parsed.flags.has("no-marker"),
      };
    case "edit":
      return {
        nodeId: requiredFlag(parsed, "node-id", "edit requires --node-id"),
        name: stringFlag(parsed, "name"),
        description: stringFlag(parsed, "description"),
      };
    case "tag":
      return {
        nodeId: requiredFlag(parsed, "node-id", "tag requires --node-id"),
        action: stringFlag(parsed, "action") || "add",
        tagId: requiredFlag(parsed, "tag", "tag requires --tag"),
      };
    case "tag-create":
      return {
        name: requiredFlag(parsed, "name", "tag-create requires --name"),
        description: stringFlag(parsed, "description"),
        extends: stringFlag(parsed, "extends"),
        checkbox: parsed.flags.has("checkbox"),
        workspace: stringFlag(parsed, "workspace"),
      };
    case "field":
      return {
        nodeId: requiredFlag(parsed, "node-id", "field requires --node-id"),
        fieldName: stringFlag(parsed, "field") || stringFlag(parsed, "field-name"),
        attributeId: stringFlag(parsed, "attribute-id"),
        value: requiredFlag(parsed, "value", "field requires --value"),
        optionId: stringFlag(parsed, "option-id"),
      };
    case "trash":
      return { nodeId: requiredFlag(parsed, "node-id", "trash requires --node-id") };
    case "done":
      return {
        nodeId: requiredFlag(parsed, "node-id", "done requires --node-id"),
        done: !parsed.flags.has("undone"),
      };
    case "move":
      return {
        nodeId: requiredFlag(parsed, "node-id", "move requires --node-id"),
        targetNodeId: requiredFlag(parsed, "target", "move requires --target"),
        sourceParentId: stringFlag(parsed, "source-parent-id"),
        position: stringFlag(parsed, "position") || "end",
        referenceNodeId: stringFlag(parsed, "reference-node-id"),
        keepSourceReference: parsed.flags.has("keep-source-reference"),
      };
  }
}

function parseArgs(argv: string[]): ParsedArgs {
  const flags = new Map<string, string | boolean | string[]>();
  const positionals: string[] = [];
  let command: string | null = null;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg.startsWith("--")) {
      // Split on the FIRST '=' only and keep the entire tail — `split("=", 2)` would discard
      // everything after the second '=', silently truncating values like `--child=A=B` or
      // `--key=a=b` (poisoning dedup/marker/reconcile identity) with a zero exit code.
      const body = arg.slice(2);
      const eqIndex = body.indexOf("=");
      const rawKey = eqIndex === -1 ? body : body.slice(0, eqIndex);
      const inlineValue = eqIndex === -1 ? undefined : body.slice(eqIndex + 1);
      if (inlineValue !== undefined) {
        addFlag(flags, rawKey, inlineValue);
      } else if (argv[i + 1] && !argv[i + 1].startsWith("-")) {
        addFlag(flags, rawKey, argv[i + 1]);
        i += 1;
      } else {
        addFlag(flags, rawKey, true);
      }
    } else if (arg.startsWith("-") && arg.length === 2) {
      addFlag(flags, arg.slice(1), true);
    } else if (!command) {
      command = arg;
    } else {
      positionals.push(arg);
    }
  }
  return { command, positionals, flags };
}

function addFlag(flags: ParsedArgs["flags"], key: string, value: string | boolean): void {
  const existing = flags.get(key);
  if (existing === undefined) {
    flags.set(key, value);
    return;
  }
  // A repeated flag accumulates ALL occurrences into an array so a single-value accessor (stringFlag)
  // can detect the duplicate and fail closed — regardless of whether an occurrence parsed as a bare
  // boolean (value dropped, e.g. `--key --key good`) or a string. This closes the boolean-then-string
  // bypass where a replaced value silently looked single-valued. listFlag accepts the array but still
  // rejects one that carries a dropped (boolean) value.
  const arr: (string | boolean)[] = Array.isArray(existing) ? existing : [existing];
  arr.push(value);
  flags.set(key, arr);
}

function printHelp(): void {
  console.log(`TanaStream - durable serialized Tana write queue

Usage:
  tanastream enqueue <op> [options]
  tanastream status [--json] [--no-health]
  tanastream drain [--once | --max N]
  tanastream reconcile [--max N]
  tanastream daemon [--verbose]
  tanastream dead-letter list [--limit N]
  tanastream dead-letter retry <id>

Ops:
  create | edit | tag | tag-create | field | trash | done | move

Enqueue examples:
  tanastream enqueue create --name "Meeting note" --target INBOX --key session-123
  tanastream enqueue edit --node-id NODE --name "Updated" --key edit-123
  tanastream enqueue tag-create --name "public-prediction" --key schema-public-prediction
  tanastream enqueue trash --node-id NODE --key trash-123
  tanastream enqueue create --payload-json '{"name":"Raw"}' --key raw-123

Common flags:
  --key VALUE              Producer idempotency key. Defaults to a content hash.
  --source VALUE           Producer/session label.
  --target VALUE           INBOX, LIBRARY, SCHEMA, or a node id.
  --priority N             Higher priority drains first; ties use insertion id.
  --max-attempts N         Attempts before dead-letter. Default: 5.
  --db PATH                Override SQLite spool path.
  --local-min-interval-ms N  Min ms between Local-route writes (drain/daemon). Default: 100. 0 disables.

Create flags:
  --name TEXT              Plain node name.
  --description TEXT       Optional description child.
  --child TEXT             Repeatable child text.
  --no-marker              Disable visible TanaStreamIdempotency child.
`);
}

function isOpType(value: unknown): value is OpType {
  return typeof value === "string" && OP_TYPES.includes(value as OpType);
}

function stringFlag(parsed: ParsedArgs, key: string): string | undefined {
  const value = parsed.flags.get(key);
  // Fail closed: a value-expecting flag parsed as a bare boolean means its value was dropped
  // (e.g. `--name -5` where `-5` was not consumed). Never silently default — error clearly.
  if (value === true) {
    throw new Error(`--${key} expects a value; for a value starting with '-', use --${key}=value`);
  }
  // Fail closed: a single-value flag given more than once arrives as an array. Silently taking a
  // default (a content-hash --key, an INBOX --target) would destroy producer-controlled idempotency
  // or routing with a zero exit code. Reject loudly.
  if (Array.isArray(value)) {
    throw new Error(`--${key} was given more than once but takes a single value; pass it exactly once`);
  }
  return typeof value === "string" ? value : undefined;
}

function numberFlag(parsed: ParsedArgs, key: string): number | undefined {
  const value = stringFlag(parsed, key);
  if (value === undefined) return undefined;
  const parsedNumber = Number(value);
  if (!Number.isFinite(parsedNumber)) throw new Error(`--${key} must be a number`);
  return parsedNumber;
}

function listFlag(parsed: ParsedArgs, key: string): string[] {
  const value = parsed.flags.get(key);
  // Fail closed: same as stringFlag — a dropped value (`--child -mid`) must error, not vanish.
  if (value === true) {
    throw new Error(`--${key} expects a value; for a value starting with '-', use --${key}=value`);
  }
  if (!value) return [];
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) {
    // A boolean element means one repetition dropped its value (`--child --child x`). Fail closed.
    if (value.some((v) => typeof v !== "string")) {
      throw new Error(`--${key} had a repetition with no value; pass a value for each --${key}`);
    }
    return value as string[];
  }
  return [];
}

function requiredFlag(parsed: ParsedArgs, key: string, message: string): string {
  const value = stringFlag(parsed, key);
  if (!value) throw new Error(message);
  return value;
}

function targetFromPayload(payload: Record<string, unknown>): string | undefined {
  if (typeof payload.targetNodeId === "string") return payload.targetNodeId;
  if (typeof payload.nodeId === "string") return payload.nodeId;
  return undefined;
}

function printJson(value: unknown): void {
  console.log(JSON.stringify(value, null, 2));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

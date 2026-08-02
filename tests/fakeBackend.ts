import { existsSync, readFileSync, writeFileSync } from "fs";
import type { ApplyRoute, ApplyResult, BackendHealth, TanaBackend, WriteRow } from "../src/types";

export class FakeBackend implements TanaBackend {
  localOpen = true;
  inputOpen = true;
  localSequence: boolean[] = [];
  inputApplyTimes: number[] = [];
  effects = new Map<string, number>();
  names = new Map<string, string>();
  transientFailures = new Map<string, number>();

  constructor(options: { localOpen?: boolean; localSequence?: boolean[] } = {}) {
    if (typeof options.localOpen === "boolean") this.localOpen = options.localOpen;
    this.localSequence = options.localSequence ?? [];
  }

  async health(): Promise<BackendHealth> {
    const localAvailable = this.localSequence.length > 0 ? this.localSequence.shift()! : this.localOpen;
    return { localAvailable, inputAvailable: this.inputOpen };
  }

  async apply(row: WriteRow, route: ApplyRoute, context: { nowMs: number }): Promise<ApplyResult> {
    const payload = row.payload as Record<string, unknown>;
    if (payload.poison) throw new Error("poison payload");

    const remainingFailures = this.transientFailures.get(row.dedupKey) ?? 0;
    if (remainingFailures > 0) {
      this.transientFailures.set(row.dedupKey, remainingFailures - 1);
      throw new Error("transient local timeout");
    }

    if (route === "input") this.inputApplyTimes.push(context.nowMs);

    const result = this.recordEffect(row, route);
    if (payload.ambiguousAfterApply) {
      throw new Error("transport died after apply before ack");
    }
    return result;
  }

  async reconcile(row: WriteRow): Promise<ApplyResult | null> {
    if (!this.effects.has(row.dedupKey)) return null;
    return {
      route: "local",
      targetNodeId: `fake-${row.id}`,
      evidence: { reconciled: true, dedupKey: row.dedupKey },
    };
  }

  private recordEffect(row: WriteRow, route: ApplyRoute): ApplyResult {
    const count = this.effects.get(row.dedupKey) ?? 0;
    this.effects.set(row.dedupKey, count + 1);
    const name = typeof row.payload.name === "string" ? row.payload.name : row.opType;
    this.names.set(row.dedupKey, name);
    return {
      route,
      targetNodeId: `fake-${row.id}`,
      evidence: { effectCount: count + 1, name },
    };
  }
}

export class FileEffectBackend extends FakeBackend {
  constructor(private readonly effectsPath: string, options: { crashKey?: string } = {}) {
    super({ localOpen: true });
    this.load();
    this.crashKey = options.crashKey;
  }

  private crashKey?: string;

  override async apply(row: WriteRow, route: ApplyRoute, context: { nowMs: number }): Promise<ApplyResult> {
    const result = await super.apply(row, route, context);
    this.save();
    if (this.crashKey && row.idempotencyKey === this.crashKey) {
      process.kill(process.pid, "SIGKILL");
    }
    return result;
  }

  override async reconcile(row: WriteRow): Promise<ApplyResult | null> {
    this.load();
    return super.reconcile(row);
  }

  private load() {
    if (!existsSync(this.effectsPath)) return;
    const raw = JSON.parse(readFileSync(this.effectsPath, "utf-8")) as Record<string, number>;
    this.effects = new Map(Object.entries(raw));
  }

  private save() {
    writeFileSync(this.effectsPath, JSON.stringify(Object.fromEntries(this.effects), null, 2));
  }
}

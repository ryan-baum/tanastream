import { existsSync, readFileSync } from "fs";
import { supertagConfigPath, supertagPath } from "./paths";
import type { ApplyResult, ApplyRoute, BackendHealth, TanaBackend, WriteRow } from "./types";

interface SupertagConfig {
  apiEndpoint?: string;
  apiToken?: string;
  defaultTargetNode?: string;
  localApi?: {
    enabled?: boolean;
    endpoint?: string;
    bearerToken?: string;
  };
}

interface CreatedNode {
  id: string;
  name: string;
}

const DEFAULT_LOCAL_ENDPOINT = "http://127.0.0.1:8262";
const IDEMPOTENCY_PREFIX = "TanaStreamIdempotency - ";

export class RealTanaBackend implements TanaBackend {
  private readonly config: SupertagConfig;
  private workspaceId: string | null = null;

  constructor(
    private readonly options: {
      configPath?: string;
      supertagBin?: string;
      timeoutMs?: number;
    } = {},
  ) {
    this.config = loadConfig(options.configPath || supertagConfigPath());
  }

  async health(): Promise<BackendHealth> {
    try {
      const response = await fetchWithTimeout(`${this.localEndpoint()}/health`, { method: "GET" }, this.timeoutMs());
      if (!response.ok) return { localAvailable: false, inputAvailable: this.hasInputConfig() };
      const detail = (await response.json().catch(() => ({}))) as Record<string, unknown>;
      return { localAvailable: true, inputAvailable: this.hasInputConfig(), detail };
    } catch {
      return { localAvailable: false, inputAvailable: this.hasInputConfig() };
    }
  }

  async apply(row: WriteRow, route: ApplyRoute, context: { nowMs: number }): Promise<ApplyResult> {
    if (route === "input") return this.applyInput(row, context.nowMs);
    return this.applyLocal(row);
  }

  async reconcile(row: WriteRow): Promise<ApplyResult | null> {
    if (row.opType !== "create") return null;
    const health = await this.health();
    if (!health.localAvailable) return null;
    const marker = markerFor(row);
    if (!marker) return null;
    const targetNodeId = await this.resolveLocalTarget(row.targetNodeId || this.config.defaultTargetNode || "INBOX");
    // Compare against the STORED name, which Tana whitespace-collapses (oneLine) at create time.
    // Matching the raw payload name would skip the marker node for any multi-space / edge-whitespace
    // title and silently re-create it on crash-replay (the BUG-2 / 25-orphan class at a new seam).
    const expectedName = typeof row.payload.name === "string" ? oneLine(row.payload.name) : null;
    // Paginate: a single limit=1000 GET misses a marker node that sorts past child #1000. The
    // seen-id + no-progress guards keep this safe (and terminating) even if the API ignores offset.
    const pageSize = 1000;
    const seen = new Set<string>();
    for (let offset = 0; ; offset += pageSize) {
      const page = await this.localJson<{ children?: Array<{ id: string; name: string }> }>(
        `/nodes/${encodeURIComponent(targetNodeId)}/children?limit=${pageSize}&offset=${offset}`,
        { method: "GET" },
      );
      const kids = page.children ?? [];
      let newCount = 0;
      for (const child of kids) {
        if (seen.has(child.id)) continue;
        seen.add(child.id);
        newCount += 1;
        if (expectedName && child.name !== expectedName) continue;
        const read = await this.readNode(child.id, 3).catch(() => null);
        if (read?.markdown && markerMatches(read.markdown, row)) {
          return {
            route: "local",
            targetNodeId: child.id,
            evidence: { reconciledByMarker: true, marker, nodeId: child.id, name: child.name },
          };
        }
      }
      if (kids.length < pageSize || newCount === 0) break;
    }
    return null;
  }

  async readNode(nodeId: string, maxDepth = 2): Promise<{ markdown: string; name?: string; description?: string | null }> {
    return this.localJson(`/nodes/${encodeURIComponent(nodeId)}?maxDepth=${maxDepth}`, { method: "GET" });
  }

  async trashNode(nodeId: string): Promise<ApplyResult> {
    const run = await this.runSupertag(["trash", "--confirm", "--", nodeId]);
    if (run.code === 0) {
      return { route: "local", targetNodeId: nodeId, evidence: { command: "supertag trash", stdout: run.stdout.trim() } };
    }
    const direct = await this.localRaw(`/nodes/${encodeURIComponent(nodeId)}/trash`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    const text = await direct.text();
    if (direct.status === 400 && text.includes("already in trash")) {
      return { route: "local", targetNodeId: nodeId, evidence: { command: "supertag trash", idempotentAlreadyInTrash: true } };
    }
    throw new Error(`trash failed: ${run.stderr || text || run.stdout}`);
  }

  private async applyLocal(row: WriteRow): Promise<ApplyResult> {
    switch (row.opType) {
      case "create":
        return this.localCreate(row);
      case "edit":
        return this.localEdit(row);
      case "tag":
        return this.localTag(row);
      case "tag-create":
        return this.localTagCreate(row);
      case "field":
        return this.localField(row);
      case "trash":
        return this.trashNode(requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId"));
      case "done":
        return this.localDone(row);
      case "move":
        return this.localMove(row);
      default:
        throw new Error(`unsupported op: ${row.opType}`);
    }
  }

  private async localCreate(row: WriteRow): Promise<ApplyResult> {
    const target = await this.resolveLocalTarget(row.targetNodeId || this.config.defaultTargetNode || "INBOX");
    const content = buildTanaPaste(row);
    const response = await this.localJson<{
      parentNodeId: string;
      targetNodeId: string;
      createdNodes: CreatedNode[];
      message: string;
    }>(`/nodes/${encodeURIComponent(target)}/import`, {
      method: "POST",
      body: JSON.stringify({ content }),
    });
    const created = response.createdNodes?.[0];
    if (!created?.id) throw new Error("Local import returned no created node id");
    const read = await this.readNode(created.id, 3);
    const marker = markerFor(row);
    if (marker && !markerMatches(read.markdown, row)) throw new Error("Local create verification failed: idempotency marker missing");
    // F1 backstop: for structured creates, verify the node landed LITERALLY (name + each child).
    // This catches any Tana Paste misparse the enqueue-time denylist missed (e.g. an unknown
    // control sequence) — it converts silent corruption into a loud dead-letter. Raw paste opts out.
    if (typeof row.payload.tanaPaste !== "string") {
      const expectedName = typeof row.payload.name === "string" ? oneLine(row.payload.name) : null;
      if (expectedName !== null && read.name !== expectedName) {
        throw new Error(`Local create verification failed: node name mismatch (expected ${JSON.stringify(expectedName)}, got ${JSON.stringify(read.name ?? null)}) — possible Tana Paste misparse`);
      }
      const childLines = readbackChildLines(read.markdown);
      // Verify the description child round-trips too (buildTanaPaste emits it as `Description: <d>`).
      // Without this a structured create could silently lose/mutate its description and still mark applied.
      if (typeof row.payload.description === "string" && row.payload.description.trim()) {
        const expectedDesc = `Description: ${oneLine(row.payload.description)}`;
        if (!childLines.includes(expectedDesc)) {
          throw new Error(`Local create verification failed: description did not land as ${JSON.stringify(expectedDesc)} — possible Tana Paste misparse`);
        }
      }
      const expectedChildren = Array.isArray(row.payload.children) ? row.payload.children : [];
      for (const child of expectedChildren) {
        const text = typeof child === "string" ? oneLine(child) : child && typeof child === "object" && "name" in child ? oneLine(String((child as { name: unknown }).name)) : "";
        if (text && !childLines.includes(text)) {
          throw new Error(`Local create verification failed: child ${JSON.stringify(text)} did not land as a child node — possible Tana Paste misparse`);
        }
      }
    }
    return {
      route: "local",
      targetNodeId: created.id,
      evidence: {
        nodeId: created.id,
        parentNodeId: response.parentNodeId,
        name: read.name ?? created.name,
        markerVerified: marker ? true : undefined,
      },
    };
  }

  private async localEdit(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const args = ["edit", nodeId];
    if (typeof row.payload.name === "string") args.push("--name", row.payload.name);
    if (typeof row.payload.description === "string") args.push("--description", row.payload.description);
    await this.mustRunSupertag(args);
    const read = await this.readNode(nodeId, 1);
    return { route: "local", targetNodeId: nodeId, evidence: { command: "supertag edit", name: read.name ?? null } };
  }

  private async localTag(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const action = (row.payload.action === "remove" ? "remove" : "add") as "add" | "remove";
    const tag = requireString(row.payload.tagId ?? row.payload.tagNameOrId, "tagId");
    await this.mustRunSupertag(["tag", action, nodeId, tag]);
    const read = await this.readNode(nodeId, 1);
    return { route: "local", targetNodeId: nodeId, evidence: { command: `supertag tag ${action}`, name: read.name ?? null } };
  }

  private async localTagCreate(row: WriteRow): Promise<ApplyResult> {
    const name = requireString(row.payload.name, "name");
    const existingId = await this.findSupertagId(name);
    if (existingId) {
      const tagId = existingId;
      return { route: "local", targetNodeId: tagId, evidence: { command: "supertag schema show", tagId, name, alreadyExisted: true } };
    }

    const args = ["tag", "create", name];
    if (typeof row.payload.description === "string" && row.payload.description.trim()) args.push("--description", row.payload.description);
    if (typeof row.payload.extends === "string" && row.payload.extends.trim()) args.push("--extends", row.payload.extends);
    if (row.payload.checkbox === true) args.push("--checkbox");
    if (typeof row.payload.workspace === "string" && row.payload.workspace.trim()) args.push("--workspace", row.payload.workspace);
    await this.mustRunSupertag(args);

    for (let attempt = 0; attempt < 20; attempt += 1) {
      const tagId = await this.findSupertagId(name);
      if (tagId) {
        return { route: "local", targetNodeId: tagId, evidence: { command: "supertag tag create + exact tagged search", tagId, name, readbackVerified: true } };
      }
      await Bun.sleep(100);
    }
    throw new Error(`created supertag ${name} was absent from exact tagged readback`);
  }

  private async findSupertagId(name: string): Promise<string | null> {
    const schema = await this.runSupertag(["schema", "show", name]);
    const schemaId = schema.code === 0 ? parseSchemaId(schema.stdout) : null;
    if (schemaId) return schemaId;
    const search = await this.runSupertag(["search", name, "--json", "--limit", "50", "--show", "--depth", "1"]);
    return search.code === 0 ? parseTaggedSearchId(search.stdout, name) : null;
  }

  private async localField(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const field = requireString(row.payload.fieldName ?? row.payload.attributeId, "fieldName");
    const value = requireString(row.payload.value ?? row.payload.content ?? row.payload.optionId, "value");
    const args = ["set-field", nodeId, field, value];
    if (typeof row.payload.attributeId === "string") args.push("--field-id", row.payload.attributeId);
    if (typeof row.payload.optionId === "string") args.push("--option-id", row.payload.optionId);
    await this.mustRunSupertag(args);
    const read = await this.readNode(nodeId, 1);
    return { route: "local", targetNodeId: nodeId, evidence: { command: "supertag set-field", name: read.name ?? null } };
  }

  private async localDone(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const done = row.payload.done !== false;
    if (done) await this.mustRunSupertag(["done", nodeId]);
    else await this.mustRunSupertag(["undone", nodeId]);
    return { route: "local", targetNodeId: nodeId, evidence: { command: done ? "supertag done" : "supertag undone" } };
  }

  private async localMove(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const targetNodeId = await this.resolveLocalTarget(requireString(row.payload.targetNodeId, "targetNodeId"));
    const body = {
      targetNodeId,
      sourceParentId: typeof row.payload.sourceParentId === "string" ? row.payload.sourceParentId : undefined,
      position: typeof row.payload.position === "string" ? row.payload.position : "end",
      referenceNodeId: typeof row.payload.referenceNodeId === "string" ? row.payload.referenceNodeId : undefined,
      keepSourceReference: row.payload.keepSourceReference === true,
    };
    const result = await this.localJson<Record<string, unknown>>(`/nodes/${encodeURIComponent(nodeId)}/move`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    return { route: "local", targetNodeId: nodeId, evidence: { command: "local-api move", result } };
  }

  private async applyInput(row: WriteRow, nowMs: number): Promise<ApplyResult> {
    if (!this.config.apiEndpoint || !this.config.apiToken) throw new Error("Input API config missing");
    const targetNodeId = row.targetNodeId || this.config.defaultTargetNode || "INBOX";
    const nodes = [buildInputNode(row)];
    const response = await fetchWithTimeout(
      this.config.apiEndpoint,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.config.apiToken}`,
        },
        body: JSON.stringify({ targetNodeId, nodes }),
      },
      this.timeoutMs(),
    );
    const text = await response.text();
    if (!response.ok) throw new Error(`Input API HTTP ${response.status}: ${text}`);
    let parsed: Record<string, unknown> = {};
    try {
      parsed = JSON.parse(text) as Record<string, unknown>;
    } catch {
      parsed = { raw: text };
    }
    return {
      route: "input",
      evidence: {
        ackLedger: true,
        reconcileOnReopen: true,
        targetNodeId,
        appliedAt: nowMs,
        responseKeys: Object.keys(parsed),
      },
    };
  }

  private async resolveLocalTarget(target: string): Promise<string> {
    if (target === "INBOX") return `${await this.defaultWorkspaceId()}_CAPTURE_INBOX`;
    if (target === "LIBRARY" || target === "STASH") return `${await this.defaultWorkspaceId()}_STASH`;
    if (target === "SCHEMA") return `${await this.defaultWorkspaceId()}_SCHEMA`;
    return target;
  }

  private async defaultWorkspaceId(): Promise<string> {
    if (this.workspaceId) return this.workspaceId;
    const workspaces = await this.localJson<Array<{ id: string; name: string }>>("/workspaces", { method: "GET" });
    const first = workspaces[0]?.id;
    if (!first) throw new Error("Local API returned no workspaces");
    this.workspaceId = first;
    return first;
  }

  private async localJson<T>(path: string, init: RequestInit): Promise<T> {
    const response = await this.localRaw(path, init);
    const text = await response.text();
    if (!response.ok) throw new Error(`Local API HTTP ${response.status}: ${text}`);
    return JSON.parse(text) as T;
  }

  private async localRaw(path: string, init: RequestInit): Promise<Response> {
    const token = this.config.localApi?.bearerToken;
    const headers = new Headers(init.headers);
    headers.set("Content-Type", "application/json");
    if (token) headers.set("Authorization", `Bearer ${token}`);
    const base = this.localEndpoint();
    return fetchWithTimeout(`${base}${path}`, { ...init, headers }, this.timeoutMs());
  }

  private async mustRunSupertag(args: string[]): Promise<{ stdout: string; stderr: string }> {
    const result = await this.runSupertag(args);
    if (result.code !== 0) {
      throw new Error(`supertag ${args[0]} failed (${result.code}): ${result.stderr || result.stdout}`);
    }
    return result;
  }

  private async runSupertag(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
    const proc = Bun.spawn([this.options.supertagBin || supertagPath(), ...args], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const timeout = new Promise<never>((_, reject) => {
      setTimeout(() => {
        proc.kill();
        reject(new Error(`supertag timed out after ${this.timeoutMs()}ms`));
      }, this.timeoutMs());
    });
    const done = Promise.all([
      proc.exited,
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]).then(([code, stdout, stderr]) => ({ code, stdout, stderr }));
    return Promise.race([done, timeout]);
  }

  private localEndpoint(): string {
    return (this.config.localApi?.endpoint || DEFAULT_LOCAL_ENDPOINT).replace(/\/$/, "");
  }

  private timeoutMs(): number {
    return this.options.timeoutMs ?? 10_000;
  }

  private hasInputConfig(): boolean {
    return Boolean(this.config.apiEndpoint && this.config.apiToken);
  }
}

export function buildTanaPaste(row: WriteRow): string {
  if (typeof row.payload.tanaPaste === "string") return row.payload.tanaPaste;
  const name = requireString(row.payload.name, "name");
  const lines = ["%%tana%%", `- ${oneLine(name)}`];
  if (typeof row.payload.description === "string" && row.payload.description.trim()) {
    lines.push(`  - Description: ${oneLine(row.payload.description)}`);
  }
  const children = Array.isArray(row.payload.children) ? row.payload.children : [];
  for (const child of children) {
    if (typeof child === "string") lines.push(`  - ${oneLine(child)}`);
    else if (child && typeof child === "object" && "name" in child) {
      lines.push(`  - ${oneLine(String((child as { name: unknown }).name))}`);
    }
  }
  const marker = markerFor(row);
  if (marker) lines.push(`  - ${marker}`);
  return lines.join("\n");
}

function buildInputNode(row: WriteRow): Record<string, unknown> {
  const payload = row.payload;
  const children = Array.isArray(payload.children) ? [...payload.children] : [];
  const marker = markerFor(row);
  if (marker) children.push({ name: marker });
  return {
    name: requireString(payload.name, "name"),
    description: typeof payload.description === "string" ? payload.description : undefined,
    children,
  };
}

export function markerFor(row: WriteRow): string | null {
  if (row.opType !== "create") return null;
  if (row.payload.includeIdempotencyMarker === false) return null;
  // Raw Tana Paste opts out of the marker: buildTanaPaste returns the raw string WITHOUT appending a
  // marker line, so requiring one would fail verification on EVERY attempt and orphan-duplicate the
  // create (the BUG-2 amplification class). Raw paste is at-least-once by design (R1-adjacent).
  if (typeof row.payload.tanaPaste === "string") return null;
  return `${IDEMPOTENCY_PREFIX}${row.dedupKey}`;
}

/** Child text lines from a Local-API read-back: strip the node-id comment and leading bullet. */
function readbackChildLines(markdown: string): string[] {
  return markdown
    .split(/\r?\n/)
    .map((line) => line.replace(/\s*<!--\s*node-id:[^>]*-->\s*$/, "").replace(/^\s*-\s+/, "").trim())
    .filter((line) => line.length > 0);
}

function markdownHasMarkerLine(markdown: string, marker: string): boolean {
  return markdown.split(/\r?\n/).some((rawLine) => {
    // The Local API read-back appends a " <!-- node-id: ... -->" comment to each
    // rendered line; strip it (and any leading bullet/indent) before matching so the
    // dedupKey is compared at its true boundary — never as a prefix of a longer key.
    const line = rawLine.replace(/\s*<!--\s*node-id:[^>]*-->\s*$/, "").trim();
    if (line === marker) return true;
    const markerOffset = line.indexOf(IDEMPOTENCY_PREFIX);
    if (markerOffset === -1) return false;
    return line.slice(markerOffset) === marker;
  });
}

export function markerMatches(markdown: string, row: WriteRow): boolean {
  const marker = markerFor(row);
  if (!marker) return false;
  return markdownHasMarkerLine(markdown, marker);
}

function loadConfig(path: string): SupertagConfig {
  if (!existsSync(path)) return {};
  return JSON.parse(readFileSync(path, "utf-8")) as SupertagConfig;
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${label} is required`);
  return value;
}

export function parseSchemaId(output: string): string | null {
  return output.match(/^ID:\s*(\S+)\s*$/m)?.[1] ?? null;
}

export function parseTaggedSearchId(output: string, exactName: string): string | null {
  try {
    const rows = JSON.parse(output) as Array<{ id?: unknown; name?: unknown; tags?: unknown }>;
    const match = rows.find((row) =>
      row.name === exactName &&
      typeof row.id === "string" &&
      typeof row.tags === "string" &&
      row.tags.split(/[,\s]+/).includes("supertag")
    );
    return typeof match?.id === "string" ? match.id : null;
  } catch {
    return null;
  }
}

function oneLine(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

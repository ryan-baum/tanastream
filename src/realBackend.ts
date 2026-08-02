import { resolveConfig, DEFAULT_LOCAL_ENDPOINT, type TanaStreamConfig } from "./config";
import type { ApplyResult, ApplyRoute, BackendHealth, TanaBackend, WriteRow } from "./types";

interface CreatedNode {
  id: string;
  name: string;
}

const IDEMPOTENCY_PREFIX = "TanaStreamIdempotency - ";

export class RealTanaBackend implements TanaBackend {
  private readonly config: TanaStreamConfig;
  private workspaceId: string | null = null;

  constructor(
    private readonly options: {
      configPath?: string;
      timeoutMs?: number;
    } = {},
  ) {
    this.config = resolveConfig(options.configPath);
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

  /**
   * REST-primary (R-5/KTD-1): the subprocess fallback is gone (R-4). Same idempotent-success
   * exception as before — a 400 containing "already in trash" counts as success (R-6's sole
   * REST exception to the "non-2xx = failed" rule).
   */
  async trashNode(nodeId: string): Promise<ApplyResult> {
    const direct = await this.localRaw(`/nodes/${encodeURIComponent(nodeId)}/trash`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    const text = await direct.text();
    if (direct.ok) {
      return { route: "local", targetNodeId: nodeId, evidence: { command: "REST /trash" } };
    }
    if (direct.status === 400 && text.includes("already in trash")) {
      return { route: "local", targetNodeId: nodeId, evidence: { command: "REST /trash", idempotentAlreadyInTrash: true } };
    }
    throw new Error(`trash failed: HTTP ${direct.status} ${text}`);
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

  /**
   * SPEC-DEFECT E-1 (see BUILD-LEDGER.md): SPEC.md §4's "Key shapes" gloss for `edit_node` reads
   * `edit_node{nodeId!, name, description}` (plain string set), but the captured live schema in
   * mcp-tools-list.json actually requires search-and-replace objects
   * (`name: {old_string, new_string, replace_all?}`). REST `/nodes/{id}/update` — also proven live
   * in §4's endpoint list ("name; description per stress report") and named as KTD-1's parenthetical
   * alternative ("edit"→"edit_node" (or REST `/update`)) — takes a plain `{name, description}` body
   * and needs no read-before-write, so it is used here as primary; `edit_node` is not called at all.
   */
  private async localEdit(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const body: Record<string, unknown> = {};
    const wantName = typeof row.payload.name === "string" ? row.payload.name : undefined;
    const wantDescription = typeof row.payload.description === "string" ? row.payload.description : undefined;
    if (wantName !== undefined) body.name = wantName;
    if (wantDescription !== undefined) body.description = wantDescription;
    await this.localJson(`/nodes/${encodeURIComponent(nodeId)}/update`, { method: "POST", body: JSON.stringify(body) });

    // STRONGER than the old subprocess path (§16): assert the read-back equals what was requested,
    // whitespace-normalized. The old path read but never asserted.
    const read = await this.readNode(nodeId, 1);
    if (wantName !== undefined) {
      const got = typeof read.name === "string" ? oneLine(read.name) : null;
      if (got !== oneLine(wantName)) {
        throw new Error(`Local edit verification failed: name mismatch (expected ${JSON.stringify(oneLine(wantName))}, got ${JSON.stringify(got)})`);
      }
    }
    if (wantDescription !== undefined) {
      const got = typeof read.description === "string" ? oneLine(read.description) : "";
      if (got !== oneLine(wantDescription)) {
        throw new Error(`Local edit verification failed: description mismatch (expected ${JSON.stringify(oneLine(wantDescription))}, got ${JSON.stringify(got)})`);
      }
    }
    return { route: "local", targetNodeId: nodeId, evidence: { command: "REST /update", name: read.name ?? null, verified: true } };
  }

  /**
   * KTD-9: tag/field ops require an ID, never a name — the standalone tool has no local
   * name-resolution index (enforced loudly at enqueue by validate.ts's assertTagIdPresent, but
   * re-checked here as defense-in-depth against a caller that enqueues directly via the spool API).
   *
   * Verification fallback rule (§16): no VERIFIED evidence in this session's context capsule that
   * tag membership renders in `/nodes/{id}` read-back markdown — asserting an unverified rendering
   * property risks false-negatives on legitimate applies. Stays at isError-false + re-read-ok
   * (= old subprocess-path strength). DEFERRED-VERIFY: a live smoke would confirm whether tags
   * render, at which point the STRONGER per-tag assertion could be added.
   */
  private async localTag(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const action = (row.payload.action === "remove" ? "remove" : "add") as "add" | "remove";
    const tagId = requireString(row.payload.tagId, "tagId");
    await this.mcpCall("tag", { nodeId, action, tagIds: [tagId] });
    const read = await this.readNode(nodeId, 1);
    return { route: "local", targetNodeId: nodeId, evidence: { command: `tag ${action}`, name: read.name ?? null } };
  }

  /**
   * KTD-1: create_tag, then confirm via list_tags readback (retry budget 20x100ms, mirrors the old
   * subprocess path's own retry loop). Implementation-defined: uses list_tags exclusively rather
   * than get_tag_schema+list_tags (§16 names both joined by "/" — read as OR) because list_tags
   * alone returns {id, name} pairs sufficient for both the pre-existence check and the post-create
   * readback, with no dependency on parsing create_tag's own response content shape (unverified in
   * this session — mcp-tools-list.json captured tool SCHEMAS, not example tool RESULTS).
   */
  private async localTagCreate(row: WriteRow): Promise<ApplyResult> {
    const name = requireString(row.payload.name, "name");
    const workspaceId = await this.defaultWorkspaceId();
    const existingId = await this.findExistingTagId(name, workspaceId);
    if (existingId) {
      return { route: "local", targetNodeId: existingId, evidence: { command: "list_tags", tagId: existingId, name, alreadyExisted: true } };
    }

    const args: Record<string, unknown> = { workspaceId, name };
    if (typeof row.payload.description === "string" && row.payload.description.trim()) args.description = row.payload.description;
    if (typeof row.payload.extends === "string" && row.payload.extends.trim()) args.extendsTagIds = [row.payload.extends];
    if (row.payload.checkbox === true) args.showCheckbox = true;
    await this.mcpCall("create_tag", args);

    for (let attempt = 0; attempt < 20; attempt += 1) {
      const tagId = await this.findExistingTagId(name, workspaceId);
      if (tagId) {
        return { route: "local", targetNodeId: tagId, evidence: { command: "create_tag + list_tags readback", tagId, name, readbackVerified: true } };
      }
      await Bun.sleep(100);
    }
    throw new Error(`created tag ${name} was absent from list_tags readback`);
  }

  private async findExistingTagId(name: string, workspaceId: string): Promise<string | null> {
    const result = await this.mcpCall("list_tags", { workspaceId, limit: 200 });
    const tags = parseMcpJson<Array<{ id?: unknown; name?: unknown }>>(result) ?? [];
    const match = tags.find((t) => t && typeof t === "object" && t.name === name && typeof t.id === "string");
    return typeof match?.id === "string" ? match.id : null;
  }

  /**
   * KTD-9: attributeId required (not a field name). Same verification-fallback rule as localTag
   * (§16) — field-value rendering in read-back markdown is unverified for every field type
   * (options/date/plain differ), so this stays at isError-false + re-read-ok. DEFERRED-VERIFY.
   */
  private async localField(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const attributeId = requireString(row.payload.attributeId, "attributeId");
    const mode = row.payload.mode === "append" ? "append" : "replace";
    const isOption = typeof row.payload.optionId === "string";
    if (isOption) {
      await this.mcpCall("set_field_option", { nodeId, attributeId, optionId: row.payload.optionId, mode });
    } else {
      const content = requireString(row.payload.value ?? row.payload.content, "value");
      await this.mcpCall("set_field_content", { nodeId, attributeId, content, mode });
    }
    const read = await this.readNode(nodeId, 1);
    return {
      route: "local",
      targetNodeId: nodeId,
      evidence: { command: isOption ? "set_field_option" : "set_field_content", name: read.name ?? null },
    };
  }

  /** §16: isError-false is the full bar (= old strength) — checkbox state isn't reliably rendered in read-back markdown. */
  private async localDone(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const done = row.payload.done !== false;
    await this.mcpCall(done ? "check_node" : "uncheck_node", { nodeId });
    return { route: "local", targetNodeId: nodeId, evidence: { command: done ? "check_node" : "uncheck_node" } };
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

  /**
   * R-6: the full /mcp failure contract. Success is judged SOLELY by `result.isError` — never HTTP
   * status. The Accept header is mandatory (its absence gets `-32000 Not Acceptable`, §4 VERIFIED).
   * Any of: non-2xx, a JSON-RPC `error` member, a malformed (non-JSON) body, `result.isError`
   * truthy, or a transport error (timeout/refused, which `fetchWithTimeout`'s own throw already
   * surfaces) routes to the caller's existing retry/dead-letter path via a thrown Error.
   */
  private async mcpCall(name: string, args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const response = await this.localRaw("/mcp", {
      method: "POST",
      headers: { Accept: "application/json, text/event-stream" },
      body: JSON.stringify({ jsonrpc: "2.0", id: Date.now(), method: "tools/call", params: { name, arguments: args } }),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`/mcp HTTP ${response.status} calling ${name}: ${text}`);
    let parsed: { result?: Record<string, unknown>; error?: { code?: number; message?: string } };
    try {
      parsed = JSON.parse(text) as typeof parsed;
    } catch {
      throw new Error(`/mcp non-JSON response calling ${name}: ${text}`);
    }
    if (parsed.error) throw new Error(`/mcp JSON-RPC error calling ${name}: ${JSON.stringify(parsed.error)}`);
    const result = parsed.result;
    if (!result || result.isError) throw new Error(`/mcp tool ${name} isError calling ${name}: ${JSON.stringify(result)}`);
    return result;
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

function requireString(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${label} is required`);
  return value;
}

/**
 * Best-effort extraction of structured data from an MCP tool result: prefers `structuredContent`
 * (newer MCP servers), else scans `content[].text` blocks for a JSON-parseable string. Returns null
 * rather than throwing when neither is present/parseable — callers treat null as "not found" and
 * fall back to their own readback loop rather than trusting a guessed shape. Unverified in this
 * session (mcp-tools-list.json captured tool SCHEMAS, not example RESULTS) — DEFERRED-VERIFY.
 */
function parseMcpJson<T>(result: Record<string, unknown>): T | null {
  if (result.structuredContent !== undefined) return result.structuredContent as T;
  const content = result.content;
  if (Array.isArray(content)) {
    for (const block of content) {
      if (block && typeof block === "object" && (block as { type?: unknown }).type === "text") {
        const text = (block as { text?: unknown }).text;
        if (typeof text === "string") {
          try {
            return JSON.parse(text) as T;
          } catch {
            // not JSON — try the next block
          }
        }
      }
    }
  }
  return null;
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

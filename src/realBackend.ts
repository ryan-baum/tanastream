import { resolveConfig, DEFAULT_LOCAL_ENDPOINT, type TanaStreamConfig } from "./config";
import { isRaw } from "./validate";
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

  /**
   * A transport failure DURING reconcile must never resolve to a definitive "not found" — that
   * would let the caller (queue.ts's drainOnce) proceed to `apply()` a write that may already
   * have landed, creating a real duplicate. The listing GET is not caught here (it propagates);
   * a per-candidate read failure doesn't silently `continue` past a node that might have BEEN
   * the marker — it's tracked, and if the search completes without a definitive match while any
   * candidate read failed, this throws instead of returning null. Only a clean sweep with zero
   * read failures returns a reliable null.
   */
  async reconcile(row: WriteRow): Promise<ApplyResult | null> {
    if (row.opType !== "create") return null;
    const health = await this.health();
    if (!health.localAvailable) return null;
    const marker = markerFor(row);
    if (!marker) return null;
    const targetNodeId = await this.resolveLocalTarget(row.targetNodeId || this.config.defaultTargetNode || "INBOX");
    // Compare against the STORED name, which Tana whitespace-collapses (oneLine) at create time.
    // Matching the raw payload name would skip the marker node for any multi-space / edge-whitespace
    // title and silently re-create it on crash-replay (an orphan-duplicate class at this seam).
    const expectedName = typeof row.payload.name === "string" ? oneLine(row.payload.name) : null;
    // Paginate: a single limit=1000 GET misses a marker node that sorts past child #1000. The
    // seen-id + no-progress guards keep this safe (and terminating) even if the API ignores offset.
    const pageSize = 1000;
    const seen = new Set<string>();
    let anyReadFailed = false;
    for (let offset = 0; ; offset += pageSize) {
      // NOT caught: a transport failure on the listing itself must propagate (fail closed).
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
        let read: { markdown: string } | null;
        try {
          read = await this.readNode(child.id, 3);
        } catch {
          // Could not confirm or deny THIS candidate — the eventual "not found" verdict is
          // unreliable if this is the only name-matching candidate. Flag and keep searching
          // (a later candidate might still resolve the match definitively).
          anyReadFailed = true;
          continue;
        }
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
    if (anyReadFailed) {
      throw new Error(
        "reconcile could not confirm the marker's absence — at least one name-matching candidate's " +
          "read-back failed transiently; refusing to report a definitive not-found (would risk a duplicate apply)",
      );
    }
    return null;
  }

  async readNode(nodeId: string, maxDepth = 2): Promise<{ markdown: string; name?: string; description?: string | null }> {
    return this.localJson(`/nodes/${encodeURIComponent(nodeId)}?maxDepth=${maxDepth}`, { method: "GET" });
  }

  /**
   * REST-primary — no subprocess fallback. A 400 containing "already in trash" counts as success
   * (the sole exception to the "non-2xx = failed" rule).
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
    // Backstop: for structured creates, verify the node landed LITERALLY (name + each child).
    // This catches any Tana Paste misparse the enqueue-time denylist missed (e.g. an unknown
    // control sequence) — it converts silent corruption into a loud dead-letter.
    // Raw paste (isRaw — not just a literal tanaPaste string) opts out: rawTanaPaste:true means
    // Tana Paste syntax in name/description/children is INTENTIONAL, so the read-back is expected
    // to differ from the literal payload (that's the whole point), and this literal-equality
    // check would misfire as a false "misparse" on every legitimate use.
    if (!isRaw(row.payload)) {
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
        const text = childDisplayText(child) ?? "";
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
   * `edit` uses REST `POST /nodes/{id}/update` rather than the `/mcp` `edit_node` tool. Tana's
   * `edit_node` tool takes `name`/`description` as search-and-replace objects
   * (`{old_string, new_string, replace_all?}`), not a plain field set — REST `/update` takes the
   * plain `{name, description}` body this code actually wants, with no read-before-write needed,
   * so it's used as primary and `edit_node` is never called.
   */
  private async localEdit(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const body: Record<string, unknown> = {};
    const wantName = typeof row.payload.name === "string" ? row.payload.name : undefined;
    const wantDescription = typeof row.payload.description === "string" ? row.payload.description : undefined;
    if (wantName !== undefined) body.name = wantName;
    if (wantDescription !== undefined) body.description = wantDescription;
    await this.localJson(`/nodes/${encodeURIComponent(nodeId)}/update`, { method: "POST", body: JSON.stringify(body) });

    // Assert the read-back equals what was requested, whitespace-normalized — don't just read it
    // and hope; a silent no-op write would otherwise mark applied without actually landing.
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
   * tag/field ops require an ID, never a name — the standalone tool has no local name-resolution
   * index (enforced loudly at enqueue by validate.ts's assertTagIdPresent, but re-checked here as
   * defense-in-depth against a caller that enqueues directly via the spool API, bypassing that
   * check).
   *
   * Verification stays at isError-false + re-read-ok rather than asserting that tag membership is
   * visible in `/nodes/{id}` read-back markdown — that rendering behavior hasn't been confirmed
   * against a live Tana instance, and asserting an unconfirmed property risks false-negatives on
   * legitimate applies. A live-verified confirmation would let this become a stronger per-tag
   * assertion (see tests/live-smoke.test.ts).
   */
  private async localTag(row: WriteRow): Promise<ApplyResult> {
    const nodeId = requireString(row.payload.nodeId ?? row.targetNodeId, "nodeId");
    const action = (row.payload.action === "remove" ? "remove" : "add") as "add" | "remove";
    const tagId = requireString(row.payload.tagId, "tagId");
    await this.mcpCall("tag", { nodeId, action, tagIds: [tagId] });
    return this.readNodeEvidence(nodeId, `tag ${action}`);
  }

  /**
   * create_tag, then confirm via a list_tags readback (retry budget 20x100ms — the tag may take a
   * moment to become visible after creation). Uses list_tags exclusively rather than also calling
   * get_tag_schema, because list_tags alone returns {id, name} pairs sufficient for both the
   * pre-existence check and the post-create readback, with no dependency on parsing create_tag's
   * own response content shape (which this codebase has never observed against a live server —
   * only the tool's input schema was captured, not an example result).
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

  /**
   * parseMcpJson's `null` means "this code doesn't know how to parse the response," NOT "no tags
   * exist" — collapsing both into an empty array would make the pre-existence check below always
   * report "not found" on a persistent parse mismatch and call `create_tag` again on every run,
   * silently minting a duplicate tag each time. A genuinely empty tags array is still a valid
   * "not found" signal (`tags.find(...)` naturally returns undefined -> null below); only an
   * UNPARSEABLE response — a real ambiguity, not an absence — throws.
   */
  private async findExistingTagId(name: string, workspaceId: string): Promise<string | null> {
    const result = await this.mcpCall("list_tags", { workspaceId, limit: 200 });
    const tags = parseMcpJson<Array<{ id?: unknown; name?: unknown }>>(result);
    if (tags === null) {
      throw new Error(
        `list_tags returned a response shape this build could not parse (neither structuredContent ` +
          `nor a JSON-parseable content[].text block) — refusing to treat that as "tag not found," ` +
          `which would risk creating a duplicate tag. Raw result: ${JSON.stringify(result).slice(0, 500)}`,
      );
    }
    const match = tags.find((t) => t && typeof t === "object" && t.name === name && typeof t.id === "string");
    return typeof match?.id === "string" ? match.id : null;
  }

  /**
   * attributeId required (not a field name). Same verification approach as localTag — field-value
   * rendering in read-back markdown hasn't been confirmed against a live Tana instance for every
   * field type (options/date/plain differ), so this stays at isError-false + re-read-ok.
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
    return this.readNodeEvidence(nodeId, isOption ? "set_field_option" : "set_field_content");
  }

  /**
   * Shared read-back-and-report tail for ops whose verification stays at the isError-false +
   * re-read-ok tier: localTag and localField had this identical block twice.
   */
  private async readNodeEvidence(nodeId: string, command: string): Promise<ApplyResult> {
    const read = await this.readNode(nodeId, 1);
    return { route: "local", targetNodeId: nodeId, evidence: { command, name: read.name ?? null } };
  }

  /** isError-false is the full verification bar here — checkbox state isn't reliably rendered in read-back markdown. */
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
   * The full /mcp failure contract. Success is judged SOLELY by `result.isError` — never HTTP
   * status. The Accept header is mandatory (its absence gets a `-32000 Not Acceptable` error).
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
    const body = extractMcpResponseBody(text, response.headers.get("content-type"));
    let parsed: { result?: Record<string, unknown>; error?: { code?: number; message?: string } };
    try {
      parsed = JSON.parse(body) as typeof parsed;
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

/**
 * A create's `children` payload entries are either a bare string or `{name: ...}`. Shared by
 * buildTanaPaste (what gets sent) and localCreate's read-back verification (what must land) —
 * keeping both in lockstep matters: if they diverge, verification could pass/fail against
 * different text than what was actually sent. Returns null (not "") for a genuinely unrecognized
 * child shape, so callers can distinguish "recognized but empty" from "not a child at all".
 */
function childDisplayText(child: unknown): string | null {
  if (typeof child === "string") return oneLine(child);
  if (child && typeof child === "object" && "name" in child) {
    return oneLine(String((child as { name: unknown }).name));
  }
  return null;
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
    const text = childDisplayText(child);
    if (text !== null) lines.push(`  - ${text}`);
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
  // Raw Tana Paste opts out of the marker under EITHER raw mechanism (isRaw checks both a literal
  // tanaPaste string and rawTanaPaste:true) — a marker appended into content the producer
  // explicitly wants Tana-Paste-reinterpreted is an assumption verification can't safely make.
  // A literal tanaPaste string bypasses buildTanaPaste's marker-append entirely (early return);
  // rawTanaPaste:true with structured fields still gets built normally but gets NO marker line
  // either. Raw paste is at-least-once by design either way (a crash mid-apply can duplicate it;
  // the marker exists specifically to prevent that for the non-raw case).
  if (isRaw(row.payload)) return null;
  return `${IDEMPOTENCY_PREFIX}${row.dedupKey}`;
}

/** The Local API read-back appends this comment to every rendered line; both markdown-parsing
 * helpers below strip it before matching. */
const NODE_ID_COMMENT_RE = /\s*<!--\s*node-id:[^>]*-->\s*$/;

/** Child text lines from a Local-API read-back: strip the node-id comment and leading bullet. */
function readbackChildLines(markdown: string): string[] {
  return markdown
    .split(/\r?\n/)
    .map((line) => line.replace(NODE_ID_COMMENT_RE, "").replace(/^\s*-\s+/, "").trim())
    .filter((line) => line.length > 0);
}

function markdownHasMarkerLine(markdown: string, marker: string): boolean {
  return markdown.split(/\r?\n/).some((rawLine) => {
    // Strip the node-id comment (and any leading bullet/indent) before matching so the
    // dedupKey is compared at its true boundary — never as a prefix of a longer key.
    const line = rawLine.replace(NODE_ID_COMMENT_RE, "").trim();
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
 * rather than throwing when neither is present/parseable — callers treat null as "unknown" and
 * fall back to their own readback loop rather than trusting a guessed shape. This shape has not
 * been confirmed against a live Tana instance's actual tool-call results — only the tools' input
 * schemas were available when this was written — hence the defensive, never-guess design.
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

/**
 * `/mcp` requests advertise `Accept: application/json, text/event-stream` — Tana's server is free
 * to answer with either. Every live call this codebase has directly observed came back as plain
 * JSON, but tolerating an SSE-framed reply defensively (rather than assuming JSON always) costs
 * nothing and avoids a false "non-JSON response" error if the server ever chooses that framing.
 * SSE framing wraps the payload in `data: <content>` lines (blank-line-terminated events, per the
 * SSE spec's multi-line-data join rule) rather than a bare JSON body. Only unwraps when the
 * response's own Content-Type says event-stream; a plain JSON body passes through untouched.
 */
function extractMcpResponseBody(text: string, contentType: string | null): string {
  if (!contentType?.includes("text/event-stream")) return text;
  const dataLines = text
    .split(/\r?\n/)
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart());
  return dataLines.length > 0 ? dataLines.join("\n") : text;
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

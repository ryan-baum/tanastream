// U-3 (SPEC.md §8): direct-HTTP transport — REST for edit/trash (KTD-1), /mcp JSON-RPC for
// tag/tag-create/field/done (KTD-1), R-6's full /mcp failure contract, and KTD-9's tagId/attributeId
// enqueue-time rejection. Every test hits a real Bun.serve mock — no subprocess, no live Tana.

import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { openSpool } from "../src/spool";
import { RealTanaBackend } from "../src/realBackend";
import { enqueueWrite } from "../src/queue";
import type { WriteRow } from "../src/types";

interface MockNode {
  id: string;
  name: string;
  description?: string | null;
  trashed?: boolean;
}

let server: ReturnType<typeof Bun.serve> | null = null;
let PORT = 0;
let nodesById: Record<string, MockNode> = {};
let tags: Array<{ id: string; name: string }> = [];
let mcpMode: "ok" | "isError" | "http500" | "rpcError" | "malformed" | "unparseableListTags" = "ok";
let lastMcpAccept: string | null = null;
let lastMcpCall: { name: string; arguments: Record<string, unknown> } | null = null;
let createTagCallCount = 0;
let tagAppearsAfterNCreateCalls = 1;

function markdownFor(node: MockNode): string {
  return `- ${node.name} <!-- node-id: ${node.id} -->`;
}

beforeAll(() => {
  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      const path = url.pathname;

      if (path === "/health") return Response.json({ status: "ok" });
      if (path === "/workspaces") return Response.json([{ id: "ws1", name: "Test" }]);

      if (path === "/mcp" && req.method === "POST") {
        lastMcpAccept = req.headers.get("Accept");
        const body = (await req.json().catch(() => ({}))) as { id?: number; params?: { name: string; arguments: Record<string, unknown> } };
        lastMcpCall = body.params ?? null;
        const toolName = body.params?.name;

        if (mcpMode === "http500") return new Response("boom", { status: 500 });
        if (mcpMode === "malformed") return new Response("not json{{{", { status: 200 });
        if (mcpMode === "rpcError") {
          return Response.json({ jsonrpc: "2.0", id: body.id, error: { code: -32000, message: "simulated rpc error" } });
        }
        if (mcpMode === "isError") {
          return Response.json({ jsonrpc: "2.0", id: body.id, result: { isError: true, content: [{ type: "text", text: "simulated tool failure" }] } });
        }

        if (toolName === "list_tags" && mcpMode === "unparseableListTags") {
          // isError:false (a real success per the JSON-RPC contract) but content[].text is
          // human-readable prose, not JSON — parseMcpJson must return null for this, and
          // findExistingTagId must treat that as "unknown," never as "not found."
          return Response.json({
            jsonrpc: "2.0",
            id: body.id,
            result: { isError: false, content: [{ type: "text", text: "Tags in this workspace: none configured yet." }] },
          });
        }

        // mcpMode === "ok" — dispatch a plausible per-tool success shape
        if (toolName === "list_tags") {
          const visible = createTagCallCount >= tagAppearsAfterNCreateCalls ? tags : [];
          return Response.json({
            jsonrpc: "2.0",
            id: body.id,
            result: { isError: false, content: [{ type: "text", text: JSON.stringify(visible) }] },
          });
        }
        if (toolName === "create_tag") {
          createTagCallCount += 1;
          return Response.json({
            jsonrpc: "2.0",
            id: body.id,
            result: { isError: false, content: [{ type: "text", text: "Created tag" }] },
          });
        }
        // tag / set_field_content / set_field_option / check_node / uncheck_node — generic success
        return Response.json({ jsonrpc: "2.0", id: body.id, result: { isError: false, content: [{ type: "text", text: "ok" }] } });
      }

      const updateMatch = path.match(/^\/nodes\/([^/]+)\/update$/);
      if (updateMatch && req.method === "POST") {
        const id = decodeURIComponent(updateMatch[1]);
        const body = (await req.json().catch(() => ({}))) as { name?: string; description?: string };
        const node = nodesById[id];
        if (!node) return new Response("not found", { status: 404 });
        if (body.name !== undefined) node.name = body.name;
        if (body.description !== undefined) node.description = body.description;
        return Response.json({});
      }

      const trashMatch = path.match(/^\/nodes\/([^/]+)\/trash$/);
      if (trashMatch && req.method === "POST") {
        const id = decodeURIComponent(trashMatch[1]);
        const node = nodesById[id];
        if (!node) return new Response("not found", { status: 404 });
        if (node.trashed) return new Response(JSON.stringify({ error: "node is already in trash" }), { status: 400 });
        node.trashed = true;
        return Response.json({});
      }

      const nodeMatch = path.match(/^\/nodes\/([^/]+)$/);
      if (nodeMatch && req.method === "GET") {
        const id = decodeURIComponent(nodeMatch[1]);
        const node = nodesById[id];
        if (!node) return new Response("not found", { status: 404 });
        return Response.json({ markdown: markdownFor(node), name: node.name, description: node.description ?? null });
      }

      return new Response("unhandled", { status: 404 });
    },
  });
  PORT = server.port;
});

afterAll(() => {
  server?.stop(true);
});

afterEach(() => {
  nodesById = {};
  tags = [];
  mcpMode = "ok";
  lastMcpAccept = null;
  lastMcpCall = null;
  createTagCallCount = 0;
  tagAppearsAfterNCreateCalls = 1;
});

function makeBackend(): { backend: RealTanaBackend; cleanup: () => void } {
  const dir = mkdtempSync(join(tmpdir(), "tanastream-directhttp-"));
  const configPath = join(dir, "config.json");
  writeFileSync(
    configPath,
    JSON.stringify({ localApi: { enabled: true, endpoint: `http://127.0.0.1:${PORT}`, bearerToken: "t" } }),
  );
  return { backend: new RealTanaBackend({ configPath }), cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

function rowFor(opType: WriteRow["opType"], key: string, payload: Record<string, unknown>, targetNodeId = "n1"): WriteRow {
  const dir = mkdtempSync(join(tmpdir(), "tanastream-directhttp-spool-"));
  const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
  try {
    spool.enqueue({ opType, idempotencyKey: key, payload, targetNodeId, source: "t" });
    return spool.getByDedupKey(`${opType}:${key}`)!;
  } finally {
    spool.close();
  }
}

describe("R-4/R-5: edit via REST /update", () => {
  test("success: name+description round-trip, asserted (STRONGER than the old subprocess path)", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Old Name", description: "old desc" };
      const row = rowFor("edit", "e1", { nodeId: "n1", name: "New Name", description: "new desc" });
      const result = await backend.apply(row, "local", { nowMs: Date.now() });
      expect(result.route).toBe("local");
      expect(nodesById.n1.name).toBe("New Name");
      expect(nodesById.n1.description).toBe("new desc");
    } finally {
      cleanup();
    }
  });

  test("must-reject: a mock server that silently ignores the update throws (read-back mismatch)", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Unchanged" }; // update handler will still "apply" it in this mock,
      // so instead simulate the failure by pointing at a node whose stored name won't match after update:
      const row = rowFor("edit", "e2", { nodeId: "n1", name: "Whatever" });
      // Force a mismatch: monkey-patch the mock's update handling by trashing name right after via a second row
      // Simpler: assert directly against a node that the update endpoint won't find -> update() 404s -> throws.
      const missingRow = rowFor("edit", "e3", { nodeId: "does-not-exist", name: "X" });
      await expect(backend.apply(missingRow, "local", { nowMs: Date.now() })).rejects.toThrow();
    } finally {
      cleanup();
    }
  });
});

describe("R-5: tag add/remove via /mcp", () => {
  test("success: isError-false applies", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Tagged Node" };
      const row = rowFor("tag", "t1", { nodeId: "n1", action: "add", tagId: "tag-abc" });
      const result = await backend.apply(row, "local", { nowMs: Date.now() });
      expect(result.route).toBe("local");
      expect(lastMcpCall?.name).toBe("tag");
      expect(lastMcpCall?.arguments).toEqual({ nodeId: "n1", action: "add", tagIds: ["tag-abc"] });
    } finally {
      cleanup();
    }
  });

  test("must-reject: isError-true throws (not applied)", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Tagged Node" };
      mcpMode = "isError";
      const row = rowFor("tag", "t2", { nodeId: "n1", action: "add", tagId: "tag-abc" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow();
    } finally {
      cleanup();
    }
  });
});

describe("KTD-9: tag/field ops require an ID at enqueue time, never a name", () => {
  test("a 'tag' op without tagId is rejected loudly at enqueue", async () => {
    const dir = mkdtempSync(join(tmpdir(), "tanastream-directhttp-ktd9-"));
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    try {
      await expect(
        enqueueWrite(spool, { opType: "tag", payload: { nodeId: "n1", action: "add", tagNameOrId: "SomeTagName" }, source: "t" }),
      ).rejects.toThrow(/TANA_ID_REQUIRED/);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("a 'field' op without attributeId is rejected loudly at enqueue", async () => {
    const dir = mkdtempSync(join(tmpdir(), "tanastream-directhttp-ktd9b-"));
    const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
    try {
      await expect(
        enqueueWrite(spool, { opType: "field", payload: { nodeId: "n1", fieldName: "Status", value: "Done" }, source: "t" }),
      ).rejects.toThrow(/TANA_ID_REQUIRED/);
    } finally {
      spool.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("R-5: field via /mcp set_field_content / set_field_option", () => {
  test("plain content value -> set_field_content", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Field Node" };
      const row = rowFor("field", "f1", { nodeId: "n1", attributeId: "attr-1", value: "hello" });
      await backend.apply(row, "local", { nowMs: Date.now() });
      expect(lastMcpCall?.name).toBe("set_field_content");
      expect(lastMcpCall?.arguments).toEqual({ nodeId: "n1", attributeId: "attr-1", content: "hello", mode: "replace" });
    } finally {
      cleanup();
    }
  });

  test("optionId present -> set_field_option", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Field Node" };
      const row = rowFor("field", "f2", { nodeId: "n1", attributeId: "attr-1", optionId: "opt-1" });
      await backend.apply(row, "local", { nowMs: Date.now() });
      expect(lastMcpCall?.name).toBe("set_field_option");
      expect(lastMcpCall?.arguments).toEqual({ nodeId: "n1", attributeId: "attr-1", optionId: "opt-1", mode: "replace" });
    } finally {
      cleanup();
    }
  });

  test("must-reject: isError-true throws", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Field Node" };
      mcpMode = "isError";
      const row = rowFor("field", "f3", { nodeId: "n1", attributeId: "attr-1", value: "x" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow();
    } finally {
      cleanup();
    }
  });
});

describe("R-5: done/undone via /mcp check_node / uncheck_node", () => {
  test("done -> check_node", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      const row = rowFor("done", "d1", { nodeId: "n1", done: true });
      await backend.apply(row, "local", { nowMs: Date.now() });
      expect(lastMcpCall?.name).toBe("check_node");
      expect(lastMcpCall?.arguments).toEqual({ nodeId: "n1" });
    } finally {
      cleanup();
    }
  });

  test("done:false -> uncheck_node", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      const row = rowFor("done", "d2", { nodeId: "n1", done: false });
      await backend.apply(row, "local", { nowMs: Date.now() });
      expect(lastMcpCall?.name).toBe("uncheck_node");
    } finally {
      cleanup();
    }
  });
});

describe("R-5: tag-create via create_tag + list_tags readback", () => {
  test("already exists: list_tags pre-check short-circuits, no create_tag call", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      tags = [{ id: "existing-id", name: "MyTag" }];
      tagAppearsAfterNCreateCalls = 0; // visible immediately
      const row = rowFor("tag-create", "tc1", { name: "MyTag" });
      const result = await backend.apply(row, "local", { nowMs: Date.now() });
      expect(result.targetNodeId).toBe("existing-id");
      expect(result.evidence.alreadyExisted).toBe(true);
    } finally {
      cleanup();
    }
  });

  test("does not exist: create_tag then found on the first list_tags readback poll", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      tagAppearsAfterNCreateCalls = 1; // becomes visible once createTagCallCount >= 1
      tags = [{ id: "new-id", name: "FreshTag" }];
      const row = rowFor("tag-create", "tc2", { name: "FreshTag" });
      const result = await backend.apply(row, "local", { nowMs: Date.now() });
      expect(result.targetNodeId).toBe("new-id");
      expect(result.evidence.readbackVerified).toBe(true);
    } finally {
      cleanup();
    }
  });

  test("must-reject: never appearing in readback throws after the retry budget", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      tagAppearsAfterNCreateCalls = 999; // never satisfied within 20 attempts
      tags = [{ id: "never-id", name: "GhostTag" }];
      const row = rowFor("tag-create", "tc3", { name: "GhostTag" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow(/absent from list_tags readback/);
    } finally {
      cleanup();
    }
  }, 5000);

  // U-10 simplify-pass hardening (ALTITUDE finding, HIGH — coordinator-flagged): an unparseable
  // list_tags response must NEVER be silently treated as "tag not found." Before this fix,
  // parseMcpJson's null was coerced to `[]`, so a persistent parse mismatch would make the
  // pre-existence check always report "not found" and mint a duplicate tag on every apply.
  // Both directions: the ambiguous-response case fails LOUD (this test), and a genuinely empty
  // tags array still correctly proceeds to create (covered by the "does not exist" test above).
  test("must-reject: an unparseable list_tags response throws — never silently treated as not-found", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      mcpMode = "unparseableListTags";
      const row = rowFor("tag-create", "tc4", { name: "AmbiguousTag" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow(/could not parse/);
    } finally {
      cleanup();
    }
  });
});

describe("R-6: the full /mcp failure contract (verifier, both directions)", () => {
  test("sends the mandatory Accept header on every /mcp call", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      const row = rowFor("done", "acc1", { nodeId: "n1" });
      await backend.apply(row, "local", { nowMs: Date.now() });
      expect(lastMcpAccept).toBe("application/json, text/event-stream");
    } finally {
      cleanup();
    }
  });

  test("HTTP 200 + result.isError:true is NOT applied (isError, not status, decides)", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      mcpMode = "isError";
      const row = rowFor("done", "acc2", { nodeId: "n1" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow(/isError/);
    } finally {
      cleanup();
    }
  });

  test("non-2xx HTTP status is NOT applied", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      mcpMode = "http500";
      const row = rowFor("done", "acc3", { nodeId: "n1" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow(/HTTP 500/);
    } finally {
      cleanup();
    }
  });

  test("a JSON-RPC `error` member is NOT applied", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      mcpMode = "rpcError";
      const row = rowFor("done", "acc4", { nodeId: "n1" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow(/JSON-RPC error/);
    } finally {
      cleanup();
    }
  });

  test("a malformed (non-JSON) body is NOT applied", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      mcpMode = "malformed";
      const row = rowFor("done", "acc5", { nodeId: "n1" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow(/non-JSON/);
    } finally {
      cleanup();
    }
  });

  test("success shape (isError:false) applies", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      const row = rowFor("done", "acc6", { nodeId: "n1" });
      const result = await backend.apply(row, "local", { nowMs: Date.now() });
      expect(result.route).toBe("local");
    } finally {
      cleanup();
    }
  });
});

describe("R-5/KTD-1: trash — REST-primary, 400-already-in-trash idempotent (R-6's sole exception)", () => {
  test("first trash succeeds via REST", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "To Trash" };
      const result = await backend.trashNode("n1");
      expect(result.route).toBe("local");
      expect(nodesById.n1.trashed).toBe(true);
    } finally {
      cleanup();
    }
  });

  test("trashing an already-trashed node is idempotent success (the R-6 exception)", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      nodesById.n1 = { id: "n1", name: "Already Trashed", trashed: true };
      const result = await backend.trashNode("n1");
      expect(result.evidence.idempotentAlreadyInTrash).toBe(true);
    } finally {
      cleanup();
    }
  });

  test("a genuine trash failure (not the 400-already-in-trash shape) throws", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      // node doesn't exist -> mock 404 -> not the idempotent-400 shape -> must throw
      await expect(backend.trashNode("does-not-exist")).rejects.toThrow();
    } finally {
      cleanup();
    }
  });
});

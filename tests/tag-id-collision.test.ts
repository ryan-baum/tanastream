// Exact-tag-ID collision matrix — the rows not already covered by tests/direct-http.test.ts.
//
// A tag write is opaque request data: whatever bytes the caller supplies as `tagId` are the bytes
// that must reach Tana's `tag` MCP tool, with no name lookup, no normalization, and no silent
// default. These cases pin the edges that a happy-path test cannot reach: non-string IDs, the
// remove arm of the failure contract, and padded IDs.
//
// Self-contained: its own port-0 Bun.serve mock, no module state shared with any other suite.

import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { openSpool } from "../src/spool";
import { RealTanaBackend } from "../src/realBackend";
import { enqueueWrite } from "../src/queue";
import type { WriteRow } from "../src/types";

let server: ReturnType<typeof Bun.serve> | null = null;
let PORT = 0;
let mcpMode: "ok" | "isError" | "rpcError" | "http500" = "ok";
let mcpCalls: Array<{ name: string; arguments: Record<string, unknown> }> = [];
const NODE_ID = "n1";

beforeAll(() => {
  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      if (path === "/health") return Response.json({ status: "ok" });
      if (path === "/workspaces") return Response.json([{ id: "ws1", name: "Test" }]);

      if (path === "/mcp" && req.method === "POST") {
        const body = (await req.json().catch(() => ({}))) as {
          id?: number;
          params?: { name: string; arguments: Record<string, unknown> };
        };
        if (body.params) mcpCalls.push(structuredClone(body.params));
        if (mcpMode === "http500") return new Response("boom", { status: 500 });
        if (mcpMode === "rpcError") {
          return Response.json({ jsonrpc: "2.0", id: body.id, error: { code: -32000, message: "simulated rpc error" } });
        }
        if (mcpMode === "isError") {
          return Response.json({
            jsonrpc: "2.0",
            id: body.id,
            result: { isError: true, content: [{ type: "text", text: "simulated tool failure" }] },
          });
        }
        return Response.json({ jsonrpc: "2.0", id: body.id, result: { isError: false, content: [{ type: "text", text: "ok" }] } });
      }

      const nodeMatch = path.match(/^\/nodes\/([^/]+)$/);
      if (nodeMatch && req.method === "GET") {
        const id = decodeURIComponent(nodeMatch[1]);
        if (id !== NODE_ID) return new Response("not found", { status: 404 });
        return Response.json({ markdown: `- Tagged Node <!-- node-id: ${id} -->`, name: "Tagged Node", description: null });
      }

      return new Response("unhandled", { status: 404 });
    },
  });
  PORT = server.port ?? 0;
});

afterAll(() => {
  server?.stop(true);
});

afterEach(() => {
  mcpMode = "ok";
  mcpCalls = [];
});

function makeBackend(): { backend: RealTanaBackend; cleanup: () => void } {
  const dir = mkdtempSync(join(tmpdir(), "tanastream-tagid-"));
  const configPath = join(dir, "config.json");
  writeFileSync(configPath, JSON.stringify({ localApi: { enabled: true, endpoint: `http://127.0.0.1:${PORT}`, bearerToken: "t" } }));
  return { backend: new RealTanaBackend({ configPath }), cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

function withSpool<T>(prefix: string, fn: (spool: ReturnType<typeof openSpool>) => T): T {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
  try {
    return fn(spool);
  } finally {
    spool.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

/** Async sibling of withSpool — closing the database in a sync `finally` would race an awaited body. */
async function withSpoolAsync<T>(prefix: string, fn: (spool: ReturnType<typeof openSpool>) => Promise<T>): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
  try {
    return await fn(spool);
  } finally {
    spool.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

function rowFor(key: string, payload: Record<string, unknown>): WriteRow {
  return withSpool("tanastream-tagid-row-", (spool) => {
    spool.enqueue({ opType: "tag", idempotencyKey: key, payload, targetNodeId: NODE_ID, source: "t" });
    return spool.getByDedupKey(`tag:${key}`)!;
  });
}

describe("a tagId that is not a string never reaches the wire", () => {
  // assertTagIdPresent gates on `typeof payload.tagId === "string"`. Each of these is a shape a
  // JSON producer can realistically emit — a numeric ID, a null from an absent lookup, a nested
  // object, an array of IDs — and every one must be refused at the queue boundary rather than
  // stringified into a plausible-looking but wrong request.
  //
  // The `null` case carries its own weight rather than riding along with the others: loosening the
  // guard to a truthy check leaves it passing, but a `null`-specific carve-out (the plausible
  // "null means clear the tag" bug) is caught by this row and by no other.
  const nonStringIds: Array<[string, unknown]> = [
    ["number", 12345],
    ["null", null],
    ["object", { id: "tag-abc" }],
    ["array", ["tag-abc"]],
    ["boolean", true],
  ];

  test.each(nonStringIds)("a %s tagId is rejected before queue mutation", async (_label, tagId) => {
    await withSpoolAsync("tanastream-tagid-nonstring-", async (spool) => {
      await expect(
        enqueueWrite(spool, { opType: "tag", payload: { nodeId: NODE_ID, action: "add", tagId }, source: "t" }),
      ).rejects.toThrow(/TANA_ID_REQUIRED/);
      expect(spool.status()).toMatchObject({ pending: 0, inflight: 0, applied: 0, dead: 0 });
      expect(mcpCalls).toHaveLength(0);
    });
  });
});

describe("the /mcp failure contract holds on the remove arm too", () => {
  // The add arm is covered elsewhere. Remove is the arm whose silent success would be worst: the
  // caller believes a tag was detached and it never was.
  const failureModes: Array<[string, typeof mcpMode]> = [
    ["result.isError:true", "isError"],
    ["a JSON-RPC error member", "rpcError"],
    ["a non-2xx HTTP status", "http500"],
  ];

  test.each(failureModes)("%s cannot become a successful tag removal", async (_label, mode) => {
    const { backend, cleanup } = makeBackend();
    try {
      mcpMode = mode;
      const row = rowFor(`remove-fail-${mode}`, { nodeId: NODE_ID, action: "remove", tagId: "tag-abc" });
      await expect(backend.apply(row, "local", { nowMs: Date.now() })).rejects.toThrow();
      // The request still has to have gone out unaltered — a failure must not also corrupt the
      // bytes we sent, or a retry would replay the wrong request.
      expect(mcpCalls).toEqual([{ name: "tag", arguments: { nodeId: NODE_ID, action: "remove", tagIds: ["tag-abc"] } }]);
    } finally {
      cleanup();
    }
  });
});

describe("a tagId is transmitted verbatim, never trimmed", () => {
  // requireString tests `.trim()` for emptiness but returns the original value. A padded ID is
  // therefore accepted and must be sent EXACTLY as supplied: trimming here would be the same
  // class of silent normalization this whole matrix exists to forbid, and it would mask a
  // producer bug rather than surfacing it as a Tana-side rejection.
  test("leading and trailing whitespace survives into the MCP arguments", async () => {
    const { backend, cleanup } = makeBackend();
    try {
      const paddedId = "  tag-abc  ";
      const row = rowFor("padded-id", { nodeId: NODE_ID, action: "add", tagId: paddedId });
      await backend.apply(row, "local", { nowMs: Date.now() });
      expect(mcpCalls).toEqual([{ name: "tag", arguments: { nodeId: NODE_ID, action: "add", tagIds: [paddedId] } }]);
    } finally {
      cleanup();
    }
  });
});

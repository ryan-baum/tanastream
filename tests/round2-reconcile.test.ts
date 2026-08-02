// Round 2 (Opus ultracode) — reconcile findings, tested against a FAITHFUL Local-API mock that
// reproduces the real read-back representation (every line carries a trailing
// " <!-- node-id: X -->" comment; created node names are whitespace-collapsed via oneLine).
//
//   N2       : reconcile's name pre-filter compared the RAW payload.name against the stored
//              oneLine()-collapsed child name -> the marker node was skipped for multi-space
//              titles -> crash-replay double-create (BUG-2 / 25-orphan class at a new seam).
//   1000-cap : the children listing was a single unpaginated GET(?limit=1000); a marker node past
//              child #1000 was never seen -> double-create. Fixed with offset paging + a
//              no-progress guard (safe even if the API ignores offset).
//   verify-the-verifier: exercise the REAL reconcile/localCreate code (not FakeBackend's trivial
//              Map.has) against the real read-back shape.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { openSpool } from "../src/spool";
import { RealTanaBackend } from "../src/realBackend";
import type { WriteRow } from "../src/types";

interface MockNode { id: string; name: string; markerKey?: string; description?: string }

let server: ReturnType<typeof Bun.serve> | null = null;
let PORT = 0;
let childrenByTarget: Record<string, MockNode[]> = {};
let supportOffset = true;
let importHandler: ((target: string, body: unknown) => unknown) | null = null;

function markdownFor(node: MockNode): string {
  const lines = [`- ${node.name} <!-- node-id: ${node.id} -->`];
  if (node.description) lines.push(`  - Description: ${node.description} <!-- node-id: ${node.id}-d -->`);
  if (node.markerKey) lines.push(`  - TanaStreamIdempotency - create:${node.markerKey} <!-- node-id: ${node.id}-m -->`);
  return lines.join("\n");
}

beforeAll(() => {
  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      const path = url.pathname;
      if (path === "/health") return Response.json({ status: "ok" });

      const childrenMatch = path.match(/^\/nodes\/([^/]+)\/children$/);
      if (childrenMatch) {
        const target = decodeURIComponent(childrenMatch[1]);
        const all = childrenByTarget[target] ?? [];
        const limit = Number(url.searchParams.get("limit") ?? "1000");
        const offset = supportOffset ? Number(url.searchParams.get("offset") ?? "0") : 0;
        const slice = all.slice(offset, offset + limit);
        return Response.json({ children: slice.map((n) => ({ id: n.id, name: n.name })) });
      }

      const importMatch = path.match(/^\/nodes\/([^/]+)\/import$/);
      if (importMatch && req.method === "POST") {
        const target = decodeURIComponent(importMatch[1]);
        const body = await req.json().catch(() => ({}));
        if (importHandler) return Response.json(importHandler(target, body));
        return Response.json({ parentNodeId: target, targetNodeId: target, createdNodes: [], message: "ok" });
      }

      const nodeMatch = path.match(/^\/nodes\/([^/]+)$/);
      if (nodeMatch) {
        const id = decodeURIComponent(nodeMatch[1]);
        for (const list of Object.values(childrenByTarget)) {
          const node = list.find((n) => n.id === id);
          if (node) return Response.json({ markdown: markdownFor(node), name: node.name, description: null });
        }
        return new Response("not found", { status: 404 });
      }
      return new Response("unhandled", { status: 404 });
    },
  });
  PORT = server.port;
});

afterAll(() => { server?.stop(true); });

function makeBackend(): { backend: RealTanaBackend; cleanup: () => void } {
  const dir = mkdtempSync(join(tmpdir(), "tanastream-r2recon-"));
  const configPath = join(dir, "config.json");
  writeFileSync(configPath, JSON.stringify({
    apiEndpoint: "https://input.example/api", apiToken: "x", defaultTargetNode: "INBOX",
    localApi: { enabled: true, endpoint: `http://127.0.0.1:${PORT}`, bearerToken: "t" },
  }));
  return { backend: new RealTanaBackend({ configPath }), cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

function rowFor(name: string, key: string, target: string, description?: string): WriteRow {
  const dir = mkdtempSync(join(tmpdir(), "tanastream-r2recon-spool-"));
  const spool = openSpool({ dbPath: join(dir, "spool.db"), recoverCorrupt: true });
  try {
    spool.enqueue({ opType: "create", idempotencyKey: key, payload: description ? { name, description } : { name }, targetNodeId: target, source: "t" });
    return spool.getByDedupKey(`create:${key}`)!;
  } finally {
    spool.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

describe("N2 — reconcile matches a created node whose stored name was whitespace-collapsed", () => {
  test("N2a reconcile FINDS the marker node when the create name had collapsed multi-space", async () => {
    childrenByTarget = { TARGET1: [{ id: "C1", name: "Project X", markerKey: "k" }] };
    supportOffset = true;
    const { backend, cleanup } = makeBackend();
    try {
      const row = rowFor("Project   X", "k", "TARGET1"); // raw double-space; Tana stored "Project X"
      const result = await backend.reconcile(row);
      expect(result?.targetNodeId).toBe("C1");
    } finally { cleanup(); }
  });

  test("N2b reconcile returns null when no child carries the marker (no false-accept)", async () => {
    childrenByTarget = { TARGET1: [{ id: "C2", name: "Project X" }] };
    const { backend, cleanup } = makeBackend();
    try {
      expect(await backend.reconcile(rowFor("Project   X", "k", "TARGET1"))).toBeNull();
    } finally { cleanup(); }
  });

  test("N2c reconcile rejects a prefix-collision marker (key 1 must not match a node carrying key 10)", async () => {
    childrenByTarget = { TARGET1: [{ id: "C10", name: "Node", markerKey: "10" }] };
    const { backend, cleanup } = makeBackend();
    try {
      expect(await backend.reconcile(rowFor("Node", "1", "TARGET1"))).toBeNull();
    } finally { cleanup(); }
  });
});

describe("1000-cap — reconcile paginates past the first 1000 children", () => {
  test("finds the marker node at index 1200 when offset paging is honored", async () => {
    const kids: MockNode[] = [];
    for (let i = 0; i < 1500; i += 1) kids.push({ id: `F${i}`, name: `Filler ${i}` });
    kids[1200] = { id: "DEEP", name: "Deep Node", markerKey: "deep" };
    childrenByTarget = { TARGET1: kids };
    supportOffset = true;
    const { backend, cleanup } = makeBackend();
    try {
      expect((await backend.reconcile(rowFor("Deep Node", "deep", "TARGET1")))?.targetNodeId).toBe("DEEP");
    } finally { cleanup(); }
  });

  test("does not infinite-loop when the API ignores offset (no-progress guard)", async () => {
    const kids: MockNode[] = [];
    for (let i = 0; i < 1200; i += 1) kids.push({ id: `G${i}`, name: `Filler ${i}` });
    childrenByTarget = { TARGET1: kids };
    supportOffset = false; // every page returns the first 1000
    const { backend, cleanup } = makeBackend();
    try {
      expect(await backend.reconcile(rowFor("Missing", "miss", "TARGET1"))).toBeNull();
    } finally { cleanup(); }
  });
});

describe("localCreate verify-the-verifier against the real read-back representation", () => {
  test("clean create round-trips and the name/child backstop passes", async () => {
    importHandler = (target) => ({ parentNodeId: target, targetNodeId: target, createdNodes: [{ id: "NEW1", name: "Hello World" }], message: "ok" });
    childrenByTarget = { ANY: [{ id: "NEW1", name: "Hello World", markerKey: "c1" }] };
    const { backend, cleanup } = makeBackend();
    try {
      const result = await backend.apply(rowFor("Hello World", "c1", "TARGET1"), "local", { nowMs: 1 });
      expect(result.targetNodeId).toBe("NEW1");
    } finally { cleanup(); importHandler = null; }
  });

  test("a name-misparse read-back dead-letters LOUDLY (backstop fires, no silent pass)", async () => {
    importHandler = (target) => ({ parentNodeId: target, targetNodeId: target, createdNodes: [{ id: "BAD1", name: "Hello" }], message: "ok" });
    childrenByTarget = { ANY: [{ id: "BAD1", name: "Hello", markerKey: "c2" }] }; // read-back "Hello" != oneLine("Hello World")
    const { backend, cleanup } = makeBackend();
    try {
      await expect(backend.apply(rowFor("Hello World", "c2", "TARGET1"), "local", { nowMs: 1 })).rejects.toThrow(/name mismatch|misparse/);
    } finally { cleanup(); importHandler = null; }
  });

  // Cato cross-vendor finding: localCreate verified name + children but NOT the description child.
  test("a clean create WITH description round-trips (description backstop passes)", async () => {
    importHandler = (target) => ({ parentNodeId: target, targetNodeId: target, createdNodes: [{ id: "DESC1", name: "Has Desc" }], message: "ok" });
    childrenByTarget = { ANY: [{ id: "DESC1", name: "Has Desc", markerKey: "d1", description: "My Description" }] };
    const { backend, cleanup } = makeBackend();
    try {
      expect((await backend.apply(rowFor("Has Desc", "d1", "TARGET1", "My Description"), "local", { nowMs: 1 })).targetNodeId).toBe("DESC1");
    } finally { cleanup(); importHandler = null; }
  });

  test("a DROPPED description dead-letters LOUDLY (description backstop fires, no silent loss)", async () => {
    importHandler = (target) => ({ parentNodeId: target, targetNodeId: target, createdNodes: [{ id: "DESC2", name: "Has Desc" }], message: "ok" });
    childrenByTarget = { ANY: [{ id: "DESC2", name: "Has Desc", markerKey: "d2" }] }; // node has NO Description child
    const { backend, cleanup } = makeBackend();
    try {
      await expect(backend.apply(rowFor("Has Desc", "d2", "TARGET1", "My Description"), "local", { nowMs: 1 })).rejects.toThrow(/description did not land|misparse/);
    } finally { cleanup(); importHandler = null; }
  });
});

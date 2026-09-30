import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { HttpTransport } from "../src/http-client.js";
import { SamyamaClient } from "../src/client.js";

/**
 * A server that answers every request with `status` and `body`, and records
 * the method, path and body of what it received (#438).
 */
function canned(
  status: number,
  body: string,
): Promise<{ url: string; seen: { method?: string; path?: string; body: string }[]; close: () => void }> {
  const seen: { method?: string; path?: string; body: string }[] = [];
  const server: Server = createServer((req, res) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      seen.push({ method: req.method, path: req.url, body: data });
      res.writeHead(status, { "Content-Type": "application/json", Connection: "close" });
      res.end(body);
    });
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      resolve({ url: `http://127.0.0.1:${port}`, seen, close: () => server.close() });
    });
  });
}

describe("nlq", () => {
  test("posts the question to /api/nlq and returns the cypher", async () => {
    const s = await canned(200, JSON.stringify({ cypher: "MATCH (n) RETURN n LIMIT 10" }));
    // timeoutMs is passed because an omitted one reaches the transport as
    // `undefined` and overrides its default; that is a separate defect.
    const client = new SamyamaClient({ url: s.url, timeoutMs: 5000, maxRetries: 0 });
    assert.equal(await client.nlq("Show me some nodes"), "MATCH (n) RETURN n LIMIT 10");
    assert.equal(s.seen.length, 1);
    assert.equal(s.seen[0].method, "POST");
    assert.equal(s.seen[0].path, "/api/nlq");
    assert.deepEqual(JSON.parse(s.seen[0].body), { question: "Show me some nodes" });
    s.close();
  });

  test("a server refusal rejects with the server's message", async () => {
    const msg = "Validation error: Generated query contains write operations or unsafe keywords";
    const s = await canned(400, JSON.stringify({ error: msg }));
    const t = new HttpTransport(s.url, { maxRetries: 0 });
    await assert.rejects(() => t.nlq("delete everything"), { message: msg });
    s.close();
  });

  test("a success without a cypher string is an error, not undefined", async () => {
    const s = await canned(200, JSON.stringify({}));
    const t = new HttpTransport(s.url, { maxRetries: 0 });
    await assert.rejects(() => t.nlq("q"), /no `cypher` string/);
    s.close();
  });
});

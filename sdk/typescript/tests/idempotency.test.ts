import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { HttpTransport } from "../src/http-client.js";
import { SamyamaClient } from "../src/client.js";

/**
 * A server that answers every `/api/query` and records the `Idempotency-Key`
 * each one carried. The first reply is held for `holdMs`, which is the case
 * where a retried write used to run twice: it had landed and the client timed
 * out waiting to hear so (#2176).
 */
function recorder(holdMs = 0): Promise<{ url: string; keys: (string | undefined)[]; close: () => void }> {
  const keys: (string | undefined)[] = [];
  const server: Server = createServer((req, res) => {
    const key = req.headers["idempotency-key"];
    keys.push(Array.isArray(key) ? key[0] : key);
    const reply = () => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ columns: [], records: [] }));
    };
    req.resume();
    req.on("end", () => (keys.length === 1 && holdMs > 0 ? setTimeout(reply, holdMs) : reply()));
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        keys,
        close: () => server.closeAllConnections?.() ?? server.close(),
      });
    });
  });
}

describe("idempotency keys (#2176)", () => {
  test("a write keeps one key across its retries", async () => {
    const s = await recorder(1_000);
    const t = new HttpTransport(s.url, { timeoutMs: 200, maxRetries: 2, retryBaseDelayMs: 10 });
    await t.query("CREATE (:Order)").catch(() => undefined);
    assert.ok(s.keys.length >= 2, `the write was not retried: ${JSON.stringify(s.keys)}`);
    assert.ok(s.keys[0], "a write was sent without a key");
    assert.ok(
      s.keys.every((k) => k === s.keys[0]),
      `a retry changed the key: ${JSON.stringify(s.keys)}`,
    );
    s.close();
  });

  test("each call has its own key, and a read-only call has none", async () => {
    const s = await recorder();
    const t = new HttpTransport(s.url, { maxRetries: 0 });
    await t.query("CREATE (:A)");
    await t.query("CREATE (:A)");
    await t.query("MATCH (n) RETURN n", "default", { readOnly: true });
    assert.equal(s.keys.length, 3);
    assert.ok(s.keys[0] && s.keys[1]);
    assert.notEqual(s.keys[0], s.keys[1], "two calls shared a key");
    assert.equal(s.keys[2], undefined, "a read-only request carried a key");
    s.close();
  });

  test("a caller can choose the key, to send a call again safely", async () => {
    const s = await recorder();
    const client = new SamyamaClient({ url: s.url, maxRetries: 0 });
    await client.queryWithIdempotencyKey("CREATE (:Paid)", "pay-42");
    await client.queryWithIdempotencyKey("CREATE (:Paid)", "pay-42");
    assert.deepEqual(s.keys, ["pay-42", "pay-42"]);
    s.close();
  });
});

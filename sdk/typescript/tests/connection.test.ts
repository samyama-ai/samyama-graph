import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { HttpTransport } from "../src/http-client.js";

/**
 * A server that accepts the connection and never replies (API-06, #1326).
 *
 * `fetch` has no timeout, so every call in this SDK used to wait for as long as
 * the process lived. A server that *refuses* the connection would not show
 * that — the failure is fast either way. Silence is the case that matters.
 */
function blackHole(): Promise<{ url: string; connections: () => number; close: () => void }> {
  let connections = 0;
  const server: Server = createServer(() => {
    connections += 1;
    // Deliberately no response.
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        connections: () => connections,
        close: () => server.close(),
      });
    });
  });
}

describe("connection management", () => {
  test("a silent server does not hang the caller", async () => {
    const s = await blackHole();
    const t = new HttpTransport(s.url, { timeoutMs: 200, maxRetries: 0 });
    const started = Date.now();
    await assert.rejects(() => t.query("RETURN 1"));
    // Without the timeout this never settles and the test times out rather
    // than failing, so the elapsed bound is the assertion that means something.
    assert.ok(Date.now() - started < 5000, "the timeout did not fire");
    s.close();
  });

  test("the default sets a deadline", () => {
    // The defect was the default, not the absence of an option.
    const t = new HttpTransport("http://127.0.0.1:1");
    assert.ok(t.connectionOptions.timeoutMs > 0);
    assert.ok(t.connectionOptions.maxRetries > 0);
  });

  test("a timeout is retried the configured number of times", async () => {
    // Counted at the server: one connection per attempt. Timing alone would
    // pass for a client that never retried and simply waited longer.
    const s = await blackHole();
    const t = new HttpTransport(s.url, {
      timeoutMs: 120,
      maxRetries: 2,
      retryBaseDelayMs: 10,
    });
    await assert.rejects(() => t.query("RETURN 1"));
    assert.equal(s.connections(), 3, "expected one attempt plus two retries");
    s.close();
  });

  test("retry can be switched off", async () => {
    const s = await blackHole();
    const t = new HttpTransport(s.url, { timeoutMs: 120, maxRetries: 0 });
    await assert.rejects(() => t.query("RETURN 1"));
    assert.equal(s.connections(), 1);
    s.close();
  });

  test("a caller's abort is not retried", async () => {
    // Cancellation is the caller saying they no longer want the answer.
    // Retrying it does the opposite of what they asked.
    const s = await blackHole();
    const t = new HttpTransport(s.url, { timeoutMs: 5000, maxRetries: 3 });
    const controller = new AbortController();
    const promise = t.query("RETURN 1", "default", { signal: controller.signal });
    setTimeout(() => controller.abort(), 50);
    await assert.rejects(() => promise);
    assert.equal(s.connections(), 1, "the caller's abort was retried");
    s.close();
  });

  test("healthy() answers false rather than throwing when nothing is there", async () => {
    const t = new HttpTransport("http://127.0.0.1:1", { timeoutMs: 200, maxRetries: 0 });
    assert.equal(await t.healthy(), false);
  });
});

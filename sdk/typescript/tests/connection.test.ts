import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { HttpTransport } from "../src/http-client.js";
import { SamyamaClient } from "../src/client.js";

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

  test("an option passed as undefined keeps its default (#1595)", () => {
    const t = new HttpTransport("http://127.0.0.1:1", {
      timeoutMs: undefined,
      maxRetries: undefined,
      retryBaseDelayMs: undefined,
    });
    assert.deepEqual(t.connectionOptions, {
      timeoutMs: 30_000,
      maxRetries: 2,
      retryBaseDelayMs: 100,
    });
  });

  test("an explicit option still overrides its default", () => {
    const t = new HttpTransport("http://127.0.0.1:1", { timeoutMs: 5, maxRetries: 0 });
    assert.deepEqual(t.connectionOptions, { timeoutMs: 5, maxRetries: 0, retryBaseDelayMs: 100 });
  });

  test("a client built with only a url gets the transport's defaults (#1595)", () => {
    // `SamyamaClient` forwards each option it was not given as `undefined`.
    // That used to overwrite every default, so `AbortSignal.timeout(undefined)`
    // threw on every call of every client built without a `timeoutMs`.
    for (const client of [
      new SamyamaClient({ url: "http://127.0.0.1:1" }),
      SamyamaClient.connectHttp("http://127.0.0.1:1"),
    ]) {
      const http = (client as unknown as { http: HttpTransport }).http;
      assert.deepEqual(http.connectionOptions, {
        timeoutMs: 30_000,
        maxRetries: 2,
        retryBaseDelayMs: 100,
      });
    }
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

  test("the public client passes its deadline to the transport", async () => {
    // The transport tests above prove the deadline fires. This proves a caller
    // who only ever touches `SamyamaClient` gets it: the options have to
    // survive the constructor and the factory, not just exist on the type.
    const s = await blackHole();
    for (const client of [
      new SamyamaClient({ url: s.url, timeoutMs: 200, maxRetries: 0 }),
      SamyamaClient.connectHttp(s.url, { timeoutMs: 200, maxRetries: 0 }),
    ]) {
      const started = Date.now();
      await assert.rejects(() => client.query("RETURN 1"));
      assert.ok(Date.now() - started < 5000, "the timeout did not fire");
    }
    assert.equal(s.connections(), 2, "maxRetries: 0 was not passed through");
    s.close();
  });

  test("healthy() answers false rather than throwing when nothing is there", async () => {
    const t = new HttpTransport("http://127.0.0.1:1", { timeoutMs: 200, maxRetries: 0 });
    assert.equal(await t.healthy(), false);
  });
});

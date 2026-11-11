import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { SamyamaClient } from "../src/client.js";
import { HttpTransport } from "../src/http-client.js";

/**
 * A server that counts how many requests it is serving at once.
 *
 * Measured on the server's side of the socket, so the limit is checked where
 * it matters -- connections arriving -- and not by asking the SDK what it
 * believes it did.
 */
function counting(
  handle: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<{ url: string; peak: () => number; seen: () => number; close: () => void }> {
  let open = 0;
  let peak = 0;
  let seen = 0;
  const server = createServer((req, res) => {
    open++;
    seen++;
    peak = Math.max(peak, open);
    res.on("close", () => open--);
    req.resume();
    req.on("end", () => handle(req, res));
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        peak: () => peak,
        seen: () => seen,
        close: () => {
          server.closeAllConnections();
          server.close();
        },
      });
    });
  });
}

/** `p`, or a rejection with `why` if it has not settled within `ms`. */
function withinMs<T>(ms: number, p: Promise<T>, why: string): Promise<T> {
  let timer: NodeJS.Timeout;
  const late = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(why)), ms);
  });
  return Promise.race([p, late]).finally(() => clearTimeout(timer));
}

const healthyAfter = (ms: number) => (_req: IncomingMessage, res: ServerResponse) =>
  setTimeout(() => {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "healthy", version: "x", storage: { nodes: 0, edges: 0 } }));
  }, ms);

describe("maxConnections (API-06, #1741)", () => {
  test("no more requests are open at once than the limit, and every call completes", async () => {
    const srv = await counting(healthyAfter(40));
    try {
      const client = new SamyamaClient({ url: srv.url, maxConnections: 3 });
      const results = await Promise.all(Array.from({ length: 20 }, () => client.status()));
      assert.equal(results.length, 20);
      assert.equal(srv.seen(), 20);
      assert.equal(srv.peak(), 3, "the server saw more than 3 at once");
    } finally {
      srv.close();
    }
  });

  test("the default limit is 10", async () => {
    const srv = await counting(healthyAfter(40));
    try {
      const client = new SamyamaClient({ url: srv.url });
      await Promise.all(Array.from({ length: 25 }, () => client.status()));
      assert.equal(srv.peak(), 10);
    } finally {
      srv.close();
    }
  });

  test("a slot is given back after an HTTP error, so errors cannot exhaust the pool", async () => {
    let n = 0;
    const srv = await counting((_req, res) => {
      n++;
      if (n <= 3) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "boom" }));
      } else {
        healthyAfter(0)(_req, res);
      }
    });
    try {
      const client = new SamyamaClient({ url: srv.url, maxConnections: 1, maxRetries: 0 });
      // A slot leaked by an error leaves the next call waiting forever; fail
      // in 5 s rather than hang the run.
      const settled = await withinMs(
        5_000,
        Promise.allSettled(Array.from({ length: 4 }, () => client.status())),
        "a call is still waiting for a slot: an HTTP error did not give its slot back",
      );
      assert.deepEqual(
        settled.map((s) => s.status),
        ["rejected", "rejected", "rejected", "fulfilled"],
      );
    } finally {
      srv.close();
    }
  });

  test("a call waiting for a slot can be cancelled, and never reaches the server", async () => {
    let releaseFirst!: () => void;
    const srv = await counting((req, res) => {
      releaseFirst = () => healthyAfter(0)(req, res);
    });
    try {
      const transport = new HttpTransport(srv.url, { maxConnections: 1, maxRetries: 0 });
      const first = transport.status();
      await new Promise((r) => setTimeout(r, 50));
      const controller = new AbortController();
      const second = transport.status({ signal: controller.signal });
      await new Promise((r) => setTimeout(r, 20));
      assert.deepEqual(transport.connections, { inUse: 1, queued: 1 });
      controller.abort(new Error("caller gave up"));
      await assert.rejects(second, /caller gave up/);
      assert.deepEqual(transport.connections, { inUse: 1, queued: 0 }, "the cancelled call left the queue");
      releaseFirst();
      await first;
      assert.equal(srv.seen(), 1, "the cancelled call opened a connection");
      assert.deepEqual(transport.connections, { inUse: 0, queued: 0 });
    } finally {
      srv.close();
    }
  });

  test("a stream holds its slot until it ends, and stopping early gives it back", async () => {
    const srv = await counting((req, res) => {
      if (req.url === "/api/query") {
        res.writeHead(200, { "Content-Type": "application/x-ndjson" });
        res.write(JSON.stringify({ columns: ["i"] }) + "\n");
        let i = 0;
        const tick = setInterval(() => {
          if (res.destroyed) return clearInterval(tick);
          res.write(JSON.stringify({ row: [i++] }) + "\n");
        }, 5);
        res.on("close", () => clearInterval(tick));
      } else {
        healthyAfter(0)(req, res);
      }
    });
    try {
      const transport = new HttpTransport(srv.url, { maxConnections: 1 });
      let waiting: Promise<unknown> | undefined;
      let rows = 0;
      for await (const _row of transport.queryStream("UNWIND range(1, 1000000) AS i RETURN i")) {
        rows++;
        if (rows === 1) {
          waiting = transport.status();
          await new Promise((r) => setTimeout(r, 30));
          assert.deepEqual(transport.connections, { inUse: 1, queued: 1 }, "the stream's slot was free");
        }
        if (rows === 5) break;
      }
      await waiting;
      assert.deepEqual(transport.connections, { inUse: 0, queued: 0 });
    } finally {
      srv.close();
    }
  });

  test("a limit that is not a whole number of at least 1 is refused", () => {
    for (const bad of [0, -1, 1.5, Number.NaN]) {
      assert.throws(() => new HttpTransport("http://127.0.0.1:1", { maxConnections: bad }), RangeError);
    }
    assert.doesNotThrow(() => new HttpTransport("http://127.0.0.1:1", { maxConnections: Infinity }));
  });
});

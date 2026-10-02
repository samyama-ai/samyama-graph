import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { SamyamaClient } from "../src/client.js";

/** A server driven by `handle`, which also records each request's body. */
function serve(
  handle: (req: IncomingMessage, res: ServerResponse, body: string) => void,
): Promise<{ url: string; bodies: string[]; headers: IncomingMessage["headers"][]; close: () => void }> {
  const bodies: string[] = [];
  const headers: IncomingMessage["headers"][] = [];
  const server = createServer((req, res) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      bodies.push(data);
      headers.push(req.headers);
      handle(req, res, data);
    });
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        bodies,
        headers,
        close: () => {
          server.closeAllConnections();
          server.close();
        },
      });
    });
  });
}

const ndjson = (res: ServerResponse) =>
  res.writeHead(200, { "Content-Type": "application/x-ndjson" });

describe("queryReadonly (#1628)", () => {
  test("asks the server to refuse a write", async () => {
    const s = await serve((_req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ columns: [], records: [] }));
    });
    const client = new SamyamaClient({ url: s.url, maxRetries: 0 });
    await client.queryReadonly("MATCH (n) RETURN n");
    await client.query("CREATE (:X)");
    assert.equal(JSON.parse(s.bodies[0]).read_only, true);
    assert.equal(JSON.parse(s.bodies[1]).read_only, undefined, "a plain query is unchanged");
    s.close();
  });

  test("rejects with the server's refusal", async () => {
    const msg = "this request is read-only (`read_only: true`) and the statement writes";
    const s = await serve((_req, res) => {
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: msg }));
    });
    const client = new SamyamaClient({ url: s.url, maxRetries: 0 });
    await assert.rejects(() => client.queryReadonly("MATCH (n) DETACH DELETE n"), { message: msg });
    s.close();
  });
});

describe("queryStream (#1632)", () => {
  test("yields every row keyed by column, then ends at the trailer", async () => {
    const s = await serve((_req, res) => {
      ndjson(res);
      res.write(JSON.stringify({ columns: ["i", "twice"] }) + "\n");
      for (let i = 1; i <= 1000; i++) res.write(JSON.stringify({ row: [i, 2 * i] }) + "\n");
      res.end(JSON.stringify({ done: true, rows: 1000 }) + "\n");
    });
    const client = new SamyamaClient({ url: s.url, maxRetries: 0 });
    let n = 0;
    for await (const row of client.queryStream("UNWIND range(1, 1000) AS i RETURN i, i * 2 AS twice")) {
      n++;
      assert.deepEqual(row, { i: n, twice: 2 * n });
    }
    assert.equal(n, 1000);
    assert.equal(s.headers[0].accept, "application/x-ndjson");
    assert.equal(JSON.parse(s.bodies[0]).read_only, true);
    s.close();
  });

  test("an error trailer throws after the rows before it", async () => {
    const s = await serve((_req, res) => {
      ndjson(res);
      res.end('{"columns":["i"]}\n{"row":[1]}\n{"error":"boom","rows":1}\n');
    });
    const client = new SamyamaClient({ url: s.url, maxRetries: 0 });
    const seen: unknown[] = [];
    await assert.rejects(async () => {
      for await (const row of client.queryStream("RETURN 1")) seen.push(row);
    }, /boom/);
    assert.deepEqual(seen, [{ i: 1 }]);
    s.close();
  });

  test("a body with no trailer is incomplete, not a result", async () => {
    const s = await serve((_req, res) => {
      ndjson(res);
      res.end('{"columns":["i"]}\n{"row":[1]}\n');
    });
    const client = new SamyamaClient({ url: s.url, maxRetries: 0 });
    await assert.rejects(async () => {
      for await (const _ of client.queryStream("RETURN 1")) void _;
    }, /without a trailer/);
    s.close();
  });

  test("stopping early closes the connection and the server stops writing", async () => {
    // The server writes rows only as fast as the socket drains -- the
    // backpressure the stream promises -- and would write forever.
    let written = 0;
    let closed!: () => void;
    const serverSawClose = new Promise<void>((r) => (closed = r));
    const s = await serve((_req, res) => {
      ndjson(res);
      res.write('{"columns":["i"]}\n');
      res.on("close", () => closed());
      const pump = () => {
        while (!res.destroyed) {
          written++;
          const line = JSON.stringify({ row: [written, "x".repeat(200)] }) + "\n";
          if (!res.write(line)) return void res.once("drain", pump);
        }
      };
      pump();
    });
    const client = new SamyamaClient({ url: s.url, maxRetries: 0 });
    let read = 0;
    for await (const row of client.queryStream("UNWIND range(1, 1000000000) AS i RETURN i")) {
      read++;
      assert.equal(row.i, read);
      if (read === 5) break;
    }
    await serverSawClose;
    const atClose = written;
    await new Promise((r) => setTimeout(r, 50));
    assert.equal(written, atClose, "the server kept writing after the client left");
    assert.ok(written < 1_000_000, `the server ran ahead unbounded: ${written} rows`);
    s.close();
  });
});

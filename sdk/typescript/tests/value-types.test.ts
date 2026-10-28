/**
 * Every value type arrives through the TypeScript SDK as the engine sent it
 * (API-04, #1728).
 *
 * The cases are `sdk/fixtures/value_types.json`, shared with the Python and
 * Rust suites. Two halves:
 *
 * - **Decoding**, always run: a local server answers with each fixture's exact
 *   JSON, and the client must decode it to `JSON.parse` of that text. This is
 *   the SDK's own half and needs no engine.
 * - **Against a real engine**, run when `SAMYAMA_URL` names one: each value as
 *   a column, inside a list, inside a map, as a node property, and streamed.
 *
 * A case the fixture marks `lossy.typescript` cannot be held exactly by a
 * JavaScript number. For those the test checks that the loss is still there,
 * so a fix (a BigInt reviver, say) fails here until the fixture is updated.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { SamyamaClient } from "../src/client.js";

interface Case {
  name: string;
  cypher: string;
  json: string;
  storable: boolean;
  lossy?: Record<string, string>;
}

/** From `dist/tests/`, where the compiled test runs, to `sdk/fixtures/`. */
const FIXTURE = new URL("../../../fixtures/value_types.json", import.meta.url);
const SDK = "typescript";
const ENGINE_URL = process.env.SAMYAMA_URL;

const cases: Case[] = JSON.parse(readFileSync(FIXTURE, "utf8")).cases;

/** Each way a value is read back, as [shape, query, the JSON text it must give]. */
function readShapes(c: Case): [string, string, string][] {
  return [
    ["column", `RETURN ${c.cypher} AS v`, c.json],
    ["list", `WITH ${c.cypher} AS x RETURN [x] AS v`, `[${c.json}]`],
    ["map", `WITH ${c.cypher} AS x RETURN {k: x} AS v`, `{"k":${c.json}}`],
  ];
}

/**
 * `got` decoded from `text` as the fixture says it must: equal to
 * `JSON.parse(text)`, or, for a case lossy in this SDK, visibly not the text.
 */
function expectDecoded(c: Case, got: unknown, text: string, where: string): void {
  if (c.lossy?.[SDK]) {
    assert.notEqual(
      JSON.stringify(got),
      text,
      `${where}: ${c.name} is marked lossy (${c.lossy[SDK]}) but arrived exactly; update the fixture`,
    );
  } else {
    assert.deepEqual(got, JSON.parse(text), `${where}: ${c.name}`);
  }
}

/** A server that answers every query with one row holding `cell`, verbatim. */
async function answering(cell: string): Promise<{ url: string; close: () => void }> {
  const server = createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(`{"columns":["v"],"records":[[${cell}]],"nodes":[],"edges":[]}`);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return {
    url: `http://127.0.0.1:${port}`,
    close: () => {
      server.closeAllConnections();
      server.close();
    },
  };
}

describe("value types: decoding the engine's JSON", () => {
  test("the fixture has unique, parseable cases", () => {
    assert.ok(cases.length > 0);
    assert.equal(new Set(cases.map((c) => c.name)).size, cases.length, "names are unique");
    for (const c of cases) assert.doesNotThrow(() => JSON.parse(c.json), c.name);
  });

  for (const c of cases) {
    test(c.name, async () => {
      const server = await answering(c.json);
      try {
        const result = await SamyamaClient.connectHttp(server.url).query(`RETURN ${c.cypher} AS v`);
        expectDecoded(c, result.records[0][0], c.json, "decode");
      } finally {
        server.close();
      }
    });
  }
});

describe("value types: against a real engine", { skip: ENGINE_URL ? false : "set SAMYAMA_URL to run" }, () => {
  const client = () => SamyamaClient.connectHttp(ENGINE_URL);

  cases.forEach((c, i) => {
    test(c.name, async () => {
      for (const [shape, query, text] of readShapes(c)) {
        const result = await client().query(query);
        expectDecoded(c, result.records[0][0], text, shape);

        const rows = [];
        for await (const row of client().queryStream(query)) rows.push(row);
        assert.equal(rows.length, 1, `stream ${shape}: one row`);
        expectDecoded(c, rows[0].v, text, `stream ${shape}`);
      }
      if (c.storable) {
        // A label per case and per run, so a rerun against the same server
        // never reads an earlier run's node.
        const label = `RoundTripTs${i}_${Date.now()}`;
        const result = await client().query(`CREATE (n:${label} {v: ${c.cypher}}) RETURN n.v AS v`);
        expectDecoded(c, result.records[0][0], c.json, "property");
      }
    });
  });
});

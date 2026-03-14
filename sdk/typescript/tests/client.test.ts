import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { SamyamaClient, HttpTransport } from "../src/index.js";
import type {
  QueryResult,
  ServerStatus,
  GraphSchema,
  NodeType,
  EdgeType,
  IndexInfo,
  ConstraintInfo,
  CsvImportResult,
  JsonImportResult,
} from "../src/index.js";

describe("SamyamaClient", () => {
  it("should create a client with default URL", () => {
    const client = new SamyamaClient();
    assert.ok(client);
  });

  it("should create a client with custom URL", () => {
    const client = SamyamaClient.connectHttp("http://localhost:9090");
    assert.ok(client);
  });

  it("should expose listGraphs", async () => {
    const client = new SamyamaClient();
    const graphs = await client.listGraphs();
    assert.deepEqual(graphs, ["default"]);
  });

  it("should have explain method", () => {
    const client = new SamyamaClient();
    assert.equal(typeof client.explain, "function");
  });

  it("should have profile method", () => {
    const client = new SamyamaClient();
    assert.equal(typeof client.profile, "function");
  });

  it("should have schema method", () => {
    const client = new SamyamaClient();
    assert.equal(typeof client.schema, "function");
  });

  it("should have importCsv method", () => {
    const client = new SamyamaClient();
    assert.equal(typeof client.importCsv, "function");
  });

  it("should have importJson method", () => {
    const client = new SamyamaClient();
    assert.equal(typeof client.importJson, "function");
  });
});

describe("HttpTransport", () => {
  it("should construct with a URL", () => {
    const transport = new HttpTransport("http://localhost:8080");
    assert.ok(transport);
  });

  it("should strip trailing slashes from URL", () => {
    const transport = new HttpTransport("http://localhost:8080///");
    assert.ok(transport);
  });

  it("should have schema method", () => {
    const transport = new HttpTransport("http://localhost:8080");
    assert.equal(typeof transport.schema, "function");
  });

  it("should have importCsv method", () => {
    const transport = new HttpTransport("http://localhost:8080");
    assert.equal(typeof transport.importCsv, "function");
  });

  it("should have importJson method", () => {
    const transport = new HttpTransport("http://localhost:8080");
    assert.equal(typeof transport.importJson, "function");
  });
});

describe("Types", () => {
  it("should allow constructing GraphSchema objects", () => {
    const schema: GraphSchema = {
      node_types: [{ label: "Person", count: 10, properties: { name: "String" } }],
      edge_types: [{ type: "KNOWS", count: 5, source_labels: ["Person"], target_labels: ["Person"], properties: {} }],
      indexes: [{ label: "Person", property: "name", type: "BTREE" }],
      constraints: [{ label: "Person", property: "email", type: "UNIQUE" }],
      statistics: { total_nodes: 10, total_edges: 5, avg_out_degree: 0.5 },
    };
    assert.equal(schema.node_types.length, 1);
    assert.equal(schema.edge_types[0].type, "KNOWS");
    assert.equal(schema.indexes[0].type, "BTREE");
    assert.equal(schema.constraints[0].type, "UNIQUE");
  });

  it("should allow constructing CsvImportResult objects", () => {
    const result: CsvImportResult = {
      status: "ok",
      nodes_created: 5,
      label: "Person",
      columns: ["name", "age"],
    };
    assert.equal(result.nodes_created, 5);
  });

  it("should allow constructing JsonImportResult objects", () => {
    const result: JsonImportResult = {
      status: "ok",
      nodes_created: 3,
      label: "City",
    };
    assert.equal(result.nodes_created, 3);
  });
});

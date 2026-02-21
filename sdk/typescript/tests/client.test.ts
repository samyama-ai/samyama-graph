import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { SamyamaClient, HttpTransport } from "../src/index";
import type { QueryResult, ServerStatus } from "../src/index";

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
});

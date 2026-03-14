import type {
  QueryResult,
  ServerStatus,
  ClientOptions,
  GraphSchema,
  CsvImportResult,
  JsonImportResult,
} from "./types.js";
import { HttpTransport } from "./http-client.js";

const DEFAULT_URL = "http://localhost:8080";

/**
 * Client for the Samyama Graph Database.
 *
 * @example
 * ```ts
 * const client = new SamyamaClient({ url: "http://localhost:8080" });
 *
 * // Create data
 * await client.query('CREATE (n:Person {name: "Alice"})');
 *
 * // Query data
 * const result = await client.queryReadonly("MATCH (n:Person) RETURN n.name");
 * console.log(result.records);
 *
 * // Schema introspection
 * const schema = await client.schema();
 * console.log(schema.node_types);
 *
 * // EXPLAIN / PROFILE
 * const plan = await client.explain("MATCH (n:Person) RETURN n");
 * const profile = await client.profile("MATCH (n:Person) RETURN n");
 * ```
 */
export class SamyamaClient {
  private http: HttpTransport;

  constructor(options?: ClientOptions) {
    const url = options?.url ?? DEFAULT_URL;
    this.http = new HttpTransport(url);
  }

  /**
   * Connect to a Samyama server via HTTP.
   * Factory method for a more readable API.
   */
  static connectHttp(url: string = DEFAULT_URL): SamyamaClient {
    return new SamyamaClient({ url });
  }

  /** Execute a read-write Cypher query */
  async query(cypher: string, graph: string = "default"): Promise<QueryResult> {
    return this.http.query(cypher, graph);
  }

  /** Execute a read-only Cypher query */
  async queryReadonly(cypher: string, graph: string = "default"): Promise<QueryResult> {
    return this.http.query(cypher, graph);
  }

  /**
   * Return the EXPLAIN plan for a Cypher query without executing it.
   * Returns the plan as text rows in the QueryResult records.
   */
  async explain(cypher: string, graph: string = "default"): Promise<QueryResult> {
    const prefixed = cypher.trimStart().toUpperCase().startsWith("EXPLAIN")
      ? cypher
      : `EXPLAIN ${cypher}`;
    return this.http.query(prefixed, graph);
  }

  /**
   * Execute a Cypher query with PROFILE instrumentation.
   * Returns plan text with actual row counts and timing per operator.
   */
  async profile(cypher: string, graph: string = "default"): Promise<QueryResult> {
    const prefixed = cypher.trimStart().toUpperCase().startsWith("PROFILE")
      ? cypher
      : `PROFILE ${cypher}`;
    return this.http.query(prefixed, graph);
  }

  /** Delete a graph (executes MATCH (n) DELETE n) */
  async deleteGraph(graph: string = "default"): Promise<void> {
    await this.http.query("MATCH (n) DELETE n", graph);
  }

  /** List graphs (OSS: always returns ["default"]) */
  async listGraphs(): Promise<string[]> {
    return ["default"];
  }

  /** Get server status */
  async status(): Promise<ServerStatus> {
    return this.http.status();
  }

  /** Get graph schema (node types, edge types, indexes, constraints, statistics) */
  async schema(): Promise<GraphSchema> {
    return this.http.schema();
  }

  /**
   * Import nodes from CSV content.
   * @param csvContent - Raw CSV string (first row = headers)
   * @param label - Node label to assign
   * @param options - Optional: idColumn, delimiter
   */
  async importCsv(
    csvContent: string,
    label: string,
    options?: { idColumn?: string; delimiter?: string },
  ): Promise<CsvImportResult> {
    return this.http.importCsv(csvContent, label, options);
  }

  /**
   * Import nodes from JSON objects.
   * @param label - Node label to assign
   * @param nodes - Array of objects, each becoming a node
   */
  async importJson(
    label: string,
    nodes: Record<string, unknown>[],
  ): Promise<JsonImportResult> {
    return this.http.importJson(label, nodes);
  }

  /** Ping the server */
  async ping(): Promise<string> {
    const s = await this.status();
    if (s.status === "healthy") {
      return "PONG";
    }
    throw new Error(`Server unhealthy: ${s.status}`);
  }
}

import type { QueryResult, ServerStatus, ClientOptions } from "./types";
import { HttpTransport } from "./http-client";

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
  async query(cypher: string, _graph: string = "default"): Promise<QueryResult> {
    return this.http.query(cypher);
  }

  /** Execute a read-only Cypher query */
  async queryReadonly(cypher: string, _graph: string = "default"): Promise<QueryResult> {
    return this.http.query(cypher);
  }

  /** Delete a graph (executes MATCH (n) DELETE n) */
  async deleteGraph(_graph: string = "default"): Promise<void> {
    await this.http.query("MATCH (n) DELETE n");
  }

  /** List graphs (OSS: always returns ["default"]) */
  async listGraphs(): Promise<string[]> {
    return ["default"];
  }

  /** Get server status */
  async status(): Promise<ServerStatus> {
    return this.http.status();
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

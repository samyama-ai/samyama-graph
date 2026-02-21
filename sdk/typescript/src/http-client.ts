import type { QueryResult, ServerStatus, ErrorResponse } from "./types";

/**
 * HTTP transport for the Samyama SDK.
 * Uses the native `fetch` API (works in Node.js 18+ and browsers).
 */
export class HttpTransport {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
  }

  /** Execute a Cypher query via POST /api/query */
  async query(cypher: string): Promise<QueryResult> {
    const response = await fetch(`${this.baseUrl}/api/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: cypher }),
    });

    if (!response.ok) {
      const body = (await response.json().catch(() => ({
        error: `HTTP ${response.status}`,
      }))) as ErrorResponse;
      throw new Error(body.error || `HTTP ${response.status}`);
    }

    return (await response.json()) as QueryResult;
  }

  /** Get server status via GET /api/status */
  async status(): Promise<ServerStatus> {
    const response = await fetch(`${this.baseUrl}/api/status`);

    if (!response.ok) {
      throw new Error(`Status endpoint returned ${response.status}`);
    }

    return (await response.json()) as ServerStatus;
  }
}

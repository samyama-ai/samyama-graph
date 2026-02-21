/** A graph node returned from a query */
export interface SdkNode {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
}

/** A graph edge returned from a query */
export interface SdkEdge {
  id: string;
  source: string;
  target: string;
  type: string;
  properties: Record<string, unknown>;
}

/** Result of executing a Cypher query */
export interface QueryResult {
  nodes: SdkNode[];
  edges: SdkEdge[];
  columns: string[];
  records: unknown[][];
}

/** Server status information */
export interface ServerStatus {
  status: string;
  version: string;
  storage: {
    nodes: number;
    edges: number;
  };
}

/** Error response from the server */
export interface ErrorResponse {
  error: string;
}

/** Options for creating a client */
export interface ClientOptions {
  /** Base URL for HTTP transport (default: http://localhost:8080) */
  url?: string;
}

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

/** Node type descriptor from schema introspection */
export interface NodeType {
  label: string;
  count: number;
  properties: Record<string, string>;
}

/** Edge type descriptor from schema introspection */
export interface EdgeType {
  type: string;
  count: number;
  source_labels: string[];
  target_labels: string[];
  properties: Record<string, string>;
}

/** Index descriptor from schema introspection */
export interface IndexInfo {
  label: string;
  property: string;
  type: string;
}

/** Constraint descriptor from schema introspection */
export interface ConstraintInfo {
  label: string;
  property: string;
  type: string;
}

/** Graph schema returned by GET /api/schema */
export interface GraphSchema {
  node_types: NodeType[];
  edge_types: EdgeType[];
  indexes: IndexInfo[];
  constraints: ConstraintInfo[];
  statistics: {
    total_nodes: number;
    total_edges: number;
    avg_out_degree: number;
  };
}

/** Result of CSV import */
export interface CsvImportResult {
  status: string;
  nodes_created: number;
  label: string;
  columns: string[];
}

/** Result of JSON import */
export interface JsonImportResult {
  status: string;
  nodes_created: number;
  label: string;
}

/** Options for creating a client */
export interface ClientOptions {
  /** Base URL for HTTP transport (default: http://localhost:8080) */
  url?: string;
}

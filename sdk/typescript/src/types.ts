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

/** Request body of POST /api/nlq */
export interface NlqRequest {
  question: string;
}

/** Successful response of POST /api/nlq: the generated, read-only Cypher */
export interface NlqResponse {
  cypher: string;
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

/** Request for subgraph sampling (POST /api/sample) */
export interface SampleRequest {
  /** Maximum nodes to return (default: 200, max: 1000) */
  max_nodes?: number;
  /** Only include these node labels (empty = all) */
  labels?: string[];
  /** Tenant/graph name */
  graph?: string;
}

/** A sampled node for visualization */
export interface SampleNode {
  id: number;
  label: string;
  name: string;
  properties: Record<string, unknown>;
}

/** A sampled edge for visualization */
export interface SampleEdge {
  id: number;
  source: number;
  target: number;
  type: string;
  properties: Record<string, unknown>;
}

/** Result of subgraph sampling */
export interface SampleResult {
  nodes: SampleNode[];
  edges: SampleEdge[];
  total_nodes: number;
  total_edges: number;
  sampled_nodes: number;
  sampled_edges: number;
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

/** A declared parameter of a catalog entry (#2026) */
export interface CatalogParam {
  name: string;
  type: "int" | "float" | "string" | "bool";
  sample: unknown;
  enum_values: unknown[] | null;
}

/** A catalog entry as GET /api/queries lists it */
export interface CatalogEntrySummary {
  id: string;
  question: string;
  paraphrases: string[];
  difficulty: string;
  unanswerable: boolean;
  params: CatalogParam[];
}

/** GET /api/queries/:id: the list fields plus the query and its ceiling */
export interface CatalogEntryDetail extends CatalogEntrySummary {
  cypher: string;
  work: number | null;
  work_ceiling: number | null;
}

/** GET /api/queries */
export interface CatalogListResult {
  total: number;
  sha256: string;
  results: CatalogEntrySummary[];
}

/** POST /api/queries/import */
export interface CatalogImportResult {
  loaded: number;
  replaced: boolean;
  sha256: string;
  by_difficulty: { easy: number; medium: number; hard: number };
  with_params: number;
}

/** POST /api/queries/:id/run */
export interface CatalogRunResult {
  columns: string[];
  rows: unknown[][];
  row_count: number;
  /** More than 10,000 rows matched and only the first 10,000 are here. */
  truncated: boolean;
  work: number | null;
  work_ceiling: number | null;
  source: "computed";
}

/** The `queries` field of a snapshot import response */
export interface CatalogLinkState {
  /** The snapshot's header names a catalog. */
  linked: boolean;
  expected_file: string | null;
  sha256: string | null;
  /** It was sent and loaded; `linked && !loaded` means ask for the file. */
  loaded: boolean;
}

/** A catalog refusal: the error, a stable code and the fields it names */
export interface CatalogError extends ErrorResponse {
  code:
    | "param_missing"
    | "param_unknown"
    | "param_type"
    | "param_not_allowed"
    | "work_ceiling"
    | "invalid_entry"
    | "query_error"
    | "query_not_found"
    | "no_catalog"
    | "catalog_mismatch"
    | "invalid_catalog"
    | "bad_upload";
  param?: string;
  declared?: string[];
  expected?: unknown;
  got?: unknown;
  allowed?: unknown[];
  limit?: number;
  params?: Record<string, unknown>;
}

/** Options for creating a client */
export interface ClientOptions {
  /** Base URL for HTTP transport (default: http://localhost:8080) */
  url?: string;
  /** Request deadline in milliseconds (default: 30000). */
  timeoutMs?: number;
  /** Retries for a timeout or connection failure (default: 2). */
  maxRetries?: number;
  /** Delay before the first retry, doubled each attempt (default: 100). */
  retryBaseDelayMs?: number;
  /**
   * Most requests this client has open at once (default: 10). Further calls
   * wait for a slot; a stream holds its slot until it ends.
   */
  maxConnections?: number;
}

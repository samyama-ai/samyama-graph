import type {
  QueryResult,
  ServerStatus,
  ErrorResponse,
  GraphSchema,
  CsvImportResult,
  JsonImportResult,
} from "./types.js";

/**
 * How the transport treats the network (API-06).
 *
 * Every field has a default, and the defaults are the point. Every call used to
 * be a bare `fetch` with no `signal`, and `fetch` has **no timeout**: a server
 * that accepts the connection and then stops talking leaves the promise pending
 * for as long as the process lives (samyama-graph#1326).
 */
export interface ConnectionOptions {
  /** Deadline for a whole request, in milliseconds. */
  timeoutMs?: number;
  /** How many times a retryable failure is retried. 0 disables retry. */
  maxRetries?: number;
  /**
   * Delay before the first retry, doubled each attempt.
   *
   * Constant-delay retry is deliberately not what this does: it is what turns
   * one slow server into a thundering herd of clients waking together.
   */
  retryBaseDelayMs?: number;
}

const DEFAULTS: Required<ConnectionOptions> = {
  timeoutMs: 30_000,
  maxRetries: 2,
  retryBaseDelayMs: 100,
};

/** Per-call overrides. `signal` lets a caller cancel. */
export interface RequestOptions {
  /** Caller's cancellation signal, combined with the timeout. */
  signal?: AbortSignal;
  /** Override the transport's timeout for this call. */
  timeoutMs?: number;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * HTTP transport for the Samyama SDK.
 * Uses the native `fetch` API (works in Node.js 18+ and browsers).
 */
export class HttpTransport {
  private baseUrl: string;
  private options: Required<ConnectionOptions>;

  constructor(baseUrl: string, options: ConnectionOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.options = { ...DEFAULTS, ...options };
  }

  /** The settings this transport was built with. */
  get connectionOptions(): Required<ConnectionOptions> {
    return { ...this.options };
  }

  /**
   * Is this failure worth retrying?
   *
   * A timeout or a connection failure, and nothing else. An HTTP error is the
   * server answering; repeating a request it has already refused achieves
   * nothing, and blindly repeating a write is how one duplicate becomes
   * several. A caller's own `AbortSignal` firing is never retried — the caller
   * asked to stop.
   */
  private static retryable(err: unknown, callerSignal?: AbortSignal): boolean {
    if (callerSignal?.aborted) return false;
    if (err instanceof DOMException && err.name === "TimeoutError") return true;
    if (err instanceof DOMException && err.name === "AbortError") return true;
    return err instanceof TypeError; // fetch's network-failure shape
  }

  /**
   * Every request goes through here.
   *
   * One place, not fifteen: the missing timeout was missing at six separate
   * `fetch` call sites, so fixing one of them fixed one of them.
   */
  private async request(
    path: string,
    init: RequestInit,
    opts: RequestOptions = {},
  ): Promise<Response> {
    const timeoutMs = opts.timeoutMs ?? this.options.timeoutMs;
    let delay = this.options.retryBaseDelayMs;

    for (let attempt = 0; ; attempt++) {
      // A fresh timeout per attempt, and the caller's signal combined with it,
      // so cancellation wins over a retry that is still waiting.
      const signals: AbortSignal[] = [AbortSignal.timeout(timeoutMs)];
      if (opts.signal) signals.push(opts.signal);
      const signal =
        signals.length === 1 ? signals[0] : AbortSignal.any(signals);

      try {
        return await fetch(`${this.baseUrl}${path}`, { ...init, signal });
      } catch (err) {
        if (
          attempt >= this.options.maxRetries ||
          !HttpTransport.retryable(err, opts.signal)
        ) {
          throw err;
        }
        await sleep(delay);
        delay *= 2;
      }
    }
  }

  /** Throw the server's error message, or the status if it sent none. */
  private static async fail(response: Response): Promise<never> {
    const body = (await response.json().catch(() => ({
      error: `HTTP ${response.status}`,
    }))) as ErrorResponse;
    throw new Error(body.error || `HTTP ${response.status}`);
  }

  private async json<T>(
    path: string,
    init: RequestInit,
    opts?: RequestOptions,
  ): Promise<T> {
    const response = await this.request(path, init, opts);
    if (!response.ok) return HttpTransport.fail(response);
    return (await response.json()) as T;
  }

  /** Execute a Cypher query via POST /api/query */
  async query(
    cypher: string,
    graph: string = "default",
    opts?: RequestOptions,
  ): Promise<QueryResult> {
    return this.json<QueryResult>(
      "/api/query",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: cypher, graph }),
      },
      opts,
    );
  }

  /** Get server status via GET /api/status */
  async status(opts?: RequestOptions): Promise<ServerStatus> {
    return this.json<ServerStatus>("/api/status", {}, opts);
  }

  /**
   * Health check: does the server answer, and does it call itself healthy?
   *
   * Returns false rather than throwing, because "is it up" is a question whose
   * negative answer is not exceptional.
   */
  async healthy(opts?: RequestOptions): Promise<boolean> {
    try {
      const s = await this.status(opts);
      return s.status === "healthy";
    } catch {
      return false;
    }
  }

  /** Get graph schema via GET /api/schema */
  async schema(opts?: RequestOptions): Promise<GraphSchema> {
    return this.json<GraphSchema>("/api/schema", {}, opts);
  }

  /** Generic POST request returning typed JSON response */
  async post<T>(
    path: string,
    body: unknown,
    opts?: RequestOptions,
  ): Promise<T> {
    return this.json<T>(
      path,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
      opts,
    );
  }

  /** Import nodes from CSV via POST /api/import/csv (multipart) */
  async importCsv(
    csvContent: string,
    label: string,
    options?: { idColumn?: string; delimiter?: string },
    opts?: RequestOptions,
  ): Promise<CsvImportResult> {
    const formData = new FormData();
    const blob = new Blob([csvContent], { type: "text/csv" });
    formData.append("file", blob, "import.csv");
    formData.append("label", label);
    if (options?.idColumn) formData.append("id_column", options.idColumn);
    if (options?.delimiter) formData.append("delimiter", options.delimiter);

    return this.json<CsvImportResult>(
      "/api/import/csv",
      { method: "POST", body: formData },
      opts,
    );
  }

  /** Import nodes from JSON via POST /api/import/json */
  async importJson(
    label: string,
    nodes: Record<string, unknown>[],
    opts?: RequestOptions,
  ): Promise<JsonImportResult> {
    return this.json<JsonImportResult>(
      "/api/import/json",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ label, nodes }),
      },
      opts,
    );
  }
}

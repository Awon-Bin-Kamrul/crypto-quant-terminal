/**
 * REST client of the terminal gateway. Every call but `health` carries the operator's bearer
 * token; failures become an {@link ApiError} with the HTTP status and the server's message.
 */

import type {
  CandlesPage,
  CommandKind,
  CommandReceipt,
  Health,
  Markets,
  SessionSnapshot,
  SessionSummary,
  Ticket,
} from "@/lib/api/types";

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }

  /** The token is missing or wrong: the operator has to sign in again. */
  get unauthorized(): boolean {
    return this.status === 401;
  }

  /** The gateway or a dependency is unreachable: worth retrying later. */
  get transient(): boolean {
    return this.status === 0 || this.status === 429 || this.status >= 500;
  }
}

export type Fetch = typeof fetch;

export interface ApiClientOptions {
  baseUrl: string;
  token: string | null;
  fetch?: Fetch;
  timeoutMs?: number;
}

function describe(body: unknown, fallback: string): string {
  if (body && typeof body === "object" && "detail" in body) {
    const detail = (body as { detail: unknown }).detail;
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) {
      return detail
        .map((item) =>
          item && typeof item === "object" && "msg" in item ? String(item.msg) : String(item),
        )
        .join("; ");
    }
  }
  return fallback;
}

export class ApiClient {
  private readonly baseUrl: string;
  private readonly token: string | null;
  private readonly fetchImpl: Fetch;
  private readonly timeoutMs: number;

  constructor(options: ApiClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token;
    this.fetchImpl = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.timeoutMs = options.timeoutMs ?? 15_000;
  }

  health(): Promise<Health> {
    return this.request<Health>("GET", "/api/v1/health", { auth: false });
  }

  checkToken(): Promise<{ ok: boolean }> {
    return this.request("GET", "/api/v1/auth/check");
  }

  markets(): Promise<Markets> {
    return this.request("GET", "/api/v1/markets");
  }

  candles(query: {
    venue: string;
    symbol: string;
    timeframe: string;
    end?: number;
    limit?: number;
  }): Promise<CandlesPage> {
    const params = new URLSearchParams({
      venue: query.venue,
      symbol: query.symbol,
      tf: query.timeframe,
    });
    if (query.end !== undefined) params.set("end", String(Math.floor(query.end)));
    if (query.limit !== undefined) params.set("limit", String(query.limit));
    return this.request("GET", `/api/v1/candles?${params.toString()}`);
  }

  sessions(limit = 100): Promise<SessionSummary[]> {
    return this.request("GET", `/api/v1/sessions?limit=${limit}`);
  }

  session(sessionId: string): Promise<SessionSnapshot> {
    return this.request("GET", `/api/v1/sessions/${encodeURIComponent(sessionId)}`);
  }

  command(sessionId: string, kind: CommandKind, reason: string): Promise<CommandReceipt> {
    return this.request("POST", `/api/v1/sessions/${encodeURIComponent(sessionId)}/commands`, {
      body: { kind, reason },
    });
  }

  streamTicket(): Promise<Ticket> {
    return this.request("POST", "/api/v1/stream/tickets");
  }

  private async request<T>(
    method: "GET" | "POST",
    path: string,
    options: { auth?: boolean; body?: unknown } = {},
  ): Promise<T> {
    const headers: Record<string, string> = { Accept: "application/json" };
    if (options.auth !== false) {
      if (!this.token) throw new ApiError(401, "not signed in");
      headers.Authorization = `Bearer ${this.token}`;
    }
    if (options.body !== undefined) headers["Content-Type"] = "application/json";
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    let response: Response;
    try {
      response = await this.fetchImpl(`${this.baseUrl}${path}`, {
        method,
        headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        signal: controller.signal,
        cache: "no-store",
      });
    } catch (error) {
      const reason = controller.signal.aborted
        ? `no answer within ${this.timeoutMs / 1000}s`
        : error instanceof Error
          ? error.message
          : String(error);
      throw new ApiError(0, `cannot reach the gateway at ${this.baseUrl}: ${reason}`);
    } finally {
      clearTimeout(timer);
    }
    let body: unknown = null;
    const text = await response.text();
    if (text) {
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
    }
    if (!response.ok) {
      throw new ApiError(
        response.status,
        describe(body, `${method} ${path} failed with HTTP ${response.status}`),
      );
    }
    return body as T;
  }
}

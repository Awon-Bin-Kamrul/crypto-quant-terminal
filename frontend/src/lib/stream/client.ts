/**
 * One live connection to the terminal gateway, shared by every panel.
 *
 * - **Transports.** WebSocket by default: one socket, topics added and removed at will.
 *   Server-Sent Events as the fallback (some proxies break WebSockets): topics are part of the
 *   URL, so changing them reconnects. With the `auto` preference, two WebSocket attempts in a
 *   row that never complete the handshake switch this client to SSE.
 * - **Authentication.** Every connection uses a fresh single-use ticket from the REST API; a
 *   rejected token stops the client in the `unauthorized` state until it is restarted.
 * - **Resilience.** Reconnects back off exponentially with jitter. A connection that has been
 *   silent for 2.5 heartbeats is presumed dead and replaced. After every (re)connect each topic
 *   is subscribed again, so every panel receives a fresh snapshot.
 * - **Integrity.** Updates carry `seq`; a gap asks the server for a new snapshot instead of
 *   applying updates on top of a state that missed some.
 */

import { toWebSocketUrl, type TransportPreference } from "@/lib/config";
import { parseFrame, type ClientFrame, type ServerFrame } from "@/lib/stream/protocol";

export type Transport = "ws" | "sse";

export type ConnectionStatus =
  | "idle"
  | "connecting"
  | "open"
  | "reconnecting"
  | "unauthorized"
  | "stopped";

export interface ConnectionInfo {
  status: ConnectionStatus;
  transport: Transport | null;
  /** Round trip of the last ping, WebSocket only. */
  latencyMs: number | null;
  lastFrameAt: number | null;
  /** Consecutive failed attempts (0 while connected). */
  failures: number;
  error: string | null;
}

export interface TopicHandler {
  snapshot(data: unknown): void;
  update(kind: string, data: unknown): void;
  error?(code: string, message: string): void;
}

/** The subset of the browser `WebSocket` the client uses. */
export interface SocketLike {
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: ((event: { code: number; reason: string }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

/** The subset of the browser `EventSource` the client uses. */
export interface EventSourceLike {
  onmessage: ((event: { data: string }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  close(): void;
}

export interface Scheduler {
  setTimeout(callback: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
  now(): number;
}

export interface BackoffOptions {
  initialMs: number;
  maxMs: number;
  factor: number;
  /** Fraction of the delay randomised in both directions. */
  jitter: number;
}

export interface StreamClientOptions {
  /** `http(s)://host:port` of the gateway. */
  baseUrl: string;
  /** A fresh stream ticket; rejects with an error whose `status` is 401 for a bad token. */
  ticket: () => Promise<string>;
  transport?: TransportPreference;
  createSocket?: (url: string) => SocketLike;
  createEventSource?: (url: string) => EventSourceLike;
  scheduler?: Scheduler;
  random?: () => number;
  backoff?: BackoffOptions;
  pingIntervalMs?: number;
}

const DEFAULT_BACKOFF: BackoffOptions = { initialMs: 500, maxMs: 30_000, factor: 2, jitter: 0.2 };
const WS_FAILURES_BEFORE_SSE = 2;
const MIN_WATCHDOG_MS = 10_000;
const TOPIC_CHANGE_DEBOUNCE_MS = 100;
const CLOSE_TOO_MANY = 4429;
const TOO_MANY_MIN_DELAY_MS = 5_000;

const realScheduler: Scheduler = {
  setTimeout: (callback, ms) => globalThis.setTimeout(callback, ms),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
  now: () => Date.now(),
};

interface Live {
  generation: number;
  transport: Transport;
  socket: SocketLike | null;
  source: EventSourceLike | null;
  /** The server said hello: the connection is usable. */
  greeted: boolean;
  /** Topics the SSE URL carries (fixed for the connection). */
  topics: ReadonlySet<string>;
}

function isUnauthorized(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "status" in error &&
    (error as { status: unknown }).status === 401
  );
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class StreamClient {
  private readonly options: StreamClientOptions;
  private readonly scheduler: Scheduler;
  private readonly random: () => number;
  private readonly backoff: BackoffOptions;
  private readonly handlers = new Map<string, Set<TopicHandler>>();
  private readonly seqs = new Map<string, number>();
  private readonly listeners = new Set<(info: ConnectionInfo) => void>();
  private readonly pings = new Map<number, number>();
  private state: ConnectionInfo = {
    status: "idle",
    transport: null,
    latencyMs: null,
    lastFrameAt: null,
    failures: 0,
    error: null,
  };
  private live: Live | null = null;
  private generation = 0;
  private wsFailures = 0;
  private forceSse = false;
  private heartbeatMs = MIN_WATCHDOG_MS;
  private nextPingId = 1;
  private reconnectTimer: unknown = null;
  private watchdogTimer: unknown = null;
  private pingTimer: unknown = null;
  private topicTimer: unknown = null;

  constructor(options: StreamClientOptions) {
    this.options = options;
    this.scheduler = options.scheduler ?? realScheduler;
    this.random = options.random ?? Math.random;
    this.backoff = options.backoff ?? DEFAULT_BACKOFF;
    this.forceSse = options.transport === "sse";
  }

  // ---------------------------------------------------------------------------- public

  get info(): ConnectionInfo {
    return this.state;
  }

  get activeTopics(): string[] {
    return [...this.handlers.keys()];
  }

  onChange(listener: (info: ConnectionInfo) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  start(): void {
    if (this.state.status === "idle" || this.state.status === "stopped" || this.state.status === "unauthorized") {
      this.setState({ status: "connecting", failures: 0, error: null });
      void this.connect();
    }
  }

  /** Drop the current connection (if any) and connect again now. */
  reconnectNow(): void {
    this.clearTimer("reconnectTimer");
    this.teardown();
    this.setState({ status: "connecting", error: null });
    void this.connect();
  }

  stop(): void {
    this.clearTimer("reconnectTimer");
    this.clearTimer("topicTimer");
    this.teardown();
    this.setState({ status: "stopped", transport: null, latencyMs: null });
  }

  subscribe(topic: string, handler: TopicHandler): () => void {
    let set = this.handlers.get(topic);
    const first = set === undefined;
    if (!set) {
      set = new Set();
      this.handlers.set(topic, set);
    }
    set.add(handler);
    this.requestSnapshot(topic, first);
    return () => this.release(topic, handler);
  }

  // ----------------------------------------------------------------------- connection

  private async connect(): Promise<void> {
    const generation = ++this.generation;
    let ticket: string;
    try {
      ticket = await this.options.ticket();
    } catch (error) {
      if (generation !== this.generation) return;
      if (isUnauthorized(error)) {
        this.setState({ status: "unauthorized", error: message(error), transport: null });
        return;
      }
      this.scheduleReconnect(message(error));
      return;
    }
    if (generation !== this.generation || this.state.status === "stopped") return;
    const transport: Transport = this.forceSse ? "sse" : "ws";
    const live: Live = {
      generation,
      transport,
      socket: null,
      source: null,
      greeted: false,
      topics: new Set(this.handlers.keys()),
    };
    this.live = live;
    this.setState({ transport });
    try {
      if (transport === "ws") this.openSocket(live, ticket);
      else this.openEventSource(live, ticket);
    } catch (error) {
      this.disconnected(live, `cannot open the ${transport} connection: ${message(error)}`);
      return;
    }
    this.armWatchdog(live);
  }

  private openSocket(live: Live, ticket: string): void {
    const url = `${toWebSocketUrl(this.options.baseUrl)}/api/v1/stream/ws?ticket=${encodeURIComponent(ticket)}`;
    const create =
      this.options.createSocket ?? ((target: string) => new WebSocket(target) as unknown as SocketLike);
    const socket = create(url);
    live.socket = socket;
    socket.onmessage = (event) => {
      if (typeof event.data === "string") this.receive(live, event.data);
    };
    socket.onclose = (event) => {
      const why = event.reason ? `${event.code} ${event.reason}` : `code ${event.code}`;
      this.disconnected(live, `connection closed (${why})`, event.code);
    };
    socket.onerror = () => {
      // A close event always follows; it carries the reason.
    };
  }

  private openEventSource(live: Live, ticket: string): void {
    const params = new URLSearchParams({ ticket });
    for (const topic of live.topics) params.append("topic", topic);
    const url = `${this.options.baseUrl}/api/v1/stream/sse?${params.toString()}`;
    const create =
      this.options.createEventSource ??
      ((target: string) => new EventSource(target) as unknown as EventSourceLike);
    const source = create(url);
    live.source = source;
    source.onmessage = (event) => this.receive(live, event.data);
    source.onerror = () => {
      // EventSource would retry by itself with the same (now spent) ticket: take over instead.
      this.disconnected(live, "event stream interrupted");
    };
  }

  private receive(live: Live, text: string): void {
    if (this.live !== live) return;
    const frame = parseFrame(text);
    if (frame === null) return;
    this.setState({ lastFrameAt: this.scheduler.now() });
    this.armWatchdog(live);
    this.handle(live, frame);
  }

  private handle(live: Live, frame: ServerFrame): void {
    switch (frame.type) {
      case "hello": {
        live.greeted = true;
        this.wsFailures = 0;
        this.heartbeatMs = Math.max(1, frame.heartbeat_s) * 1000;
        this.setState({ status: "open", failures: 0, error: null });
        this.seqs.clear();
        if (live.transport === "ws") {
          for (const topic of this.handlers.keys()) this.send(live, { op: "subscribe", topic });
          this.schedulePing(live);
        } else if (!sameTopics(live.topics, this.handlers)) {
          this.scheduleTopicChange();
        }
        this.armWatchdog(live);
        return;
      }
      case "snapshot": {
        this.seqs.set(frame.topic, frame.seq);
        for (const handler of this.handlers.get(frame.topic) ?? []) handler.snapshot(frame.data);
        return;
      }
      case "update": {
        const last = this.seqs.get(frame.topic);
        if (last === undefined) return; // no snapshot yet: it is on its way
        if (frame.seq !== last + 1) {
          this.seqs.delete(frame.topic);
          this.resync(live, frame.topic);
          return;
        }
        this.seqs.set(frame.topic, frame.seq);
        for (const handler of this.handlers.get(frame.topic) ?? []) {
          handler.update(frame.kind, frame.data);
        }
        return;
      }
      case "error": {
        if (frame.topic === null) {
          this.setState({ error: frame.message });
          return;
        }
        if (frame.code === "unavailable" || frame.code === "invalid_topic") {
          this.seqs.delete(frame.topic);
        }
        for (const handler of this.handlers.get(frame.topic) ?? []) {
          handler.error?.(frame.code, frame.message);
        }
        return;
      }
      case "pong": {
        const sentAt = frame.id === null ? undefined : this.pings.get(frame.id);
        if (sentAt !== undefined && frame.id !== null) {
          this.pings.delete(frame.id);
          this.setState({ latencyMs: Math.max(0, this.scheduler.now() - sentAt) });
        }
        return;
      }
      case "subscribed":
      case "unsubscribed":
      case "heartbeat":
        return;
    }
  }

  private disconnected(live: Live, reason: string, code?: number): void {
    if (this.live !== live) return;
    if (live.transport === "ws" && !live.greeted) {
      this.wsFailures += 1;
      if (this.options.transport !== "ws" && this.wsFailures >= WS_FAILURES_BEFORE_SSE) {
        this.forceSse = true;
      }
    }
    this.teardown();
    if (this.state.status === "stopped") return;
    this.scheduleReconnect(reason, code === CLOSE_TOO_MANY ? TOO_MANY_MIN_DELAY_MS : 0);
  }

  private scheduleReconnect(reason: string, minimumMs = 0): void {
    const failures = this.state.failures + 1;
    const base = Math.min(
      this.backoff.maxMs,
      this.backoff.initialMs * this.backoff.factor ** (failures - 1),
    );
    const spread = base * this.backoff.jitter * (this.random() * 2 - 1);
    const delay = Math.max(minimumMs, Math.round(base + spread));
    this.setState({ status: "reconnecting", failures, error: reason, latencyMs: null });
    this.clearTimer("reconnectTimer");
    this.reconnectTimer = this.scheduler.setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect();
    }, delay);
  }

  private teardown(): void {
    const live = this.live;
    this.live = null;
    this.clearTimer("watchdogTimer");
    this.clearTimer("pingTimer");
    this.pings.clear();
    this.seqs.clear();
    if (live === null) return;
    if (live.socket) {
      live.socket.onmessage = null;
      live.socket.onclose = null;
      live.socket.onerror = null;
      live.socket.onopen = null;
      try {
        live.socket.close(1000, "client reconnect");
      } catch {
        // already closed
      }
    }
    if (live.source) {
      live.source.onmessage = null;
      live.source.onerror = null;
      live.source.close();
    }
  }

  // --------------------------------------------------------------------------- topics

  private requestSnapshot(topic: string, first: boolean): void {
    const live = this.live;
    if (live === null || !live.greeted) return; // subscribed on hello
    if (live.transport === "ws") {
      this.seqs.delete(topic);
      this.send(live, { op: "subscribe", topic });
    } else if (first || live.topics.has(topic)) {
      this.scheduleTopicChange();
    }
  }

  private release(topic: string, handler: TopicHandler): void {
    const set = this.handlers.get(topic);
    if (!set || !set.delete(handler) || set.size > 0) return;
    this.handlers.delete(topic);
    this.seqs.delete(topic);
    const live = this.live;
    if (live === null || !live.greeted) return;
    if (live.transport === "ws") this.send(live, { op: "unsubscribe", topic });
    else this.scheduleTopicChange();
  }

  private resync(live: Live, topic: string): void {
    if (live.transport === "ws") this.send(live, { op: "subscribe", topic });
    else this.scheduleTopicChange();
  }

  /** SSE topics are fixed per connection: reconnect (once for a burst of changes). */
  private scheduleTopicChange(): void {
    if (this.topicTimer !== null) return;
    this.topicTimer = this.scheduler.setTimeout(() => {
      this.topicTimer = null;
      const live = this.live;
      if (live === null || live.transport !== "sse") return;
      this.teardown();
      this.setState({ status: "connecting" });
      void this.connect();
    }, TOPIC_CHANGE_DEBOUNCE_MS);
  }

  // --------------------------------------------------------------------------- timers

  private armWatchdog(live: Live): void {
    this.clearTimer("watchdogTimer");
    const timeout = Math.max(MIN_WATCHDOG_MS, this.heartbeatMs * 2.5);
    this.watchdogTimer = this.scheduler.setTimeout(() => {
      this.watchdogTimer = null;
      this.disconnected(live, `no message for ${Math.round(timeout / 1000)}s`);
    }, timeout);
  }

  private schedulePing(live: Live): void {
    const interval = this.options.pingIntervalMs ?? 15_000;
    if (interval <= 0) return;
    const ping = (): void => {
      if (this.live !== live) return;
      const id = this.nextPingId++;
      this.pings.set(id, this.scheduler.now());
      this.send(live, { op: "ping", id });
      this.pingTimer = this.scheduler.setTimeout(ping, interval);
    };
    this.clearTimer("pingTimer");
    this.pingTimer = this.scheduler.setTimeout(ping, Math.min(interval, 1_000));
  }

  private clearTimer(name: "reconnectTimer" | "watchdogTimer" | "pingTimer" | "topicTimer"): void {
    const handle = this[name];
    if (handle !== null) {
      this.scheduler.clearTimeout(handle);
      this[name] = null;
    }
  }

  // ---------------------------------------------------------------------------- misc

  private send(live: Live, frame: ClientFrame): void {
    if (live.socket === null) return;
    try {
      live.socket.send(JSON.stringify(frame));
    } catch {
      // The close event follows and reconnects.
    }
  }

  private setState(changes: Partial<ConnectionInfo>): void {
    const next = { ...this.state, ...changes };
    const changed = (Object.keys(changes) as (keyof ConnectionInfo)[]).some(
      (key) => next[key] !== this.state[key],
    );
    if (!changed) return;
    this.state = next;
    for (const listener of this.listeners) listener(next);
  }
}

function sameTopics(a: ReadonlySet<string>, b: ReadonlyMap<string, unknown>): boolean {
  if (a.size !== b.size) return false;
  for (const topic of a) if (!b.has(topic)) return false;
  return true;
}

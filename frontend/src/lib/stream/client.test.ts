import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  StreamClient,
  type EventSourceLike,
  type Scheduler,
  type SocketLike,
  type TopicHandler,
} from "@/lib/stream/client";

// ------------------------------------------------------------------------------- fakes

class ManualScheduler implements Scheduler {
  time = 1_000_000;
  private nextId = 1;
  private timers: { id: number; at: number; callback: () => void }[] = [];

  setTimeout(callback: () => void, ms: number): number {
    const id = this.nextId++;
    this.timers.push({ id, at: this.time + ms, callback });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.timers = this.timers.filter((timer) => timer.id !== handle);
  }

  now(): number {
    return this.time;
  }

  pending(): number[] {
    return this.timers.map((timer) => timer.at - this.time).sort((a, b) => a - b);
  }

  advance(ms: number): void {
    const target = this.time + ms;
    for (;;) {
      const due = this.timers
        .filter((timer) => timer.at <= target)
        .sort((a, b) => a.at - b.at || a.id - b.id)[0];
      if (!due) break;
      this.timers = this.timers.filter((timer) => timer !== due);
      this.time = due.at;
      due.callback();
    }
    this.time = target;
  }
}

class FakeSocket implements SocketLike {
  onopen: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  readonly sent: Record<string, unknown>[] = [];
  closed: { code?: number; reason?: string } | null = null;

  constructor(readonly url: string) {}

  send(data: string): void {
    this.sent.push(JSON.parse(data) as Record<string, unknown>);
  }

  close(code?: number, reason?: string): void {
    this.closed = { code, reason };
  }

  serve(frame: Record<string, unknown>): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }

  drop(code = 1006, reason = ""): void {
    this.onclose?.({ code, reason });
  }

  ops(): string[] {
    return this.sent.map((frame) => `${String(frame.op)}:${String(frame.topic ?? frame.id)}`);
  }
}

class FakeEventSource implements EventSourceLike {
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  closed = false;

  constructor(readonly url: string) {}

  close(): void {
    this.closed = true;
  }

  serve(frame: Record<string, unknown>): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }

  topics(): string[] {
    return new URL(this.url).searchParams.getAll("topic");
  }
}

class RecordingHandler implements TopicHandler {
  readonly events: string[] = [];

  snapshot(data: unknown): void {
    this.events.push(`snapshot:${JSON.stringify(data)}`);
  }

  update(kind: string, data: unknown): void {
    this.events.push(`${kind}:${JSON.stringify(data)}`);
  }

  error(code: string, message: string): void {
    this.events.push(`error:${code}:${message}`);
  }
}

const flush = async (): Promise<void> => {
  for (let i = 0; i < 5; i += 1) await new Promise((resolve) => setImmediate(resolve));
};

function rig(options: { transport?: "auto" | "ws" | "sse"; ticket?: () => Promise<string> } = {}) {
  const scheduler = new ManualScheduler();
  const sockets: FakeSocket[] = [];
  const sources: FakeEventSource[] = [];
  let tickets = 0;
  const client = new StreamClient({
    baseUrl: "http://gateway:8000",
    ticket: options.ticket ?? (async () => `t${++tickets}`),
    transport: options.transport ?? "auto",
    scheduler,
    random: () => 0.5,
    backoff: { initialMs: 500, maxMs: 8_000, factor: 2, jitter: 0.2 },
    pingIntervalMs: 15_000,
    createSocket: (url) => {
      const socket = new FakeSocket(url);
      sockets.push(socket);
      return socket;
    },
    createEventSource: (url) => {
      const source = new FakeEventSource(url);
      sources.push(source);
      return source;
    },
  });
  return { client, scheduler, sockets, sources };
}

const hello = { type: "hello", v: 1, connection: "c1", heartbeat_s: 15 };

// ------------------------------------------------------------------------------- tests

describe("StreamClient over WebSocket", () => {
  it("subscribes on hello, applies snapshots and numbered updates, resyncs on a gap", async () => {
    const { client, sockets } = rig();
    const handler = new RecordingHandler();
    client.subscribe("book:binance:BTC/USDT", handler);
    client.start();
    await flush();
    const socket = sockets[0]!;
    assert.equal(socket.url, "ws://gateway:8000/api/v1/stream/ws?ticket=t1");
    assert.equal(client.info.status, "connecting");
    socket.serve(hello);
    assert.equal(client.info.status, "open");
    assert.equal(client.info.transport, "ws");
    assert.deepEqual(socket.ops(), ["subscribe:book:binance:BTC/USDT"]);

    socket.serve({ type: "update", topic: "book:binance:BTC/USDT", seq: 1, kind: "book", data: 0 });
    assert.deepEqual(handler.events, [], "updates before the snapshot are ignored");
    socket.serve({ type: "snapshot", topic: "book:binance:BTC/USDT", seq: 0, data: { n: 1 } });
    socket.serve({ type: "update", topic: "book:binance:BTC/USDT", seq: 1, kind: "book", data: 2 });
    socket.serve({ type: "update", topic: "book:binance:BTC/USDT", seq: 3, kind: "book", data: 4 });
    assert.deepEqual(handler.events, ['snapshot:{"n":1}', "book:2"]);
    assert.deepEqual(socket.ops().slice(1), ["subscribe:book:binance:BTC/USDT"]);
    socket.serve({ type: "snapshot", topic: "book:binance:BTC/USDT", seq: 0, data: { n: 5 } });
    socket.serve({ type: "update", topic: "book:binance:BTC/USDT", seq: 1, kind: "book", data: 6 });
    assert.deepEqual(handler.events.slice(2), ['snapshot:{"n":5}', "book:6"]);
  });

  it("adds, shares and releases topics on the open socket", async () => {
    const { client, sockets } = rig();
    client.start();
    await flush();
    const socket = sockets[0]!;
    socket.serve(hello);
    const first = new RecordingHandler();
    const second = new RecordingHandler();
    const releaseFirst = client.subscribe("session:s1", first);
    const releaseSecond = client.subscribe("session:s1", second);
    assert.deepEqual(socket.ops(), ["subscribe:session:s1", "subscribe:session:s1"]);
    socket.serve({ type: "snapshot", topic: "session:s1", seq: 0, data: 1 });
    assert.equal(first.events.length, 1);
    assert.equal(second.events.length, 1);
    releaseFirst();
    assert.deepEqual(socket.ops().length, 2, "a topic stays while one handler remains");
    releaseSecond();
    assert.deepEqual(socket.ops().slice(2), ["unsubscribe:session:s1"]);
    assert.deepEqual(client.activeTopics, []);
  });

  it("reconnects with backoff and a fresh ticket, then resubscribes", async () => {
    const { client, sockets, scheduler } = rig();
    client.subscribe("health:binance", new RecordingHandler());
    client.start();
    await flush();
    sockets[0]!.serve(hello);
    sockets[0]!.drop(1006);
    assert.equal(client.info.status, "reconnecting");
    assert.equal(client.info.failures, 1);
    assert.deepEqual(scheduler.pending(), [500]);
    scheduler.advance(500);
    await flush();
    const second = sockets[1]!;
    assert.equal(second.url.endsWith("ticket=t2"), true);
    second.drop(1006); // fails before hello
    assert.deepEqual(scheduler.pending(), [1_000]);
    scheduler.advance(1_000);
    await flush();
    sockets[2]!.serve(hello);
    assert.equal(client.info.status, "open");
    assert.equal(client.info.failures, 0);
    assert.deepEqual(sockets[2]!.ops(), ["subscribe:health:binance"]);
  });

  it("stops at an unauthorized token and resumes on start()", async () => {
    let valid = false;
    const { client, sockets, scheduler } = rig({
      ticket: async () => {
        if (!valid) throw Object.assign(new Error("missing or invalid bearer token"), { status: 401 });
        return "t-ok";
      },
    });
    client.start();
    await flush();
    assert.equal(client.info.status, "unauthorized");
    assert.equal(client.info.error, "missing or invalid bearer token");
    assert.deepEqual(scheduler.pending(), []);
    assert.equal(sockets.length, 0);
    valid = true;
    client.start();
    await flush();
    assert.equal(sockets.length, 1);
  });

  it("measures latency with pings and replaces a silent connection", async () => {
    const { client, sockets, scheduler } = rig();
    client.start();
    await flush();
    const socket = sockets[0]!;
    socket.serve({ ...hello, heartbeat_s: 4 });
    scheduler.advance(1_000);
    assert.deepEqual(socket.ops(), ["ping:1"]);
    scheduler.advance(40);
    socket.serve({ type: "pong", id: 1, ts: 0 });
    assert.equal(client.info.latencyMs, 40);
    scheduler.advance(9_999);
    assert.equal(client.info.status, "open");
    scheduler.advance(1);
    assert.equal(client.info.status, "reconnecting");
    assert.match(client.info.error ?? "", /no message for 10s/);
    assert.deepEqual(socket.closed, { code: 1000, reason: "client reconnect" });
  });

  it("waits longer when the gateway has no connection slot left", async () => {
    const { client, sockets, scheduler } = rig();
    client.start();
    await flush();
    sockets[0]!.drop(4429, "too many stream connections");
    assert.deepEqual(scheduler.pending(), [5_000]);
    assert.match(client.info.error ?? "", /4429 too many stream connections/);
  });

  it("routes topic errors to their handlers and stops cleanly", async () => {
    const { client, sockets, scheduler } = rig();
    const handler = new RecordingHandler();
    client.subscribe("session:ghost", handler);
    client.start();
    await flush();
    sockets[0]!.serve(hello);
    sockets[0]!.serve({ type: "error", topic: "session:ghost", code: "unavailable", message: "gone" });
    assert.deepEqual(handler.events, ["error:unavailable:gone"]);
    client.stop();
    assert.equal(client.info.status, "stopped");
    assert.deepEqual(scheduler.pending(), []);
    sockets[0]!.drop(1006);
    assert.equal(client.info.status, "stopped", "a stopped client does not reconnect");
  });
});

describe("StreamClient over Server-Sent Events", () => {
  it("falls back to SSE after two WebSocket handshakes fail", async () => {
    const { client, sockets, sources, scheduler } = rig();
    client.subscribe("candles:binance:BTC/USDT:1m", new RecordingHandler());
    client.start();
    await flush();
    sockets[0]!.drop(1006);
    scheduler.advance(500);
    await flush();
    sockets[1]!.drop(1006);
    scheduler.advance(1_000);
    await flush();
    assert.equal(sockets.length, 2);
    const source = sources[0]!;
    assert.deepEqual(source.topics(), ["candles:binance:BTC/USDT:1m"]);
    assert.equal(new URL(source.url).searchParams.get("ticket"), "t3");
    source.serve(hello);
    assert.equal(client.info.transport, "sse");
    assert.equal(client.info.status, "open");
  });

  it("reconnects with the new topic list when topics change", async () => {
    const { client, sources, scheduler } = rig({ transport: "sse" });
    const handler = new RecordingHandler();
    client.subscribe("health:binance", handler);
    client.start();
    await flush();
    sources[0]!.serve(hello);
    sources[0]!.serve({ type: "snapshot", topic: "health:binance", seq: 0, data: 1 });
    client.subscribe("session:s1", new RecordingHandler());
    client.subscribe("book:binance:BTC/USDT", new RecordingHandler());
    scheduler.advance(100);
    await flush();
    assert.equal(sources[0]!.closed, true);
    assert.deepEqual(sources[1]!.topics().sort(), [
      "book:binance:BTC/USDT",
      "health:binance",
      "session:s1",
    ]);
    // a gap on SSE also reconnects (a new snapshot for every topic)
    sources[1]!.serve(hello);
    sources[1]!.serve({ type: "snapshot", topic: "health:binance", seq: 0, data: 2 });
    sources[1]!.serve({ type: "update", topic: "health:binance", seq: 2, kind: "health", data: 3 });
    scheduler.advance(100);
    await flush();
    assert.equal(sources.length, 3);
    assert.deepEqual(handler.events, ["snapshot:1", "snapshot:2"]);
  });

  it("takes over when the event stream breaks instead of letting it retry the spent ticket", async () => {
    const { client, sources, scheduler } = rig({ transport: "sse" });
    client.start();
    await flush();
    sources[0]!.onerror?.({});
    assert.equal(sources[0]!.closed, true);
    assert.equal(client.info.status, "reconnecting");
    scheduler.advance(500);
    await flush();
    assert.equal(new URL(sources[1]!.url).searchParams.get("ticket"), "t2");
  });
});

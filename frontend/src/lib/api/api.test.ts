import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { ApiClient, ApiError, type Fetch } from "@/lib/api/client";
import { apiBaseUrl, toWebSocketUrl, transportPreference } from "@/lib/config";
import { parseFrame, topics } from "@/lib/stream/protocol";

interface Call {
  url: string;
  init: RequestInit;
}

function fakeFetch(status: number, body: unknown, calls: Call[] = []): Fetch {
  return (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(input), init: init ?? {} });
    return new Response(typeof body === "string" ? body : JSON.stringify(body), { status });
  }) as Fetch;
}

describe("ApiClient", () => {
  it("sends the bearer token and parses JSON", async () => {
    const calls: Call[] = [];
    const client = new ApiClient({
      baseUrl: "http://gateway:8000/",
      token: "secret",
      fetch: fakeFetch(200, [{ id: "mom-1" }], calls),
    });
    const sessions = await client.sessions(5);
    assert.deepEqual(sessions, [{ id: "mom-1" }]);
    assert.equal(calls[0]?.url, "http://gateway:8000/api/v1/sessions?limit=5");
    assert.deepEqual(calls[0]?.init.headers, {
      Accept: "application/json",
      Authorization: "Bearer secret",
    });
  });

  it("encodes candle queries and command bodies", async () => {
    const calls: Call[] = [];
    const client = new ApiClient({ baseUrl: "http://g", token: "t", fetch: fakeFetch(202, {}, calls) });
    await client.candles({ venue: "binance", symbol: "BTC/USDT", timeframe: "1h", end: 1.9, limit: 3 });
    assert.equal(
      calls[0]?.url,
      "http://g/api/v1/candles?venue=binance&symbol=BTC%2FUSDT&tf=1h&end=1&limit=3",
    );
    await client.command("mom 1", "flatten", "risk off");
    assert.equal(calls[1]?.url, "http://g/api/v1/sessions/mom%201/commands");
    assert.equal(calls[1]?.init.method, "POST");
    assert.equal(calls[1]?.init.body, JSON.stringify({ kind: "flatten", reason: "risk off" }));
  });

  it("turns failures into ApiError with the server's message", async () => {
    const denied = new ApiClient({
      baseUrl: "http://g",
      token: "bad",
      fetch: fakeFetch(401, { detail: "missing or invalid bearer token" }),
    });
    await assert.rejects(denied.markets(), (error: unknown) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.unauthorized, true);
      assert.equal(error.message, "missing or invalid bearer token");
      return true;
    });
    const invalid = new ApiClient({
      baseUrl: "http://g",
      token: "t",
      fetch: fakeFetch(422, { detail: [{ msg: "Field required" }, { msg: "too long" }] }),
    });
    await assert.rejects(invalid.markets(), /Field required; too long/);
    const down = new ApiClient({
      baseUrl: "http://g",
      token: "t",
      fetch: (async () => {
        throw new TypeError("fetch failed");
      }) as Fetch,
    });
    await assert.rejects(down.markets(), (error: unknown) => {
      assert.ok(error instanceof ApiError && error.transient && error.status === 0);
      assert.match(error.message, /cannot reach the gateway at http:\/\/g: fetch failed/);
      return true;
    });
    const anonymous = new ApiClient({ baseUrl: "http://g", token: null, fetch: fakeFetch(200, {}) });
    await assert.rejects(anonymous.markets(), /not signed in/);
    assert.deepEqual(await anonymous.health(), {});
  });
});

describe("config", () => {
  it("derives stream URLs and reads preferences", () => {
    assert.equal(toWebSocketUrl("https://cq.example:8443"), "wss://cq.example:8443");
    assert.equal(toWebSocketUrl("http://127.0.0.1:8000"), "ws://127.0.0.1:8000");
    assert.throws(() => toWebSocketUrl("ftp://x"), /not an http/);
    assert.equal(apiBaseUrl(), process.env.NEXT_PUBLIC_CQ_API_URL?.replace(/\/+$/, "") || "http://127.0.0.1:8000");
    assert.ok(["auto", "ws", "sse"].includes(transportPreference()));
  });
});

describe("protocol", () => {
  it("parses well-formed frames and rejects the rest", () => {
    assert.deepEqual(parseFrame('{"type":"snapshot","topic":"a","seq":0,"data":1}'), {
      type: "snapshot",
      topic: "a",
      seq: 0,
      data: 1,
    });
    assert.deepEqual(parseFrame('{"type":"error","topic":null,"code":"bad_request"}'), {
      type: "error",
      topic: null,
      code: "bad_request",
      message: "",
    });
    assert.equal(parseFrame("not json"), null);
    assert.equal(parseFrame('{"type":"update","topic":"a","seq":"1"}'), null);
    assert.equal(parseFrame('{"type":"mystery"}'), null);
    assert.equal(parseFrame("[1,2]"), null);
    assert.equal(topics.candles("binance", "BTC/USDT:USDT", "1h"), "candles:binance:BTC/USDT:USDT:1h");
    assert.equal(topics.session("mom-1"), "session:mom-1");
  });
});

/**
 * Stream topic handlers that write into the stores. Components subscribe to a topic with one
 * of these (through `useTopic`) and read the result from the store.
 */

import type {
  BookPayload,
  BookSnapshot,
  Candle,
  CandlesSnapshot,
  FeedHealth,
  SessionSnapshot,
} from "@/lib/api/types";
import type { TopicHandler } from "@/lib/stream/client";
import { useMarketStore } from "@/stores/market";
import { useSessionStore } from "@/stores/session";

export function candlesHandler(topic: string): TopicHandler {
  const market = useMarketStore.getState;
  return {
    snapshot: (data) => market().setCandles(topic, (data as CandlesSnapshot).candles),
    update: (kind, data) => {
      if (kind === "candle") market().applyCandle(topic, data as Candle);
    },
    error: (_code, message) => market().setTopicError(topic, message),
  };
}

export function bookHandler(topic: string): TopicHandler {
  const market = useMarketStore.getState;
  return {
    snapshot: (data) => market().setBook(topic, (data as BookSnapshot).book),
    update: (kind, data) => {
      if (kind === "book") market().setBook(topic, data as BookPayload);
    },
    error: (_code, message) => market().setTopicError(topic, message),
  };
}

export function healthHandler(venue: string): TopicHandler {
  const market = useMarketStore.getState;
  return {
    snapshot: (data) => market().setHealth(venue, data as FeedHealth),
    update: (kind, data) => {
      if (kind === "health") market().setHealth(venue, data as FeedHealth);
    },
  };
}

export function sessionHandler(sessionId: string): TopicHandler {
  const sessions = useSessionStore.getState;
  return {
    snapshot: (data) => sessions().setSnapshot(sessionId, data as SessionSnapshot),
    update: (kind, data) => sessions().applyUpdate(sessionId, kind, data),
    error: (code, message) => sessions().setError(sessionId, code, message),
  };
}

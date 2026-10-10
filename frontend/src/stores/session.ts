/** The client's copy of each subscribed trading session (see `lib/session/reducer.ts`). */

import { create } from "zustand";

import type { SessionSnapshot } from "@/lib/api/types";
import { applyUpdate, fromSnapshot, type SessionView } from "@/lib/session/reducer";

export interface SessionEntry {
  view: SessionView | null;
  status: "loading" | "ready" | "unavailable" | "error";
  error: string | null;
}

interface SessionState {
  sessions: Record<string, SessionEntry>;
  setSnapshot(sessionId: string, snapshot: SessionSnapshot): void;
  applyUpdate(sessionId: string, kind: string, data: unknown): void;
  setError(sessionId: string, code: string, message: string): void;
}

const LOADING: SessionEntry = { view: null, status: "loading", error: null };

export function sessionEntry(state: Pick<SessionState, "sessions">, sessionId: string | null): SessionEntry {
  return (sessionId !== null ? state.sessions[sessionId] : undefined) ?? LOADING;
}

export const useSessionStore = create<SessionState>()((set) => ({
  sessions: {},
  setSnapshot(sessionId, snapshot) {
    set((state) => ({
      sessions: {
        ...state.sessions,
        [sessionId]: { view: fromSnapshot(snapshot), status: "ready", error: null },
      },
    }));
  },
  applyUpdate(sessionId, kind, data) {
    set((state) => {
      const entry = state.sessions[sessionId];
      if (!entry?.view) return state;
      const view = applyUpdate(entry.view, kind, data);
      if (view === entry.view) return state;
      return { sessions: { ...state.sessions, [sessionId]: { ...entry, view } } };
    });
  },
  setError(sessionId, code, message) {
    set((state) => {
      const entry = state.sessions[sessionId] ?? LOADING;
      return {
        sessions: {
          ...state.sessions,
          [sessionId]: {
            ...entry,
            status: code === "unavailable" ? "unavailable" : "error",
            error: message,
          },
        },
      };
    });
  },
}));

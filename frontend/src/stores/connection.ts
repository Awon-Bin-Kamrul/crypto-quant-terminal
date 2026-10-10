/** The stream connection's state, for the status indicators. */

import { create } from "zustand";

import type { ConnectionInfo } from "@/lib/stream/client";

interface ConnectionState extends ConnectionInfo {
  update(info: ConnectionInfo): void;
}

/** Frames arrive many times a second; the "last message" time is only refreshed this often. */
const LAST_FRAME_RESOLUTION_MS = 1_000;

export const useConnectionStore = create<ConnectionState>()((set, get) => ({
  status: "idle",
  transport: null,
  latencyMs: null,
  lastFrameAt: null,
  failures: 0,
  error: null,
  update(info) {
    const current = get();
    const frameMoved =
      info.lastFrameAt !== null &&
      (current.lastFrameAt === null || info.lastFrameAt - current.lastFrameAt >= LAST_FRAME_RESOLUTION_MS);
    const changed =
      info.status !== current.status ||
      info.transport !== current.transport ||
      info.latencyMs !== current.latencyMs ||
      info.failures !== current.failures ||
      info.error !== current.error ||
      frameMoved;
    if (!changed) return;
    set({ ...info, lastFrameAt: frameMoved ? info.lastFrameAt : current.lastFrameAt });
  },
}));

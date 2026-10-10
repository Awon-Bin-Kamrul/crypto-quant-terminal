"use client";

import * as React from "react";

import { ApiClient } from "@/lib/api/client";
import { apiBaseUrl, transportPreference } from "@/lib/config";
import { StreamClient, type TopicHandler } from "@/lib/stream/client";
import { useAuthStore } from "@/stores/auth";
import { useConnectionStore } from "@/stores/connection";

interface Services {
  api: ApiClient;
  stream: StreamClient;
}

const ServicesContext = React.createContext<Services | null>(null);

/**
 * The REST client and the shared stream connection for one signed-in operator. The stream is
 * started on mount and stopped on unmount; a token the gateway rejects signs the operator out.
 */
export function TerminalServices({ token, children }: { token: string; children: React.ReactNode }) {
  const services = React.useMemo<Services>(() => {
    const baseUrl = apiBaseUrl();
    const api = new ApiClient({ baseUrl, token });
    const stream = new StreamClient({
      baseUrl,
      transport: transportPreference(),
      ticket: async () => (await api.streamTicket()).ticket,
    });
    return { api, stream };
  }, [token]);

  React.useEffect(() => {
    const { stream } = services;
    const off = stream.onChange((info) => {
      useConnectionStore.getState().update(info);
      if (info.status === "unauthorized") {
        useAuthStore.getState().signOut("The gateway rejected the token. Sign in again.");
      }
    });
    stream.start();
    return () => {
      off();
      stream.stop();
    };
  }, [services]);

  return <ServicesContext.Provider value={services}>{children}</ServicesContext.Provider>;
}

export function useServices(): Services {
  const services = React.useContext(ServicesContext);
  if (services === null) throw new Error("useServices outside <TerminalServices>");
  return services;
}

/** Subscribe `handler` to `topic` while mounted (nothing when either is null). */
export function useTopic(topic: string | null, handler: TopicHandler | null): void {
  const { stream } = useServices();
  React.useEffect(() => {
    if (topic === null || handler === null) return undefined;
    return stream.subscribe(topic, handler);
  }, [stream, topic, handler]);
}

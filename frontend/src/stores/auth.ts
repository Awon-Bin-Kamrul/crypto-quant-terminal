/**
 * The operator's API token. Kept for the browser tab (sessionStorage) unless the operator asks
 * to be remembered on this device (localStorage). It never leaves the browser except as the
 * `Authorization` header of calls to the gateway.
 */

import { create } from "zustand";

import { readItem, writeItem } from "@/lib/storage";

const KEY = "cq.token";

interface AuthState {
  token: string | null;
  remembered: boolean;
  hydrated: boolean;
  /** Why the operator was signed out (a token the gateway rejected), shown at sign-in. */
  notice: string | null;
  hydrate(): void;
  signIn(token: string, remember: boolean): void;
  signOut(notice?: string): void;
}

export const useAuthStore = create<AuthState>()((set) => ({
  token: null,
  remembered: false,
  hydrated: false,
  notice: null,
  hydrate() {
    const remembered = readItem("local", KEY);
    const token = remembered ?? readItem("session", KEY);
    set({ token, remembered: remembered !== null, hydrated: true });
  },
  signIn(token, remember) {
    writeItem(remember ? "local" : "session", KEY, token);
    writeItem(remember ? "session" : "local", KEY, null);
    set({ token, remembered: remember, notice: null });
  },
  signOut(notice) {
    writeItem("local", KEY, null);
    writeItem("session", KEY, null);
    set({ token: null, remembered: false, notice: notice ?? null });
  },
}));

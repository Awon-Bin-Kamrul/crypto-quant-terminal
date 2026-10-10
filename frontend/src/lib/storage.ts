/**
 * Browser storage that never throws: private windows, blocked site data and server rendering
 * all behave as an empty store.
 */

type Area = "local" | "session";

function area(which: Area): Storage | null {
  if (typeof window === "undefined") return null;
  try {
    return which === "local" ? window.localStorage : window.sessionStorage;
  } catch {
    return null;
  }
}

export function readItem(which: Area, key: string): string | null {
  try {
    return area(which)?.getItem(key) ?? null;
  } catch {
    return null;
  }
}

export function writeItem(which: Area, key: string, value: string | null): void {
  try {
    const store = area(which);
    if (!store) return;
    if (value === null) store.removeItem(key);
    else store.setItem(key, value);
  } catch {
    // storage full or disabled: the value simply is not remembered
  }
}

export function readJson<T>(which: Area, key: string): T | null {
  const text = readItem(which, key);
  if (text === null) return null;
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

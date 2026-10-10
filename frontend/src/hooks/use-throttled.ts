"use client";

import * as React from "react";

/** `value`, but changing at most once per `intervalMs` (the latest value always lands). */
export function useThrottled<T>(value: T, intervalMs: number): T {
  const [shown, setShown] = React.useState(value);
  const last = React.useRef(0);
  const timer = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const latest = React.useRef(value);
  latest.current = value;

  React.useEffect(() => {
    const wait = last.current + intervalMs - Date.now();
    if (wait <= 0) {
      last.current = Date.now();
      setShown(value);
      return undefined;
    }
    if (timer.current === null) {
      timer.current = setTimeout(() => {
        timer.current = null;
        last.current = Date.now();
        setShown(latest.current);
      }, wait);
    }
    return undefined;
  }, [value, intervalMs]);

  React.useEffect(
    () => () => {
      if (timer.current !== null) clearTimeout(timer.current);
    },
    [],
  );
  return shown;
}

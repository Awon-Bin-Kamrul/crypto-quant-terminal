"use client";

import * as React from "react";

/** The current time, refreshed every `intervalMs` (for ages and clocks). */
export function useNow(intervalMs = 1_000): number {
  const [now, setNow] = React.useState(() => Date.now());
  React.useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}

/** Chart timeframes the gateway serves, and their lengths. */

export const TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"] as const;
export type Timeframe = (typeof TIMEFRAMES)[number];

const MINUTE = 60_000;

const DURATIONS: Record<string, number> = {
  "1m": MINUTE,
  "3m": 3 * MINUTE,
  "5m": 5 * MINUTE,
  "15m": 15 * MINUTE,
  "30m": 30 * MINUTE,
  "1h": 60 * MINUTE,
  "2h": 120 * MINUTE,
  "4h": 240 * MINUTE,
  "6h": 360 * MINUTE,
  "8h": 480 * MINUTE,
  "12h": 720 * MINUTE,
  "1d": 1_440 * MINUTE,
};

export function isTimeframe(value: string): value is Timeframe {
  return (TIMEFRAMES as readonly string[]).includes(value);
}

/** Length of one candle in milliseconds; throws for an unknown timeframe. */
export function timeframeMs(timeframe: string): number {
  const duration = DURATIONS[timeframe];
  if (duration === undefined) throw new Error(`unknown timeframe ${timeframe}`);
  return duration;
}

/** Open time of the candle containing `ms` (buckets aligned to the Unix epoch, as stored). */
export function bucketStart(ms: number, timeframe: string): number {
  const duration = timeframeMs(timeframe);
  return Math.floor(ms / duration) * duration;
}

/**
 * Number and time formatting for the terminal. Every figure is rendered in a monospace,
 * tabular font, so formats keep a stable number of decimals per magnitude: a price that ticks
 * from 99.99 to 100.00 keeps its width. Times are UTC — the exchange's clock, and the only
 * zone in which candle boundaries line up.
 */

const cache = new Map<string, Intl.NumberFormat>();

function formatter(minimum: number, maximum: number, extra: Intl.NumberFormatOptions = {}): Intl.NumberFormat {
  const key = `${minimum}:${maximum}:${JSON.stringify(extra)}`;
  let format = cache.get(key);
  if (!format) {
    format = new Intl.NumberFormat("en-US", {
      minimumFractionDigits: minimum,
      maximumFractionDigits: maximum,
      ...extra,
    });
    cache.set(key, format);
  }
  return format;
}

/** Decimals that suit a price of this magnitude (BTC at 60 000 → 2, a meme coin → 8). */
export function priceDecimals(price: number): number {
  const magnitude = Math.abs(price);
  if (magnitude === 0 || !Number.isFinite(magnitude)) return 2;
  if (magnitude >= 1_000) return 2;
  if (magnitude >= 10) return 3;
  if (magnitude >= 1) return 4;
  const leadingZeros = Math.floor(-Math.log10(magnitude));
  return Math.min(10, leadingZeros + 4);
}

const DASH = "—";

export function formatPrice(value: number | null | undefined, decimals?: number): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  const places = decimals ?? priceDecimals(value);
  return formatter(places, places).format(value);
}

/** Quantities: up to 8 decimals, trailing zeros dropped. */
export function formatQty(value: number | null | undefined, maxDecimals = 8): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  return formatter(0, maxDecimals).format(value);
}

/** Money in the quote currency, 2 decimals; `signed` shows a + for gains. */
export function formatMoney(
  value: number | null | undefined,
  options: { signed?: boolean; decimals?: number } = {},
): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  const places = options.decimals ?? 2;
  const text = formatter(places, places).format(Math.abs(value));
  if (value < 0 && Math.abs(value) >= 0.5 * 10 ** -places) return `−${text}`;
  return options.signed && value > 0 ? `+${text}` : text;
}

/** A fraction as a percentage: 0.0123 → "1.23%". */
export function formatPct(
  fraction: number | null | undefined,
  options: { signed?: boolean; decimals?: number } = {},
): string {
  if (fraction === null || fraction === undefined || !Number.isFinite(fraction)) return DASH;
  const places = options.decimals ?? 2;
  const text = formatter(places, places).format(Math.abs(fraction) * 100);
  if (fraction < 0 && Math.abs(fraction) * 100 >= 0.5 * 10 ** -places) return `−${text}%`;
  return options.signed && fraction > 0 ? `+${text}%` : `${text}%`;
}

/** Large figures in compact form: 1 234 567 → "1.23M". */
export function formatCompact(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  return formatter(0, 2, { notation: "compact" }).format(value);
}

function pad(value: number, width = 2): string {
  return String(value).padStart(width, "0");
}

/** "14:05:09" (UTC). */
export function formatTime(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return DASH;
  const date = new Date(ms);
  return `${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
}

/** "2026-09-29 14:05:09" (UTC). */
export function formatDateTime(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return DASH;
  const date = new Date(ms);
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${formatTime(ms)}`;
}

/** "3s", "4m 10s", "2h 5m", "3d 4h" — for ages and durations. */
export function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) return DASH;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h ${minutes % 60}m`;
  return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}

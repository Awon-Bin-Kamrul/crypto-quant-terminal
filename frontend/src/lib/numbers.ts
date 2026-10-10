/**
 * Decimal strings from the gateway become numbers only for display and arithmetic on screen;
 * the backend keeps the exact values. `num` never returns NaN: unparseable input is 0, and
 * `optNum` keeps "unknown" distinct from zero.
 */

export function num(value: string | number | null | undefined): number {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function optNum(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** -1, 0 or 1 (0 also for values within `epsilon` of zero). */
export function sign(value: number, epsilon = 1e-12): -1 | 0 | 1 {
  if (value > epsilon) return 1;
  if (value < -epsilon) return -1;
  return 0;
}

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  formatCompact,
  formatDateTime,
  formatDuration,
  formatMoney,
  formatPct,
  formatPrice,
  formatQty,
  formatTime,
  priceDecimals,
} from "@/lib/format";

describe("format", () => {
  it("chooses price decimals by magnitude", () => {
    assert.equal(priceDecimals(64_250.5), 2);
    assert.equal(priceDecimals(245.123), 3);
    assert.equal(priceDecimals(1.2345), 4);
    assert.equal(priceDecimals(0.012345), 5); // four significant digits
    assert.equal(priceDecimals(0.00001234), 8);
    assert.equal(formatPrice(64_250.5), "64,250.50");
    assert.equal(formatPrice(0.00001234), "0.00001234");
    assert.equal(formatPrice(null), "—");
    assert.equal(formatPrice(Number.NaN), "—");
  });

  it("formats quantities, money and percentages", () => {
    assert.equal(formatQty(0.12), "0.12");
    assert.equal(formatQty(1_250), "1,250");
    assert.equal(formatMoney(1234.5), "1,234.50");
    assert.equal(formatMoney(-12.345), "−12.35");
    assert.equal(formatMoney(-0.001), "0.00", "no negative zero");
    assert.equal(formatMoney(3, { signed: true }), "+3.00");
    assert.equal(formatPct(0.0123), "1.23%");
    assert.equal(formatPct(-0.05, { signed: true }), "−5.00%");
    assert.equal(formatPct(0.2, { signed: true, decimals: 1 }), "+20.0%");
    assert.equal(formatCompact(1_234_567), "1.23M");
  });

  it("formats UTC times and durations", () => {
    const ms = Date.UTC(2026, 8, 29, 4, 5, 9);
    assert.equal(formatTime(ms), "04:05:09");
    assert.equal(formatDateTime(ms), "2026-09-29 04:05:09");
    assert.equal(formatDuration(4_200), "4s");
    assert.equal(formatDuration(250_000), "4m 10s");
    assert.equal(formatDuration(2 * 3_600_000 + 5 * 60_000), "2h 5m");
    assert.equal(formatDuration(76 * 3_600_000), "3d 4h");
    assert.equal(formatDuration(-1), "—");
  });
});

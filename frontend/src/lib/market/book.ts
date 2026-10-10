/** Order book ladder rows with cumulative depth, for the book panel. */

import type { BookPayload } from "@/lib/api/types";

export interface LadderRow {
  price: number;
  size: number;
  /** Size summed from the touch outwards. */
  total: number;
  /** `total` as a fraction of the deeper side's total, for the depth bar. */
  depth: number;
}

export interface Ladder {
  asks: LadderRow[];
  bids: LadderRow[];
  spread: number | null;
  spreadBps: number | null;
  mid: number | null;
}

function cumulate(levels: readonly [number, number][], limit: number): LadderRow[] {
  const rows: LadderRow[] = [];
  let total = 0;
  for (const [price, size] of levels.slice(0, limit)) {
    total += size;
    rows.push({ price, size, total, depth: 0 });
  }
  return rows;
}

/** `levels` per side, asks listed best (lowest) first like bids. */
export function ladder(book: BookPayload | null, levels: number): Ladder {
  if (book === null) return { asks: [], bids: [], spread: null, spreadBps: null, mid: null };
  const asks = cumulate(book.asks, levels);
  const bids = cumulate(book.bids, levels);
  const deepest = Math.max(asks.at(-1)?.total ?? 0, bids.at(-1)?.total ?? 0);
  for (const row of [...asks, ...bids]) row.depth = deepest > 0 ? row.total / deepest : 0;
  return { asks, bids, spread: book.spread, spreadBps: book.spread_bps, mid: book.mid };
}

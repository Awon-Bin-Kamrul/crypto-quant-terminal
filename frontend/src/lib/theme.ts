/**
 * The terminal palette in hex, for canvases (the chart library parses colors itself). These are
 * the sRGB values of the oklch tokens in `app/globals.css`; keep the two in step.
 */
export const palette = {
  background: "#090c11",
  panel: "#0e1218",
  popover: "#11161d",
  text: "#e4e8ed",
  mutedText: "#8d96a2",
  border: "#222830",
  grid: "rgba(34, 40, 48, 0.55)",
  crosshair: "rgba(141, 150, 162, 0.55)",
  primary: "#40b8ee",
  buy: "#35cc86",
  buySoft: "rgba(53, 204, 134, 0.35)",
  sell: "#f8535a",
  sellSoft: "rgba(248, 83, 90, 0.35)",
  warning: "#f3b94c",
  violet: "#ae84f2",
} as const;

export const chartFont =
  '"JetBrains Mono", "SF Mono", ui-monospace, Menlo, Consolas, "Liberation Mono", monospace';

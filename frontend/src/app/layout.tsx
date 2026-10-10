import type { Metadata, Viewport } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "Crypto Quant Terminal",
  description: "Live markets, trading sessions, positions and PnL from the Crypto Quant gateway.",
  robots: { index: false, follow: false },
};

export const viewport: Viewport = {
  themeColor: "#090c11",
  colorScheme: "dark",
  width: "device-width",
  initialScale: 1,
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" className="dark">
      <body>{children}</body>
    </html>
  );
}

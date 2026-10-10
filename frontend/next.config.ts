import type { NextConfig } from "next";

/**
 * The web terminal is a client-rendered single page: every byte of market and trading data
 * comes from the terminal gateway (`cq api serve`) at runtime, so nothing is fetched at build
 * time and the build needs no secrets.
 */
const nextConfig: NextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  output: "standalone",
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "X-Frame-Options", value: "DENY" },
          { key: "Referrer-Policy", value: "no-referrer" },
          { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=()" },
        ],
      },
    ];
  },
};

export default nextConfig;

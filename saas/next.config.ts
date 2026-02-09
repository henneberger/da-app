import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  turbopack: {
    // Avoid Next.js inferring a parent as the workspace root when multiple lockfiles exist.
    root: __dirname,
  },
};

export default nextConfig;

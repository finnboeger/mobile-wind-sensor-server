/**
 * Configuration for Wind Dashboard
 */

interface DashboardConfig {
  // PocketBase configuration
  // Used by this server, e.g. http://pocketbase:8090 inside Docker
  pocketbaseUrl: string;
  // Handed to the browser; falls back to pocketbaseUrl
  pocketbasePublicUrl: string;
  pocketbaseAdminEmail: string;
  pocketbaseAdminPassword: string;

  // UI default settings
  defaultTimeframeMinutes: number; // 5, 10, 15, or 30
  defaultDirectionAverageMode: "weighted" | "arithmetic";
  defaultSmoothingWindowMinutes: number;

  // Server configuration
  port: number;
  host: string;
  logLevel: "debug" | "info" | "warn" | "error";
}

export function loadConfig(): DashboardConfig {
  // The server talks to PocketBase directly, so startup doesn't depend on the
  // public URL (reverse proxy, DNS) being reachable from inside the container
  const pocketbaseUrl = process.env.POCKETBASE_URL || process.env.POCKETBASE_PUBLIC_URL;
  if (!pocketbaseUrl) {
    throw new Error("Missing POCKETBASE_URL environment variable");
  }
  const pocketbaseAdminEmail = process.env.POCKETBASE_ADMIN_EMAIL;
  if (!pocketbaseAdminEmail) {
    throw new Error("Missing POCKETBASE_ADMIN_EMAIL environment variable");
  }

  const pocketbaseAdminPassword = process.env.POCKETBASE_ADMIN_PASSWORD;
  if (!pocketbaseAdminPassword) {
    throw new Error("Missing POCKETBASE_ADMIN_PASSWORD environment variable");
  }

  return {
    pocketbaseUrl,
    pocketbasePublicUrl: process.env.POCKETBASE_PUBLIC_URL || pocketbaseUrl,
    pocketbaseAdminEmail,
    pocketbaseAdminPassword,
    defaultTimeframeMinutes:
      parseInt(process.env.DEFAULT_TIMEFRAME_MINUTES || "15") || 15,
    defaultDirectionAverageMode: "weighted",
    defaultSmoothingWindowMinutes:
      parseInt(process.env.DEFAULT_SMOOTHING_WINDOW_MINUTES || "1") || 1,
    port: parseInt(process.env.PORT || "3000"),
    host: process.env.HOST || "0.0.0.0",
    logLevel: (process.env.LOG_LEVEL || "info") as any,
  };
}

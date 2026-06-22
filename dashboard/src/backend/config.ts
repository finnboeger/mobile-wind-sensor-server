/**
 * Configuration for Wind Dashboard
 */

interface DashboardConfig {
  // PocketBase configuration
  pocketbaseUrl: string;
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
  const pocketbaseUrl = process.env.POCKETBASE_PUBLIC_URL || process.env.POCKETBASE_URL;
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

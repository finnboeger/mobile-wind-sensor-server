/**
 * Environment configuration for Dashboard Server
 * 
 * Docker mode: reads from environment variables
 * Bare-metal mode: reads from config.json
 */

interface DashboardConfig {
  // PocketBase configuration
  pocketbaseUrl: string;
  pocketbaseAdminToken: string;

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
  const isDev = process.env.NODE_ENV !== "production";

  // Check if this is docker mode (POCKETBASE_URL env var exists) or bare-metal mode (config.json)
  const isDockerMode = !!process.env.POCKETBASE_URL;

  if (isDockerMode) {
    // Docker mode: read from env vars
    return loadConfigFromEnv();
  } else {
    // Bare-metal mode: read from config.json
    return loadConfigFromFile();
  }
}

function loadConfigFromEnv(): DashboardConfig {
  const pocketbaseUrl = process.env.POCKETBASE_URL;
  if (!pocketbaseUrl) {
    throw new Error(
      "Missing POCKETBASE_URL environment variable (Docker mode)"
    );
  }

  const pocketbaseAdminToken = process.env.POCKETBASE_ADMIN_TOKEN;
  if (!pocketbaseAdminToken) {
    throw new Error(
      "Missing POCKETBASE_ADMIN_TOKEN environment variable (Docker mode)"
    );
  }

  return {
    pocketbaseUrl: pocketbaseUrl.replace(/\/$/, ""), // Remove trailing slash
    pocketbaseAdminToken,
    defaultTimeframeMinutes: parseInt(
      process.env.DEFAULT_TIMEFRAME_MINUTES || "15",
      10
    ),
    defaultDirectionAverageMode: (
      process.env.DIRECTION_AVERAGE_MODE || "weighted"
    ) as "weighted" | "arithmetic",
    defaultSmoothingWindowMinutes: parseInt(
      process.env.DEFAULT_SMOOTHING_WINDOW_MINUTES || "5",
      10
    ),
    port: parseInt(process.env.PORT || "3000", 10),
    host: process.env.HOST || "0.0.0.0",
    logLevel: (process.env.LOG_LEVEL || "info") as
      | "debug"
      | "info"
      | "warn"
      | "error",
  };
}

function loadConfigFromFile(): DashboardConfig {
  try {
    const fs = require("fs");
    const path = require("path");

    const configPath = path.join(process.cwd(), "config.json");
    if (!fs.existsSync(configPath)) {
      throw new Error(
        `config.json not found at ${configPath} (Bare-metal mode)`
      );
    }

    const configFile = JSON.parse(fs.readFileSync(configPath, "utf-8"));

    return {
      pocketbaseUrl: (configFile.pocketbaseUrl || "").replace(/\/$/, ""),
      pocketbaseAdminToken: configFile.adminToken || "",
      defaultTimeframeMinutes:
        configFile.defaultTimeframeMinutes || 15,
      defaultDirectionAverageMode:
        configFile.directionAverageMode || "weighted",
      defaultSmoothingWindowMinutes:
        configFile.defaultSmoothingWindowMinutes || 5,
      port: configFile.port || 3000,
      host: configFile.host || "localhost",
      logLevel: configFile.logLevel || "info",
    };
  } catch (error) {
    throw new Error(
      `Failed to load bare-metal configuration: ${error instanceof Error ? error.message : "Unknown error"}`
    );
  }
}

export { DashboardConfig };

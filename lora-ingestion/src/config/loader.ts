/**
 * Configuration loader for LoRaWAN Ingestion Service
 * 
 * This service only runs in bare-metal mode.
 * Configuration is loaded from config.json (MQTT env vars are ignored).
 */

import * as fs from "fs";
import * as path from "path";

export interface LoRaConfig {
  // PocketBase configuration
  pocketbaseUrl: string;
  adminToken: string;

  // LoRaWAN listener configuration
  httpPort: number;
  httpHost: string;

  // Logging
  logLevel: "debug" | "info" | "warn" | "error";
}

export function loadConfig(): LoRaConfig {
  try {
    const configPath = path.join(process.cwd(), "config.json");

    if (!fs.existsSync(configPath)) {
      throw new Error(`config.json not found at ${configPath}`);
    }

    const configFile = JSON.parse(fs.readFileSync(configPath, "utf-8"));

    // Validate required fields
    if (!configFile.pocketbaseUrl) {
      throw new Error("Missing pocketbaseUrl in config.json");
    }
    if (!configFile.adminToken) {
      throw new Error("Missing adminToken in config.json");
    }

    return {
      pocketbaseUrl: configFile.pocketbaseUrl.replace(/\/$/, ""),
      adminToken: configFile.adminToken,
      httpPort: configFile.httpPort || 3001,
      httpHost: configFile.httpHost || "0.0.0.0",
      logLevel: configFile.logLevel || "info",
    };
  } catch (error) {
    throw new Error(
      `Failed to load LoRaWAN configuration: ${error instanceof Error ? error.message : "Unknown error"}`
    );
  }
}

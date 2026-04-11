import express from "express";
import PocketBase from "pocketbase";
import { loadConfig } from "./config/env";
import { bootstrapPocketBase } from "./pocketbase/bootstrap";

const app = express();
let pocketbase: PocketBase;

/**
 * Dashboard Server
 * 
 * Responsibilities:
 * - PocketBase bootstrap (collection/index creation, security rules)
 * - REST API for UI queries
 * - WebSocket API for realtime updates (via PocketBase subscriptions)
 * - Configuration loading (docker env vars or bare-metal config.json)
 */

async function main() {
  try {
    // Load configuration
    console.log("Loading configuration...");
    const config = loadConfig();
    console.log(`Configuration loaded: ${config.pocketbaseUrl}`);

    // Initialize PocketBase client
    pocketbase = new PocketBase(config.pocketbaseUrl);
    pocketbase.authStore.save(config.pocketbaseAdminToken);

    // Bootstrap PocketBase collections and security rules
    console.log("Bootstrapping PocketBase...");
    const bootstrapResult = await bootstrapPocketBase(pocketbase);
    console.log("Bootstrap result:", bootstrapResult);

    // Setup Express middleware
    app.use(express.json());

    // Health check endpoint
    app.get("/health", (req, res) => {
      res.json({
        status: "ok",
        pocketbaseConnected: !!pocketbase,
        timestamp: new Date().toISOString(),
      });
    });

    // Configuration endpoint (returns UI defaults, no secrets)
    app.get("/api/config", (req, res) => {
      res.json({
        defaultTimeframeMinutes: config.defaultTimeframeMinutes,
        defaultDirectionAverageMode: config.defaultDirectionAverageMode,
        defaultSmoothingWindowMinutes: config.defaultSmoothingWindowMinutes,
        pocketbaseUrl: config.pocketbaseUrl,
      });
    });

    // Start server
    app.listen(config.port, config.host, () => {
      console.log(
        `✓ Dashboard Server listening on http://${config.host}:${config.port}`
      );
      console.log(`  Health check: http://${config.host}:${config.port}/health`);
      console.log(`  Config endpoint: http://${config.host}:${config.port}/api/config`);
    });
  } catch (error) {
    console.error(
      "Failed to start Dashboard Server:",
      error instanceof Error ? error.message : error
    );
    process.exit(1);
  }
}

main();

export { app, pocketbase };

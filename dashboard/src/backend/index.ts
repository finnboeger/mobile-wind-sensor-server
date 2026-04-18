import express from "express";
import path from "path";
import PocketBase from "pocketbase";
import { loadConfig } from "./config";
import { bootstrapPocketBase } from "./bootstrap";

const app = express();
let pocketbase: PocketBase;

/**
 * Wind Dashboard Server
 * 
 * Single unified Node.js app that:
 * - Serves Vue 3 SPA frontend
 * - Ensures PocketBase collections exist on startup
 * - Provides REST API for frontend queries
 * - Supports both Docker and bare-metal deployments
 */

async function main() {
  try {
    // Load configuration
    console.log("Loading configuration...");
    const config = loadConfig();
    console.log(`Configuration loaded: ${config.pocketbaseUrl}`);

    // Initialize PocketBase client
    pocketbase = new PocketBase(config.pocketbaseUrl);

    // Bootstrap PocketBase collections and security rules
    console.log("Bootstrapping PocketBase...");
    const bootstrapResult = await bootstrapPocketBase(
      pocketbase,
      config.pocketbaseAdminEmail,
      config.pocketbaseAdminPassword
    );
    console.log("Bootstrap result:", bootstrapResult);

    // Setup Express middleware
    app.use(express.json());

    // Serve static Vue frontend
    const frontendDir = path.join(__dirname, "../public");
    app.use(express.static(frontendDir));

    // API endpoints
    app.get("/health", (req, res) => {
      res.json({
        status: "ok",
        pocketbaseConnected: !!pocketbase,
        timestamp: new Date().toISOString(),
      });
    });

    app.get("/api/config", (req, res) => {
      res.json({
        defaultTimeframeMinutes: config.defaultTimeframeMinutes,
        defaultDirectionAverageMode: config.defaultDirectionAverageMode,
        defaultSmoothingWindowMinutes: config.defaultSmoothingWindowMinutes,
        pocketbaseUrl: config.pocketbaseUrl,
      });
    });

    // SPA fallback - serve index.html for all other routes
    app.get("*", (req, res) => {
      res.sendFile(path.join(frontendDir, "index.html"));
    });

    // Start server
    app.listen(config.port, config.host, () => {
      console.log(`✓ Wind Dashboard started at http://${config.host}:${config.port}`);
      console.log(`✓ PocketBase: ${config.pocketbaseUrl}`);
    });
  } catch (error) {
    console.error(
      "Failed to start server:",
      error instanceof Error ? error.message : error
    );
    process.exit(1);
  }
}

main();

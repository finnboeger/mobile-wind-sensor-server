import express, { Request, Response } from "express";
import PocketBase from "pocketbase";
import { loadConfig } from "./config/loader";
import { LoRaListener } from "./loraListener";

/**
 * LoRaWAN Ingestion Service
 * 
 * Bare-metal-only service that:
 * - Receives LoRaWAN application data frames via HTTP POST
 * - Validates and parses frame payloads
 * - Writes to PocketBase measurements collection
 * 
 * Configuration via config.json:
 * - pocketbaseUrl, adminToken
 * - httpPort, httpHost
 * - logLevel (debug, info, warn, error)
 */

const app = express();
let loraListener: LoRaListener;
let pocketbase: PocketBase;

async function main() {
  try {
    // Load configuration
    console.log("Loading configuration...");
    const config = loadConfig();
    console.log(
      `Configuration loaded: ${config.pocketbaseUrl} (port ${config.httpPort})`
    );

    // Initialize PocketBase client
    pocketbase = new PocketBase(config.pocketbaseUrl);
    pocketbase.authStore.save(config.adminToken);

    // Authenticate with PocketBase
    console.log("Authenticating with PocketBase...");
    try {
      await pocketbase.collection("measurements").getList(1, 1);
      console.log("✓ PocketBase authenticated");
    } catch (error: any) {
      console.warn("PocketBase connection warning:", error.message);
    }

    // Initialize LoRa listener
    loraListener = new LoRaListener(pocketbase, config);

    // Setup Express
    app.use(express.json());

    // Health check endpoint
    app.get("/health", (req: Request, res: Response) => {
      res.json({
        status: "ok",
        pocketbaseConnected: !!pocketbase,
        timestamp: new Date().toISOString(),
      });
    });

    // LoRaWAN frame ingestion endpoint
    app.post("/api/lora/frames", async (req: Request, res: Response) => {
      try {
        const frame = req.body;
        await loraListener.processFrame(frame);
        res.json({ status: "ok" });
      } catch (error) {
        console.error("Frame processing error:", error);
        res.status(400).json({ status: "error", message: "Frame processing failed" });
      }
    });

    // Statistics endpoint
    app.get("/api/stats", (req: Request, res: Response) => {
      const stats = loraListener.getStats();
      res.json(stats);
    });

    // Start server
    app.listen(config.httpPort, config.httpHost, () => {
      console.log(
        `✓ LoRaWAN Ingestion Service listening on http://${config.httpHost}:${config.httpPort}`
      );
      console.log(`  Health check: http://${config.httpHost}:${config.httpPort}/health`);
      console.log(`  Ingest frames: POST http://${config.httpHost}:${config.httpPort}/api/lora/frames`);
      console.log(`  Statistics: http://${config.httpHost}:${config.httpPort}/api/stats`);
    });

    // Log stats every 30 seconds
    setInterval(() => {
      const stats = loraListener.getStats();
      console.log(
        `[STATS] Received: ${stats.framesReceived}, Processed: ${stats.framesProcessed}, Failed: ${stats.framesFailed}`
      );
    }, 30000);

    console.log("✓ LoRaWAN Ingestion Service started successfully");

    // Graceful shutdown
    process.on("SIGINT", () => {
      console.log("\nShutting down...");
      process.exit(0);
    });

    process.on("SIGTERM", () => {
      console.log("\nShutting down...");
      process.exit(0);
    });
  } catch (error) {
    console.error(
      "Failed to start LoRaWAN Ingestion Service:",
      error instanceof Error ? error.message : error
    );
    process.exit(1);
  }
}

main();

export { app, loraListener, pocketbase };

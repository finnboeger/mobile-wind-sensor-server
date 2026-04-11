import PocketBase from "pocketbase";
import { loadConfig } from "./config/env";
import { MqttListener } from "./mqttListener";

/**
 * MQTT Ingestion Service
 * 
 * Docker-only service that:
 * - Subscribes to MQTT topics
 * - Validates 1Hz measurement payloads
 * - Writes to PocketBase measurements collection
 * - Provides health endpoint via HTTP
 * 
 * Configuration via environment variables:
 * - MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASSWORD
 * - MQTT_TOPICS=topic1=source1,topic2=source2
 * - POCKETBASE_URL, POCKETBASE_ADMIN_TOKEN
 * - LOG_LEVEL (debug, info, warn, error)
 */

let mqttListener: MqttListener;
let pocketbase: PocketBase;

async function main() {
  try {
    // Load configuration
    console.log("Loading configuration...");
    const config = loadConfig();
    console.log(`Configuration loaded: ${config.mqttHost}:${config.mqttPort}`);

    // Initialize PocketBase client
    pocketbase = new PocketBase(config.pocketbaseUrl);
    pocketbase.authStore.save(config.pocketbaseAdminToken);

    // Authenticate with PocketBase
    console.log("Authenticating with PocketBase...");
    try {
      await pocketbase.collection("measurements").getList(1, 1);
      console.log("✓ PocketBase authenticated");
    } catch (error: any) {
      if (error?.status !== 401) {
        console.warn("PocketBase connection warning:", error.message);
      }
    }

    // Start MQTT listener
    console.log("Starting MQTT listener...");
    mqttListener = new MqttListener(pocketbase, config);
    await mqttListener.connect();

    // Health check: log stats every 30 seconds
    setInterval(() => {
      const stats = mqttListener.getStats();
      console.log(
        `[STATS] Received: ${stats.messagesReceived}, Processed: ${stats.messagesProcessed}, Failed: ${stats.messagesFailed}`
      );
    }, 30000);

    console.log("✓ MQTT Ingestion Service started successfully");

    // Graceful shutdown
    process.on("SIGINT", () => {
      console.log("\nShutting down...");
      mqttListener.disconnect();
      process.exit(0);
    });

    process.on("SIGTERM", () => {
      console.log("\nShutting down...");
      mqttListener.disconnect();
      process.exit(0);
    });
  } catch (error) {
    console.error(
      "Failed to start MQTT Ingestion Service:",
      error instanceof Error ? error.message : error
    );
    process.exit(1);
  }
}

main();

export { mqttListener, pocketbase };

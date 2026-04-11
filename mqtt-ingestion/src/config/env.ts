/**
 * Environment configuration for MQTT Ingestion Service
 * 
 * This service only runs in Docker mode.
 * All settings are read from environment variables.
 */

interface MqttConfig {
  // MQTT broker connection
  mqttHost: string;
  mqttPort: number;
  mqttUser?: string;
  mqttPassword?: string;
  mqttTopics: Record<string, string>; // topic -> source_id mapping

  // PocketBase configuration
  pocketbaseUrl: string;
  pocketbaseAdminToken: string;

  // Service configuration
  logLevel: "debug" | "info" | "warn" | "error";
}

export function loadConfig(): MqttConfig {
  const mqttHost = process.env.MQTT_HOST;
  if (!mqttHost) {
    throw new Error("Missing MQTT_HOST environment variable");
  }

  const mqttTopicsStr = process.env.MQTT_TOPICS;
  if (!mqttTopicsStr) {
    throw new Error(
      "Missing MQTT_TOPICS environment variable (format: topic1=source1,topic2=source2)"
    );
  }

  // Parse MQTT topics
  const mqttTopics: Record<string, string> = {};
  mqttTopicsStr.split(",").forEach((mapping) => {
    const [topic, sourceId] = mapping.split("=");
    if (topic && sourceId) {
      mqttTopics[topic.trim()] = sourceId.trim();
    }
  });

  if (Object.keys(mqttTopics).length === 0) {
    throw new Error("No valid MQTT topic mappings found");
  }

  const pocketbaseUrl = process.env.POCKETBASE_URL;
  if (!pocketbaseUrl) {
    throw new Error("Missing POCKETBASE_URL environment variable");
  }

  const pocketbaseAdminToken = process.env.POCKETBASE_ADMIN_TOKEN;
  if (!pocketbaseAdminToken) {
    throw new Error("Missing POCKETBASE_ADMIN_TOKEN environment variable");
  }

  return {
    mqttHost,
    mqttPort: parseInt(process.env.MQTT_PORT || "1883", 10),
    mqttUser: process.env.MQTT_USER,
    mqttPassword: process.env.MQTT_PASSWORD,
    mqttTopics,
    pocketbaseUrl: pocketbaseUrl.replace(/\/$/, ""),
    pocketbaseAdminToken,
    logLevel: (process.env.LOG_LEVEL || "info") as
      | "debug"
      | "info"
      | "warn"
      | "error",
  };
}

export { MqttConfig };

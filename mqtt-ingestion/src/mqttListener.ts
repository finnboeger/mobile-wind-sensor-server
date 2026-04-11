import * as mqtt from "mqtt";
import PocketBase from "pocketbase";
import { tryParseMeasurement, MeasurementPayload } from "@wind-dashboard/shared";
import { MqttConfig } from "./config/env";

interface MqttListenerStats {
  messagesReceived: number;
  messagesProcessed: number;
  messagesFailed: number;
  lastProcessedAt?: Date;
}

export class MqttListener {
  private client: mqtt.MqttClient | null = null;
  private pocketbase: PocketBase;
  private config: MqttConfig;
  private stats: MqttListenerStats = {
    messagesReceived: 0,
    messagesProcessed: 0,
    messagesFailed: 0,
  };

  constructor(pocketbase: PocketBase, config: MqttConfig) {
    this.pocketbase = pocketbase;
    this.config = config;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const connectionUrl = `mqtt://${this.config.mqttHost}:${this.config.mqttPort}`;

      console.log(`Connecting to MQTT broker: ${connectionUrl}`);
      console.log(`Topics: ${Object.keys(this.config.mqttTopics).join(", ")}`);

      this.client = mqtt.connect(connectionUrl, {
        username: this.config.mqttUser,
        password: this.config.mqttPassword,
        reconnectPeriod: 5000,
        connectTimeout: 10000,
      });

      this.client.on("connect", () => {
        console.log("✓ Connected to MQTT broker");

        // Subscribe to all topics
        Object.keys(this.config.mqttTopics).forEach((topic) => {
          this.client!.subscribe(topic, (err) => {
            if (err) {
              console.error(`Failed to subscribe to ${topic}:`, err.message);
            } else {
              console.log(`✓ Subscribed to: ${topic}`);
            }
          });
        });

        resolve();
      });

      this.client.on("error", (err) => {
        console.error("MQTT connection error:", err.message);
        reject(err);
      });

      this.client.on("message", (topic, payload) => {
        this.handleMessage(topic, payload);
      });

      this.client.on("disconnect", () => {
        console.warn("Disconnected from MQTT broker");
      });

      // Set a timeout for connection
      setTimeout(() => {
        if (!this.client?.connected) {
          reject(new Error("MQTT connection timeout"));
        }
      }, 15000);
    });
  }

  private async handleMessage(topic: string, payload: Buffer): Promise<void> {
    this.stats.messagesReceived++;

    try {
      // Get source_id from topic mapping
      const sourceId = this.config.mqttTopics[topic];
      if (!sourceId) {
        console.warn(`Received message on unmapped topic: ${topic}`);
        return;
      }

      // Parse JSON payload
      const jsonString = payload.toString("utf-8");
      const rawData = JSON.parse(jsonString);

      // Ensure source_id is set
      const data = { ...rawData, source_id: sourceId };

      // Validate against measurement schema
      const measurement = tryParseMeasurement(data);
      if (!measurement) {
        console.warn(`Invalid measurement format from ${sourceId}:`, data);
        this.stats.messagesFailed++;
        return;
      }

      // Upsert to PocketBase
      await this.writeMeasurement(measurement);
      this.stats.messagesProcessed++;
      this.stats.lastProcessedAt = new Date();

      if (this.config.logLevel === "debug") {
        console.debug(
          `✓ Processed measurement from ${sourceId} at ${measurement.ts}`
        );
      }
    } catch (error) {
      console.error(
        `Failed to process MQTT message:`,
        error instanceof Error ? error.message : error
      );
      this.stats.messagesFailed++;
    }
  }

  private async writeMeasurement(measurement: any): Promise<void> {
    // Create or update record in measurements collection
    // Filter by source_id + ts to find existing record
    try {
      const existing = await this.pocketbase
        .collection("measurements")
        .getFirstListItem(
          `source_id = "${measurement.source_id}" && ts = "${measurement.ts.toISOString()}"`
        )
        .catch(() => null);

      if (existing) {
        // Update existing record
        await this.pocketbase
          .collection("measurements")
          .update(existing.id, measurement);
      } else {
        // Create new record
        await this.pocketbase
          .collection("measurements")
          .create(measurement);
      }
    } catch (error) {
      console.error(
        `Failed to write measurement to PocketBase:`,
        error instanceof Error ? error.message : error
      );
      throw error;
    }
  }

  disconnect(): void {
    if (this.client) {
      this.client.end();
      console.log("Disconnected from MQTT broker");
    }
  }

  getStats(): MqttListenerStats {
    return { ...this.stats };
  }

  isConnected(): boolean {
    return this.client?.connected || false;
  }
}

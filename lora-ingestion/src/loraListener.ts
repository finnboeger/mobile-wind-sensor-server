import PocketBase from "pocketbase";
import { tryParseMeasurement } from "@wind-dashboard/shared";
import { LoRaConfig } from "./config/loader";

interface LoRaListenerStats {
  framesReceived: number;
  framesProcessed: number;
  framesFailed: number;
  lastProcessedAt?: Date;
}

/**
 * LoRaWAN Listener
 * 
 * Receives LoRaWAN application data frames (via HTTP POST from gateway/decoder)
 * and writes validated measurements to PocketBase.
 */
export class LoRaListener {
  private pocketbase: PocketBase;
  private config: LoRaConfig;
  private stats: LoRaListenerStats = {
    framesReceived: 0,
    framesProcessed: 0,
    framesFailed: 0,
  };

  constructor(pocketbase: PocketBase, config: LoRaConfig) {
    this.pocketbase = pocketbase;
    this.config = config;
  }

  /**
   * Process a LoRaWAN application data frame
   * 
   * Expected format (example):
   * {
   *   "sourceId": "lora_sensor_1",
   *   "timestamp": "2024-01-15T10:30:00Z",
   *   "payload": {
   *     "windDir": 245,
   *     "windSpeed": 8.5
   *   }
   * }
   */
  async processFrame(frame: any): Promise<void> {
    this.stats.framesReceived++;

    try {
      if (!frame.sourceId || !frame.timestamp || !frame.payload) {
        console.warn("Invalid LoRaWAN frame format:", frame);
        this.stats.framesFailed++;
        return;
      }

      // Map LoRaWAN payload to measurement schema
      const measurement = {
        source_id: frame.sourceId,
        ts: new Date(frame.timestamp),
        true_wind_dir_deg: frame.payload.windDir || frame.payload.wind_dir,
        true_wind_speed_mps: frame.payload.windSpeed || frame.payload.wind_speed,
        gps_lat: frame.payload.lat || frame.payload.latitude,
        gps_lng: frame.payload.lng || frame.payload.longitude,
        sensor_heading_deg: frame.payload.heading || frame.payload.sensorHeading,
        sensor_speed_mps: frame.payload.speed || frame.payload.sensorSpeed,
        apparent_wind_dir_deg: frame.payload.appWindDir,
        apparent_wind_speed_mps: frame.payload.appWindSpeed,
      };

      // Remove undefined fields
      Object.keys(measurement).forEach(
        (key) =>
          measurement[key as keyof typeof measurement] === undefined &&
          delete measurement[key as keyof typeof measurement]
      );

      // Validate against measurement schema
      const validated = tryParseMeasurement(measurement);
      if (!validated) {
        console.warn(
          `Invalid measurement format from ${frame.sourceId}:`,
          measurement
        );
        this.stats.framesFailed++;
        return;
      }

      // Write to PocketBase
      await this.writeMeasurement(validated);
      this.stats.framesProcessed++;
      this.stats.lastProcessedAt = new Date();

      if (this.config.logLevel === "debug") {
        console.debug(
          `✓ Processed LoRaWAN frame from ${frame.sourceId} at ${frame.timestamp}`
        );
      }
    } catch (error) {
      console.error(
        `Failed to process LoRaWAN frame:`,
        error instanceof Error ? error.message : error
      );
      this.stats.framesFailed++;
    }
  }

  private async writeMeasurement(measurement: any): Promise<void> {
    try {
      // Check if record already exists
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

  getStats(): LoRaListenerStats {
    return { ...this.stats };
  }
}

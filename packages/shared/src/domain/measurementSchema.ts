import { z } from "zod";

/**
 * Measurement schema represents a single snapshot from a wind sensor.
 * Designed for a single measurements collection in PocketBase.
 * 
 * One row per source_id per second (enforced by unique index on source_id + ts).
 * Gust calculation is frontend-only and not stored in the database.
 */

export const MeasurementSchema = z.object({
  // Database metadata
  id: z.string().optional(), // PocketBase auto-generated ID
  created: z.date().optional(), // PocketBase auto-generated
  updated: z.date().optional(), // PocketBase auto-generated

  // Source identification (indexed, part of unique constraint)
  source_id: z.string().min(1, "source_id is required"),

  // Timestamp (indexed, part of unique constraint)
  ts: z.date().describe("Measurement timestamp, one per source per second"),

  // GPS coordinates (optional)
  gps_lat: z.number().nullable().optional(),
  gps_lng: z.number().nullable().optional(),

  // Sensor heading and speed (optional, for apparent wind calculation)
  sensor_heading_deg: z.number().nullable().optional(),
  sensor_speed_mps: z.number().nullable().optional(),

  // True wind (primary display)
  true_wind_dir_deg: z
    .number()
    .min(0)
    .max(359.99)
    .describe("True wind direction in degrees [0, 360)"),
  true_wind_speed_mps: z
    .number()
    .min(0)
    .describe("True wind speed in meters per second"),

  // Apparent wind (optional, for completeness)
  apparent_wind_dir_deg: z.number().nullable().optional(),
  apparent_wind_speed_mps: z.number().nullable().optional(),
});

export type Measurement = z.infer<typeof MeasurementSchema>;

/**
 * Measurement without auto-generated database fields (for API submission)
 */
export const MeasurementPayloadSchema = MeasurementSchema.omit({
  id: true,
  created: true,
  updated: true,
});

export type MeasurementPayload = z.infer<typeof MeasurementPayloadSchema>;

/**
 * Validates and parses a raw measurement object
 */
export function parseMeasurement(raw: unknown): Measurement {
  return MeasurementSchema.parse(raw);
}

/**
 * Safely validates a raw measurement object, returns null on validation error
 */
export function tryParseMeasurement(raw: unknown): Measurement | null {
  try {
    return MeasurementSchema.parse(raw);
  } catch {
    return null;
  }
}

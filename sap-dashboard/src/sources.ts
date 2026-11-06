import type PocketBase from "pocketbase";
import { ms } from "./format";
import type { Measurement } from "./types";

export interface SourceHealth {
  sourceId: string;
  latestTsMs: number | null;
  // How long the latest fix took to reach PocketBase
  delayMs: number | null;
  // Newest fix SAP could take (position and true wind)
  sendableTsMs: number | null;
}

// Sources quiet for longer than this are left out (e.g. sensors of an earlier event)
export const FORGET_AFTER_MS = 24 * 3600_000;
// Fixes read per source, newest first, to find the latest sendable one
const RECENT_PER_SOURCE = 50;
// Safety stop for the source lookup
const MAX_SOURCES = 200;
const FIELDS = "source_id,ts,created_at,gps_lat,gps_lng,true_wind_dir_deg,true_wind_speed_mps";

/**
 * Every source_id in measurements, in order. PocketBase has no "distinct", so
 * each step asks for the first source after the previous one, which the
 * (source_id, ts) index answers without a scan.
 */
export async function findSources(pb: PocketBase): Promise<string[]> {
  const ids: string[] = [];
  while (ids.length < MAX_SOURCES) {
    const after = ids[ids.length - 1];
    const page = await pb.collection("measurements").getList<Pick<Measurement, "source_id">>(1, 1, {
      filter: after === undefined ? "" : pb.filter("source_id > {:after}", { after }),
      sort: "source_id",
      fields: "source_id",
      skipTotal: true,
      requestKey: null,
    });
    if (page.items.length === 0) break;
    ids.push(page.items[0].source_id);
  }
  return ids;
}

/**
 * The health of each source, read per source so one that stopped sending
 * still shows up, with its age, instead of dropping out of the newest records.
 */
export async function loadSources(pb: PocketBase, sourceIds: string[], nowMs: number): Promise<SourceHealth[]> {
  const sources = await Promise.all(
    sourceIds.map(async (sourceId) => {
      const page = await pb.collection("measurements").getList<Measurement>(1, RECENT_PER_SOURCE, {
        filter: pb.filter("source_id = {:sourceId}", { sourceId }),
        sort: "-ts",
        fields: FIELDS,
        skipTotal: true,
        requestKey: null,
      });
      return sourceHealth(sourceId, page.items);
    })
  );
  return sources
    .filter((s) => s.latestTsMs !== null && nowMs - s.latestTsMs < FORGET_AFTER_MS)
    .sort((a, b) => a.sourceId.localeCompare(b.sourceId));
}

/** Health of one source from its newest measurements, newest first. */
export function sourceHealth(sourceId: string, newestFirst: Measurement[]): SourceHealth {
  const latest = newestFirst[0];
  const latestTsMs = ms(latest?.ts);
  const createdMs = ms(latest?.created_at);
  const sendable = newestFirst.find(isSendable);
  return {
    sourceId,
    latestTsMs,
    delayMs: createdMs !== null && latestTsMs !== null ? createdMs - latestTsMs : null,
    sendableTsMs: ms(sendable?.ts),
  };
}

/** Same rule as the uplink's convert.py: a position and true wind; 0/0 is a GPS without fix. */
export function isSendable(m: Measurement): boolean {
  const hasPosition = m.gps_lat != null && m.gps_lng != null && !(m.gps_lat === 0 && m.gps_lng === 0);
  const hasWind = m.true_wind_dir_deg != null && m.true_wind_speed_mps != null;
  return hasPosition && hasWind;
}

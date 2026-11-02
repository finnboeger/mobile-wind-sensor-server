import { ms } from "./format";
import type { RaceUpload, UplinkStatus } from "./types";

export interface RaceRow {
  key: string;
  regattaName: string;
  raceName: string;
  current: boolean;
  finished: boolean;
  tracked: boolean;
  firstFixMs: number | null;
  lastFixMs: number | null;
  lastAcceptedMs: number | null;
  sources: string[];
  state: { label: string; level: string };
}

// A race counts as accepting while SAP took fixes for it this recently
const ACCEPTING_MS = 120_000;

export const raceKey = (regatta: string, race: string) => `${regatta}\u0000${race}`;

/**
 * One row per race that accepted wind or is tracked in the event: current
 * races first, then by last accepted fix, newest first.
 */
export function buildRaces(
  status: UplinkStatus | null,
  uploads: RaceUpload[],
  currentKeys: Set<string>,
  nowMs: number
): RaceRow[] {
  const rows = new Map<string, RaceRow>();
  const row = (regattaName: string, raceName: string): RaceRow => {
    const key = raceKey(regattaName, raceName);
    let r = rows.get(key);
    if (!r) {
      r = {
        key, regattaName, raceName,
        current: currentKeys.has(key),
        finished: false, tracked: false,
        firstFixMs: null, lastFixMs: null, lastAcceptedMs: null,
        sources: [],
        state: { label: "", level: "" },
      };
      rows.set(key, r);
    }
    return r;
  };

  for (const t of status?.tracked_races || []) {
    const r = row(t.regattaName, t.raceName);
    r.tracked = true;
    r.finished = !!t.finished;
  }
  for (const c of status?.current_races || []) row(c.regattaName, c.raceName);
  for (const u of uploads) {
    const r = row(u.regatta_name, u.race_name);
    r.sources.push(u.source_id);
    const first = ms(u.first_fix);
    const last = ms(u.last_fix);
    const acceptedAt = ms(u.last_accepted_at);
    if (first !== null && (r.firstFixMs === null || first < r.firstFixMs)) r.firstFixMs = first;
    if (last !== null && (r.lastFixMs === null || last > r.lastFixMs)) r.lastFixMs = last;
    if (acceptedAt !== null && (r.lastAcceptedMs === null || acceptedAt > r.lastAcceptedMs)) r.lastAcceptedMs = acceptedAt;
  }

  for (const r of rows.values()) {
    const acceptingNow = r.lastAcceptedMs !== null && nowMs - r.lastAcceptedMs < ACCEPTING_MS;
    if (r.current && acceptingNow) r.state = { label: "Accepting wind", level: "ok" };
    else if (r.current) r.state = { label: "Current, no wind accepted lately", level: "warn" };
    else if (r.finished) r.state = { label: "Finished", level: "" };
    else if (r.tracked) r.state = { label: "Tracked, not current", level: "" };
    else r.state = { label: "Earlier race", level: "" };
  }

  return [...rows.values()].sort((a, b) => {
    if (a.current !== b.current) return a.current ? -1 : 1;
    if (a.lastFixMs !== b.lastFixMs) return (b.lastFixMs ?? -Infinity) - (a.lastFixMs ?? -Infinity);
    return a.raceName.localeCompare(b.raceName);
  });
}

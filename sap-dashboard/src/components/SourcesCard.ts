import { computed, defineComponent, type PropType } from "vue";
import { ageClass, ago, duration, ms } from "../format";
import type { Measurement } from "../types";

// The newest measurements, newest first
export default defineComponent({
  props: {
    measurements: { type: Array as PropType<Measurement[]>, required: true },
  },
  setup(props) {
    interface SourceHealth {
      sourceId: string;
      latestTsMs: number | null;
      delayMs: number | null;
      sendableTsMs: number | null;
    }

    const sources = computed(() => {
      const bySource = new Map<string, SourceHealth>();
      for (const m of props.measurements) {
        const tsMs = ms(m.ts);
        let entry = bySource.get(m.source_id);
        if (!entry) {
          const createdMs = ms(m.created_at);
          entry = {
            sourceId: m.source_id,
            latestTsMs: tsMs,
            delayMs: createdMs !== null && tsMs !== null ? createdMs - tsMs : null,
            sendableTsMs: null,
          };
          bySource.set(m.source_id, entry);
        }
        // Same rule as the uplink's convert.py: 0/0 is a GPS without fix
        const hasPosition = m.gps_lat != null && m.gps_lng != null && !(m.gps_lat === 0 && m.gps_lng === 0);
        const hasWind = m.true_wind_dir_deg != null && m.true_wind_speed_mps != null;
        if (hasPosition && hasWind && tsMs !== null && (entry.sendableTsMs === null || tsMs > entry.sendableTsMs)) {
          entry.sendableTsMs = tsMs;
        }
        if (tsMs !== null && (entry.latestTsMs === null || tsMs > entry.latestTsMs)) entry.latestTsMs = tsMs;
      }
      return [...bySource.values()].sort((a, b) => a.sourceId.localeCompare(b.sourceId));
    });

    const level = computed(() => {
      if (sources.value.length === 0) return "bad";
      const newest = Math.max(...sources.value.map((s) => s.latestTsMs ?? 0));
      return ageClass(newest || null, 10, 60);
    });

    return { sources, level, ageClass, ago, duration };
  },
});

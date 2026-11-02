import { computed, defineComponent, type PropType } from "vue";
import { ageClass, ago, clock, ms } from "../format";
import type { UplinkStatus } from "../types";

export default defineComponent({
  props: {
    status: { type: Object as PropType<UplinkStatus | null>, default: null },
    alive: { type: Boolean, required: true },
  },
  setup(props) {
    const level = computed(() => {
      if (!props.status || !props.alive || !props.status.configured) return "bad";
      return props.status.realtime === "disconnected" ? "warn" : "ok";
    });

    const raceSelection = computed(() => {
      const s = props.status;
      if (!s) return "—";
      if (s.all_races) return "every tracked race this login may update";
      if (s.event_id) return `current races of event ${s.event_id}${s.course_area ? `, course area ${s.course_area}` : ""}`;
      return (s.current_races || []).map((r) => `${r.regattaName} / ${r.raceName}`).join(", ") || "—";
    });

    return { level, raceSelection, ageClass, ago, clock, ms };
  },
});

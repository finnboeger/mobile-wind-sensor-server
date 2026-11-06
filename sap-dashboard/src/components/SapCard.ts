import { computed, defineComponent, type PropType } from "vue";
import { ageClass, ago, clock, ms, now } from "../format";
import type { UplinkStatus } from "../types";

export default defineComponent({
  props: {
    status: { type: Object as PropType<UplinkStatus | null>, default: null },
    hasCurrentRace: { type: Boolean, required: true },
  },
  setup(props) {
    // An error newer than SAP's last answer is still unresolved
    const currentError = computed(() => {
      const errorAt = ms(props.status?.last_error_at);
      const sentAt = ms(props.status?.last_sent_at);
      return errorAt !== null && (sentAt === null || errorAt > sentAt);
    });

    // A wrong or expired token isn't refused: SAP treats it as anonymous and adds the wind to no race
    const notAccepted = computed(() => {
      const s = props.status;
      if (!s || !props.hasCurrentRace) return false;
      const sentAt = ms(s.last_sent_at);
      const acceptedAt = ms(s.last_accepted_at);
      if (sentAt === null || now.value - sentAt > 120_000) return false;
      return acceptedAt === null || sentAt - acceptedAt >= 120_000;
    });

    const level = computed(() => {
      if (!props.status || currentError.value) return "bad";
      if (notAccepted.value) return "warn";
      if (!props.hasCurrentRace) return "idle";
      return ageClass(ms(props.status.last_accepted_at), 30, 300);
    });

    return { currentError, notAccepted, level, ageClass, ago, clock, ms, now };
  },
});

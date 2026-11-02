import { defineComponent, type PropType } from "vue";
import { ago, dateTime } from "../format";
import type { RaceRow } from "../races";

export default defineComponent({
  props: {
    races: { type: Array as PropType<RaceRow[]>, required: true },
    // Show the backfill buttons
    backfill: { type: Boolean, default: false },
  },
  emits: {
    backfill: (_race: RaceRow | null) => true,
  },
  setup(_props, { emit }) {
    return { emit, ago, dateTime };
  },
});

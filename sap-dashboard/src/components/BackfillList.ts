import { defineComponent, type PropType } from "vue";
import { dateTime, ms } from "../format";
import type { Backfill } from "../types";

export default defineComponent({
  props: {
    backfills: { type: Array as PropType<Backfill[]>, required: true },
  },
  setup() {
    const LEVELS: Record<string, string> = { done: "ok", running: "info", pending: "info", failed: "bad" };

    return { LEVELS, dateTime, ms };
  },
});

import { defineComponent, type PropType } from "vue";
import { ago, dateTime } from "../format";
import type { RaceRow } from "../races";

export default defineComponent({
  props: {
    races: { type: Array as PropType<RaceRow[]>, required: true },
  },
  setup() {
    return { ago, dateTime };
  },
});

import { computed, defineComponent, type PropType } from "vue";
import { ageClass, ago, duration } from "../format";
import type { SourceHealth } from "../sources";

export default defineComponent({
  props: {
    sources: { type: Array as PropType<SourceHealth[]>, required: true },
  },
  setup(props) {
    // As fresh as the newest source: a single stopped sensor shows in its own row
    const level = computed(() => {
      if (props.sources.length === 0) return "bad";
      const newest = Math.max(...props.sources.map((s) => s.latestTsMs ?? 0));
      return ageClass(newest || null, 10, 60);
    });
    return { level, ageClass, ago, duration };
  },
});

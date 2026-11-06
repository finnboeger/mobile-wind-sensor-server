import { defineComponent, type PropType, ref, watch } from "vue";
import type PocketBase from "pocketbase";
import { now } from "../format";
import { message, pbDate } from "../pocketbase";
import type { RaceRow } from "../races";

export default defineComponent({
  props: {
    pb: { type: Object as PropType<PocketBase>, required: true },
    races: { type: Array as PropType<RaceRow[]>, required: true },
    race: { type: Object as PropType<RaceRow | null>, default: null },
  },
  emits: {
    requested: () => true,
  },
  setup(props, { emit }) {
    function localInput(t: number): string {
      const d = new Date(t);
      const pad = (n: number) => String(n).padStart(2, "0");
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    // Whole days around what SAP took so far: the gaps are usually before the
    // first accepted fix (tracking started late) or after the last
    const start = new Date(props.race?.firstFixMs ?? now.value);
    start.setHours(0, 0, 0, 0);
    const end = new Date(props.race?.lastFixMs ?? now.value);
    end.setHours(23, 59, 0, 0);

    const raceKey = ref(props.race?.key || "");
    const from = ref(localInput(start.getTime()));
    const until = ref(localInput(Math.min(end.getTime(), now.value)));
    const count = ref<number | null>(null);
    const error = ref("");
    const busy = ref(false);

    let countTimer: number | undefined;
    watch(
      [from, until],
      () => {
        count.value = null;
        window.clearTimeout(countTimer);
        countTimer = window.setTimeout(countMeasurements, 300);
      },
      { immediate: true }
    );

    async function countMeasurements() {
      const fromMs = new Date(from.value).getTime();
      const untilMs = new Date(until.value).getTime();
      if (!(untilMs > fromMs)) return;
      try {
        const page = await props.pb.collection("measurements").getList(1, 1, {
          filter: props.pb.filter("ts > {:from} && ts <= {:until}", { from: pbDate(fromMs), until: pbDate(untilMs) }),
          fields: "id",
          requestKey: "backfill-count",
        });
        count.value = page.totalItems;
      } catch {
        // A newer count replaced this one
      }
    }

    async function submit() {
      const fromMs = new Date(from.value).getTime();
      const untilMs = new Date(until.value).getTime();
      if (!(untilMs > fromMs)) {
        error.value = "Until must be after From";
        return;
      }
      const race = props.races.find((r) => r.key === raceKey.value);
      busy.value = true;
      error.value = "";
      try {
        await props.pb.collection("sap_backfills").create({
          regatta_name: race?.regattaName || "",
          race_name: race?.raceName || "",
          range_from: pbDate(fromMs),
          range_until: pbDate(untilMs),
          status: "pending",
        });
        emit("requested");
      } catch (e) {
        error.value = `Could not start the backfill: ${message(e)}`;
      } finally {
        busy.value = false;
      }
    }

    return { raceKey, from, until, count, error, busy, submit };
  },
});

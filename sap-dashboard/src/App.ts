import { computed, defineComponent, onMounted, onUnmounted, ref, shallowRef } from "vue";
import type PocketBase from "pocketbase";
import SapCard from "./components/SapCard.vue";
import SourcesCard from "./components/SourcesCard.vue";
import UplinkCard from "./components/UplinkCard.vue";
import { ageClass, ms } from "./format";
import { STATUS_ID, connect, isNotFound, message } from "./pocketbase";
import type { Measurement, UplinkStatus } from "./types";

export default defineComponent({
  components: { SapCard, SourcesCard, UplinkCard },
  setup() {
    const pb = shallowRef<PocketBase | null>(null);
    const loadError = ref("");
    const status = ref<UplinkStatus | null>(null);
    const recent = ref<Measurement[]>([]);

    const uplinkAlive = computed(() => ageClass(ms(status.value?.heartbeat_at), 60, 120) !== "bad");

    // A dead uplink's current races are stale
    const currentKeys = computed(() => {
      if (!uplinkAlive.value) return new Set<string>();
      return new Set((status.value?.current_races || []).map((r) => `${r.regattaName}\u0000${r.raceName}`));
    });

    async function loadStatus() {
      try {
        status.value = await pb.value!.collection("sap_uplink_status").getOne<UplinkStatus>(STATUS_ID, { requestKey: null });
      } catch (e) {
        if (!isNotFound(e)) throw e;
        status.value = null;
      }
    }

    async function loadMeasurements() {
      // The newest records cover every source that sent something lately
      const page = await pb.value!.collection("measurements").getList<Measurement>(1, 200, {
        sort: "-created_at",
        fields: "source_id,ts,created_at,gps_lat,gps_lng,true_wind_dir_deg,true_wind_speed_mps",
        skipTotal: true,
        requestKey: null,
      });
      recent.value = page.items;
    }

    async function loadAll() {
      try {
        await Promise.all([loadStatus(), loadMeasurements()]);
        loadError.value = "";
      } catch (e) {
        loadError.value = `Could not load from PocketBase: ${message(e)}`;
      }
    }

    const timers: number[] = [];

    onMounted(async () => {
      try {
        pb.value = await connect();
      } catch (e) {
        loadError.value = `Could not load config.json: ${message(e)}`;
        return;
      }
      await loadAll();
      // Realtime keeps the uplink's records current; polling covers dropped
      // subscriptions and the measurement ages
      const subscribe = (collection: string, load: () => Promise<void>) =>
        pb.value!.collection(collection).subscribe("*", () => load().catch(() => {})).catch(() => {});
      subscribe("sap_uplink_status", loadStatus);
      timers.push(window.setInterval(() => loadMeasurements().catch(() => {}), 5000));
      timers.push(window.setInterval(loadAll, 30000));
    });

    onUnmounted(() => {
      timers.forEach((t) => window.clearInterval(t));
      pb.value?.realtime.unsubscribe();
    });

    return { loadError, status, recent, uplinkAlive, currentKeys };
  },
});

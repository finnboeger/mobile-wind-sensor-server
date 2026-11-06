import { computed, defineComponent, onMounted, onUnmounted, ref, shallowRef } from "vue";
import type PocketBase from "pocketbase";
import SapCard from "./components/SapCard.vue";
import SourcesCard from "./components/SourcesCard.vue";
import UplinkCard from "./components/UplinkCard.vue";
import { ageClass, ms } from "./format";
import { STATUS_ID, connect, isNotFound, message } from "./pocketbase";
import { findSources, loadSources, type SourceHealth } from "./sources";
import type { UplinkStatus } from "./types";

export default defineComponent({
  components: { SapCard, SourcesCard, UplinkCard },
  setup() {
    const pb = shallowRef<PocketBase | null>(null);
    const loadError = ref("");
    const status = ref<UplinkStatus | null>(null);
    const sources = ref<SourceHealth[]>([]);

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

    // Looked up again every few minutes, for sensors added meanwhile
    let sourceIds: string[] = [];
    let sourcesFoundAt = 0;

    async function loadMeasurements() {
      if (Date.now() - sourcesFoundAt > 300_000) {
        sourceIds = await findSources(pb.value!);
        sourcesFoundAt = Date.now();
      }
      sources.value = await loadSources(pb.value!, sourceIds, Date.now());
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

    return { loadError, status, sources, uplinkAlive, currentKeys };
  },
});

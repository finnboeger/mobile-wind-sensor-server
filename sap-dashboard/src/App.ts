import { defineComponent, onMounted, onUnmounted, ref, shallowRef } from "vue";
import type PocketBase from "pocketbase";
import { STATUS_ID, connect, isNotFound, message } from "./pocketbase";
import type { UplinkStatus } from "./types";

export default defineComponent({
  setup() {
    const pb = shallowRef<PocketBase | null>(null);
    const loadError = ref("");
    const status = ref<UplinkStatus | null>(null);

    async function loadStatus() {
      try {
        status.value = await pb.value!.collection("sap_uplink_status").getOne<UplinkStatus>(STATUS_ID, { requestKey: null });
      } catch (e) {
        if (!isNotFound(e)) throw e;
        status.value = null;
      }
    }

    async function loadAll() {
      try {
        await Promise.all([loadStatus()]);
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
      timers.push(window.setInterval(loadAll, 30000));
    });

    onUnmounted(() => {
      timers.forEach((t) => window.clearInterval(t));
      pb.value?.realtime.unsubscribe();
    });

    return { loadError, status };
  },
});

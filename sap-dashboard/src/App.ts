import { computed, defineComponent, onMounted, onUnmounted, ref, shallowRef } from "vue";
import type PocketBase from "pocketbase";
import AdminDialog from "./components/AdminDialog.vue";
import BackfillList from "./components/BackfillList.vue";
import RacesTable from "./components/RacesTable.vue";
import SapCard from "./components/SapCard.vue";
import SettingsForm from "./components/SettingsForm.vue";
import SourcesCard from "./components/SourcesCard.vue";
import UplinkCard from "./components/UplinkCard.vue";
import { ageClass, ms, now } from "./format";
import { STATUS_ID, connect, isNotFound, message } from "./pocketbase";
import { buildRaces, raceKey } from "./races";
import { findSources, loadSources, type SourceHealth } from "./sources";
import type { Backfill, RaceUpload, UplinkStatus } from "./types";

export default defineComponent({
  components: { AdminDialog, BackfillList, RacesTable, SapCard, SettingsForm, SourcesCard, UplinkCard },
  setup() {
    const pb = shallowRef<PocketBase | null>(null);
    const loadError = ref("");
    const status = ref<UplinkStatus | null>(null);
    const uploads = ref<RaceUpload[]>([]);
    const backfills = ref<Backfill[]>([]);
    const sources = ref<SourceHealth[]>([]);
    const dialog = ref<"settings" | null>(null);

    const uplinkAlive = computed(() => ageClass(ms(status.value?.heartbeat_at), 60, 120) !== "bad");

    // A dead uplink's current races are stale
    const currentKeys = computed(() => {
      if (!uplinkAlive.value) return new Set<string>();
      return new Set((status.value?.current_races || []).map((r) => raceKey(r.regattaName, r.raceName)));
    });

    const races = computed(() => buildRaces(status.value, uploads.value, currentKeys.value, now.value));

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

    // Each flush of the uplink updates one record per race and source: reload once
    let uploadsTimer: number | undefined;
    function loadUploadsSoon(): Promise<void> {
      window.clearTimeout(uploadsTimer);
      uploadsTimer = window.setTimeout(() => loadUploads().catch(() => {}), 1000);
      return Promise.resolve();
    }

    async function loadUploads() {
      uploads.value = await pb.value!.collection("sap_race_uploads").getFullList<RaceUpload>({ requestKey: null });
    }

    async function loadBackfills() {
      const page = await pb.value!.collection("sap_backfills").getList<Backfill>(1, 10, { sort: "-created", requestKey: null });
      backfills.value = page.items;
    }

    async function loadMeasurements() {
      if (Date.now() - sourcesFoundAt > 300_000) {
        sourceIds = await findSources(pb.value!);
        sourcesFoundAt = Date.now();
      }
      sources.value = await loadSources(pb.value!, sourceIds, Date.now());
    }

    async function loadAll() {
      try {
        await Promise.all([loadStatus(), loadUploads(), loadBackfills(), loadMeasurements()]);
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
      subscribe("sap_race_uploads", loadUploadsSoon);
      subscribe("sap_backfills", loadBackfills);
      timers.push(window.setInterval(() => loadMeasurements().catch(() => {}), 5000));
      timers.push(window.setInterval(loadAll, 30000));
    });

    onUnmounted(() => {
      timers.forEach((t) => window.clearInterval(t));
      pb.value?.realtime.unsubscribe();
    });

    return { pb, loadError, status, backfills, sources, dialog, uplinkAlive, currentKeys, races };
  },
});

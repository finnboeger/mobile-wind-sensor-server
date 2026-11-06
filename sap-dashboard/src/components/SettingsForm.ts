import { defineComponent, onMounted, type PropType, reactive, ref } from "vue";
import type PocketBase from "pocketbase";
import { SETTINGS_ID, isNotFound, message } from "../pocketbase";

export default defineComponent({
  props: {
    pb: { type: Object as PropType<PocketBase>, required: true },
  },
  emits: {
    saved: () => true,
  },
  setup(props, { emit }) {
    const settings = reactive({ sap_url: "", access_token: "", event_id: "", course_area: "" });
    const showToken = ref(false);
    const error = ref("");
    const busy = ref(false);

    onMounted(async () => {
      try {
        const record = await props.pb.collection("sap_settings").getOne(SETTINGS_ID);
        for (const key of Object.keys(settings) as (keyof typeof settings)[]) settings[key] = record[key] || "";
      } catch (e) {
        if (!isNotFound(e)) error.value = `Could not load the settings: ${message(e)}`;
      }
    });

    async function save() {
      if (settings.sap_url && !/^https?:\/\//.test(settings.sap_url)) {
        error.value = "The SAP server must start with http:// or https://";
        return;
      }
      busy.value = true;
      error.value = "";
      const data = { ...settings, sap_url: settings.sap_url.replace(/\/+$/, "") };
      try {
        try {
          await props.pb.collection("sap_settings").update(SETTINGS_ID, data);
        } catch (e) {
          if (!isNotFound(e)) throw e;
          await props.pb.collection("sap_settings").create({ id: SETTINGS_ID, ...data });
        }
        emit("saved");
      } catch (e) {
        error.value = `Could not save: ${message(e)}`;
      } finally {
        busy.value = false;
      }
    }

    return { settings, showToken, error, busy, save };
  },
});

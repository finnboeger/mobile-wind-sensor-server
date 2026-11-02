import { defineComponent, type PropType, ref } from "vue";
import type PocketBase from "pocketbase";

// Shows the slot only to a logged-in PocketBase superuser, a login form otherwise
export default defineComponent({
  props: {
    pb: { type: Object as PropType<PocketBase>, required: true },
    title: { type: String, required: true },
  },
  emits: {
    close: () => true,
  },
  setup(props, { emit }) {
    const isAdmin = ref(props.pb.authStore.isValid && props.pb.authStore.isSuperuser);
    const email = ref("");
    const password = ref("");
    const error = ref("");
    const busy = ref(false);

    async function login() {
      busy.value = true;
      error.value = "";
      try {
        await props.pb.collection("_superusers").authWithPassword(email.value, password.value);
        password.value = "";
        isAdmin.value = true;
      } catch {
        error.value = "Login failed. Check email and password.";
      } finally {
        busy.value = false;
      }
    }

    function logout() {
      props.pb.authStore.clear();
      isAdmin.value = false;
    }

    return { emit, isAdmin, email, password, error, busy, login, logout };
  },
});

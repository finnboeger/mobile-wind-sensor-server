<template>
  <form class="dialog-form" @submit.prevent="save">
    <label>
      SAP server
      <input v-model.trim="settings.sap_url" placeholder="https://my-event.sapsailing.com" />
    </label>
    <label>
      Access token
      <span class="with-toggle">
        <input v-model.trim="settings.access_token" :type="showToken ? 'text' : 'password'" autocomplete="off" />
        <button type="button" class="btn small" @click="showToken = !showToken">{{ showToken ? "Hide" : "Show" }}</button>
      </span>
    </label>
    <label>
      Event id
      <input v-model.trim="settings.event_id" placeholder="from the event page URL" />
    </label>
    <label>
      Course area
      <input v-model.trim="settings.course_area" placeholder="all course areas" />
    </label>
    <p class="muted small">
      Empty fields use the uplink's environment (SAP_URL, SAP_BEARER_TOKEN, SAP_EVENT_ID, SAP_COURSE_AREA).
      The uplink picks up changes within about 10 seconds.
    </p>
    <div v-if="error" class="banner error small">{{ error }}</div>
    <div class="panel-foot"><button class="btn primary" :disabled="busy">Save</button></div>
  </form>
</template>

<script lang="ts" src="./SettingsForm.ts"></script>

<style scoped>
.with-toggle {
  display: flex;
  gap: 8px;
}
</style>

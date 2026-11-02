<template>
  <div class="card">
    <h2><span :class="['dot', level]"></span>Uplink service</h2>
    <p v-if="!status" class="muted">The uplink hasn't reported yet. Is the sap compose profile enabled?</p>
    <dl v-else>
      <dt>Last heartbeat</dt>
      <dd :class="ageClass(ms(status.heartbeat_at), 60, 120)">{{ ago(ms(status.heartbeat_at)) }}</dd>
      <dt>Settings</dt>
      <dd :class="status.configured ? '' : 'bad'">{{ status.configured ? "OK" : status.problem }}</dd>
      <dt>SAP server</dt>
      <dd>{{ status.sap_url || "—" }}</dd>
      <dt>Races</dt>
      <dd>{{ raceSelection }}</dd>
      <dt>Realtime</dt>
      <dd :class="status.realtime === 'disconnected' ? 'warn' : ''">{{ status.realtime || "—" }}</dd>
      <dt>Forwarded up to</dt>
      <dd>{{ status.cursor ? `${clock(ms(status.cursor))} (${ago(ms(status.cursor))})` : "—" }}</dd>
      <dt>Running since</dt>
      <dd>{{ clock(ms(status.started_at)) }}</dd>
    </dl>
  </div>
</template>

<script lang="ts" src="./UplinkCard.ts"></script>

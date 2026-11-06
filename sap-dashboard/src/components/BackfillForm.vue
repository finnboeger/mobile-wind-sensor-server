<template>
  <form class="dialog-form" @submit.prevent="submit">
    <label>
      Race
      <select v-model="raceKey">
        <option value="">Every tracked race of the event</option>
        <option v-for="r in races" :key="r.key" :value="r.key">{{ r.regattaName }} / {{ r.raceName }}</option>
      </select>
    </label>
    <label>From <input v-model="from" type="datetime-local" required /></label>
    <label>Until <input v-model="until" type="datetime-local" required /></label>
    <p class="muted small">
      {{ count === null ? "Counting measurements…" : `${count} measurements in this range.` }}
      SAP only keeps fixes inside the race's tracking interval and ignores fixes it already has, so a generous
      range is fine.
    </p>
    <div v-if="error" class="banner error small">{{ error }}</div>
    <div class="panel-foot"><button class="btn primary" :disabled="busy">Start backfill</button></div>
  </form>
</template>

<script lang="ts" src="./BackfillForm.ts"></script>

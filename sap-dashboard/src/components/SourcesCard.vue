<template>
  <div class="card">
    <h2><span :class="['dot', level]"></span>Wind data in PocketBase</h2>
    <p v-if="sources.length === 0" class="muted">No measurements yet.</p>
    <table v-else>
      <thead>
        <tr><th>Source</th><th>Latest fix</th><th>Arrived after</th><th>Latest sendable</th></tr>
      </thead>
      <tbody>
        <tr v-for="s in sources" :key="s.sourceId">
          <td>{{ s.sourceId }}</td>
          <td :class="ageClass(s.latestTsMs, 10, 60)">{{ ago(s.latestTsMs) }}</td>
          <td>{{ s.delayMs === null ? "—" : duration(s.delayMs) }}</td>
          <td :class="ageClass(s.sendableTsMs, 10, 60)" title="Newest fix with GPS position and true wind; SAP needs both">
            {{ s.sendableTsMs === null ? "none recently" : ago(s.sendableTsMs) }}
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script lang="ts" src="./SourcesCard.ts"></script>

<style scoped>
td,
th {
  padding-right: 12px;
}
</style>

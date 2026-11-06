<template>
  <div class="page">
    <header>
      <h1>SAP forwarding</h1>
      <button v-if="pb" class="icon-btn" @click="dialog = 'settings'" aria-label="SAP settings">⚙</button>
    </header>

    <div v-if="loadError" class="banner error">{{ loadError }}</div>

    <section class="cards">
      <SourcesCard :sources="sources" />
      <UplinkCard :status="status" :alive="uplinkAlive" />
      <SapCard :status="status" :has-current-race="currentKeys.size > 0" />
    </section>

    <RacesTable :races="races" backfill @backfill="openBackfill" />
    <BackfillList :backfills="backfills" />

    <AdminDialog v-if="pb && dialog === 'settings'" :pb="pb" title="SAP settings" @close="dialog = null">
      <SettingsForm :pb="pb" @saved="dialog = null" />
    </AdminDialog>
    <AdminDialog v-if="pb && dialog === 'backfill'" :pb="pb" title="Backfill" @close="dialog = null">
      <BackfillForm :pb="pb" :races="races" :race="backfillRace" @requested="dialog = null; loadBackfills()" />
    </AdminDialog>
  </div>
</template>

<script lang="ts" src="./App.ts"></script>

<style scoped>
.page {
  min-height: 100vh;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

h1 { font-size: 1.4rem; }

.icon-btn {
  background: none;
  border: none;
  font-size: 1.6rem;
  cursor: pointer;
  line-height: 1;
}

.cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
}
</style>

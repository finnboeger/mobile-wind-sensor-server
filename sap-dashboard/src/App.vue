<template>
  <div class="page">
    <header>
      <h1>SAP forwarding</h1>
    </header>

    <div v-if="loadError" class="banner error">{{ loadError }}</div>

    <section class="cards">
      <SourcesCard :sources="sources" />
      <UplinkCard :status="status" :alive="uplinkAlive" />
      <SapCard :status="status" :has-current-race="currentKeys.size > 0" />
    </section>

    <RacesTable :races="races" />
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

.cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
}
</style>

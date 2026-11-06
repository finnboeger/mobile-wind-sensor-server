<template>
  <div class="page">
    <header>
      <h1>SAP forwarding</h1>
    </header>

    <div v-if="loadError" class="banner error">{{ loadError }}</div>
    <pre v-else-if="status" class="small">{{ status }}</pre>
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
</style>

<template>
  <section class="card">
    <div class="head">
      <h2>Races</h2>
      <button v-if="backfill" class="btn" @click="emit('backfill', null)">Backfill…</button>
    </div>
    <p v-if="races.length === 0" class="muted">No race has received wind yet.</p>
    <div v-else class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Race</th>
            <th>State</th>
            <th>First accepted fix</th>
            <th>Last accepted fix</th>
            <th>Sources</th>
            <th v-if="backfill"></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in races" :key="r.key" :class="{ active: r.current }">
            <td>
              <div class="race-name">{{ r.raceName }}</div>
              <div class="muted small">{{ r.regattaName }}</div>
            </td>
            <td><span :class="['badge', r.state.level]">{{ r.state.label }}</span></td>
            <td>{{ dateTime(r.firstFixMs) }}</td>
            <td>
              {{ dateTime(r.lastFixMs) }}
              <div v-if="r.lastFixMs !== null" class="muted small">{{ ago(r.lastFixMs) }}</div>
            </td>
            <td class="small">
              <div v-for="s in r.sources" :key="s">{{ s }}</div>
            </td>
            <td v-if="backfill"><button class="btn small" @click="emit('backfill', r)">Backfill</button></td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script lang="ts" src="./RacesTable.ts"></script>

<style scoped>
.head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.head h2 { margin: 0; }

tr.active td { background: #f1fbf4; }

.race-name { font-weight: 600; min-width: 110px; }
</style>

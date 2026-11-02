<template>
  <section v-if="backfills.length" class="card">
    <h2>Backfills</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Requested</th><th>Race</th><th>Range</th><th>State</th><th>Sent / accepted</th></tr>
        </thead>
        <tbody>
          <tr v-for="b in backfills" :key="b.id">
            <td>{{ dateTime(ms(b.created)) }}</td>
            <td>{{ b.race_name ? `${b.regatta_name} / ${b.race_name}` : "Every tracked race" }}</td>
            <td>{{ dateTime(ms(b.range_from)) }} – {{ dateTime(ms(b.range_until)) }}</td>
            <td>
              <span :class="['badge', LEVELS[b.status] || '']">{{ b.status }}</span>
              <div v-if="b.message" class="muted small">{{ b.message }}</div>
            </td>
            <td>{{ b.fixes_sent ?? 0 }} / {{ b.fixes_accepted ?? 0 }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script lang="ts" src="./BackfillList.ts"></script>

<template>
  <div class="card">
    <h2><span :class="['dot', level]"></span>SAP</h2>
    <p v-if="!status" class="muted">—</p>
    <template v-else>
      <dl>
        <dt>Last accepted</dt>
        <dd :class="ageClass(ms(status.last_accepted_at), 30, 300)">{{ ago(ms(status.last_accepted_at)) }}</dd>
        <dt>Last answer</dt>
        <dd>{{ ago(ms(status.last_sent_at)) }}</dd>
        <dt>Sent / accepted</dt>
        <dd>{{ status.stats?.fixes_sent ?? 0 }} / {{ status.stats?.fixes_accepted ?? 0 }} since start</dd>
        <dt>Skipped</dt>
        <dd>
          {{ status.stats?.records_skipped ?? 0 }} without position or true wind,
          {{ status.stats?.batches_rejected ?? 0 }} batches refused
        </dd>
      </dl>
      <div v-if="currentError" class="banner error small">
        {{ clock(ms(status.last_error_at)) }}: {{ status.last_error }}
      </div>
      <div v-if="notAccepted" class="banner warn small">
        SAP answers, but no current race takes the fixes. Check that the access token is valid and may update the
        race, and that the race's tracking interval covers now.
      </div>
    </template>
  </div>
</template>

<script lang="ts" src="./SapCard.ts"></script>

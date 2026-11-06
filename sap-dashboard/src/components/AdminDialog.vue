<template>
  <div class="overlay" @click.self="emit('close')">
    <div class="panel">
      <div class="panel-head">
        <h2>{{ isAdmin ? title : "Log in" }}</h2>
        <button v-if="isAdmin" class="btn link" @click="logout">Log out</button>
        <button class="icon-btn" @click="emit('close')" aria-label="Close">✕</button>
      </div>

      <slot v-if="isAdmin" />
      <form v-else class="dialog-form" @submit.prevent="login">
        <p class="muted small">Log in with a PocketBase superuser.</p>
        <label>Email <input v-model="email" type="email" autocomplete="username" required /></label>
        <label>Password <input v-model="password" type="password" autocomplete="current-password" required /></label>
        <div v-if="error" class="banner error small">{{ error }}</div>
        <div class="panel-foot"><button class="btn primary" :disabled="busy">Log in</button></div>
      </form>
    </div>
  </div>
</template>

<script lang="ts" src="./AdminDialog.ts"></script>

<style scoped>
.overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.45);
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
}

.panel {
  background: #fff;
  border-radius: 14px;
  padding: 24px;
  width: 100%;
  max-width: 440px;
  box-shadow: 0 16px 48px rgba(0, 0, 0, 0.2);
}

.panel-head {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
}

.panel-head h2 {
  font-size: 1.2rem;
  margin-right: auto;
}

.icon-btn {
  background: none;
  border: none;
  font-size: 1.1rem;
  color: #666;
  cursor: pointer;
}
</style>

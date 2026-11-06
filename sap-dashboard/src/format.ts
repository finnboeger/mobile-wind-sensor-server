import { ref } from "vue";

// One clock for every age shown on the page
export const now = ref(Date.now());
window.setInterval(() => (now.value = Date.now()), 1000);

// PocketBase dates look like "2026-10-28 12:00:00.000Z"
export function ms(value?: string): number | null {
  if (!value) return null;
  const t = new Date(value.replace(" ", "T")).getTime();
  return Number.isFinite(t) ? t : null;
}

export function duration(deltaMs: number): string {
  const s = Math.max(0, Math.round(deltaMs / 1000));
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} min ${s % 60} s`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} min`;
  return `${Math.floor(s / 86400)} d ${Math.floor((s % 86400) / 3600)} h`;
}

export function ago(t: number | null): string {
  return t === null ? "never" : `${duration(now.value - t)} ago`;
}

export function clock(t: number | null): string {
  return t === null ? "—" : new Date(t).toLocaleTimeString();
}

export function dateTime(t: number | null): string {
  if (t === null) return "—";
  const d = new Date(t);
  const sameDay = d.toDateString() === new Date(now.value).toDateString();
  return sameDay ? d.toLocaleTimeString() : d.toLocaleString();
}

// "ok" up to okSeconds old, "warn" up to warnSeconds, "bad" beyond or never
export function ageClass(t: number | null, okSeconds: number, warnSeconds: number): string {
  if (t === null) return "bad";
  const age = (now.value - t) / 1000;
  return age <= okSeconds ? "ok" : age <= warnSeconds ? "warn" : "bad";
}

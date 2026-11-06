import PocketBase from "pocketbase";

// Fixed record ids, shared with sap-uplink/src/pocketbase_store.py
export const STATUS_ID = "sapuplinkstatus";
export const SETTINGS_ID = "sapuplinkconfig";

export async function connect(): Promise<PocketBase> {
  // Written by the container's entrypoint from POCKETBASE_PUBLIC_URL
  const config = await (await fetch("./config.json")).json();
  return new PocketBase(config.pocketbaseUrl);
}

export function isNotFound(e: unknown): boolean {
  return (e as { status?: number })?.status === 404;
}

export function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// PocketBase stores and filters dates as "YYYY-MM-DD HH:MM:SS.sssZ"
export function pbDate(t: number): string {
  return new Date(t).toISOString().replace("T", " ");
}

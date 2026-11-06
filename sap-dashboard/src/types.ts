export interface Race {
  regattaName: string;
  raceName: string;
  finished?: boolean;
}

// The uplink's status record (sap-uplink/src/status.py)
export interface UplinkStatus {
  started_at?: string;
  heartbeat_at?: string;
  configured?: boolean;
  problem?: string;
  sap_url?: string;
  event_id?: string;
  course_area?: string;
  realtime?: string;
  cursor?: string;
  last_sent_at?: string;
  last_accepted_at?: string;
  last_error_at?: string;
  last_error?: string;
  current_races?: Race[];
  all_races?: boolean;
  tracked_races?: Race[];
  stats?: Record<string, number>;
}

export interface RaceUpload {
  regatta_name: string;
  race_name: string;
  source_id: string;
  first_fix: string;
  last_fix: string;
  fixes_accepted: number;
  last_accepted_at: string;
}

export interface Backfill {
  id: string;
  regatta_name: string;
  race_name: string;
  range_from: string;
  range_until: string;
  status: string;
  message: string;
  fixes_sent: number;
  fixes_accepted: number;
  created: string;
}

export interface Measurement {
  source_id: string;
  ts: string;
  created_at: string;
  gps_lat?: number;
  gps_lng?: number;
  true_wind_dir_deg?: number | null;
  true_wind_speed_mps?: number | null;
}

// Mirrors the Go API schema (api/openapi.yaml -> Earthquake, EarthquakePage).

export interface Earthquake {
  id: string;
  occurred_at: string;
  updated_at?: string;
  latitude: number;
  longitude: number;
  depth_km: number | null;
  magnitude: number | null;
  magnitude_type: string | null;
  place: string | null;
  status: string | null;
  event_type: string | null;
  alert_level: string | null;
  tsunami: boolean | null;
  significance: number | null;
  source: string;
  source_url?: string | null; // USGS event page
  version: number;
  distance_km?: number | null;
}

export interface EarthquakePage {
  data: Earthquake[];
  pagination: { next_cursor: string | null };
}

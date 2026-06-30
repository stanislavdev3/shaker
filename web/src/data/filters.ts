// Filter state lives in the URL. This module is the single source of truth for
// translating between URLSearchParams and the API query params, so it can be
// unit-tested without React.

export interface Filters {
  minMagnitude: string; // "" = all
  minDepth: string;
  maxDepth: string;
  from: string; // ISO datetime or relative window key (see WINDOWS)
  tsunami: boolean;
  alertLevel: string; // "" = any
}

export const EMPTY_FILTERS: Filters = {
  minMagnitude: "",
  minDepth: "",
  maxDepth: "",
  from: "",
  tsunami: false,
  alertLevel: "",
};

// Relative time windows offered in the UI. Value is hours back from now.
export const WINDOWS: Record<string, number> = {
  "1h": 1,
  "24h": 24,
  "7d": 24 * 7,
  "30d": 24 * 30,
};

export function filtersFromSearch(search: URLSearchParams): Filters {
  return {
    minMagnitude: search.get("min_mag") ?? "",
    minDepth: search.get("min_depth") ?? "",
    maxDepth: search.get("max_depth") ?? "",
    from: search.get("from") ?? "",
    tsunami: search.get("tsunami") === "1",
    alertLevel: search.get("alert") ?? "",
  };
}

// Only non-default values are written, keeping shareable URLs clean.
export function filtersToSearch(f: Filters, search: URLSearchParams): URLSearchParams {
  const out = new URLSearchParams(search);
  const set = (key: string, value: string) =>
    value ? out.set(key, value) : out.delete(key);
  set("min_mag", f.minMagnitude);
  set("min_depth", f.minDepth);
  set("max_depth", f.maxDepth);
  set("from", f.from);
  set("alert", f.alertLevel);
  if (f.tsunami) out.set("tsunami", "1");
  else out.delete("tsunami");
  return out;
}

// Translates UI filters into the backend's query parameters.
// `now` is injected so the relative-window math is deterministic in tests.
export function filtersToApiParams(f: Filters, now: Date): Record<string, string> {
  const params: Record<string, string> = { limit: "100", sort: "occurred_at_desc" };
  if (f.minMagnitude) params.min_magnitude = f.minMagnitude;
  if (f.minDepth) params.min_depth_km = f.minDepth;
  if (f.maxDepth) params.max_depth_km = f.maxDepth;
  if (f.tsunami) params.tsunami = "true";
  if (f.alertLevel) params.alert_level = f.alertLevel;
  if (f.from) {
    const hours = WINDOWS[f.from];
    params.from = hours
      ? new Date(now.getTime() - hours * 3600_000).toISOString()
      : f.from;
  }
  return params;
}

// The stream broadcasts every earthquake change regardless of filters, so a
// live event must be checked client-side before it enters a filtered list.
// `now` is injected for deterministic tests.
export function matchesFilters(
  e: {
    magnitude: number | null;
    depth_km: number | null;
    tsunami: boolean | null;
    alert_level: string | null;
    occurred_at: string;
  },
  f: Filters,
  now: Date,
): boolean {
  if (f.minMagnitude && (e.magnitude ?? -Infinity) < Number(f.minMagnitude)) return false;
  if (f.minDepth && (e.depth_km ?? -Infinity) < Number(f.minDepth)) return false;
  if (f.maxDepth && (e.depth_km ?? Infinity) > Number(f.maxDepth)) return false;
  if (f.tsunami && !e.tsunami) return false;
  if (f.alertLevel && e.alert_level !== f.alertLevel) return false;
  if (f.from) {
    const hours = WINDOWS[f.from];
    const floor = hours ? now.getTime() - hours * 3600_000 : Date.parse(f.from);
    if (Date.parse(e.occurred_at) < floor) return false;
  }
  return true;
}

// Stable key for TanStack Query. Excludes relative-window resolution so the
// cache key doesn't churn every second; the resolved `from` is computed in the
// query fn instead.
export function queryKeyParams(f: Filters): Record<string, string> {
  const params: Record<string, string> = {};
  if (f.minMagnitude) params.min_magnitude = f.minMagnitude;
  if (f.minDepth) params.min_depth_km = f.minDepth;
  if (f.maxDepth) params.max_depth_km = f.maxDepth;
  if (f.tsunami) params.tsunami = "true";
  if (f.alertLevel) params.alert_level = f.alertLevel;
  if (f.from) params.from = f.from;
  return params;
}

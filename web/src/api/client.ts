import type { Earthquake, EarthquakePage } from "./types";

// RFC 9457 problem details surface a `detail` string; fall back to status text.
async function request<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { signal, headers: { Accept: "application/json" } });
  const body = res.status === 204 ? null : await res.json().catch(() => null);
  if (!res.ok) {
    const detail = body && typeof body === "object" ? body.detail : null;
    throw new Error(detail || `HTTP ${res.status}`);
  }
  return body as T;
}

export function fetchEarthquakes(
  params: Record<string, string>,
  cursor: string | null,
  signal?: AbortSignal,
): Promise<EarthquakePage> {
  const query = new URLSearchParams(params);
  if (cursor) query.set("cursor", cursor);
  return request<EarthquakePage>(`/v1/earthquakes?${query.toString()}`, signal);
}

export async function fetchEarthquake(id: string, signal?: AbortSignal): Promise<Earthquake> {
  const body = await request<{ data: Earthquake }>(`/v1/earthquakes/${id}`, signal);
  return body.data;
}

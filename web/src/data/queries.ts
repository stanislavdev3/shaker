import {
  useInfiniteQuery,
  useQuery,
  type InfiniteData,
  type QueryClient,
} from "@tanstack/react-query";
import { fetchEarthquake, fetchEarthquakes } from "../api/client";
import type { Earthquake, EarthquakePage } from "../api/types";
import { filtersToApiParams, queryKeyParams, type Filters } from "./filters";
import { upsert } from "./realtime";

type Pages = InfiniteData<EarthquakePage, string | null>;

export const earthquakesKey = (f: Filters) =>
  ["earthquakes", queryKeyParams(f)] as const;

export function useEarthquakes(filters: Filters) {
  return useInfiniteQuery({
    queryKey: earthquakesKey(filters),
    initialPageParam: null as string | null,
    queryFn: ({ pageParam, signal }) =>
      // `now` resolved per fetch so relative windows stay current without
      // churning the cache key.
      fetchEarthquakes(filtersToApiParams(filters, new Date()), pageParam, signal),
    getNextPageParam: (last) => last.pagination.next_cursor,
    staleTime: 30_000,
  });
}

// Flattened, deduped view of all loaded pages.
export function flattenPages(
  pages: { data: Earthquake[] }[] | undefined,
): Earthquake[] {
  if (!pages) return [];
  const seen = new Map<string, Earthquake>();
  for (const page of pages) for (const e of page.data) seen.set(e.id, e);
  return [...seen.values()];
}

export function useEarthquake(id: string | undefined) {
  return useQuery({
    queryKey: ["earthquake", id],
    queryFn: ({ signal }) => fetchEarthquake(id!, signal),
    enabled: !!id,
  });
}

// Upsert a live earthquake into the infinite-query cache: update in place if it
// already exists on any page, otherwise prepend to the first (newest) page.
export function applyLiveChange(prev: Pages | undefined, eq: Earthquake): Pages | undefined {
  if (!prev || prev.pages.length === 0) return prev;
  if (prev.pages.some((p) => p.data.some((e) => e.id === eq.id))) {
    return {
      ...prev,
      pages: prev.pages.map((p) =>
        p.data.some((e) => e.id === eq.id) ? { ...p, data: upsert(p.data, eq) } : p,
      ),
    };
  }
  const [first, ...rest] = prev.pages;
  return { ...prev, pages: [{ ...first, data: upsert(first.data, eq) }, ...rest] };
}

export function writeLiveChange(
  client: QueryClient,
  filters: Filters,
  eq: Earthquake,
): void {
  client.setQueryData<Pages>(earthquakesKey(filters), (prev) => applyLiveChange(prev, eq));
}

// Called by the realtime controller on reconnect: socket history is best-effort,
// so the list is the source of truth and must be refetched.
export function resyncEarthquakes(client: QueryClient): void {
  void client.invalidateQueries({ queryKey: ["earthquakes"] });
}

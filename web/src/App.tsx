import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import type { Earthquake } from "./api/types";
import {
  filtersFromSearch,
  filtersToSearch,
  matchesFilters,
  queryKeyParams,
  type Filters as FiltersT,
} from "./data/filters";
import {
  flattenPages,
  resyncEarthquakes,
  useEarthquakes,
  writeLiveChange,
} from "./data/queries";
import {
  RealtimeController,
  shouldBuffer,
  streamUrl,
  upsert,
  type ConnState,
} from "./data/realtime";
import { ConnectionBadge } from "./ui/ConnectionBadge";
import { DetailDrawer } from "./ui/DetailDrawer";
import { EventFeed } from "./ui/EventFeed";
import { Filters } from "./ui/Filters";
import { MapView } from "./ui/MapView";
import { Topbar } from "./ui/Topbar";

export function App() {
  const client = useQueryClient();
  const [search, setSearch] = useSearchParams();
  const filters = useMemo(() => filtersFromSearch(search), [search]);
  const selectedId = search.get("event");

  const query = useEarthquakes(filters);
  const events = useMemo(() => flattenPages(query.data?.pages), [query.data]);
  const selected = useMemo(
    () => events.find((e) => e.id === selectedId) ?? null,
    [events, selectedId],
  );

  const [connState, setConnState] = useState<ConnState>("reconnecting");
  const [buffered, setBuffered] = useState<Earthquake[]>([]);
  const [sheetOpen, setSheetOpen] = useState(false);

  // Refs hold the latest values for the long-lived realtime handlers.
  const atTopRef = useRef(true);
  const filtersRef = useRef(filters);
  filtersRef.current = filters;
  const idsRef = useRef<Set<string>>(new Set());
  idsRef.current = useMemo(() => new Set(events.map((e) => e.id)), [events]);

  // Clear buffered events when the filter set changes — they may no longer match.
  const filterKey = JSON.stringify(queryKeyParams(filters));
  useEffect(() => setBuffered([]), [filterKey]);

  // One realtime controller for the app's lifetime.
  useEffect(() => {
    const controller = new RealtimeController(streamUrl(), {
      onState: setConnState,
      onResync: () => {
        setBuffered([]);
        resyncEarthquakes(client);
      },
      onChange: (eq) => {
        if (!matchesFilters(eq, filtersRef.current, new Date())) return;
        if (shouldBuffer(eq, idsRef.current, atTopRef.current)) {
          setBuffered((prev) => upsert(prev, eq));
        } else {
          writeLiveChange(client, filtersRef.current, eq);
        }
      },
    });
    controller.start();
    return () => controller.stop();
  }, [client]);

  const updateFilters = useCallback(
    (next: FiltersT) => setSearch(filtersToSearch(next, search), { replace: true }),
    [search, setSearch],
  );

  const select = useCallback(
    (id: string) => {
      const next = new URLSearchParams(search);
      next.set("event", id);
      setSearch(next, { replace: true });
    },
    [search, setSearch],
  );

  const closeDetail = useCallback(() => {
    const next = new URLSearchParams(search);
    next.delete("event");
    setSearch(next, { replace: true });
  }, [search, setSearch]);

  const applyBuffer = useCallback(() => {
    buffered.forEach((eq) => writeLiveChange(client, filtersRef.current, eq));
    setBuffered([]);
  }, [buffered, client]);

  return (
    <div className="app">
      <Topbar>
        <span className="source-label">USGS DATA</span>
      </Topbar>

      <div className="workspace">
        <MapView events={events} selected={selected} onSelect={select} />

        <aside className={`feed-panel ${sheetOpen ? "open" : ""}`}>
          <div className="feed-head sheet-toggle" onClick={() => setSheetOpen((o) => !o)}>
            <div>
              <p className="eyebrow">LATEST REPORTS</p>
              <h2>Event feed</h2>
            </div>
            <ConnectionBadge state={connState} />
          </div>

          <Filters filters={filters} onChange={updateFilters} />

          <EventFeed
            events={events}
            selectedId={selectedId}
            onSelect={select}
            bufferedCount={buffered.length}
            onApplyBuffer={applyBuffer}
            hasNextPage={!!query.hasNextPage}
            isFetchingNextPage={query.isFetchingNextPage}
            onLoadMore={() => query.fetchNextPage()}
            isError={query.isError}
            errorMessage={(query.error as Error)?.message}
            onAtTopChange={(atTop) => (atTopRef.current = atTop)}
          />
        </aside>
      </div>

      {selectedId && <DetailDrawer id={selectedId} onClose={closeDetail} />}
    </div>
  );
}

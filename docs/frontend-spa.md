# Frontend SPA Design

Standalone single-page app for the public earthquake map. No SSR — for a realtime
panel it adds no value. Replaces the embedded `internal/httpapi/web/{index.html,app.js,styles.css}`.

## Stack

- React + TypeScript + Vite.
- MapLibre GL JS — WebGL map, clustering, heatmap, globe. One GeoJSON source, no per-event DOM markers.
- TanStack Query — HTTP cache, cursor pagination, retries, reconnect resync.
- React Router — two routes (`/` map, `/event/:id` standalone page).
- WebSocket = change signal only. HTTP API is the source of truth.
- Basemap: MapLibre demo tiles (`demotiles.maplibre.org`) — free, vector, globe, no key.
  Swapping to a keyed provider later is a one-line style-URL change in `MapView`.

## Layout (`web/`)

Vite project at repo root `web/`. Builds into `internal/httpapi/web/dist/`, embedded by Go.

Three layers, each independently testable:

- **`api/`** — typed fetch client + TS types mirroring `Earthquake` / `EarthquakePage`.
  Knows the HTTP contract, nothing about React.
- **`data/`** — TanStack Query hooks (`useEarthquakes` infinite/cursor query,
  `useEarthquake(id)`) + a realtime WebSocket controller (plain class/store, not a
  hook-per-frame). Only this layer talks to `api/` and the socket.
- **`ui/`** — `MapView`, `EventFeed`, `Filters`, `DetailDrawer`, `EventPage`,
  `ConnectionBadge`, `NewEventsPill`, desktop-split / mobile-bottom-sheet shells.

App state that must survive reload or be shareable (filters, selected event id) lives
in the **URL**. Everything else is Query cache or local component state. No Redux/Zustand.

## Build & embed

- Dev: `vite dev` on :5173, proxies `/v1` → `:8080`.
- `make frontend`: `npm ci && vite build` → `internal/httpapi/web/dist/`.
- Go `//go:embed web/dist` serves it.
- **Backend change**: `frontendHandler` gains SPA fallback — unmatched non-`/v1`,
  non-asset GET serves `index.html` (today `http.FileServer` 404s `/event/:id` deep links).
- `dist/` gitignored except a committed placeholder `index.html` so `go build` compiles
  without node. `make build` and the Dockerfile gain a node stage before `go build`.

## Map

One GeoJSON source from the Query cache. Three layers:

- clustered circles (cluster / cluster-count / unclustered-point),
- circle radius + color by magnitude,
- heatmap at low zoom, faded out as you zoom in.

Globe projection on. React writes via `map.getSource().setData()` on data change — never
per frame. Hover popup; click selects event (updates URL), syncs feed + drawer.
`prefers-reduced-motion` disables fly-to and pulse.

## Event feed + filters

- Filters: magnitude, depth, time window, tsunami, alert → URL search params → Query key.
- Cursor pagination via `useInfiniteQuery` on the API's `next_cursor`.
- Selected event highlighted, two-way synced with the map.

## Realtime UX

WebSocket controller on `/v1/stream`. `earthquake_changed` already carries the full
earthquake (with `id`, `version`).

- Feed scrolled to top → prepend live.
- Otherwise → buffer, show **"N new events"** pill, apply on click. Never reshuffle under
  the user's cursor.
- `notification_changed` → ignored by public UI.
- States: **Live / Reconnecting / Offline**.
- On reconnect → `invalidateQueries` refetch. Socket is best-effort; HTTP is truth.
- Dedupe by `id` + `version`.

No backend WS change: current payload already exceeds the id+version+type minimum.

## Detail drawer + event page

Click → drawer (desktop): coords, depth, magnitude, source, status/tsunami/alert
indicators, version + updated_at, USGS link, mini region map. `/event/:id` renders the
same content full-page (shareable, reuses `useEarthquake`).

## Mobile

Same data layer; layout via media query. Full-screen map, feed as a bottom sheet
(native `<dialog>`, no heavy lib), filters in a slide-over panel.

## Style & a11y

Port the current dark palette (`:root` CSS tokens) so it looks continuous. Keyboard-
navigable feed/drawer, focus management on drawer open/close, `prefers-reduced-motion`.

## Testing

Vitest units: cursor/filter→query-key mapping, realtime buffer ("top vs not-top",
dedupe by id+version), URL state round-trip. WebGL map rendering: manual verification.
Backend SPA-fallback: one Go test.

## Deferred

`/admin` notification UI (separate future app), keyed vector tiles, offline tile caching, i18n.

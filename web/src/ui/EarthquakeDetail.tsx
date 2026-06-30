import type { Earthquake } from "../api/types";
import { formatDepth, formatMagnitude, relativeTime } from "../util/format";
import { MiniMap } from "./MiniMap";

// Prefer the exact USGS event page (source_url); fall back to a region map for
// the rare row without one.
function usgsLink(e: Earthquake): string {
  if (e.source_url) return e.source_url;
  const pad = 4;
  const extent = `${e.latitude - pad},${e.longitude - pad},${e.latitude + pad},${e.longitude + pad}`;
  return `https://earthquake.usgs.gov/earthquakes/map/?extent=${extent}`;
}

export function EarthquakeDetail({ e }: { e: Earthquake }) {
  return (
    <>
      <p className="eyebrow">{e.source.toUpperCase()} EVENT</p>
      <h1>M {formatMagnitude(e.magnitude)} · {e.place ?? "Unknown location"}</h1>

      <div className="badges">
        {e.tsunami && <span className="badge tsunami">Tsunami</span>}
        {e.alert_level && <span className="badge alert">Alert: {e.alert_level}</span>}
        {e.status && <span className="badge">{e.status}</span>}
        {e.event_type && <span className="badge">{e.event_type}</span>}
      </div>

      <dl className="facts">
        <div><dt>Magnitude</dt><dd>{formatMagnitude(e.magnitude)} {e.magnitude_type ?? ""}</dd></div>
        <div><dt>Depth</dt><dd>{formatDepth(e.depth_km)}</dd></div>
        <div><dt>Latitude</dt><dd>{e.latitude.toFixed(3)}</dd></div>
        <div><dt>Longitude</dt><dd>{e.longitude.toFixed(3)}</dd></div>
        <div><dt>Occurred</dt><dd>{relativeTime(e.occurred_at)}</dd></div>
        <div><dt>Source</dt><dd>{e.source}</dd></div>
        <div><dt>Version</dt><dd>{e.version}</dd></div>
        <div><dt>Updated</dt><dd>{e.updated_at ? relativeTime(e.updated_at) : "—"}</dd></div>
      </dl>

      <MiniMap lat={e.latitude} lon={e.longitude} />

      <a className="usgs" href={usgsLink(e)} target="_blank" rel="noreferrer">
        {e.source_url ? "View on USGS ↗" : "View region on USGS ↗"}
      </a>
    </>
  );
}

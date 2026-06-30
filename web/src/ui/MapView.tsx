import { useEffect, useRef } from "react";
import maplibregl, {
  type GeoJSONSource,
  type MapGeoJSONFeature,
} from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import type { Earthquake } from "../api/types";

// Demo tiles: free, vector, globe-capable, no API key. Swap this one URL for a
// keyed provider in production.
const STYLE_URL = "https://demotiles.maplibre.org/style.json";
const SOURCE = "quakes";

const reducedMotion =
  typeof matchMedia !== "undefined" &&
  matchMedia("(prefers-reduced-motion: reduce)").matches;

function toFeatureCollection(events: Earthquake[]): GeoJSON.FeatureCollection {
  return {
    type: "FeatureCollection",
    features: events.map((e) => ({
      type: "Feature",
      id: undefined,
      geometry: { type: "Point", coordinates: [e.longitude, e.latitude] },
      properties: {
        id: e.id,
        mag: e.magnitude ?? 0,
        place: e.place ?? "Unknown location",
      },
    })),
  };
}

interface Props {
  events: Earthquake[];
  selected: Earthquake | null;
  onSelect: (id: string) => void;
}

export function MapView({ events, selected, onSelect }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const readyRef = useRef(false);
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;
  // Latest events, read by refreshData so the map-load handler and the data
  // effect never set stale (empty) data when load and the first fetch race.
  const eventsRef = useRef(events);
  eventsRef.current = events;

  // Create the map exactly once.
  useEffect(() => {
    if (!containerRef.current) return;
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: STYLE_URL,
      center: [0, 20],
      zoom: 1.4,
      attributionControl: { compact: true },
    });
    mapRef.current = map;
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

    map.on("load", () => {
      map.setProjection({ type: "globe" });
      map.addSource(SOURCE, {
        type: "geojson",
        data: toFeatureCollection(events),
        cluster: true,
        clusterRadius: 50,
        clusterMaxZoom: 6,
      });

      // Density heatmap, visible only when zoomed out.
      map.addLayer({
        id: "quakes-heat",
        type: "heatmap",
        source: SOURCE,
        maxzoom: 4,
        paint: {
          "heatmap-weight": ["interpolate", ["linear"], ["get", "mag"], 0, 0, 8, 1],
          "heatmap-intensity": 0.8,
          "heatmap-opacity": ["interpolate", ["linear"], ["zoom"], 0, 0.7, 4, 0],
          "heatmap-color": [
            "interpolate", ["linear"], ["heatmap-density"],
            0, "rgba(88,214,223,0)",
            0.3, "rgba(88,214,223,0.5)",
            0.6, "rgba(185,231,111,0.7)",
            1, "rgba(255,111,125,0.9)",
          ],
        },
      });

      // Clusters.
      map.addLayer({
        id: "quakes-cluster",
        type: "circle",
        source: SOURCE,
        filter: ["has", "point_count"],
        paint: {
          "circle-color": "rgba(88,214,223,0.25)",
          "circle-stroke-color": "#58d6df",
          "circle-stroke-width": 1,
          "circle-radius": ["interpolate", ["linear"], ["get", "point_count"], 2, 14, 100, 34],
        },
      });
      map.addLayer({
        id: "quakes-cluster-count",
        type: "symbol",
        source: SOURCE,
        filter: ["has", "point_count"],
        layout: { "text-field": ["get", "point_count_abbreviated"], "text-size": 12 },
        paint: { "text-color": "#edf6fb" },
      });

      // Individual quakes: radius + color by magnitude.
      map.addLayer({
        id: "quakes-point",
        type: "circle",
        source: SOURCE,
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-radius": ["interpolate", ["linear"], ["get", "mag"], 0, 4, 8, 22],
          "circle-color": [
            "interpolate", ["linear"], ["get", "mag"],
            0, "#58d6df", 3, "#b9e76f", 5, "#ff9b62", 6, "#ff6f7d",
          ],
          "circle-opacity": 0.85,
          "circle-stroke-width": 1,
          "circle-stroke-color": "rgba(7,17,31,0.7)",
        },
      });

      readyRef.current = true;
      refreshData();

      const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12 });
      map.on("mouseenter", "quakes-point", (e) => {
        map.getCanvas().style.cursor = "pointer";
        const f = e.features?.[0] as MapGeoJSONFeature | undefined;
        if (!f) return;
        const p = f.properties as { mag: number; place: string };
        popup
          .setLngLat(e.lngLat)
          .setHTML(`<strong>M ${Number(p.mag).toFixed(1)}</strong><br>${escapeHtml(p.place)}`)
          .addTo(map);
      });
      map.on("mouseleave", "quakes-point", () => {
        map.getCanvas().style.cursor = "";
        popup.remove();
      });
      map.on("click", "quakes-point", (e) => {
        const f = e.features?.[0];
        const id = f?.properties?.id as string | undefined;
        if (id) onSelectRef.current(id);
      });
      map.on("click", "quakes-cluster", (e) => {
        const f = map.queryRenderedFeatures(e.point, { layers: ["quakes-cluster"] })[0];
        const clusterId = f?.properties?.cluster_id;
        const src = map.getSource(SOURCE) as GeoJSONSource;
        if (clusterId == null) return;
        void src.getClusterExpansionZoom(clusterId).then((zoom) => {
          map.easeTo({ center: (f.geometry as GeoJSON.Point).coordinates as [number, number], zoom });
        });
      });
    });

    return () => {
      map.remove();
      mapRef.current = null;
      readyRef.current = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function refreshData() {
    const src = mapRef.current?.getSource(SOURCE) as GeoJSONSource | undefined;
    src?.setData(toFeatureCollection(eventsRef.current));
  }

  // Push data updates to the source — React never re-renders the map itself.
  useEffect(() => {
    if (readyRef.current) refreshData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events]);

  // Fly to the selected event.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !selected) return;
    const target: [number, number] = [selected.longitude, selected.latitude];
    if (reducedMotion) map.jumpTo({ center: target, zoom: Math.max(map.getZoom(), 4) });
    else map.flyTo({ center: target, zoom: Math.max(map.getZoom(), 4), speed: 0.8 });
  }, [selected]);

  return (
    <div className="map-wrap">
      <div className="map-canvas" ref={containerRef} />
      <div className="map-legend" aria-hidden="true">
        Magnitude
        <div className="row"><span className="dot" style={{ background: "#b9e76f" }} /> &lt; 5</div>
        <div className="row"><span className="dot" style={{ background: "#ff9b62" }} /> 5–6</div>
        <div className="row"><span className="dot" style={{ background: "#ff6f7d" }} /> 6+</div>
      </div>
    </div>
  );
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!,
  );
}

import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";

const STYLE_URL = "https://demotiles.maplibre.org/style.json";

// Small static locator map for the detail views. Independent of the main map.
export function MiniMap({ lat, lon }: { lat: number; lon: number }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    const map = new maplibregl.Map({
      container: ref.current,
      style: STYLE_URL,
      center: [lon, lat],
      zoom: 4,
      interactive: false,
      attributionControl: false,
    });
    map.on("load", () => {
      new maplibregl.Marker({ color: "#ff6f7d" }).setLngLat([lon, lat]).addTo(map);
    });
    return () => map.remove();
  }, [lat, lon]);
  return <div className="mini-map" ref={ref} aria-hidden="true" />;
}

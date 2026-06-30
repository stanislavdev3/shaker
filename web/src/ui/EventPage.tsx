import { Link, useParams } from "react-router-dom";
import { useEarthquake } from "../data/queries";
import { EarthquakeDetail } from "./EarthquakeDetail";
import { Topbar } from "./Topbar";

export function EventPage() {
  const { id } = useParams<{ id: string }>();
  const { data, isLoading, isError, error } = useEarthquake(id);

  return (
    <div className="app">
      <Topbar />
      <div className="detail" style={{ maxWidth: 720, margin: "0 auto", width: "100%" }}>
        <Link to="/" className="eyebrow">← Back to map</Link>
        {isLoading && <p style={{ color: "var(--muted)" }}>Loading…</p>}
        {isError && <p style={{ color: "var(--red)" }}>{(error as Error)?.message || "Failed to load"}</p>}
        {data && <EarthquakeDetail e={data} />}
      </div>
    </div>
  );
}

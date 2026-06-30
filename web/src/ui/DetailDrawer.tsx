import { useEffect, useRef } from "react";
import { Link } from "react-router-dom";
import { useEarthquake } from "../data/queries";
import { EarthquakeDetail } from "./EarthquakeDetail";

interface Props {
  id: string;
  onClose: () => void;
}

export function DetailDrawer({ id, onClose }: Props) {
  const { data, isLoading, isError, error } = useEarthquake(id);
  const closeRef = useRef<HTMLButtonElement>(null);

  // Focus management: move focus into the drawer on open, restore on close,
  // close on Escape.
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    closeRef.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      previous?.focus();
    };
  }, [onClose]);

  return (
    <>
      <div className="drawer-backdrop" onClick={onClose} />
      <aside className="drawer" role="dialog" aria-modal="true" aria-label="Earthquake details">
        <div className="detail">
          <div className="detail-head">
            <Link to={`/event/${id}`} className="eyebrow">Open full page ↗</Link>
            <button ref={closeRef} className="close-btn" onClick={onClose} aria-label="Close details">
              ×
            </button>
          </div>
          {isLoading && <p style={{ color: "var(--muted)" }}>Loading…</p>}
          {isError && <p style={{ color: "var(--red)" }}>{(error as Error)?.message || "Failed to load"}</p>}
          {data && <EarthquakeDetail e={data} />}
        </div>
      </aside>
    </>
  );
}

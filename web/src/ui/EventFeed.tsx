import { useRef } from "react";
import type { Earthquake } from "../api/types";
import {
  formatDepth,
  formatMagnitude,
  magnitudeClass,
  relativeTime,
} from "../util/format";

interface Props {
  events: Earthquake[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  bufferedCount: number;
  onApplyBuffer: () => void;
  hasNextPage: boolean;
  isFetchingNextPage: boolean;
  onLoadMore: () => void;
  isError: boolean;
  errorMessage?: string;
  // Reports whether the list is scrolled to the top, so live events only
  // auto-prepend when the user isn't reading further down.
  onAtTopChange: (atTop: boolean) => void;
}

export function EventFeed({
  events,
  selectedId,
  onSelect,
  bufferedCount,
  onApplyBuffer,
  hasNextPage,
  isFetchingNextPage,
  onLoadMore,
  isError,
  errorMessage,
  onAtTopChange,
}: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);

  const handleScroll = () => {
    const el = scrollRef.current;
    if (el) onAtTopChange(el.scrollTop <= 8);
  };

  const applyAndScroll = () => {
    onApplyBuffer();
    scrollRef.current?.scrollTo({ top: 0 });
  };

  return (
    <div className="feed-scroll" ref={scrollRef} onScroll={handleScroll}>
      {bufferedCount > 0 && (
        <button className="new-pill" onClick={applyAndScroll}>
          ↑ {bufferedCount} new event{bufferedCount > 1 ? "s" : ""}
        </button>
      )}

      {isError && <div className="empty-state">{errorMessage || "Failed to load events"}</div>}
      {!isError && events.length === 0 && <div className="empty-state">No matching events</div>}

      {events.map((e) => (
        <button
          key={e.id}
          className={`event-item ${e.id === selectedId ? "selected" : ""}`}
          onClick={() => onSelect(e.id)}
          aria-pressed={e.id === selectedId}
        >
          <span className={`magnitude ${magnitudeClass(e.magnitude)}`}>
            {formatMagnitude(e.magnitude)}
          </span>
          <span className="event-info">
            <strong title={e.place ?? "Unknown location"}>{e.place ?? "Unknown location"}</strong>
            <span>
              {formatDepth(e.depth_km)} deep · {e.magnitude_type ?? "unknown"}
            </span>
          </span>
          <time className="event-time" dateTime={e.occurred_at}>
            {relativeTime(e.occurred_at)}
          </time>
        </button>
      ))}

      {hasNextPage && (
        <div className="load-more">
          <button onClick={onLoadMore} disabled={isFetchingNextPage}>
            {isFetchingNextPage ? "Loading…" : "Load more"}
          </button>
        </div>
      )}
    </div>
  );
}

import type { ConnState } from "../data/realtime";

const LABELS: Record<ConnState, string> = {
  live: "Live",
  reconnecting: "Reconnecting",
  offline: "Offline",
};

export function ConnectionBadge({ state }: { state: ConnState }) {
  return (
    <span className={`connection ${state}`} role="status" aria-live="polite">
      <span className="connection-dot" aria-hidden="true" />
      {LABELS[state]}
    </span>
  );
}

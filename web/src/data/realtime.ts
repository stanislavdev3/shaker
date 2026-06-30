import type { Earthquake } from "../api/types";

export type ConnState = "live" | "reconnecting" | "offline";

// --- Pure feed helpers (unit-tested without a socket) ----------------------

// Replace by id keeping the newer version; ignore stale (older-version) updates;
// keep the list sorted newest-first. Dedupe is by id, staleness by version.
export function upsert(list: Earthquake[], eq: Earthquake): Earthquake[] {
  const existing = list.find((e) => e.id === eq.id);
  if (existing && eq.version < existing.version) return list;
  const next = existing
    ? list.map((e) => (e.id === eq.id ? eq : e))
    : [eq, ...list];
  return [...next].sort(
    (a, b) => Date.parse(b.occurred_at) - Date.parse(a.occurred_at),
  );
}

// Decides whether a live change waits behind the "N new" pill. A change to an
// already-displayed event applies in place (even mid-scroll); a brand-new event
// applies immediately only when the feed is at the top, otherwise it buffers so
// the list never reshuffles under the user's cursor.
export function shouldBuffer(
  eq: Earthquake,
  displayedIds: Set<string>,
  atTop: boolean,
): boolean {
  if (displayedIds.has(eq.id)) return false;
  return !atTop;
}

// --- WebSocket controller --------------------------------------------------

interface Handlers {
  onState: (s: ConnState) => void;
  onChange: (eq: Earthquake) => void;
  onResync: () => void; // socket is best-effort; refetch HTTP on reconnect
}

export class RealtimeController {
  private socket: WebSocket | null = null;
  private delay = 1000;
  private stopped = false;
  private connectedBefore = false;
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private url: string,
    private handlers: Handlers,
  ) {}

  start(): void {
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.socket?.close();
    this.socket = null;
  }

  private connect(): void {
    const socket = new WebSocket(this.url);
    this.socket = socket;
    socket.onopen = () => {
      this.delay = 1000;
      this.handlers.onState("live");
      if (this.connectedBefore) this.handlers.onResync();
      this.connectedBefore = true;
    };
    socket.onmessage = (ev) => {
      let msg: { type?: string; data?: { earthquake?: Earthquake } };
      try {
        msg = JSON.parse(ev.data);
      } catch {
        return;
      }
      if (msg.type === "earthquake_changed" && msg.data?.earthquake) {
        this.handlers.onChange(msg.data.earthquake);
      }
    };
    socket.onerror = () => socket.close();
    socket.onclose = () => {
      if (this.stopped) return;
      const online = typeof navigator === "undefined" || navigator.onLine;
      this.handlers.onState(online ? "reconnecting" : "offline");
      this.timer = setTimeout(() => this.connect(), this.delay);
      this.delay = Math.min(this.delay * 2, 15000);
    };
  }
}

export function streamUrl(): string {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${location.host}/v1/stream`;
}

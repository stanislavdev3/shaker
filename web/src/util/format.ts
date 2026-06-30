const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

export function relativeTime(value: string): string {
  const seconds = Math.round((Date.parse(value) - Date.now()) / 1000);
  const abs = Math.abs(seconds);
  if (abs < 60) return rtf.format(seconds, "second");
  if (abs < 3600) return rtf.format(Math.round(seconds / 60), "minute");
  if (abs < 86400) return rtf.format(Math.round(seconds / 3600), "hour");
  return rtf.format(Math.round(seconds / 86400), "day");
}

export function magnitudeClass(m: number | null): string {
  if (m == null) return "";
  if (m >= 6) return "major";
  if (m < 3) return "minor";
  return "";
}

export function formatMagnitude(m: number | null): string {
  return m == null ? "—" : m.toFixed(1);
}

export function formatDepth(d: number | null): string {
  return d == null ? "—" : `${d.toFixed(1)} km`;
}

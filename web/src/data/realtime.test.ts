import { describe, expect, it } from "vitest";
import type { Earthquake } from "../api/types";
import { shouldBuffer, upsert } from "./realtime";
import { applyLiveChange } from "./queries";
import type { InfiniteData } from "@tanstack/react-query";
import type { EarthquakePage } from "../api/types";

function quake(id: string, occurred: string, version = 1, magnitude = 5): Earthquake {
  return {
    id,
    occurred_at: occurred,
    latitude: 0,
    longitude: 0,
    depth_km: 10,
    magnitude,
    magnitude_type: "mw",
    place: "somewhere",
    status: "reviewed",
    event_type: "earthquake",
    alert_level: null,
    tsunami: null,
    significance: null,
    source: "usgs",
    version,
  };
}

describe("upsert", () => {
  const a = quake("a", "2026-06-29T10:00:00Z");
  const b = quake("b", "2026-06-29T11:00:00Z");

  it("inserts newest-first", () => {
    expect(upsert([a], b).map((e) => e.id)).toEqual(["b", "a"]);
  });

  it("replaces in place with a newer version (dedupe by id)", () => {
    const a2 = quake("a", "2026-06-29T10:00:00Z", 2, 7);
    const out = upsert([a], a2);
    expect(out).toHaveLength(1);
    expect(out[0].magnitude).toBe(7);
  });

  it("ignores a stale (older-version) update", () => {
    const a2 = quake("a", "2026-06-29T10:00:00Z", 2, 7);
    const stale = quake("a", "2026-06-29T10:00:00Z", 1, 3);
    const out = upsert([a2], stale);
    expect(out[0].magnitude).toBe(7);
  });
});

describe("shouldBuffer", () => {
  const ids = new Set(["a", "b"]);

  it("does not buffer updates to already-displayed events", () => {
    expect(shouldBuffer(quake("a", "x"), ids, false)).toBe(false);
  });

  it("does not buffer new events when scrolled to the top", () => {
    expect(shouldBuffer(quake("z", "x"), ids, true)).toBe(false);
  });

  it("buffers new events when scrolled away from the top", () => {
    expect(shouldBuffer(quake("z", "x"), ids, false)).toBe(true);
  });
});

describe("applyLiveChange (infinite-query cache)", () => {
  const page = (data: Earthquake[]): EarthquakePage => ({
    data,
    pagination: { next_cursor: null },
  });
  const data: InfiniteData<EarthquakePage, string | null> = {
    pages: [page([quake("b", "2026-06-29T11:00:00Z")]), page([quake("a", "2026-06-29T10:00:00Z")])],
    pageParams: [null, "cursor"],
  };

  it("prepends a brand-new event to the first page", () => {
    const out = applyLiveChange(data, quake("c", "2026-06-29T12:00:00Z"));
    expect(out!.pages[0].data.map((e) => e.id)).toEqual(["c", "b"]);
  });

  it("updates an existing event on whatever page it lives on", () => {
    const out = applyLiveChange(data, quake("a", "2026-06-29T10:00:00Z", 2, 9));
    expect(out!.pages[1].data[0].magnitude).toBe(9);
    expect(out!.pages[0].data).toHaveLength(1); // not duplicated onto page 0
  });
});

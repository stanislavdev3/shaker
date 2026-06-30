import { describe, expect, it } from "vitest";
import {
  EMPTY_FILTERS,
  filtersFromSearch,
  filtersToApiParams,
  filtersToSearch,
  matchesFilters,
  type Filters,
} from "./filters";

const NOW = new Date("2026-06-29T12:00:00Z");

const sample: Filters = {
  minMagnitude: "5",
  minDepth: "10",
  maxDepth: "",
  from: "24h",
  tsunami: true,
  alertLevel: "orange",
};

describe("URL <-> filters round-trip", () => {
  it("survives a round-trip through search params", () => {
    const search = filtersToSearch(sample, new URLSearchParams());
    expect(filtersFromSearch(search)).toEqual(sample);
  });

  it("omits default/empty values from the URL", () => {
    const search = filtersToSearch(EMPTY_FILTERS, new URLSearchParams());
    expect(search.toString()).toBe("");
  });

  it("preserves unrelated params (e.g. selected event)", () => {
    const search = filtersToSearch(sample, new URLSearchParams("event=abc"));
    expect(search.get("event")).toBe("abc");
  });
});

describe("filtersToApiParams", () => {
  it("maps UI filters to backend query params", () => {
    const p = filtersToApiParams(sample, NOW);
    expect(p.min_magnitude).toBe("5");
    expect(p.min_depth_km).toBe("10");
    expect(p.tsunami).toBe("true");
    expect(p.alert_level).toBe("orange");
    expect(p.from).toBe("2026-06-28T12:00:00.000Z"); // 24h before NOW
  });
});

describe("matchesFilters", () => {
  const base = {
    magnitude: 6,
    depth_km: 12,
    tsunami: true,
    alert_level: "orange",
    occurred_at: "2026-06-29T11:00:00Z",
  };

  it("accepts an event meeting all criteria", () => {
    expect(matchesFilters(base, sample, NOW)).toBe(true);
  });

  it("rejects below minimum magnitude", () => {
    expect(matchesFilters({ ...base, magnitude: 4 }, sample, NOW)).toBe(false);
  });

  it("rejects a non-tsunami event when tsunami-only", () => {
    expect(matchesFilters({ ...base, tsunami: false }, sample, NOW)).toBe(false);
  });

  it("rejects events older than the time window", () => {
    expect(
      matchesFilters({ ...base, occurred_at: "2026-06-01T00:00:00Z" }, sample, NOW),
    ).toBe(false);
  });

  it("rejects a mismatched alert level", () => {
    expect(matchesFilters({ ...base, alert_level: "green" }, sample, NOW)).toBe(false);
  });
});

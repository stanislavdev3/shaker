import { WINDOWS, type Filters as FiltersT } from "../data/filters";

interface Props {
  filters: FiltersT;
  onChange: (next: FiltersT) => void;
}

export function Filters({ filters, onChange }: Props) {
  const set = <K extends keyof FiltersT>(key: K, value: FiltersT[K]) =>
    onChange({ ...filters, [key]: value });

  return (
    <div className="filters">
      <div className="grid">
        <label>
          Minimum magnitude
          <select value={filters.minMagnitude} onChange={(e) => set("minMagnitude", e.target.value)}>
            <option value="">All</option>
            <option value="2.5">M 2.5+</option>
            <option value="4">M 4.0+</option>
            <option value="5">M 5.0+</option>
            <option value="6">M 6.0+</option>
          </select>
        </label>
        <label>
          Time window
          <select value={filters.from} onChange={(e) => set("from", e.target.value)}>
            <option value="">Any time</option>
            {Object.keys(WINDOWS).map((key) => (
              <option key={key} value={key}>
                Last {key}
              </option>
            ))}
          </select>
        </label>
        <label>
          Min depth (km)
          <input
            type="number"
            inputMode="decimal"
            value={filters.minDepth}
            onChange={(e) => set("minDepth", e.target.value)}
          />
        </label>
        <label>
          Max depth (km)
          <input
            type="number"
            inputMode="decimal"
            value={filters.maxDepth}
            onChange={(e) => set("maxDepth", e.target.value)}
          />
        </label>
        <label>
          Alert level
          <select value={filters.alertLevel} onChange={(e) => set("alertLevel", e.target.value)}>
            <option value="">Any</option>
            <option value="green">Green</option>
            <option value="yellow">Yellow</option>
            <option value="orange">Orange</option>
            <option value="red">Red</option>
          </select>
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={filters.tsunami}
            onChange={(e) => set("tsunami", e.target.checked)}
          />
          Tsunami only
        </label>
      </div>
    </div>
  );
}

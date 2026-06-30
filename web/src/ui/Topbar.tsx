import { Link } from "react-router-dom";
import type { ReactNode } from "react";

export function Topbar({ children }: { children?: ReactNode }) {
  return (
    <header className="topbar">
      <Link className="brand" to="/" aria-label="Earthquake Monitor home">
        <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
        <span>Earthquake <b>Monitor</b></span>
      </Link>
      {children ?? <span className="source-label">USGS DATA</span>}
    </header>
  );
}

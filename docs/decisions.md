# Architecture decisions and assumptions

## Production product phase

The service is no longer being designed as an MVP. New ingestion, correlation,
notification, observability, recovery, and operational decisions must be suitable for
a production system. Temporary shortcuts must be explicitly documented together with
their removal criteria.

## EMSC is the low-latency source

The EMSC standing-order WebSocket is the primary source for low-latency earthquake
alerts. EMSC FDSN is the recovery and catalogue channel for EMSC outages and missed
WebSocket messages. USGS remains an independent realtime and historical source. A
WebSocket observation is preliminary; an EMSC FDSN or USGS catalogue observation can
confirm the canonical incident.

## Cross-provider observations are associated with canonical incidents

Provider records and every material provider revision remain independently stored.
An earthquake incident is a separate canonical entity to which EMSC and USGS
observations can be associated. Association uses explicit provider identifiers or an
authoritative identity mapping first and a conservative, versioned heuristic only as
a fallback. Heuristic links record their score, method, algorithm version, and audit
history and can be corrected without deleting source data.

False merges are more harmful than temporary duplicates. Ambiguous candidates are not
automatically associated. See [event-correlation.md](event-correlation.md).

## PostgreSQL is the notification queue

Delivery creation must be atomic with earthquake changes. PostgreSQL provides this atomicity, uniqueness constraints, durable storage, `FOR UPDATE SKIP LOCKED`, and abandoned-lock recovery without another operational dependency.

## Administration is embedded and management-only

The private administration interface is server-rendered by the API role of the Go
modular monolith. It uses embedded HTMX, CSS, and MapLibre assets and introduces no
separate frontend service or Node.js runtime. Cloudflare Access protects a dedicated
hostname, and the origin independently validates the Access application JWT.

The interface manages and investigates individual incidents, provider observations,
subscriptions, deliveries, correlation decisions, and administrative audit records.
It does not render health, counters, rates, queue aggregates, time-series charts,
ingestion summaries, or logs. Grafana and Loki remain the only observability UI; admin
pages provide contextual deep links. See [admin.md](admin.md).

## Personal alerts use local shaking intensity

New personal Telegram subscriptions use expected Modified Mercalli Intensity at the
subscriber point instead of a fixed epicentral radius and minimum magnitude. Preliminary
estimates use the Allen, Wald, and Worden (2012) hypocentral-distance IPE. The alert
decision uses the one-sigma upper estimate to reduce false negatives. An integer MMI
selection represents a category whose lower decision boundary is `threshold - 0.5`;
this versioned product policy is separate from the IPE version. The message shows the
central estimate and range. Model inputs, assumptions, versions, uncertainty, effective
boundary, and decisions are immutable audit records. A future observed ShakeMap can
replace the preliminary estimate without discarding it.

Existing magnitude-and-radius subscriptions are not silently converted because no
scientifically valid one-to-one mapping exists. They remain in legacy mode until the
user chooses an MMI threshold.

## Cursor pagination

Offset pagination becomes unstable as new reports arrive and becomes increasingly expensive. Integrity-protected cursors carry the sort and deterministic tie-break tuple. They reject tampering and reuse under another ordering.

## Baseline notification suppression

A fresh installation sees an entire rolling daily feed as new. The first completed synchronization establishes state but intentionally does not notify subscribers, avoiding a startup burst of old reports.

## At-least-once delivery

The service can fail after a webhook accepts a request and before the database update commits. Avoiding that ambiguity would require cooperation from the receiver or a distributed protocol. Deliveries therefore carry stable IDs and receivers are expected to deduplicate.

## Raw payload separation

Normalized columns support stable application queries while source records retain the latest provider representation and revisions retain material historical representations. Public list responses do not expose raw provider payloads.

## Additional assumptions

- The USGS `updated` timestamp is the provider ordering authority. An equal timestamp with a different canonical payload is treated as a material correction.
- A 20,000-result historical page uses the documented FDSN offset mechanism; 24-hour default windows reduce limit pressure.
- The maximum public and webhook subscription radius is 2,000 km. Personal MMI alert
  candidate radii are event-dependent and bounded internally to half the Earth's
  circumference.
- Public searches are bounded to 200 rows per page. No fixed time-range limit is imposed because cursor queries use indexed deterministic order.
- The supplied PostGIS database has permission to install `postgis` and `pgcrypto`.
- Compose applies the initial migration only when creating a new database volume; Atlas is the migration mechanism for existing environments.
- The administrative JSON API retains its deployment-level bearer key for automation.
  The browser administration interface uses Cloudflare Access identities, application
  JWT validation, role bindings, CSRF protection, and an append-only action audit.
- Realtime client updates use PostgreSQL `LISTEN/NOTIFY` because notifications are emitted after transaction commit and require no additional broker. The WebSocket stream is best effort; the HTTP API remains authoritative.
- Public notification WebSocket messages are deliberately content-free resynchronization hints. Protected delivery records are fetched separately with the administrative API key.

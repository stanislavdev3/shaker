# Architecture decisions and assumptions

## USGS-only MVP

USGS provides a stable realtime GeoJSON feed and a historical FDSN API with the normalized fields needed by the product. The application uses a provider interface so another source can be added without changing domain trigger rules or public API models.

## Cross-provider deduplication is deferred

There is no dependable universal earthquake identity across providers. Time-and-distance heuristics can incorrectly merge nearby events or split revisions. The MVP maps each provider identity to one canonical record and does not infer cross-provider identity.

## PostgreSQL is the notification queue

Delivery creation must be atomic with earthquake changes. PostgreSQL provides this atomicity, uniqueness constraints, durable storage, `FOR UPDATE SKIP LOCKED`, and abandoned-lock recovery without another operational dependency.

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
- The maximum public and subscription radius is 2,000 km.
- Public searches are bounded to 200 rows per page. No fixed time-range limit is imposed because cursor queries use indexed deterministic order.
- The supplied PostGIS database has permission to install `postgis` and `pgcrypto`.
- Compose applies the initial migration only when creating a new database volume; Atlas is the migration mechanism for existing environments.
- Administrative authentication is a single deployment-level bearer key because user accounts and OAuth are explicit non-goals.
- Realtime browser updates use PostgreSQL `LISTEN/NOTIFY` because notifications are emitted after transaction commit and require no additional broker. The WebSocket stream is best effort; the HTTP API remains authoritative.
- Public notification WebSocket messages are deliberately content-free resynchronization hints. Protected delivery records are fetched separately with the administrative API key.

# Event correlation and lifecycle

## Current implementation status

The storage foundation is implemented by migration `202607140001`: canonical incidents
have lifecycle and field provenance, material provider observations are retained,
association history is seeded for existing records, and Telegram alert projections can
persist a remote `message_id` and desired/delivered incident versions. USGS observations
are classified as confirmed, reviewed, or retracted from their provider status.

The correlation scorer is implemented as a pure domain policy but automatic heuristic
association is intentionally not enabled until paired EMSC and USGS replay fixtures
have been used to calibrate its gates and acceptance thresholds. Telegram delivery is
connected to the message projection: the initial send persists `message_id`, and later
canonical versions converge through `editMessageText` for private alerts and the
configured global channel.

## Data model

The production model separates facts received from providers from the product entity
shown to users:

1. A provider record is identified by `(provider, external_id)` and retains its raw
   observations and material revisions. Each observation records its channel, such as
   EMSC standing-order WebSocket, EMSC FDSN, or USGS catalogue data. EMSC WebSocket and
   FDSN payloads carrying the same `unid` therefore update one EMSC provider record.
2. A canonical incident groups observations believed to describe the same physical
   earthquake. It has its own version, lifecycle, and selected canonical fields.
3. An association links an observation to an incident and records the method,
   confidence score, algorithm version, evidence, creator, and timestamps.
4. A Telegram alert projection maps `(subscription_id, incident_id)` to the remote
   chat and `message_id`, plus desired and delivered incident versions.

Source observations are never rewritten to look like another provider. Correcting an
association moves or removes the link and recomputes affected incidents while keeping
the complete audit trail.

## Incident lifecycle

Lifecycle is independent of a provider's own status field:

| Lifecycle | Meaning |
| --- | --- |
| `preliminary` | The incident is supported only by a low-latency EMSC WebSocket observation. |
| `confirmed` | At least one associated EMSC FDSN or USGS catalogue observation supports the incident. |
| `reviewed` | An associated provider explicitly marks its solution as reviewed. |
| `retracted` | Provider evidence or an operator decision invalidates the incident. |

Lifecycle normally advances monotonically, except that any state can become
`retracted`. Losing one provider does not retract an incident while another confirming
observation remains active. Every transition creates a canonical revision.

## Association order

Correlation follows this order:

1. Reuse the incident already linked to the same provider identity.
2. Use identifiers embedded by a provider or an authoritative cross-provider identity
   mapping. Identity lookup enriches ingestion asynchronously and is not allowed to
   block the WebSocket alert path.
3. Search a bounded time and geographic candidate window and calculate a deterministic
   score from origin-time difference, epicentral distance, magnitude difference when
   present, and depth difference when present.
4. Automatically associate only when the best candidate exceeds the configured high
   confidence threshold and is separated from the runner-up by a configured margin.
5. Keep an ambiguous observation unassociated and create a distinct incident. It can
   be reconciled after authoritative identity evidence arrives or by a future admin
   workflow.

The candidate gates, weights, acceptance threshold, and ambiguity margin are a named,
versioned policy. They must be calibrated against replayed historical EMSC and USGS
fixtures before production activation. Changes to the policy apply prospectively;
bulk reassociation is a separate audited operation. The system prefers a temporary
duplicate over merging two nearby earthquakes.

## Canonical field selection

Canonical values are selected from observations, not blended numerically. The
selection policy is deterministic and records which observation supplied each field.
A reviewed solution outranks a catalogue solution, which outranks a WebSocket-only
solution. Within the same class, the newest non-stale provider revision wins according
to a stable provider policy. Nullable fields remain nullable and a missing lower-rank
value cannot erase an available higher-rank value.

This field-level provenance allows API clients and operators to explain why magnitude,
location, depth, or place changed. A change to any user-visible canonical field or the
incident lifecycle increments the incident version.

## Telegram convergence

The database stores a desired Telegram representation for each matching subscription
and incident. A worker locks that projection and:

1. calls `sendMessage` when no remote `message_id` exists and persists the returned ID;
2. calls `editMessageText` when the desired incident version is newer than the
   delivered version;
3. rechecks the desired version after each remote call so updates arriving during I/O
   are not lost;
4. retries edits against a known `message_id` so they converge on the newest desired
   text;
5. sends a single correction message only when editing the original is impossible.

Intermediate revisions may be coalesced, but lifecycle changes and the final rendered
state are never lost. Confirmation and retraction edits bypass normal debounce. The
projection and its retry jobs remain in PostgreSQL so replicas coordinate without an
external broker.

Telegram Bot API does not provide an idempotency key for `sendMessage`. If Telegram
accepts the initial message but its response is lost, the service cannot recover the
unknown `message_id` and a retry can create a duplicate. Initial send is therefore
at-least-once, this ambiguity is recorded and measured, and all operations after a
successfully persisted `message_id` converge by editing that message.

## Recovery and operational controls

EMSC FDSN recovery starts from a persisted checkpoint with overlap after WebSocket
disconnects. Both EMSC and USGS ingestion are idempotent. Recovery and backfill update
observations and canonical incidents but suppress new-alert bursts; they may still
repair or confirm a previously delivered preliminary message.

Production metrics must cover WebSocket connection state, message lag, reconnects,
invalid frames, FDSN recovery lag, unassociated and ambiguous observations,
association methods and confidence, lifecycle transition latency, Telegram send/edit
latency, edit failures, and preliminary incidents that remain unconfirmed past an
alerting threshold.

## Implementation sequence

1. Introduce provider observations, canonical incidents, audited associations, and
   lifecycle revisions without changing the public API response shape.
2. Implement and replay-test the correlation policy against paired historical EMSC and
   USGS fixtures. Enable automatic heuristic association only after measuring false
   merge and false split rates.
3. Add EMSC FDSN ingestion and recovery, then the bounded reconnecting WebSocket client.
4. Add Telegram alert projections, capture `message_id` from `sendMessage`, and support
   `editMessageText` with coalescing and retry.
5. Expose provenance and lifecycle through a versioned API change and add operational
   dashboards and alerts before enabling EMSC-driven notifications in production.

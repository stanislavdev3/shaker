# Architecture

## Components

```mermaid
flowchart LR
    EMSCWS[EMSC standing-order WebSocket] --> Providers[Provider adapters]
    EMSCFDSN[EMSC FDSN API] --> Providers
    USGS[USGS realtime and FDSN APIs] --> Providers
    Providers --> Ingestion[Ingestion application service]
    Ingestion --> Correlation[Incident correlation]
    Correlation --> DB[(PostgreSQL and PostGIS)]
    Client[Mobile and public API clients] --> API[HTTP API]
    Admin[Administrator] --> API
    API --> DB
    DB -- LISTEN / NOTIFY --> API
    DB --> Worker[Notification worker]
    Worker --> Webhook[Subscriber webhook]
    Worker --> Telegram[Telegram Bot API]
```

The executable is one modular monolith with `api`, `worker`, `all`, and `backfill` commands. Domain packages contain normalized models and trigger rules. Provider, HTTP, and PostgreSQL packages are adapters and dependencies point inward.

The service exposes JSON, GeoJSON, and WebSocket APIs for mobile and other API clients; it does not serve a web frontend. PostgreSQL triggers publish compact change references after commit. A dedicated API connection listens for those references, reloads normalized earthquake data, and broadcasts it through a bounded WebSocket hub. Notification changes emit only a content-free public resynchronization hint; administrative details remain available exclusively through the authenticated API.

## Ingestion flow

The provider fetches and parses data outside a database transaction. Realtime responses use persisted ETag and Last-Modified validators. Features are parsed independently so one malformed feature does not discard a valid feed.

The service opens bounded transactions of at most 250 observations. Each provider
observation is identified by `(provider, external_id)`. A locked source record is
checked by source timestamp and canonical payload hash. Stale responses cannot
overwrite newer source data; identical data updates only last-seen timestamps;
material updates append a provider revision.

Provider observations are associated with a canonical incident. Explicit identities
and authoritative mappings take precedence over conservative heuristic correlation.
Canonical field selection, lifecycle transitions, subscription matching, and delivery
projection updates occur transactionally. See
[event-correlation.md](event-correlation.md).

The first successful realtime poll is a baseline. Its transaction completes before the baseline flag is stored, and it creates no notifications. Backfill and recovery use the same idempotent writes but never evaluate notifications.

## Notification flow

```mermaid
sequenceDiagram
    participant I as Ingestion
    participant P as PostgreSQL
    participant W as Worker
    participant H as Webhook
    I->>P: Update earthquake and insert delivery in one transaction
    W->>P: Claim batch with FOR UPDATE SKIP LOCKED
    P-->>W: Jobs marked processing
    W->>H: Signed POST outside transaction
    H-->>W: 2xx or failure
    W->>P: Mark sent, retry, or dead
```

The unique delivery key makes job creation duplicate-safe. A lock timeout allows another worker to recover abandoned processing jobs. Webhook delivery validates DNS on every attempt, pins dialing to a validated address, disables redirects, and bounds time and response bytes. Telegram bot registration uses bounded long polling, a persistent update offset, and a PostgreSQL advisory lock so only one worker replica consumes bot updates. Telegram alert projections retain the Telegram `message_id`: the first preliminary version sends a message and later canonical versions edit that same message. Webhooks continue to use immutable, versioned deliveries.

## Database responsibilities

PostgreSQL stores canonical incidents and their lifecycle, provider records, immutable
material observations, association history, raw payloads, canonical revisions,
ingestion runs, provider cache/checkpoint state, subscriptions, Telegram message
projections, and deliveries. PostGIS performs radius and bounding-box filtering.
PostgreSQL row locks coordinate concurrent ingestion and notification workers.

## Failure recovery and scaling

Provider checkpoints and validators survive process restarts. An outage longer than 24 hours initiates an overlapping historical recovery before normal polling. Backfill checkpoints after each completed window.

API replicas are stateless except for their process-local rate limiter. Worker replicas coordinate with uniqueness constraints, row locks, and `SKIP LOCKED`. Webhook delivery remains at least once because a process can fail after the remote endpoint accepts a request but before the database records success.

Each API replica holds a PostgreSQL listener and its own WebSocket clients. PostgreSQL `NOTIFY` fan-out reaches every listening replica. WebSocket messages are best effort; bounded client buffers prevent slow clients from applying backpressure, and reconnecting clients reload current state through the public HTTP API.

# Administration interface

## Purpose and boundary

The administration interface is the service management plane. It supports
investigating individual incidents, provider observations, subscriptions, and
notification deliveries, and it provides a small set of explicit, audited
operations.

It is not an observability dashboard. Health, counters, rates, queue aggregates,
time-series charts, ingestion summaries, provider connection status, and logs belong
in Grafana and Loki. Admin pages may link to Grafana or Loki with an incident,
subscription, delivery, request, or provider identifier already inserted into the
query, but they must not reproduce those dashboards.

The interface is private and is not a public web version of the earthquake product.
The public root route remains unavailable. When administration is disabled,
`/admin/*` returns `404`.

## Architecture and technology

The interface is served only by the `api` service:

```text
/api/*      Public and mobile JSON APIs
/admin/api/* Administrative automation APIs
/admin/*    Private server-rendered administration interface
/           404
```

The target interface stack uses:

- Go `html/template` for context-aware HTML escaping;
- HTMX for future command confirmations and partial page updates;
- a small service-owned CSS layer;
- the MapLibre GL JS CSP bundle for maps after a production map style is configured;
- `go:embed` for templates, JavaScript, CSS, and license notices.

The initial read-only release needs no client-side executable code and uses signed
cursor links plus an external map link. When HTMX and MapLibre are introduced, their
assets will be pinned and stored with the application. Production does not load
executable assets from a CDN and does not require a Node.js runtime. Map tiles and the
map style are supplied through a configured production endpoint. The map degrades to
coordinates and external map links if the tile endpoint is unavailable.

Admin HTTP handlers are transport adapters. They do not execute SQL or contain
correlation, notification, or subscription business rules. They call application
services, which depend on domain types and repository interfaces. PostgreSQL-specific
queries remain in the PostgreSQL adapter.

The server-rendered interface calls application services directly instead of making
HTTP requests to its own JSON API. Existing administrative JSON endpoints remain
available for automation but are not used as an internal service boundary.

## Authentication and authorization

Production access is protected by a path-scoped Cloudflare Access application at
`eq.screaming.dog/admin`; its policy is inherited by descendant paths. The public `/api/*` namespace on the same
hostname is outside that Access application. The origin also verifies the Cloudflare
application JWT; the presence of a header or cookie alone is not trusted. Verification
includes:

- RS256 signature against a bounded, cached JWKS set;
- expected issuer and application audience;
- expiration and not-before timestamps;
- application-token type;
- an authenticated subject and email identity.

The interface has three roles:

| Role | Permissions |
| --- | --- |
| `viewer` | Read operational entities with sensitive fields masked |
| `operator` | Viewer permissions plus safe delivery and subscription actions |
| `owner` | Operator permissions plus sensitive-data reveal, role management, and manual correlation |

Role bindings are stored in `admin_role_bindings`. Initial owners are bootstrapped
from deployment configuration, after which role changes are performed through audited
owner-only commands. A production auth bypass is prohibited. Development bypass, if
implemented, must be explicit, development-only, and bound to a local environment.

All mutating requests use `POST`, validate `Origin`, require a CSRF token tied to the
authenticated identity, and use strict form decoding. Responses set a restrictive CSP,
frame denial, content-type protection, referrer policy, and no-store caching for
sensitive pages.

## Audit trail and sensitive data

Every mutation writes an `admin_audit_log` record in the same database transaction as
the affected state. An audit record contains:

- actor subject and email;
- role used for authorization;
- action and resource type;
- resource identifier;
- before and after representations with secrets removed;
- mandatory operator reason where required;
- request ID, source IP, and user agent;
- creation timestamp.

Audit records are append-only. They are not updated or deleted through the interface.

Telegram chat identifiers and subscriber coordinates are personal operational data.
They are masked by default. Revealing them requires the `owner` role and creates an
audit record. Webhook secrets, encryption keys, provider credentials, Telegram tokens,
and Cloudflare credentials are never rendered. A newly rotated webhook secret is shown
once and is not recoverable afterwards.

## Pages

### Incidents

`/admin/incidents` provides bounded cursor pagination and URL-based filters for time,
provider, lifecycle, magnitude, and event identity. It shows individual records, not
aggregated metrics.

`/admin/incidents/{id}` includes:

- canonical incident fields and lifecycle;
- epicenter map and coordinates;
- associated EMSC and USGS source records;
- immutable provider observations and revisions;
- canonical provenance for each field;
- association method, confidence, algorithm version, and evidence;
- local MMI evaluations, model and decision-policy versions, selected category,
  effective decision boundary, uncertainty, assumptions, and decision;
- immutable notification matching runs with their prefilter radius, decision counts,
  model errors, and created triggers;
- Telegram alert projections and webhook deliveries for the incident;
- deep links to filtered Grafana and Loki views.

Raw provider payloads are collapsed by default, size-bounded, and owner-only where they
may contain sensitive or unexpectedly large data.

### Subscriptions

`/admin/subscriptions` and `/admin/subscriptions/{id}` show:

- Telegram or webhook channel;
- active, paused, or disabled status;
- personal or global-channel kind;
- RU or EN notification language;
- MMI threshold or legacy magnitude-and-radius configuration;
- masked Telegram identity and location;
- related alerts, deliveries, and intensity evaluations;
- creation and update timestamps.

An administrator cannot silently replace a Telegram user's location, language, or MMI
threshold. Those settings remain user-controlled through Telegram. Operators may pause
or disable a subscription and may enqueue a setup reminder when that capability is
implemented.

Webhook subscription management may create, edit, pause, disable, and rotate a secret.
There is no hard delete in the first production version.

### Notifications

`/admin/notifications` and `/admin/notifications/{id}` show individual durable work
items and Telegram projections.

The list uses server-filtered tabs for the global Telegram channel, private Telegram
chats, and webhook deliveries. Each tab has an independent signed pagination cursor.
Each row includes:

- delivery or projection status;
- incident and subscription links;
- desired and delivered earthquake versions;
- trigger and lifecycle;
- bounded payload preview;
- Telegram message identifier;
- attempt count, next attempt, and last error;
- RU or EN Telegram message preview;
- deep links to worker logs.

The page may retry one eligible `dead` or `retry` delivery. It does not display queue
depth, rates, or delivery charts; those remain in Grafana.

### Correlation

`/admin/correlation` supports investigation of canonical incidents and potential
duplicates. It shows source records side by side, time and distance deltas, magnitude
and depth differences, current associations, confidence, and algorithm evidence.

Manual merge and unmerge are owner-only and are implemented after the read-only
interface. The workflow requires:

1. selecting explicit incidents or source records;
2. generating a side-effect-free preview;
3. showing every affected association, canonical field, lifecycle, and alert;
4. entering a mandatory reason;
5. confirming the exact operation;
6. applying the command and audit record atomically.

Source records and provider observations are never deleted or overwritten. An unmerge
ends the active association and creates the required canonical incident state while
preserving the previous association history.

### Audit

`/admin/audit` is owner-only and offers bounded filters for actor, action, resource,
and time. It displays append-only administrative actions and sensitive-field reveal
events. Audit data is operational history, not a metric dashboard.

## Allowed operations

The initial read-only release has no mutations. Safe operations are added separately:

- retry one eligible notification delivery;
- pause or disable one subscription;
- enqueue one Telegram setup reminder;
- create or edit a webhook subscription;
- rotate a webhook secret with one-time display;
- manage administrator role bindings;
- preview and apply manual merge or unmerge.

There are no bulk mutations in the first version. Every command is context-aware,
bounded, idempotent where possible, authorized independently, and protected against a
stale confirmation by checking the expected resource version.

Provider reconnect and recovery controls are deferred until they have explicit,
idempotent application commands. Provider health and ingestion history remain in
Grafana and Loki.

## Grafana and Loki integration

The admin interface generates deep links using configured base URLs and escaped query
parameters. Supported contexts include:

- API logs by request ID;
- worker logs by earthquake ID;
- notification logs by subscription, delivery, or Telegram projection ID;
- provider logs by provider and external ID;
- correlation logs by canonical incident ID.

Metrics emitted by the admin HTTP adapter are consumed only by Prometheus and Grafana:
request count, latency, error count, authentication failures, authorization failures,
and action outcomes. No metrics panels are rendered in the admin interface.

## Configuration and deployment

Expected TOML deployment configuration:

```toml
[administration]
enabled = true
host = "eq.screaming.dog"
cloudflare_team_domain = "example.cloudflareaccess.com"
cloudflare_audience = "application-audience-tag"
bootstrap_owners = ["admin@example.com"]
grafana_base_url = "https://grafana.example.com"
```

These names are implemented and included in `config.example.toml`.
`administration.development_email` is an explicit development-only Access bypass;
configuration loading rejects it outside `app.environment = "development"`. Production
configuration files contain secrets, are mounted read-only, and are never committed.

The existing Docker image contains the embedded admin assets. The worker image and
binary remains identical, but non-API roles do not register admin routes. `make deploy`
continues to build one image, wait for PostgreSQL, apply Atlas migrations, and update
the API and worker containers. No separate frontend container, Node.js runtime,
broker, cache, or database is introduced.

Cloudflare Tunnel routes `eq.screaming.dog` to the existing API service. Cloudflare
Access applies the identity policy to `/admin` and its descendants before traffic
reaches the origin; `/api/*` remains public.

## Current implementation status

The first deployable, read-only milestone is implemented:

- Cloudflare Access RS256 verification with bounded JWKS size and cache lifetime;
- database-backed roles, owner bootstrap, host isolation, CSRF middleware, restrictive
  security headers, and disabled-route behavior;
- signed cursor pagination for incidents, subscriptions, notifications, and audit;
- incident filters and detail views for sources, observations, associations,
  provenance, revisions, and local intensity evaluations;
- masked subscription views that never query encrypted webhook secrets;
- unified bounded webhook-delivery and Telegram-projection views with payload previews;
- owner-only append-only audit history and contextual Grafana links.

An external map link is the current no-script fallback. Embedded MapLibre, audited
sensitive-data reveal, safe commands, role management, and manual correlation remain
in the later phases below.

## Implementation sequence

### Phase 1: specification and foundation

- finalize routes, roles, authorization matrix, and configuration;
- add `admin_role_bindings` and append-only `admin_audit_log` migrations;
- implement Cloudflare Access JWT verification with bounded JWKS refresh;
- implement RBAC, CSRF, security headers, request identity, and sensitive-field masking;
- add an embedded layout, navigation, error pages, and disabled-admin behavior.

### Phase 2: read-only operational entities

- incident list and detail;
- map and provider observations;
- revisions, provenance, associations, and intensity evaluations;
- subscription list and detail;
- notification list, detail, and message preview;
- audit list;
- Grafana and Loki deep links.

This is the first deployable milestone.

### Phase 3: safe commands

- delivery retry;
- subscription pause and disable;
- Telegram setup reminder;
- webhook subscription management and secret rotation;
- role-binding management;
- atomic audit for every command.

### Phase 4: manual correlation

- duplicate candidate inspection;
- merge and unmerge preview;
- version-checked owner confirmation;
- canonical state recalculation;
- immutable association and administrative audit history.

### Phase 5: production rollout

- Cloudflare Access application and policy;
- hostname Tunnel route and path-scoped Access application;
- CSP-compatible MapLibre and configured map style;
- Grafana admin HTTP/action panels and Loki links;
- staged read-only enablement before mutations;
- production runbook and rollback procedure.

## Verification requirements

The implementation must include:

- JWT verification tests using a local JWKS fixture or `httptest`, never live Cloudflare;
- authentication, authorization, and role-matrix tests;
- CSRF and origin validation tests;
- HTML escaping and CSP tests;
- tests proving secrets never appear in HTML or audit data;
- tests for masked and audited sensitive-data reveal;
- bounded pagination and strict form-decoding tests;
- PostgreSQL integration tests for role bindings and atomic audit records;
- delivery retry and subscription state-transition tests;
- correlation invariants and merge/unmerge integration tests;
- tests that `/` remains `404`, `/admin/*` is `404` when disabled, and unauthenticated
  enabled requests disclose no data;
- `make fmt`, `make test`, `go vet ./...`, and `make lint`.

Provider tests continue to use fixtures or `httptest`; admin development and tests do
not call live Cloudflare, map, Telegram, EMSC, USGS, Grafana, or Loki services.

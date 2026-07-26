# Microservice architecture

## Service boundaries

The application is deployed as independently scalable services from one Go repository:

```text
provider-worker (EMSC, USGS, GEOFON, KNDC)
    -> provider.observations.v1
    -> core
    -> PostgreSQL + transactional outbox
    -> incident.changes.v1
    -> notification-service -> Telegram, webhook, future channels
    -> api-service -> REST, WebSocket, private administration
```

One provider-worker binary is deployed once per provider. Each deployment has an
independent consumer-independent checkpoint and failure domain. EMSC's standing-order
and FDSN channels remain in the same EMSC deployment because they share the
authoritative EMSC identity.

Each deployment mounts its own read-only TOML configuration. Provider-worker files omit
database and notification credentials, while core, notification, and API receive only
the sections required by their ownership boundary. See [configuration.md](configuration.md).

Provider workers own provider HTTP/WebSocket access, parsing, normalization, bounded
recovery, and publication. They do not connect to the core PostgreSQL database. Every
message retains the original provider payload and identifies its normalization contract.

Core is the only writer for provider observations, source records, associations,
canonical incidents, canonical revisions, and correlation audit history. It consumes
provider messages with an inbox record in the same PostgreSQL transaction as domain
changes. Duplicate Kafka delivery therefore does not duplicate a source revision.

Canonical changes are inserted into an outbox in the same transaction as the canonical
database update. An outbox relay publishes them to Kafka and marks them delivered only
after broker acknowledgement. Consumers must tolerate a publish being repeated after
an acknowledgement ambiguity.

Notification-service owns subscription matching, notification projections, retry
policy, and channel delivery state. Telegram remains an adapter within that service
until independent channel scaling or isolation is needed.

API-service is a separate deployment. During the migration it may use a read-only role
against the core PostgreSQL database. A Kafka-built read projection can replace that
shared read path later without changing core ownership. Administrative commands that
mutate core state retain their legacy database path during the staged cutover. Before
read-only API credentials are enforced, they must move to an explicit authenticated core
command API rather than direct API-service database writes.

## Event contracts

`provider.observations.v1` is keyed by `provider:external_id`. It contains a stable
message ID, provider identity, observation channel, solution class, normalized nullable
fields, source timestamps, and the unchanged raw provider payload.

`incident.changes.v1` is keyed by canonical incident UUID. It contains the current
canonical version, the previous snapshot for update-trigger evaluation, ingestion and
baseline state, and a snapshot sufficient for downstream matching and read projections.

Contract fields are additive within a major version. Removing a field or changing its
meaning requires a new topic major version. Consumers must reject unsupported schema
names without committing their Kafka offsets.

## Delivery guarantees

Kafka delivery is at least once. Ordering is guaranteed only for one message key within
one partition. Provider message IDs are deterministic for a material provider
observation. Core inbox uniqueness provides idempotency; the core outbox closes the
PostgreSQL/Kafka dual-write gap. Kafka offsets are committed only after the database
transaction commits. A new consumer group starts at the earliest retained offset so a
service deployed after its producer can catch up; existing groups resume at their committed offsets.

## Migration sequence

1. Add versioned contracts, core inbox, and core outbox while retaining the existing
   in-process path.
2. Deploy provider-worker instances in shadow mode and compare their Kafka output with
   existing ingestion records.
3. Enable the core Kafka consumer and disable direct provider writes one provider at a
   time.
4. Move matching and delivery processing to notification-service.
5. Deploy API-service separately with read-only database credentials; introduce an
   independent read projection only when operationally justified.

Every cutover is reversible by provider or consumer group. Source observations and
association audit records are never rewritten or discarded during migration.

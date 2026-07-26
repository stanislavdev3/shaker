# Engineering guide

This repository is a Go microservice monorepo. Provider workers publish observations to Kafka; the core service consumes, correlates, audits, and persists them; the notification service consumes canonical incident changes and owns delivery processing; the API service serves read-only product and administrative queries. Domain packages must not import HTTP, Kafka, PostgreSQL, provider-specific code, or metrics.

Keep code in the existing `cmd`, `internal`, `api`, `migrations`, `deploy`, `docs`, and `testdata` boundaries. Do not add generic `utils`, `helpers`, or `common` packages. Do not introduce an ORM or cache. Kafka is the only application broker. Kafka contracts must be versioned and backward compatible. Provider workers must not access the core database. Only core may mutate canonical incidents, provider observations, associations, and their audit history. API database access is read-only; notification state belongs to the notification service. Cross-provider association must preserve provider observations, provenance, confidence, and an audit trail; never discard or overwrite source records during a merge.

Kafka delivery is at least once. Consumers must use durable inbox deduplication, and database-backed publishers must use a transactional outbox. Never rely on an atomic Kafka/PostgreSQL dual write. Partition provider observations by `(provider, external_id)` and canonical changes by incident ID so revisions remain ordered per identity.

After changes, run `make fmt`, `make test`, `go vet ./...`, and `make lint`. Integration tests require `test.integration.toml` pointing to isolated PostgreSQL/PostGIS and Kafka instances. Provider tests must use fixtures or `httptest`, never live provider services.

Use parameterized SQL, bounded I/O, context-aware calls, strict JSON request decoding, and a controllable clock for time-sensitive logic. Preserve nullable provider values. Never log or commit secrets. All code, comments, tests, configuration examples, and documentation must remain in English.
Application configuration must come only from strictly decoded TOML files. Do not add environment-variable configuration or secret overrides.

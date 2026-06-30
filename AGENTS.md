# Engineering guide

This repository is a Go modular monolith. Transport, USGS, and PostgreSQL adapters depend on application/domain packages; domain packages must not import HTTP, PostgreSQL, provider-specific code, or metrics.

Keep code in the existing `cmd`, `internal`, `api`, `migrations`, `deploy`, `docs`, and `testdata` boundaries. Do not add generic `utils`, `helpers`, or `common` packages. Do not introduce an ORM, external broker, cache, microservice, or cross-provider event merge.

After changes, run `make fmt`, `make test`, `go vet ./...`, and `make lint`. Repository integration tests require `TEST_DATABASE_URL` pointing to an isolated PostgreSQL database with PostGIS. Provider tests must use fixtures or `httptest`, never the live USGS service.

Use parameterized SQL, bounded I/O, context-aware calls, strict JSON request decoding, and a controllable clock for time-sensitive logic. Preserve nullable provider values. Never log or commit secrets. All code, comments, tests, configuration examples, and documentation must remain in English.

# Earthquake Backend Service

This service collects earthquake reports from USGS, normalizes them in PostgreSQL/PostGIS, exposes public JSON and GeoJSON APIs, and delivers signed webhook notifications. It reports events after USGS publishes them; it is not an earthquake early-warning system.

The deployment is a modular monolith with separate `api` and `worker` process roles. PostgreSQL is the source of truth, revision audit store, persistent provider checkpoint store, and transactional notification queue. No external broker or cache is required.

## Prerequisites and local startup

Install Docker with Compose. For host-based development, install Go 1.26.x, Node 22.x (for the frontend build), Atlas, PostgreSQL client tools, and golangci-lint.

```bash
cp .env.example .env
openssl rand -base64 32
# Put that value in SECRETS_ENCRYPTION_KEY and replace ADMIN_API_KEY.
docker compose up --build
```

The API is available at `http://localhost:8080`. PostgreSQL is intentionally not published to the host. The initial SQL migration is applied by the Postgres initialization hook on a new Compose volume. For an existing database, set `DATABASE_URL` and run:

```bash
make migrate
```

Open `http://localhost:8080/` for the embedded live monitoring UI — a React + MapLibre single-page app (source in `web/`, see [docs/frontend-spa.md](docs/frontend-spa.md)). It loads the current event page over HTTP and applies committed earthquake changes from `ws://localhost:8080/v1/stream`.

The frontend is a Vite app whose production build is embedded into the Go binary. Build it with `make frontend` before `go build`, or use `make build` which runs both. For frontend development with hot reload, run `cd web && npm install && npm run dev` (proxies `/v1` to `:8080`).

If the schema changed while retaining a local volume, use `docker compose down -v` only when discarding local data is acceptable.

## Commands and execution roles

```bash
earthquake-service api
earthquake-service worker
earthquake-service all
earthquake-service backfill --from 2026-01-01T00:00:00Z --to 2026-02-01T00:00:00Z
```

`all` is intended for local use. Compose runs `api` and `worker` separately. A backfill is split into configurable 24-hour windows, checkpoints after each window, resumes safely, and never creates notifications.

## Configuration

All configuration is supplied via environment variables. See [.env.example](.env.example) for the complete set. Production requires an HTTPS webhook URL, a strong administrative API key, and a base64-encoded 32-byte AES key. `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true` is rejected outside development.

The worker polls USGS every 60 seconds by default. Its first successful poll against an empty provider state is a baseline and suppresses notifications. Conditional request validators and checkpoints survive restarts.

## API examples

```bash
curl 'http://localhost:8080/v1/earthquakes?min_magnitude=5&limit=50'
curl 'http://localhost:8080/v1/earthquakes?latitude=40.1&longitude=74.2&radius_km=250'
curl -H 'Accept: application/geo+json' 'http://localhost:8080/v1/earthquakes'
curl 'http://localhost:8080/v1/earthquakes/00000000-0000-0000-0000-000000000000'
```

Administrative requests use `Authorization: Bearer <ADMIN_API_KEY>`. Create a subscription:

```bash
curl -X POST http://localhost:8080/v1/admin/notification-subscriptions \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"operations","webhook_url":"https://receiver.example/webhooks/earthquakes","minimum_magnitude":5,"notify_on_new":true}'
```

If the server generates the webhook secret it returns it once. Later reads never return it.

## Webhook verification

The receiver must compute HMAC-SHA256 over `<X-Earthquake-Timestamp>.<raw request body>` with the subscription secret and compare it in constant time with `X-Earthquake-Signature`. Track `X-Earthquake-Delivery-ID` to suppress duplicates. Any 2xx response acknowledges delivery.

```go
message := append([]byte(timestamp+"."), rawBody...)
mac := hmac.New(sha256.New, []byte(secret))
mac.Write(message)
expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
valid := subtle.ConstantTimeCompare([]byte(expected), []byte(received)) == 1
```

## Development and tests

```bash
make fmt
make test
go vet ./...
make lint
make openapi-check
make build
```

Integration tests require a migrated, isolated PostGIS database:

```bash
export TEST_DATABASE_URL='postgres://earthquake:earthquake@localhost:5432/earthquake_test?sslmode=disable'
for migration in migrations/*.sql; do psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration"; done
make test-integration
```

Frontend unit tests (Vitest) run with `make frontend-test` or `cd web && npm test`.

Generated OpenAPI server code is not committed: the current handlers are deliberately hand-written because the API is small, while `api/openapi.yaml` is linted in CI. `make generate` is deterministic and currently has no generated targets.

## Known limitations

- USGS is the only enabled provider, and cross-provider merging is intentionally absent.
- Notifications support webhooks only; email, FCM, and APNs are extension points, not implementations.
- Delivery is at least once. Receivers must deduplicate delivery IDs.
- Subscription PATCH treats omitted properties as unchanged; explicit JSON `null` cannot currently clear nullable filters.
- Public rate limiting is process-local and intentionally lightweight.
- Queue-depth gauges are registered but periodic database collection is not yet implemented.
- OpenTelemetry HTTP instrumentation uses the global provider; configuring an OTLP exporter is reserved for deployment integration.
- Realtime WebSocket delivery is best effort. Slow or disconnected browsers resynchronize by fetching the current HTTP API page after reconnecting.

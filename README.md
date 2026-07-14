# Earthquake Backend Service

This service collects low-latency and catalogue earthquake observations from EMSC and
USGS, normalizes and correlates them in PostgreSQL/PostGIS, exposes public JSON and
GeoJSON APIs, and delivers Telegram and signed webhook notifications. It is an
earthquake notification service, not a seismic early-warning network.

The deployment is a modular monolith with separate `api` and `worker` process roles. PostgreSQL is the source of truth, revision audit store, persistent provider checkpoint store, and transactional notification queue. No external broker or cache is required.

## Prerequisites and local startup

Install Docker with Compose. For host-based development, install Go 1.26.x, Atlas, PostgreSQL client tools, and golangci-lint.

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

If the schema changed while retaining a local volume, use `docker compose down -v` only when discarding local data is acceptable.

## Production image and deployment

The application repository owns the production Dockerfile and produces a
self-contained image with the Go service:

```bash
make image
```

The default image name is `shaker:latest`; override it with `IMAGE=...` when
publishing to a registry. The home-server deployment compose consumes this
prebuilt image and contains no application build context:

```bash
make deploy
make deploy IMAGE=registry.example/shaker:release-tag
```

`make deploy` builds the image, waits for the production PostgreSQL service,
applies pending Atlas migrations, and only then updates the API and worker
containers. If a migration fails, the application containers are not updated.
The migration runner uses the pinned `arigaio/atlas:1.2.3` image, so Atlas does
not need to be installed on the deployment host.

The first migration-aware deployment baselines the existing production schema
at `202607130001`. Override `DEPLOY_MIGRATION_BASELINE` only when adopting Atlas
for another pre-existing database; after Atlas creates its revision table, the
baseline option has no effect on normal deployments.

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

Set `EMSC_ENABLED=true` on worker roles to enable the EMSC standing-order
WebSocket for preliminary low-latency alerts and the EMSC FDSN catalogue for
confirmation and overlapping recovery. Both channels share the EMSC `unid`, so
confirmation edits the original Telegram message. See
[`docs/emsc.md`](docs/emsc.md) for rollout controls and the current
cross-provider correlation boundary.

Set `TELEGRAM_BOT_TOKEN` to enable the Telegram bot in the worker. Users send `/start`,
share their location, choose RU or EN, and select an expected local MMI threshold from
II through VI. See [docs/telegram.md](docs/telegram.md).

Set `TELEGRAM_GLOBAL_CHANNEL=@eqmonitor` to publish all normalized earthquake incidents
worldwide to a channel where the bot has post and edit administrator permissions.

## API examples

```bash
curl 'http://localhost:8080/api/earthquakes?min_magnitude=5&limit=50'
curl 'http://localhost:8080/api/earthquakes?latitude=40.1&longitude=74.2&radius_km=250'
curl -H 'Accept: application/geo+json' 'http://localhost:8080/api/earthquakes'
curl 'http://localhost:8080/api/earthquakes/00000000-0000-0000-0000-000000000000'
```

Public and mobile clients use the unversioned `/api` namespace. Administrative JSON
automation remains under the Cloudflare Access-protected `/admin/api` namespace.

Administrative requests use `Authorization: Bearer <ADMIN_API_KEY>`. Create a subscription:

```bash
curl -X POST http://localhost:8080/admin/api/notification-subscriptions \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"operations","webhook_url":"https://receiver.example/webhooks/earthquakes","minimum_magnitude":5,"notify_on_new":true}'
```

If the server generates the webhook secret it returns it once. Later reads never return it.

A private, server-rendered administration interface is available on `/admin` for the
API role when `ADMIN_ENABLED=true`. It verifies Cloudflare Access at the origin, then
applies database-backed viewer/operator/owner roles. Configure a dedicated
`ADMIN_HOST`, the Access team domain and audience, and at least one bootstrap owner.
Its scope, security model, screens, deployment, and remaining implementation sequence
are specified in [docs/admin.md](docs/admin.md). Metrics and logs remain exclusively
in Grafana and Loki.

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

Generated OpenAPI server code is not committed: the current handlers are deliberately hand-written because the API is small, while `api/openapi.yaml` is linted in CI. `make generate` is deterministic and currently has no generated targets.

## Known limitations

- EMSC and USGS observations are associated conservatively; ambiguous cross-provider
  candidates remain separate until stronger evidence or a future manual admin action
  is available. See `docs/event-correlation.md`.
- Notifications support Telegram and webhooks; email, FCM, and APNs are extension points, not implementations.
- Delivery is at least once. Receivers must deduplicate delivery IDs.
- Subscription PATCH treats omitted properties as unchanged; explicit JSON `null` cannot currently clear nullable filters.
- Public rate limiting is process-local and intentionally lightweight.
- Queue-depth gauges are registered but periodic database collection is not yet implemented.
- OpenTelemetry HTTP instrumentation uses the global provider; configuring an OTLP exporter is reserved for deployment integration.
- Realtime WebSocket delivery is best effort. Slow or disconnected clients must resynchronize through the HTTP API after reconnecting.

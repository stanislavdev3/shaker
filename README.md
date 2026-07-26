# Earthquake Backend Service

This service collects low-latency and catalogue earthquake observations from EMSC and
USGS, normalizes and correlates them in PostgreSQL/PostGIS, exposes public JSON and
GeoJSON APIs, and delivers Telegram and signed webhook notifications. It is an
earthquake notification service, not a seismic early-warning network.

The target deployment is a microservice architecture built from one Go repository.
Provider workers publish versioned observations to Kafka, core correlates and persists
them in PostgreSQL/PostGIS, notification-service owns delivery processing, and
api-service serves read traffic. The existing `worker` and `all` roles remain during
the provider-by-provider cutover. See [docs/microservices.md](docs/microservices.md).

## Prerequisites and local startup

Install Docker with Compose. For host-based development, install Go 1.26.x, Atlas, PostgreSQL client tools, and golangci-lint.

```bash
cp config.example.toml config.toml
openssl rand -base64 32
# Put that value in security.encryption_key and replace api.admin_api_key.
docker compose up --build
```

The API is available at `http://localhost:8080`. PostgreSQL is intentionally not published to the host. The initial SQL migration is applied by the Postgres initialization hook on a new Compose volume. Atlas remains the migration tool for an existing database.

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
earthquake-service --config /etc/shaker/config.toml api
earthquake-service --config /etc/shaker/config.toml core
earthquake-service --config /etc/shaker/config.toml provider-worker emsc
earthquake-service --config /etc/shaker/config.toml provider-worker usgs
earthquake-service --config /etc/shaker/config.toml provider-worker geofon
earthquake-service --config /etc/shaker/config.toml provider-worker kndc
earthquake-service --config /etc/shaker/config.toml notification
earthquake-service --config /etc/shaker/config.toml worker
earthquake-service --config /etc/shaker/config.toml all
earthquake-service --config /etc/shaker/config.toml backfill --from 2026-01-01T00:00:00Z --to 2026-02-01T00:00:00Z
```

The provider name is an explicit subcommand argument; its endpoint, limits, polling
interval, and state file come from the matching `providers.<name>` TOML table. Deploy
the same binary separately for `emsc`, `usgs`, `geofon`, and `kndc`. `core` consumes
provider observations and publishes canonical changes through a transactional outbox.
`notification` consumes canonical
changes, and runs Telegram, webhook, and subscription delivery processing. The legacy
`worker` and `all` roles support reversible migration.
A backfill is split into configurable 24-hour windows, checkpoints after each window,
resumes safely, and never creates notifications.

## Configuration

Application configuration is read exclusively from a TOML file selected by `--config`
(default `config.toml`); environment variables are not read and cannot override file values.
See [config.example.toml](config.example.toml) for every setting. The decoder rejects
unknown fields, malformed durations, and files larger than 1 MiB. Production requires
an HTTPS webhook URL, a strong administrative API key, and a base64-encoded 32-byte AES
key. Keep production files outside the repository with permissions limited to the
service account. `notification.webhook.allow_private_networks=true` is rejected outside
development. Production should use a separate least-privilege file per service; see
[docs/configuration.md](docs/configuration.md).

The worker polls USGS every 60 seconds by default. Its first successful poll against an empty provider state is a baseline and suppresses notifications. Conditional request validators and checkpoints survive restarts.

Run `provider-worker emsc` to enable the standing-order
WebSocket for preliminary low-latency alerts and the EMSC FDSN catalogue for
confirmation and overlapping recovery. Both channels share the EMSC `unid`, so
confirmation edits the original Telegram message. See
[`docs/emsc.md`](docs/emsc.md) for rollout controls and the current
cross-provider correlation boundary.

Deploy `provider-worker geofon` and `provider-worker kndc` to add
the GEOFON global FDSN catalogue and the KNDC Central Asia urgent bulletin. Both
catalogues use the same audited cross-provider correlation path as USGS and EMSC.
See [`docs/catalog-providers.md`](docs/catalog-providers.md) for source contracts,
polling controls, and KNDC's non-versioned endpoint caveat.

Set `notification.telegram.bot_token` to enable the Telegram bot. Users send `/start`,
share their location, choose RU or EN, and select an expected local MMI threshold from
II through VI. See [docs/telegram.md](docs/telegram.md).

Set `notification.telegram.global_channel = "@eqmonitor"` to publish all normalized earthquake incidents
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
API role when `administration.enabled = true`. It verifies Cloudflare Access at the origin, then
applies database-backed viewer/operator/owner roles. Configure a dedicated
`administration.host`, the Access team domain and audience, and at least one bootstrap owner.
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
cp test.integration.example.toml test.integration.toml
# Point database.url and kafka.broker at isolated test services, then apply migrations.
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

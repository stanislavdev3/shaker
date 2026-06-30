GO ?= go
GOCACHE ?= /tmp/earthquake-service-go-cache
export GOCACHE

.PHONY: build frontend frontend-test test test-unit test-integration lint fmt generate migrate migrate-down compose-up compose-down backfill openapi-check
frontend:
	cd web && npm ci && npm run build
	touch internal/httpapi/web/dist/.gitkeep
frontend-test:
	cd web && npm ci && npm test
build: frontend
	$(GO) build ./cmd/earthquake-service
test:
	$(GO) test ./...
test-unit:
	$(GO) test $$(go list ./... | grep -v /integration)
test-integration:
	$(GO) test -tags=integration ./internal/repository/postgres
lint:
	golangci-lint run ./...
fmt:
	gofmt -w $$(find cmd internal -name '*.go')
generate:
	$(GO) generate ./...
migrate:
	atlas migrate apply --env local
migrate-down:
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f deploy/down.sql
compose-up:
	docker compose up --build
compose-down:
	docker compose down
backfill:
	$(GO) run ./cmd/earthquake-service backfill --from "$$FROM" --to "$$TO"
openapi-check:
	docker run --rm -v "$$(pwd):/spec" redocly/cli:1.34.5 lint /spec/api/openapi.yaml

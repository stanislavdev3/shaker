GO ?= go
GOCACHE ?= /tmp/earthquake-service-go-cache
SERVER_DIR ?= ../../server
DEPLOY_COMPOSE := $(SERVER_DIR)/services/screaming-dog/docker-compose.yml
DEPLOY_ENV := $(SERVER_DIR)/.env
IMAGE ?= shaker:latest
export GOCACHE

.PHONY: build image frontend frontend-test test test-unit test-integration lint fmt generate migrate migrate-down compose-up compose-down backfill openapi-check deploy
frontend:
	cd web && npm ci && npm run build
	touch internal/httpapi/web/dist/.gitkeep
frontend-test:
	cd web && npm ci && npm test
build: frontend
	$(GO) build ./cmd/earthquake-service
image:
	docker build --pull -t "$(IMAGE)" .
test: frontend
	$(GO) test ./...
test-unit: frontend
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
deploy: image
	test -f "$(DEPLOY_COMPOSE)"
	test -f "$(DEPLOY_ENV)"
	SHAKER_IMAGE="$(IMAGE)" docker compose --env-file "$(DEPLOY_ENV)" -f "$(DEPLOY_COMPOSE)" up -d
	docker compose --env-file "$(DEPLOY_ENV)" -f "$(DEPLOY_COMPOSE)" ps

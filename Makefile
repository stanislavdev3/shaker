GO ?= go
GOCACHE ?= /tmp/earthquake-service-go-cache
SERVER_DIR ?= ../../server
DEPLOY_COMPOSE := $(SERVER_DIR)/services/screaming-dog/docker-compose.yml
DEPLOY_MIGRATE_COMPOSE := deploy/docker-compose.migrate.yml
DEPLOY_ENV := $(SERVER_DIR)/.env
IMAGE ?= shaker:latest
ATLAS_IMAGE ?= arigaio/atlas:1.2.3
DEPLOY_MIGRATION_BASELINE ?= 202607130001
export GOCACHE

.PHONY: build image test test-unit test-integration lint fmt generate migrate migrate-down compose-up compose-down backfill openapi-check deploy
build:
	$(GO) build ./cmd/earthquake-service
image:
	docker build --pull -t "$(IMAGE)" .
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
deploy: image
	test -f "$(DEPLOY_COMPOSE)"
	test -f "$(DEPLOY_ENV)"
	test -f "$(DEPLOY_MIGRATE_COMPOSE)"
	SHAKER_IMAGE="$(IMAGE)" docker compose --env-file "$(DEPLOY_ENV)" -f "$(DEPLOY_COMPOSE)" up -d --wait postgres
	SHAKER_REPOSITORY_DIR="$(CURDIR)" ATLAS_IMAGE="$(ATLAS_IMAGE)" DEPLOY_MIGRATION_BASELINE="$(DEPLOY_MIGRATION_BASELINE)" docker compose --env-file "$(DEPLOY_ENV)" -f "$(DEPLOY_COMPOSE)" -f "$(DEPLOY_MIGRATE_COMPOSE)" run --rm --no-deps migrate
	SHAKER_IMAGE="$(IMAGE)" docker compose --env-file "$(DEPLOY_ENV)" -f "$(DEPLOY_COMPOSE)" up -d
	docker compose --env-file "$(DEPLOY_ENV)" -f "$(DEPLOY_COMPOSE)" ps

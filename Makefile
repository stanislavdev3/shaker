GO ?= go
GOCACHE ?= /tmp/earthquake-service-go-cache
IMAGE ?= shaker:latest
INTEGRATION_CONFIG ?= test.integration.toml
export GOCACHE

.PHONY: build image test test-unit test-integration lint fmt generate migrate migrate-down compose-up compose-down backfill openapi-check release
build:
	$(GO) build ./cmd/earthquake-service
image:
	docker build --pull -t "$(IMAGE)" .
test:
	$(GO) test ./...
test-unit:
	$(GO) test $$(go list ./... | grep -v /integration)
test-integration:
	$(GO) test -tags=integration ./internal/repository/postgres -args -integration-config "$(abspath $(INTEGRATION_CONFIG))"
	$(GO) test -tags=integration ./internal/kafka -args -integration-config "$(abspath $(INTEGRATION_CONFIG))"
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
release: test openapi-check
	@test -n "$(VERSION)" || (echo 'Usage: make release VERSION=v0.2.0' >&2; exit 1)
	@case "$(VERSION)" in v[0-9]*.[0-9]*.[0-9]*) ;; *) echo 'VERSION must look like v0.2.0' >&2; exit 1 ;; esac
	@test -z "$$(git status --porcelain)" || (echo 'Working tree must be clean' >&2; exit 1)
	@test "$$(git branch --show-current)" = master || (echo 'Release must be created from master' >&2; exit 1)
	@! git rev-parse -q --verify "refs/tags/$(VERSION)" >/dev/null || (echo 'Tag $(VERSION) already exists' >&2; exit 1)
	git push origin master
	git tag -a "$(VERSION)" -m "Release $(VERSION)"
	git push origin "$(VERSION)"

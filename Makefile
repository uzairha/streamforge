SHELL := /bin/bash
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

SERVICES := ingester normalizer aggregator api
COMPOSE  := docker compose -f deploy/compose/docker-compose.yml

PROTOC_GEN_GO_VERSION         := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION    := v1.5.1
PROTOC_GEN_GRPC_GATEWAY_VERSION := v2.30.0

.PHONY: help
help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: tools
tools: ## Install pinned codegen plugins into $(GOBIN)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@$(PROTOC_GEN_GRPC_GATEWAY_VERSION)

.PHONY: proto
proto: ## Lint protobuf and regenerate ./gen
	buf lint
	buf generate

.PHONY: build
build: ## Build every service binary into ./bin
	@mkdir -p bin
	@for s in $(SERVICES); do echo "build $$s"; go build -o bin/$$s ./cmd/$$s; done

.PHONY: test
test: ## Run all tests with the race detector (integration tests need Docker)
	go test -race -count=1 ./...

.PHONY: test-unit
test-unit: ## Run only fast unit tests (no Docker)
	go test -race -count=1 -short ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: run-%
run-%: ## Run one service, e.g. make run-ingester
	go run ./cmd/$*

.PHONY: up
up: ## Start local infra (Redpanda, TimescaleDB, Prometheus, Grafana, Jaeger)
	$(COMPOSE) up -d

.PHONY: down
down: ## Stop local infra
	$(COMPOSE) down

.PHONY: ps
ps: ## Show local infra status
	$(COMPOSE) ps

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin

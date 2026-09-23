SHELL := /bin/bash
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

SERVICES := ingester normalizer aggregator api
COMPOSE  := docker compose -f deploy/compose/docker-compose.yml
COMPOSE_FULL := $(COMPOSE) -f deploy/compose/docker-compose.services.yml

IMAGE_REGISTRY ?= streamforge
IMAGE_TAG      ?= dev
KIND_CLUSTER   ?= streamforge
HELM_RELEASE   ?= streamforge
HELM_NAMESPACE ?= streamforge
CHART          := deploy/helm/streamforge

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

.PHONY: up-full
up-full: ## Start infra AND the four services as containers
	$(COMPOSE_FULL) up -d

.PHONY: down-full
down-full: ## Stop the full containerised stack
	$(COMPOSE_FULL) down

.PHONY: images
images: $(addprefix image-,$(SERVICES)) ## Build a container image for every service

.PHONY: image-%
image-%: ## Build one service image, e.g. make image-api
	docker build --build-arg SERVICE=$* -t $(IMAGE_REGISTRY)/$*:$(IMAGE_TAG) .

.PHONY: kind-up
kind-up: ## Create the local kind cluster
	kind create cluster --name $(KIND_CLUSTER) --config deploy/kind/kind-config.yaml

.PHONY: kind-down
kind-down: ## Delete the local kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: kind-load
kind-load: images ## Build images and side-load them into kind (no registry)
	@for s in $(SERVICES); do \
		echo "load $(IMAGE_REGISTRY)/$$s:$(IMAGE_TAG)"; \
		kind load docker-image $(IMAGE_REGISTRY)/$$s:$(IMAGE_TAG) --name $(KIND_CLUSTER); \
	done

.PHONY: deploy
deploy: ## Install/upgrade the Helm release on the current kube context
	helm upgrade --install $(HELM_RELEASE) $(CHART) \
		--namespace $(HELM_NAMESPACE) --create-namespace \
		--values $(CHART)/values-kind.yaml \
		--wait --timeout 5m

.PHONY: undeploy
undeploy: ## Uninstall the Helm release
	helm uninstall $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

.PHONY: helm-lint
helm-lint: ## Lint and render the chart
	helm lint $(CHART)
	helm template $(HELM_RELEASE) $(CHART) --values $(CHART)/values-kind.yaml >/dev/null

.PHONY: k8s-status
k8s-status: ## Show pods in the release namespace
	kubectl get pods,svc -n $(HELM_NAMESPACE)

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin

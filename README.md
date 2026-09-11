# StreamForge

A real-time blockchain event pipeline in Go. Chain events are ingested over a
streaming source, published to Kafka, normalized and enriched by a consumer
group, folded into windowed aggregates, and served over a gRPC API with a
REST/JSON gateway. The whole stack runs locally on Docker Compose and deploys to
Kubernetes via a Helm chart, with Prometheus/Grafana metrics and OpenTelemetry
tracing.

> Status: **M2 — live Ethereum source.** `SOURCE=ethereum` subscribes to
> `newHeads` over the keyless public endpoint and, when `ETH_FETCH_BODIES=true`
> (the default), fetches each block's body to emit one event per transaction
> too. Connection drops reconnect with exponential backoff and full jitter
> (`ETH_MAX_BACKOFF`), resetting once a connection has stayed up 60s; a bounded
> dedup window (`ETH_DEDUP_WINDOW`) suppresses events replayed by a resubscribe
> or an endpoint hiccup. The normalizer/aggregator real implementations, the
> gRPC server, and the Helm deploy land in M3–M6 (see [Roadmap](#roadmap)).

## Architecture

```mermaid
flowchart LR
  subgraph sources[Event sources]
    syn[Synthetic generator]
    eth[Ethereum WebSocket RPC]
  end
  syn & eth --> ing[ingester]
  ing -- raw-events --> k[(Kafka / Redpanda)]
  k -- raw-events --> norm[normalizer]
  norm -- normalized-events --> k
  k -- normalized-events --> agg[aggregator]
  agg -- aggregates --> k
  agg --> ts[(TimescaleDB)]
  k -- normalized-events --> api[api  gRPC + REST]
  ts --> api
  api --> client[clients / demo dashboard]
  ing & norm & agg & api -.metrics.-> prom[(Prometheus)]
  prom --> graf[Grafana]
  ing & norm & agg & api -.traces.-> jae[Jaeger]
```

| Service      | Role |
|--------------|------|
| `ingester`   | Reads an event source, decodes to a canonical `ChainEvent`, produces to `raw-events` |
| `normalizer` | Consumer group on `raw-events`; validates + enriches; produces to `normalized-events` |
| `aggregator` | Windowed stream processing over `normalized-events`; writes closed windows to `aggregates` and TimescaleDB |
| `api`        | gRPC server (server-streaming live events, aggregate queries, stats) + grpc-gateway REST |

The event source is an interface with two implementations: a **synthetic
generator** (no network, drives CI, demos, and load tests) and a **live
Ethereum** `newHeads` subscription against a keyless public endpoint.

## Tech

Go 1.27 · gRPC · Protocol Buffers (`buf`) · Kafka API via Redpanda · TimescaleDB ·
Prometheus · Grafana · OpenTelemetry · Jaeger · Docker · Kubernetes · Helm ·
golangci-lint · GitHub Actions

## Prerequisites

- Go 1.27+
- Docker + Docker Compose
- `buf` (`brew install buf`)
- `golangci-lint` (`brew install golangci-lint`)

## Quickstart

```bash
make tools       # install pinned protoc-gen-go / protoc-gen-go-grpc
make proto       # lint protobuf + regenerate ./gen
make build       # build all four service binaries into ./bin
make test        # go test -race ./...  (integration tests need Docker)
make test-unit   # go test -race -short ./...  (fast, no Docker)
make lint        # golangci-lint

make up          # start Redpanda, TimescaleDB, Prometheus, Grafana, Jaeger
make run-ingester   # decode synthetic events -> produce to raw-events
SOURCE=ethereum make run-ingester   # subscribe to live Ethereum newHeads instead
make down
```

The ingester serves Prometheus metrics and a liveness probe on `METRICS_ADDR`
(`:2112` by default): `GET /metrics`, `GET /healthz`. Key series:
`streamforge_events_ingested_total{chain,type}`,
`streamforge_events_produced_total{topic}`,
`streamforge_produce_errors_total{topic}`, `streamforge_source_up`,
`streamforge_source_reconnects_total`, `streamforge_events_deduped_total`.

Local endpoints once `make up` is running:

| Service           | URL |
|-------------------|-----|
| Redpanda broker   | `localhost:19092` |
| Redpanda Console   | http://localhost:8085 |
| TimescaleDB       | `localhost:5432` (`streamforge` / `streamforge`) |
| Prometheus        | http://localhost:9091 |
| Grafana           | http://localhost:3000 (anonymous admin) |
| Jaeger UI         | http://localhost:16686 |

## Configuration

All services are configured through environment variables; see
[`.env.example`](.env.example) for the full list and defaults. `config.Load`
fails fast on malformed or out-of-range values.

## Layout

```
cmd/<service>/      service entrypoints
internal/config/    env-driven configuration + validation
internal/source/    event source interface + synthetic generator
internal/kafka/     franz-go producer wrapper (protobuf values, acks=all)
internal/telemetry/ structured logging + Prometheus metrics / health server
proto/              protobuf contracts (buf)
gen/                generated Go (committed; CI checks it is current)
deploy/compose/     local infra stack
deploy/prometheus/  scrape config
deploy/grafana/     datasource + dashboard provisioning
```

## Roadmap

| Milestone | Scope |
|-----------|-------|
| **M0** | Scaffold, protobuf contracts, config/telemetry, synthetic source, CI |
| **M1** | Ingester → Kafka producer, Prometheus metrics, health endpoint, integration tests (testcontainers) |
| **M2** | Live Ethereum WebSocket source: reconnect/backoff, dedup |
| M3 | Normalizer + aggregator: consumer groups, tumbling windows, TimescaleDB, checkpointing |
| M4 | gRPC API (server-streaming) + grpc-gateway REST + demo dashboard + API-key auth |
| M5 | Dockerfiles, Helm chart, `kind` cluster, Grafana dashboards, OpenTelemetry + Jaeger |
| M6 | k6 load test, tuning (partitions / batching / parallelism), end-to-end lag write-up |

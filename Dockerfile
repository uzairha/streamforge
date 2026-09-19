# syntax=docker/dockerfile:1.7
#
# One Dockerfile builds all four services: the build stage differs only by
# which ./cmd package it compiles, so a base-image bump happens in one place
# instead of four.
#
#   docker build --build-arg SERVICE=ingester -t streamforge/ingester:dev .
#
# Every dependency is pure Go (pgx, franz-go, prometheus, otel), so the binary
# links statically with CGO off and runs on distroless/static — no libc, no
# shell, no package manager in the runtime image.

ARG GO_VERSION=1.27.1

FROM golang:${GO_VERSION} AS build
ARG SERVICE
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
# -trimpath keeps build paths out of the binary; -s -w drop DWARF and the
# symbol table. If you ever need delve or symbolised pprof inside a container,
# drop -s -w rather than switching base images.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "${SERVICE}" && \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w" \
      -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
# The :nonroot tag bakes in UID 65532, which is what lets the Helm chart set
# runAsNonRoot without having to pick a UID itself.
USER nonroot:nonroot
ENTRYPOINT ["/service"]

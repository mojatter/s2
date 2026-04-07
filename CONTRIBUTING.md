# Contributing

## Prerequisites

- Go 1.24+
- Docker (for E2E tests)

## Build

```sh
make build
```

## Tests

```sh
# Unit tests only (fast)
make test

# Integration tests — starts an in-process HTTP server, exercises S3 API via AWS SDK
make test-integration

# E2E tests — builds a Docker image, runs AWS CLI against it
make test-e2e
```

`make test` uses `-short` to skip integration tests. Running `go test ./...` without `-short` includes them.

## Design notes

Past API design decisions and audits live under [`docs/`](docs/). They are not user-facing documentation, but they record *why* certain shapes were chosen and are useful when proposing changes:

- [`docs/api-audit.md`](docs/api-audit.md) — public API audit and verdicts heading into the v0.2 stabilization.

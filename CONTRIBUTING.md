# Contributing

## Prerequisites

- Go 1.26+
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

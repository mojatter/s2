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

## Terminology

Use these names consistently in documentation, commit messages, and PR descriptions:

| Referring to | Write it as |
|---|---|
| The Go package and `Storage` interface that callers import | **S2 library** (prose), `s2` (code) |
| The standalone S3-compatible daemon shipped as a CLI / Docker image | **S2 Server** (prose), `s2-server` (command, package path) |
| The project as a whole (library + server) | **S2** |

```
                ┌───────────────────────────────┐
                │              S2               │  ← umbrella project
                └───────────────────────────────┘
                  │                         │
      ┌───────────┴──────────┐   ┌──────────┴───────────┐
      │      S2 library      │◄──┤      S2 Server       │
      │   (Go package `s2`)  │   │  (`s2-server` CLI /  │
      │                      │   │   Docker image)      │
      │  Storage interface   │   │                      │
      │  + value types       │   │  S3 API on :9000     │
      │                      │   │                      │
      └──────────┬───────────┘   └──────────────────────┘
                 │
                 │ implemented by
                 ▼
         ┌───────────────────────────────┐
         │   Backends                    │
         │   osfs · memfs · s3 · gcs ·   │
         │   azblob                      │
         └───────────────────────────────┘
```

Avoid calling the library a "client" — it includes backends like `osfs` and `memfs` that do not connect to anything, so "client" wrongly implies it is a consumer of S2 Server.

## Design notes

Past API design decisions and audits live under [`docs/`](docs/). They are not user-facing documentation, but they record *why* certain shapes were chosen and are useful when proposing changes:

- [`docs/api-audit.md`](docs/api-audit.md) — public API audit and verdicts heading into the v0.2 stabilization.

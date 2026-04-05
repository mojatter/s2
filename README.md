# S2 — Simple Storage

S2 is a lightweight object storage library and S3-compatible server written in Go.
It provides a unified interface for multiple storage backends and an embeddable S3-compatible server — all in a single package.

## Features

- **Unified Storage Interface** — One API for local filesystem, in-memory, and AWS S3 backends
- **S3-Compatible Server** — Serve any backend over S3 APIs; a drop-in replacement for MinIO in local development
- **Lightweight** — Minimal dependencies, single binary, `go install` ready
- **Pluggable Backends** — Register storage implementations with a blank import
- **Web Console** — Built-in browser interface for managing buckets and objects

## Install

```sh
go get github.com/mojatter/s2
```

To install the S2 server CLI:

```sh
go install github.com/mojatter/s2/cmd/s2-server@latest
```

## Quick Start

### As a Library

Define your storage backends in a JSON config file:

```json
{
  "assets": {
    "type": "osfs",
    "root": "/var/data/assets"
  },
  "backups": {
    "type": "s3",
    "root": "my-backup-bucket"
  }
}
```

Load and use them with `s2env`:

```go
package main

import (
	"context"
	"fmt"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2env"
)

func main() {
	ctx := context.Background()

	// Load all storages from config file
	storages, err := s2env.Load(ctx, "s2.json")
	if err != nil {
		panic(err)
	}

	// Use a named storage
	assets := storages["assets"]

	// Put an object
	obj := s2.NewObjectBytes("hello.txt", []byte("Hello, S2!"))
	if err := assets.Put(ctx, obj); err != nil {
		panic(err)
	}

	// List objects
	objects, _, err := assets.List(ctx, "", 100)
	if err != nil {
		panic(err)
	}
	for _, o := range objects {
		fmt.Println(o.Name())
	}
}
```

`s2env` automatically registers all built-in backends (`osfs`, `memfs`, `s3`), so no blank imports are needed.

### As a Local S3 Server

Start the server:

```sh
s2-server
# Listening on :9000
```

Then access it with any S3 client:

```go
package main

import (
	"context"
	"fmt"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/s3" // Register S3 backend
)

func main() {
	ctx := context.Background()
	strg, err := s2.NewStorage(ctx, s2.Config{
		Type: s2.TypeS3,
		Root: "my-bucket",
		S3: &s2.S3Config{
			EndpointURL: "http://localhost:9000/s3api",
		},
	})
	if err != nil {
		panic(err)
	}
	objects, _, err := strg.List(ctx, "", 1000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", objects)
}
```

Or use the AWS CLI:

```sh
aws --endpoint-url http://localhost:9000/s3api s3 ls
aws --endpoint-url http://localhost:9000/s3api s3 cp ./file.txt s3://my-bucket/file.txt
```

## Storage Backends

| Type | Import | Description |
|------|--------|-------------|
| `osfs` | `github.com/mojatter/s2/fs` | Local filesystem storage |
| `memfs` | `github.com/mojatter/s2/fs` | In-memory filesystem (great for testing) |
| `s3` | `github.com/mojatter/s2/s3` | AWS S3 (and any S3-compatible service) |

Backends are registered via blank imports. Import only what you need:

```go
import (
	_ "github.com/mojatter/s2/fs" // osfs + memfs
	_ "github.com/mojatter/s2/s3" // AWS S3
)
```

## S3 Backend Configuration

When using the `s3` backend, you can provide S3-specific settings via `S3Config`. Any field left empty falls back to the AWS SDK defaults (environment variables, `~/.aws/config`, IAM roles, etc.).

```go
strg, err := s2.NewStorage(ctx, s2.Config{
    Type: s2.TypeS3,
    Root: "my-bucket/optional-prefix",
    S3: &s2.S3Config{
        EndpointURL:    "http://localhost:9000/s3api",
        Region:         "ap-northeast-1",
        AccessKeyID:    "minioadmin",
        SecretAccessKey: "minioadmin",
    },
})
```

With `s2env`, use the `"s3"` key in JSON:

```json
{
  "local": {
    "type": "s3",
    "root": "dev-bucket",
    "s3": {
      "endpoint_url": "http://localhost:9000/s3api",
      "access_key_id": "minioadmin",
      "secret_access_key": "minioadmin"
    }
  },
  "prod": {
    "type": "s3",
    "root": "prod-bucket",
    "s3": {
      "region": "ap-northeast-1"
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `endpoint_url` | Custom S3-compatible endpoint URL |
| `region` | AWS region (e.g. `ap-northeast-1`) |
| `access_key_id` | AWS access key ID |
| `secret_access_key` | AWS secret access key |

When `S3Config` is nil or all fields are empty, the standard AWS SDK credential chain is used.

## Storage Interface

```go
type Storage interface {
	Type() Type
	Sub(ctx context.Context, prefix string) (Storage, error)
	List(ctx context.Context, prefix string, limit int) ([]Object, []string, error)
	ListAfter(ctx context.Context, prefix string, limit int, after string) ([]Object, []string, error)
	ListRecursive(ctx context.Context, prefix string, limit int) ([]Object, error)
	ListRecursiveAfter(ctx context.Context, prefix string, limit int, after string) ([]Object, error)
	Get(ctx context.Context, name string) (Object, error)
	Exists(ctx context.Context, name string) (bool, error)
	Put(ctx context.Context, obj Object) error
	PutMetadata(ctx context.Context, name string, metadata Metadata) error
	Copy(ctx context.Context, src, dst string) error
	Move(ctx context.Context, src, dst string) error
	Delete(ctx context.Context, name string) error
	DeleteRecursive(ctx context.Context, prefix string) error
	SignedURL(ctx context.Context, name string, ttl time.Duration) (string, error)
}
```

## Server Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S2_SERVER_CONFIG` | — | Path to JSON config file |
| `S2_SERVER_LISTEN` | `:9000` | Listen address |
| `S2_SERVER_TYPE` | `osfs` | Storage backend type |
| `S2_SERVER_ROOT` | `/var/lib/s2` | Root directory for bucket data |

`S2_SERVER_LISTEN`, `S2_SERVER_TYPE`, and `S2_SERVER_ROOT` take precedence over the config file.
Other settings (such as S3 backend options) are not configurable via environment variables — use `S2_SERVER_CONFIG` to point to a JSON config file instead.

### Config File

```json
{
  "listen": ":9000",
  "type": "osfs",
  "root": "/var/lib/s2"
}
```

```sh
s2-server -f config.json
```

### S3 API Endpoints

| Method | Path | Operation |
|--------|------|-----------|
| GET | `/s3api` | ListBuckets |
| GET | `/s3api/{bucket}` | ListObjects |
| GET | `/s3api/{bucket}/{key...}` | GetObject |
| HEAD | `/s3api/{bucket}/{key...}` | HeadObject |
| PUT | `/s3api/{bucket}/{key...}` | PutObject |
| DELETE | `/s3api/{bucket}/{key...}` | DeleteObject |

## Why S2?

MinIO was the go-to S3-compatible server for local development, but it entered maintenance mode in December 2025 and was archived in February 2026. S2 fills this gap with a different philosophy:

- **Library-first** — Use S2 as a Go library with a clean interface, or run it as a server. Most alternatives are server-only.
- **Truly lightweight** — Single binary, no external dependencies, starts in milliseconds.
- **Test-friendly** — Use `memfs` backend for fast, isolated tests without Docker or external processes.

## License

MIT

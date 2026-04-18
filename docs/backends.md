# Backend Configuration

Configuration options and authentication details for each backend. Each backend is configured via `s2.Config`; the JSON examples below show what that struct looks like encoded.

To load multiple named backends from a single JSON file, use [`s2env`](https://pkg.go.dev/github.com/mojatter/s2/s2env) — its top-level object maps storage names to `s2.Config` values.

## OSFS

Local filesystem storage.

```go
strg, err := s2.NewStorage(ctx, s2.Config{
    Type: s2.TypeOSFS,
    Root: "/var/data/my-bucket",
})
```

As JSON:

```json
{
  "type": "osfs",
  "root": "/var/data/my-bucket"
}
```

| Field | Description |
|-------|-------------|
| `root` | Filesystem path under which objects are stored |

Access control is governed by filesystem permissions.

## MEMFS

In-memory storage designed for **tests and local development** — nothing is persisted. Swap in `memfs` where you would otherwise start a Docker container or create a temp directory for tests.

```go
strg, err := s2.NewStorage(ctx, s2.Config{
    Type: s2.TypeMemFS,
})
```

As JSON:

```json
{ "type": "memfs" }
```

`memfs` takes no configuration fields.

**Production caveats** — `memfs` holds every object body in process memory. It is **not** intended for production workloads:

- All objects live in RAM for the lifetime of the process; nothing is persisted.
- The default upload limit is **16 MiB** (vs. 5 GiB for `osfs`/`s3`) to protect the host from accidental OOM. Set `S2_SERVER_MAX_UPLOAD_SIZE` (or `Config.MaxUploadSize`) to raise it if you genuinely need larger uploads against memfs.
- There is no total-memory budget or backpressure across concurrent uploads.

If you need to handle large files, use `osfs` or `s3` instead.

## S3

When using the `s3` backend, you can provide S3-specific settings via `S3Config`. Any field left empty falls back to the AWS SDK defaults (environment variables, `~/.aws/config`, IAM roles, etc.).

```go
strg, err := s2.NewStorage(ctx, s2.Config{
    Type: s2.TypeS3,
    Root: "my-bucket/optional-prefix",
    S3: &s2.S3Config{
        EndpointURL:    "http://localhost:9000",
        Region:         "ap-northeast-1",
        AccessKeyID:    "s2user",
        SecretAccessKey: "s2password",
    },
})
```

As JSON:

```json
{
  "type": "s3",
  "root": "my-bucket/optional-prefix",
  "s3": {
    "endpoint_url": "http://localhost:9000",
    "region": "ap-northeast-1",
    "access_key_id": "s2user",
    "secret_access_key": "s2password"
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

## GCS

When using the `gcs` backend, authentication uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) by default. Run `gcloud auth application-default login` for local development.

```go
strg, err := s2.NewStorage(ctx, s2.Config{
    Type: s2.TypeGCS,
    Root: "my-bucket/optional-prefix",
    // GCS: nil — ADC is used automatically
})
```

To use a service account key file:

```go
strg, err := s2.NewStorage(ctx, s2.Config{
    Type: s2.TypeGCS,
    Root: "my-bucket",
    GCS: &s2.GCSConfig{
        CredentialsFile: "/path/to/service-account.json",
    },
})
```

As JSON:

```json
{
  "type": "gcs",
  "root": "my-bucket/assets",
  "gcs": {
    "credentials_file": "/path/to/service-account.json"
  }
}
```

| Field | Description |
|-------|-------------|
| `credentials_file` | Path to a service account JSON key file |

## Azure Blob Storage

When using the `azblob` backend, you can authenticate with a connection string, shared key, or [DefaultAzureCredential](https://learn.microsoft.com/en-us/azure/developer/go/azure-sdk-authentication).

```go
// Shared key authentication
strg, err := s2.NewStorage(ctx, s2.Config{
    Type: s2.TypeAzblob,
    Root: "my-container/optional-prefix",
    Azblob: &s2.AzblobConfig{
        AccountName: "mystorageaccount",
        AccountKey:  "base64-encoded-key",
    },
})
```

As JSON:

```json
{
  "type": "azblob",
  "root": "my-container",
  "azblob": {
    "account_name": "mystorageaccount",
    "account_key": "base64-encoded-key"
  }
}
```

| Field | Description |
|-------|-------------|
| `account_name` | Azure storage account name |
| `account_key` | Shared key for the storage account |
| `connection_string` | Full Azure Storage connection string (takes precedence over name+key) |

Authentication priority: `connection_string` > `account_name`+`account_key` > DefaultAzureCredential.

## Combining backends with s2env

To manage several named storages from a single JSON file, use [`s2env`](https://pkg.go.dev/github.com/mojatter/s2/s2env). Its top-level object is a map of storage name → `s2.Config`, so each entry takes the same shape as the per-backend examples above:

```json
{
  "assets": {
    "type": "osfs",
    "root": "/var/data/assets"
  },
  "tests": {
    "type": "memfs"
  },
  "uploads": {
    "type": "s3",
    "root": "uploads-bucket",
    "s3": {
      "region": "ap-northeast-1"
    }
  },
  "backups": {
    "type": "gcs",
    "root": "my-backups",
    "gcs": {
      "credentials_file": "/etc/s2/sa.json"
    }
  }
}
```

Load and access them by name:

```go
storages, err := s2env.Load(ctx, "s2.json")
if err != nil {
    panic(err)
}
assets := storages["assets"]
```

`s2env` registers all built-in backends automatically — no blank imports required.

## Cherry-picking backends with s2.LoadConfigsFile

`s2env` is convenient because it auto-registers every built-in backend, but that pulls in the AWS, GCS, and Azure SDKs as transitive dependencies. If you want to control which backends — and which SDKs — your binary depends on, use `s2.LoadConfigsFile` from the main package and blank-import only the backends you need:

```go
import (
    "github.com/mojatter/s2"
    _ "github.com/mojatter/s2/fs" // osfs + memfs only — no cloud SDKs pulled in
    _ "github.com/mojatter/s2/s3" // add only what you actually use
)

// ...
configs, err := s2.LoadConfigsFile("s2.json")
if err != nil {
    panic(err)
}
storages, err := configs.Storages(ctx)
if err != nil {
    panic(err)
}
assets := storages["assets"]
```

Any backend type referenced in the JSON that has not been blank-imported will fail at `Storages(ctx)` time.

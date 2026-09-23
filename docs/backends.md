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

## Object names

Every name a `Storage` method takes is used as written: it must satisfy [`fs.ValidPath`](https://pkg.go.dev/io/fs#ValidPath) and not be `"."`. A name that only resolves after cleaning — `../x`, `/x`, `./x`, `a//b`, `x/` — is rejected rather than folded, so the name a caller authorizes is the name the backend reads and writes. Rejections wrap `s2.ErrInvalidName`; s2-server answers them with `400 InvalidArgument`.

`ListOptions.Prefix`, `ListOptions.StartAfter`, `DeleteRecursive` and `Sub` select objects rather than name one, so they also take `""` and one trailing `/`.

On `s3`, `gcs` and `azblob`, `Exists("")` is the one exception: it reports the storage root, which exists by construction, without a round trip. `"/"` is a spelling of the same thing and is refused like any other non-canonical name.

`fs.ValidPath` requires valid UTF-8, which means a key holding raw bytes that are not UTF-8 is refused even on `s3`, `gcs` and `azblob`, where the provider itself would store it.

A key that an `s3`, `gcs` or `azblob` root already holds in a non-canonical form — a zero-byte `photos/` folder marker written by another tool, or a key holding bytes that are not UTF-8 — is still listed, but `Get`, `Delete`, `Copy` and `SignedURL` refuse it. s2 addresses names through `path.Join`, which never produces a trailing slash, so such a key was never reached as spelled: the call resolved to the neighbouring object instead. The refusal replaces a wrong answer with an error. Removing one takes `DeleteRecursive` over the enclosing prefix, or a tool that speaks the provider's API directly.

On `osfs` and `memfs` the element `.meta` is reserved at every depth: it holds the JSON sidecar of another object, so neither `.meta/a.txt` nor `docs/.meta/a.txt` is an object of its own and both are refused. Every depth, because a `Sub` writes its own sidecars beside the names it scopes, and a listing hides the directory wherever it appears — a name reaching through one would be stored and read but never listed. `Sub` refuses it too, so no caller-supplied prefix reaches the sidecars; s2-server keeps its own per-bucket state in `.buckets/`, an ordinary prefix on every backend, rather than reaching in.

An `osfs` or `memfs` root may already hold a key under `.meta`: nothing refused the name before v0.18.1, so an s2-server of that vintage stored one on request. Such a key is refused now, and no listing ever reported it. It cannot be read back, because s2 cannot tell it apart from the sidecar of the object beside it — that indistinguishability is what the reservation closes. Removing one takes `DeleteRecursive` over the enclosing folder, deleting the bucket, or, on `osfs`, deleting the file from the root directory.

A third-party `Storage` should call `s2.ValidateName` and `s2.ValidatePrefix` at the entry of every method that takes a name or a prefix; `s2test.TestStorageNameEscape` checks that it does.

## Content-Type and ETag

Both are attributes of `s2.Object`, not entries in `Metadata()`. Each backend answers them from what it stores natively, and a `List` result carries the ETag without extra requests. Content-Type is different: S3's listing does not return it, so on an `s3` root every listed object reports `""` and only `Get` has the real value.

| Backend | Content-Type when the caller supplies one | Content-Type when the caller supplies none | ETag |
|-------|-------------|-------------|-------------|
| `osfs`, `memfs` | Stored in the JSON sidecar | Nothing is stored; `ContentType()` is `""` | MD5 of the body, computed on `Put`; `"<mtime hex>-<size hex>"` when no sidecar holds one |
| `s3` | Stored as the object's `Content-Type` | The provider's own default, `binary/octet-stream` | The provider's `ETag` |
| `gcs` | Stored as the object's `contentType` | Nothing is stored; `ContentType()` is `""` | The MD5 attribute as quoted hex, else the provider's opaque `Etag` |
| `azblob` | Stored as the blob's `Content-Type` | The provider's own default, `application/octet-stream` | The `Content-MD5` s2 sets on upload, else the provider's opaque `ETag` |

When `ContentType()` is `""`, s2-server guesses from the key's extension and falls back to `binary/octet-stream`. Stored objects answer `""` only on `osfs`, `memfs` and `gcs`, so the same body uploaded to an `s3` or `azblob` root answers that provider's default and never reaches the guess.

The console's upload form is the one place that guesses before storing: it keeps the type the browser sent, unless that is missing or `application/octet-stream`, in which case it stores the extension's type — and nothing at all when the key's extension says nothing. The S3 API stores only what the client sent, so the same key can end up with a stored type through the console and none through `PutObject`.

`ETag` values are quoted, as S3 returns them. s2 never assembles the composite `md5-of-md5s-N` form for multipart uploads; the ETag is the MD5 of the whole assembled body. On an `s3` root with SSE-KMS or SSE-C the provider's ETag is not the body's MD5, and s2 passes it through unchanged.

A pre-v0.18.0 s2-server kept both values as `s2-etag` and `s2-content-type` metadata keys. Those objects are still read: `osfs` and `memfs` recognise the older flat sidecar, and `s3` and `gcs` lift the two keys into the attributes and hide them from `Metadata()`.

On `s3` and `gcs` that lift happens only for an object carrying `s2-etag`, which makes the name reserved on those roots. s2-server drops an incoming `x-amz-meta-s2-etag` header so a client cannot trigger it, but a library caller that writes the key itself gets its object read as a pre-v0.18.0 write, with `s2-content-type` promoted to `ContentType()`. All of this is scheduled for removal in v1.0.0 ([#247](https://github.com/mojatter/s2/issues/247)).

### Metadata names on azblob

Azure accepts only metadata names that are valid C# identifiers — letters, digits and `_`, not starting with a digit. A name containing `-` or `.` is rejected with `400 InvalidMetadata`, so `x-amz-meta-my-key` fails on an `azblob` root where it succeeds elsewhere ([#246](https://github.com/mojatter/s2/issues/246)). s2 passes names through unchanged rather than encoding them.

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

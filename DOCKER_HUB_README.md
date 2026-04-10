# mojatter/s2-server

Lightweight S3-compatible object storage server written in Go.
A drop-in replacement for MinIO in local development and CI environments.

## Quick Start

```sh
docker run -p 9000:9000 -p 9001:9001 mojatter/s2-server
```

S2 serves the S3 API on `:9000` at the root path (same layout as MinIO), and the Web Console on `:9001`. Use any S3 client without extra configuration:

```sh
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp ./file.txt s3://my-bucket/
```

Access the Web Console at http://localhost:9001.

## Persistent Storage

```sh
docker run -p 9000:9000 -p 9001:9001 -v /your/data:/var/lib/s2 mojatter/s2-server
```

## docker-compose

```yaml
services:
  s2-server:
    image: mojatter/s2-server
    ports:
      - "9000:9000" # S3 API
      - "9001:9001" # Web Console
    volumes:
      - ./data:/var/lib/s2
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `S2_SERVER_LISTEN` | `:9000` | S3 API listen address |
| `S2_SERVER_CONSOLE_LISTEN` | `:9001` | Web Console listen address (set empty to disable) |
| `S2_SERVER_HEALTH_PATH` | `/healthz` | Health check path on the S3 listener (set empty to disable) |
| `S2_SERVER_TYPE` | `osfs` | Storage backend (`osfs`, `memfs`) |
| `S2_SERVER_ROOT` | `/var/lib/s2` | Root directory for bucket data |
| `S2_SERVER_USER` | — | Username for authentication (disables auth if empty) |
| `S2_SERVER_PASSWORD` | — | Password for authentication |
| `S2_SERVER_BUCKETS` | — | Comma-separated list of buckets to create on startup |
| `S2_SERVER_CONFIG` | — | Path to JSON config file |

### Authentication

Set `S2_SERVER_USER` and `S2_SERVER_PASSWORD` to enable authentication:

```sh
docker run -p 9000:9000 -p 9001:9001 \
  -e S2_SERVER_USER=myuser \
  -e S2_SERVER_PASSWORD=mypassword \
  mojatter/s2-server
```

- **Web Console** — HTTP Basic Auth
- **S3 API** — AWS Signature Version 4 (`S2_SERVER_USER` as Access Key ID, `S2_SERVER_PASSWORD` as Secret Access Key)

```sh
AWS_ACCESS_KEY_ID=myuser AWS_SECRET_ACCESS_KEY=mypassword \
  aws --endpoint-url http://localhost:9000 s3 ls
```

Authentication is disabled by default.

### S3 API Endpoints

| Method | Path | Operation |
|---|---|---|
| GET | `/` | ListBuckets |
| PUT | `/{bucket}` | CreateBucket |
| HEAD | `/{bucket}` | HeadBucket |
| DELETE | `/{bucket}` | DeleteBucket |
| GET | `/{bucket}` | ListObjectsV2 |
| GET | `/{bucket}/{key}` | GetObject (Range supported) |
| HEAD | `/{bucket}/{key}` | HeadObject |
| PUT | `/{bucket}/{key}` | PutObject / CopyObject |
| DELETE | `/{bucket}/{key}` | DeleteObject |
| POST | `/{bucket}?delete` | DeleteObjects |
| POST | `/{bucket}/{key}?uploads` | CreateMultipartUpload |
| PUT | `/{bucket}/{key}?uploadId&partNumber` | UploadPart |
| POST | `/{bucket}/{key}?uploadId` | CompleteMultipartUpload |
| DELETE | `/{bucket}/{key}?uploadId` | AbortMultipartUpload |
| GET, HEAD | `/healthz` | Health check (configurable via `S2_SERVER_HEALTH_PATH`) |

## Links

- [GitHub](https://github.com/mojatter/s2)
- [Documentation](https://github.com/mojatter/s2#readme)

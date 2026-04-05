# mojatter/s2-server

Lightweight S3-compatible object storage server written in Go.
A drop-in replacement for MinIO in local development and CI environments.

## Quick Start

```sh
docker run -p 9000:9000 mojatter/s2-server
```

Access the Web Console at http://localhost:9000, or use any S3 client:

```sh
aws --endpoint-url http://localhost:9000/s3api s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000/s3api s3 cp ./file.txt s3://my-bucket/
```

## Persistent Storage

```sh
docker run -p 9000:9000 -v /your/data:/var/lib/s2 mojatter/s2-server
```

## docker-compose

```yaml
services:
  s2-server:
    image: mojatter/s2-server
    ports:
      - "9000:9000"
    volumes:
      - ./data:/var/lib/s2
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `S2_SERVER_LISTEN` | `:9000` | Listen address |
| `S2_SERVER_TYPE` | `osfs` | Storage backend (`osfs`, `memfs`) |
| `S2_SERVER_ROOT` | `/var/lib/s2` | Root directory for bucket data |
| `S2_SERVER_CONFIG` | — | Path to JSON config file |

### S3 API Endpoints

| Method | Path | Operation |
|---|---|---|
| GET | `/s3api/{bucket}` | ListObjects |
| GET | `/s3api/{bucket}/{key}` | GetObject |
| PUT | `/s3api/{bucket}/{key}` | PutObject |
| DELETE | `/s3api/{bucket}/{key}` | DeleteObject |

## Links

- [GitHub](https://github.com/mojatter/s2)
- [Documentation](https://github.com/mojatter/s2#readme)

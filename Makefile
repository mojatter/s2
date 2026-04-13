VERSION := $(shell git describe --tags --always --dirty)
LDFLAGS := -s -w -X github.com/mojatter/s2/server.version=$(VERSION)

.PHONY: build
build:
	go build -ldflags "$(LDFLAGS)" -o bin/s2-server ./cmd/s2-server

.PHONY: test
test:
	go test -short ./...

.PHONY: test-integration
test-integration:
	go test -v -count=1 ./server/handlers/s3api/ -run Integration

# bench runs Go-native microbenchmarks against the storage backend and the
# HTTP handler. Shorter than bench-warp and requires no external binaries,
# so it's the right knob for in-loop regression checks.
BENCH_GO_PACKAGES ?= ./fs/ ./server/handlers/s3api/
BENCH_GO_RE       ?= BenchmarkPutObject|BenchmarkGetObject|BenchmarkHTTPPutObject|BenchmarkHTTPGetObject
BENCH_GO_TIME     ?= 2s

.PHONY: bench
bench:
	go test $(BENCH_GO_PACKAGES) -run='^$$' -bench='$(BENCH_GO_RE)' -benchmem -benchtime=$(BENCH_GO_TIME)

.PHONY: test-e2e
test-e2e:
	docker compose -f s2test/e2e/docker-compose.yml run --build --rm test; \
	rc1=$$?; \
	docker compose -f s2test/e2e/docker-compose.yml run --build --rm test-sdk; \
	rc2=$$?; \
	docker compose -f s2test/e2e/docker-compose.yml down; \
	[ $$rc1 -eq 0 ] && [ $$rc2 -eq 0 ]

# bench-warp runs a `warp mixed` benchmark against a fresh in-process
# s2-server. It expects the github.com/minio/warp binary on PATH; the
# easiest way to install it is `go install github.com/minio/warp@latest`.
#
# Tunables (override on the command line):
#   BENCH_PORT     S3 listen port for the test server (default 9100)
#   BENCH_DATA     Storage root directory (default /tmp/s2-bench-data)
#   BENCH_BUCKET   Bucket name created on startup (default warp-benchmark)
#   BENCH_OBJSIZE  warp --obj.size value (default 1MiB)
#   BENCH_OBJECTS  warp --objects value (default 250)
#   BENCH_CONC     warp --concurrent value (default 8)
#   BENCH_TIME     warp --duration value (default 30s)
BENCH_PORT    ?= 9100
BENCH_DATA    ?= /tmp/s2-bench-data
BENCH_BUCKET  ?= warp-benchmark
BENCH_OBJSIZE ?= 1MiB
BENCH_OBJECTS ?= 250
BENCH_CONC    ?= 8
BENCH_TIME    ?= 30s

.PHONY: bench-warp
bench-warp: build
	@command -v warp >/dev/null || { echo "warp not found on PATH; install with: go install github.com/minio/warp@latest"; exit 1; }
	@rm -rf $(BENCH_DATA) && mkdir -p $(BENCH_DATA)
	@S2_SERVER_ROOT=$(BENCH_DATA) S2_SERVER_LISTEN=:$(BENCH_PORT) S2_SERVER_CONSOLE_LISTEN= S2_SERVER_BUCKETS=$(BENCH_BUCKET) ./bin/s2-server >/tmp/s2-bench-warp.log 2>&1 & \
	pid=$$!; \
	trap "kill $$pid 2>/dev/null" EXIT INT TERM; \
	sleep 1; \
	warp mixed \
	  --host=localhost:$(BENCH_PORT) \
	  --access-key=warpuser --secret-key=warpsecretkey123 \
	  --tls=false \
	  --bucket=$(BENCH_BUCKET) \
	  --obj.size=$(BENCH_OBJSIZE) \
	  --objects=$(BENCH_OBJECTS) \
	  --concurrent=$(BENCH_CONC) \
	  --duration=$(BENCH_TIME) \
	  --no-color

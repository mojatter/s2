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

.PHONY: test-e2e
test-e2e:
	docker compose -f s2test/e2e/docker-compose.yml run --build --rm test; \
	rc=$$?; \
	docker compose -f s2test/e2e/docker-compose.yml down; \
	exit $$rc

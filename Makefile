VERSION := $(shell git describe --tags --always --dirty)
LDFLAGS := -s -w -X github.com/mojatter/s2/server.version=$(VERSION)

.PHONY: build
build:
	go build -ldflags "$(LDFLAGS)" -o bin/s2-server ./cmd/s2-server

FROM golang:1.24-alpine AS builder
WORKDIR /app
# DefaultRoot is overridden via -ldflags so the Docker image defaults to
# /var/lib/s2 while a bare "go install" binary keeps the friendlier
# relative "data" default (see server/config.go).
RUN --mount=target=. GOOS=linux CGO_ENABLED=0 \
    go build \
      -ldflags "-s -w -X github.com/mojatter/s2/server.DefaultRoot=/var/lib/s2" \
      -o /s2-server ./cmd/s2-server

FROM busybox AS init
RUN mkdir -p /var/lib/s2

FROM scratch
COPY --from=builder /s2-server /usr/sbin/s2-server
COPY --from=init /var/lib/s2 /var/lib/s2
VOLUME /var/lib/s2
EXPOSE 9000 9001
ENTRYPOINT ["/usr/sbin/s2-server"]

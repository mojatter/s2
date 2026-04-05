FROM golang:1.26-alpine AS builder
WORKDIR /app
RUN --mount=target=. GOOS=linux CGO_ENABLED=0 \
    go build -o /s2-server ./cmd/s2-server

FROM busybox AS init
RUN mkdir -p /var/lib/s2

FROM scratch
COPY --from=builder /s2-server /usr/sbin/s2-server
COPY --from=init /var/lib/s2 /var/lib/s2
VOLUME /var/lib/s2
EXPOSE 9000
ENTRYPOINT ["/usr/sbin/s2-server"]

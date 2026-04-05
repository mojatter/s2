FROM golang:1.26-alpine

WORKDIR /app

RUN --mount=target=. GOOS=linux CGO_ENABLED=0 \
    go build -o /s2-server ./cmd/server/main.go

FROM scratch

COPY --from=0 /s2-server /usr/sbin/s2-server

EXPOSE 9000

ENTRYPOINT [ "/usr/sbin/s2-server" ]

FROM golang:1.26-alpine AS builder
# Source is COPYed rather than bind-mounted so that GOWORK=on (see below)
# can freely rewrite go.work.sum inside the image layer without ever
# touching the host checkout.
COPY . /app
WORKDIR /app/cmd/s2-server
# DefaultRoot is overridden via -ldflags so the Docker image defaults to
# /var/lib/s2 while a bare "go install" binary keeps the friendlier
# relative "./data" default (see server/config.go).
# GOWORK is off by default so production builds use the published module
# versions from cmd/s2-server/go.mod, not workspace replacements - this
# is what a "go install" of the tagged release would actually resolve to.
# E2E builds (see s2test/e2e) override this to /app/go.work via the
# GOWORK build arg so they exercise in-repo module changes that haven't
# been tagged/released yet.
ARG GOWORK=off
RUN GOOS=linux CGO_ENABLED=0 GOWORK=${GOWORK} \
    go build \
      -ldflags "-s -w -X github.com/mojatter/s2/server.DefaultRoot=/var/lib/s2" \
      -o /s2-server .

FROM busybox AS init
RUN mkdir -p /var/lib/s2

FROM scratch
COPY --from=builder /s2-server /usr/sbin/s2-server
COPY --from=init /var/lib/s2 /var/lib/s2
VOLUME /var/lib/s2
EXPOSE 9000 9001
ENTRYPOINT ["/usr/sbin/s2-server"]

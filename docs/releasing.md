# Releasing

Maintainer-only notes for cutting a new version of S2. The repo is a
multi-module Go workspace, which makes release tagging more nuanced
than a single-module repo.

## Modules and tags

The repo ships several independently tagged Go modules:

| Module path | Tag prefix | Notes |
|---|---|---|
| `github.com/mojatter/s2` | `v*` | Root module. Tag push triggers the release workflow (binary, Docker image, GitHub Release). |
| `github.com/mojatter/s2/s3` | `s3/v*` | S3 backend |
| `github.com/mojatter/s2/gcs` | `gcs/v*` | GCS backend |
| `github.com/mojatter/s2/azblob` | `azblob/v*` | Azure Blob backend |
| `github.com/mojatter/s2/s2env` | `s2env/v*` | Env-var wiring |
| `github.com/mojatter/s2/server` | `server/v*` | S3-compatible server (handlers, console). |
| `github.com/mojatter/s2/cmd/s2-server` | `cmd/s2-server/v*` | Binary entry point. Tagged for `go install` users. |

Only the root tag (`v*`) triggers CI / GoReleaser. The submodule tags
are publication-only, for `go get`.

## Release flow (example: v0.11.0)

### 1. Bump submodule `go.mod` files

Open a single PR that bumps every submodule's intra-repo requires
to the new version:

- `s3/go.mod`, `gcs/go.mod`, `azblob/go.mod`, `server/go.mod`: bump
  `github.com/mojatter/s2` to `v0.11.0`
- `s2env/go.mod`: also bump `s3`, `gcs`, `azblob` to `v0.11.0`
- `cmd/s2-server/go.mod`: bump `github.com/mojatter/s2`, `s3`, `gcs`,
  `azblob`, `server` to `v0.11.0`
- Run `go mod tidy` in each module locally to refresh `go.sum`

CI does not run `go mod tidy` in the per-submodule loop, so it is
fine that the new tags do not yet exist on `proxy.golang.org`. `go
test` resolves intra-repo deps via `go.work`. `cmd/s2-server` has no
`replace` directives, but the workspace still covers it during CI.

Merge the PR.

### 2. Tag and push

Create the seven tags at the merge commit:

```sh
git tag -a v0.11.0               -m "Release v0.11.0"
git tag -a s3/v0.11.0             -m "Release s3/v0.11.0"
git tag -a gcs/v0.11.0            -m "Release gcs/v0.11.0"
git tag -a azblob/v0.11.0         -m "Release azblob/v0.11.0"
git tag -a s2env/v0.11.0          -m "Release s2env/v0.11.0"
git tag -a server/v0.11.0         -m "Release server/v0.11.0"
git tag -a cmd/s2-server/v0.11.0  -m "Release cmd/s2-server/v0.11.0"
```

Push the submodule tags first, then the root tag **separately**:

```sh
git push origin s3/v0.11.0 gcs/v0.11.0 azblob/v0.11.0 s2env/v0.11.0 server/v0.11.0 cmd/s2-server/v0.11.0
git push origin v0.11.0
```

The root tag must be pushed alone. Pushing multiple tags in a single
`git push` has occasionally failed to fire GitHub Actions. The
submodule tags are safe to batch because they do not match any
workflow trigger.

### 3. Replace the auto-generated release notes

GoReleaser fills the GitHub Release with a generated changelog. Replace
it with a hand-written version:

```sh
gh release edit v0.11.0 --notes "$(cat <<'EOF'
## Highlights
...
## Changes
...
## Upgrading
...
## Full Changelog
https://github.com/mojatter/s2/compare/v0.10.0...v0.11.0
EOF
)"
```

See [v0.3.0](https://github.com/mojatter/s2/releases/tag/v0.3.0) for
the canonical format.

## Gotchas

### `proxy.golang.org` tags are immutable

Once you push a tag, `proxy.golang.org` fetches its content on the
first `go get` and caches it permanently. Rewriting the tag on GitHub
afterwards does not update the proxy. You can check whether a tag is
already frozen:

```sh
curl -sI https://proxy.golang.org/github.com/mojatter/s2/s3/@v/v0.11.0.info
```

A `200 OK` means the content is cached. If you tagged with a stale
`go.mod`, the recovery path is to cut a new patch version (`v0.11.1`)
and add `retract v0.11.0` to the respective `go.mod`.

### `GOWORK=off` paths bypass the workspace

`go mod tidy` and the production Docker build (`server/Dockerfile.goreleaser`,
which sets `GOWORK=off`) both ignore `go.work` and resolve intra-repo
deps through `proxy.golang.org`. CI is configured to use `go test`
without a preceding `go mod tidy`, so the bump PR passes even before
the new tags are published. But:

- Running `go mod tidy` locally on the bump branch will fail until the
  new tags exist. Run it after pushing tags, or skip it (CI will
  accept your branch).
- The GoReleaser binary build relies on `cmd/s2-server/go.mod`'s
  pinned requires. Step 1 above bumps them; if you skip that, the
  binary will resolve intra-repo paths through the previous version's
  proxy contents.

### Multi-tag push can miss webhooks

Pushing several tags in one `git push` has occasionally failed to
trigger the corresponding GitHub Actions workflow. Push the root `v*`
tag alone; non-trigger submodule tags can be batched.

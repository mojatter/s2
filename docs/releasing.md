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
| `github.com/mojatter/s2/cmd/s2-server` | `cmd/s2-server/v*` | Binary entry point. Tagged for `go install` users. |

Only the root tag (`v*`) triggers CI / GoReleaser. The submodule tags
are publication-only, for `go get`.

## Release flow (example: v0.10.0)

### 1. Bump submodule `go.mod` files

Open a single PR that bumps `s3/go.mod`, `gcs/go.mod`, `azblob/go.mod`,
and `s2env/go.mod`:

- `require github.com/mojatter/s2` → `v0.10.0`
- In `s2env/go.mod`, also bump `s3`, `gcs`, `azblob` to `v0.10.0`
- Run `go mod tidy` in each module

These four submodules all carry `replace github.com/mojatter/s2 => ../`
(and `s2env` has `replace` for `s3`, `gcs`, `azblob` as well), so
`go mod tidy` in CI resolves against local source and passes even
though the new tags do not exist yet.

Merge the PR.

### 2. Tag and push

Create the five tags at the merge commit:

```sh
git tag -a v0.10.0        -m "Release v0.10.0"
git tag -a s3/v0.10.0     -m "Release s3/v0.10.0"
git tag -a gcs/v0.10.0    -m "Release gcs/v0.10.0"
git tag -a azblob/v0.10.0 -m "Release azblob/v0.10.0"
git tag -a s2env/v0.10.0  -m "Release s2env/v0.10.0"
```

Push the submodule tags first, then the root tag **separately**:

```sh
git push origin s3/v0.10.0 gcs/v0.10.0 azblob/v0.10.0 s2env/v0.10.0
git push origin v0.10.0
```

The root tag must be pushed alone. Pushing multiple tags in a single
`git push` has occasionally failed to fire GitHub Actions. The
submodule tags are safe to batch because they do not match any
workflow trigger.

### 3. Replace the auto-generated release notes

GoReleaser fills the GitHub Release with a generated changelog. Replace
it with a hand-written version:

```sh
gh release edit v0.10.0 --notes "$(cat <<'EOF'
## Highlights
...
## Changes
...
## Upgrading
...
## Full Changelog
https://github.com/mojatter/s2/compare/v0.9.1...v0.10.0
EOF
)"
```

See [v0.3.0](https://github.com/mojatter/s2/releases/tag/v0.3.0) for
the canonical format.

### 4. Optional: bump `cmd/s2-server`

The GoReleaser-built binary and Docker image always ship current source
(thanks to `go.work`), but `go install github.com/mojatter/s2/cmd/s2-server@latest`
reads the pinned versions in `cmd/s2-server/go.mod`. To keep that path
current:

- Open a follow-up PR bumping `cmd/s2-server/go.mod` to require the new
  versions of `s2`, `s3`, `gcs`, and `azblob`. This PR can only be
  opened **after** step 2, because `cmd/s2-server/go.mod` has no
  `replace` directives, so `go mod tidy` in CI needs the new versions
  to actually exist on `proxy.golang.org`.
- Merge, then tag `cmd/s2-server/v0.10.0` at the merge commit and push.

Skipping this step is acceptable if binaries and Docker images are the
primary consumption path for your users.

## Gotchas

### `proxy.golang.org` tags are immutable

Once you push a tag, `proxy.golang.org` fetches its content on the
first `go get` and caches it permanently. Rewriting the tag on GitHub
afterwards does not update the proxy. You can check whether a tag is
already frozen:

```sh
curl -sI https://proxy.golang.org/github.com/mojatter/s2/s3/@v/v0.10.0.info
```

A `200 OK` means the content is cached. If you tagged with a stale
`go.mod`, the recovery path is to cut a new patch version (`v0.10.1`)
and add `retract v0.10.0` to the respective `go.mod`.

### `go mod tidy` ignores `go.work`

Workspace resolution (`go.work`) only applies to `go build`, `go test`,
and similar commands. `go mod tidy` reads the module's own `go.mod`
and fetches from the proxy. This is why the release flow splits
`cmd/s2-server` (no `replace`) from the other submodules (with
`replace`).

### Multi-tag push can miss webhooks

Pushing several tags in one `git push` has occasionally failed to
trigger the corresponding GitHub Actions workflow. Push the root `v*`
tag alone; non-trigger submodule tags can be batched.

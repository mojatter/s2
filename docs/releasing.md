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

## Release flow (example: v0.11.1)

The flow is **two PRs**. The five modules that carry `replace`
directives are bumped and tagged first; `cmd/s2-server` (which has no
`replace`) follows once those tags are published. This is what v0.11.0
(#119 → #120) and v0.11.1 (#130 → #131) actually did — a single PR that
also bumps `cmd/s2-server` would fail the e2e build (see step 3).

### 1. Bump the replace-module `go.mod` files

Open a PR that bumps the intra-repo requires to the new version in the
five modules that have `replace` directives:

- `s3/go.mod`, `gcs/go.mod`, `azblob/go.mod`, `server/go.mod`: bump
  `github.com/mojatter/s2` to `v0.11.1`
- `s2env/go.mod`: also bump `s3`, `gcs`, `azblob` to `v0.11.1`

Do **not** bump `cmd/s2-server` here — it is handled in step 3.

`go.sum` does not change: the `replace` directives resolve these
modules locally, and CI resolves them through `go.work`, so the PR is
green before any new tag exists. Don't run `go mod tidy` on the branch
(it fails until the tags are published); just edit the `require` lines.

Merge the PR.

### 2. Tag and push (six tags)

Create six tags at the merge commit — the root tag plus the five
bumped submodules (**not** `cmd/s2-server`):

```sh
git tag -a v0.11.1        -m "Release v0.11.1"
git tag -a s3/v0.11.1     -m "Release s3/v0.11.1"
git tag -a gcs/v0.11.1    -m "Release gcs/v0.11.1"
git tag -a azblob/v0.11.1 -m "Release azblob/v0.11.1"
git tag -a s2env/v0.11.1  -m "Release s2env/v0.11.1"
git tag -a server/v0.11.1 -m "Release server/v0.11.1"
```

Push the submodule tags first, then the root tag **separately**:

```sh
git push origin s3/v0.11.1 gcs/v0.11.1 azblob/v0.11.1 s2env/v0.11.1 server/v0.11.1
git push origin v0.11.1
```

The root tag must be pushed alone. Pushing multiple tags in a single
`git push` has occasionally failed to fire GitHub Actions. The
submodule tags are safe to batch because they do not match any
workflow trigger.

The root tag triggers GoReleaser. At this commit `cmd/s2-server` still
requires the **previous** release's intra-repo versions (it is bumped
in step 3), so the binary links the previous version of the library
code. That is fine when the change is a dependency/security bump pinned
directly in `cmd/s2-server/go.mod` (e.g. `golang.org/x/net`): MVS picks
the higher version from `cmd/s2-server`'s own requires regardless of
the library versions. If a release changes library *code* the binary
must ship, do step 3 first and push the root tag afterwards, so the
GoReleaser build sees the bumped requires.

### 3. Bump `cmd/s2-server` (after the tags are published)

`cmd/s2-server` has no `replace` directives, so its `GOWORK=off` build
— the e2e image (`server/Dockerfile` builds `cmd/s2-server`) and the
GoReleaser binary — resolves intra-repo deps from `proxy.golang.org`.
It can only require the new version once steps 1–2 have published the
tags; bumping it earlier breaks the e2e build, which cannot fetch the
not-yet-published version.

Open a follow-up PR (the tags now exist, so `go get` resolves them):

```sh
cd cmd/s2-server
GOWORK=off go get \
  github.com/mojatter/s2@v0.11.1 \
  github.com/mojatter/s2/s3@v0.11.1 \
  github.com/mojatter/s2/gcs@v0.11.1 \
  github.com/mojatter/s2/azblob@v0.11.1 \
  github.com/mojatter/s2/server@v0.11.1
GOWORK=off go mod tidy
```

Merge it, then tag at the merge commit and push:

```sh
git tag -a cmd/s2-server/v0.11.1 -m "Release cmd/s2-server/v0.11.1"
git push origin cmd/s2-server/v0.11.1
```

`cmd/s2-server/v*` is publication-only and triggers no workflow.

### 4. Replace the auto-generated release notes

GoReleaser fills the GitHub Release with a generated changelog. Replace
it with a hand-written version:

```sh
gh release edit v0.11.1 --notes "$(cat <<'EOF'
## Highlights
...
## Changes
...
## Upgrading
...
## Full Changelog
https://github.com/mojatter/s2/compare/v0.11.0...v0.11.1
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
  pinned requires, which are bumped in step 3 (after tagging). At the
  root tag the binary therefore still links the previous library
  version; step 2 explains why that is acceptable for
  dependency/security bumps and when to reorder.

### Multi-tag push can miss webhooks

Pushing several tags in one `git push` has occasionally failed to
trigger the corresponding GitHub Actions workflow. Push the root `v*`
tag alone; non-trigger submodule tags can be batched.

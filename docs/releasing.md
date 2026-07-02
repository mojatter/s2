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
(including `cmd/s2-server/v*`) are publication-only, for `go get`.

## Release flow (example: v0.11.1)

The flow is **two PRs**. The five modules that carry `replace`
directives are bumped and tagged first; `cmd/s2-server` (which has no
`replace`) follows once those tags are published. This is what v0.11.0
(#119 → #120) and v0.11.1 (#130 → #131) actually did — a single PR that
also bumps `cmd/s2-server` would fail before the submodule tags exist
(`go get`/`go mod tidy` can't resolve a version that isn't published
yet).

Both PRs can be done at your own pace: pushing the root tag right
after step 2 always produces a correct GitHub Release / Docker image,
regardless of whether `cmd/s2-server` has been bumped yet (see
"Why the root tag doesn't need to wait for step 3" below). Step 3 has
no urgency — it only affects `go install .../cmd/s2-server@vX.Y.Z`,
not what step 2 already published.

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

### 2. Tag all six, push (submodules batched, root alone)

At the merge commit, create the root tag plus the five submodule tags
(**not** `cmd/s2-server` — it isn't bumped yet):

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

The root tag must be pushed alone — pushing multiple tags in a single
`git push` has occasionally failed to fire GitHub Actions. Pushing the
root tag triggers the release workflow: `tests` (unit tests + lint)
gates GoReleaser, but e2e does not (see below), so this always
produces a correct GitHub Release / Docker image. Proceed to step 3
whenever.

#### Why the root tag doesn't need to wait for step 3

GoReleaser builds without `GOWORK=off`, so `go.work` resolves everything
from local source, not `cmd/s2-server/go.mod`'s pins. The published
binary/Docker image is always correct. Only `go install
.../cmd/s2-server@vX.Y.Z` and the e2e Docker build (which sets
`GOWORK=off` on purpose) depend on step 3 — and neither gates the release.

### 3. Bump `cmd/s2-server` (after the submodule tags are published)

`cmd/s2-server` has no `replace` directives, so its `GOWORK=off` build
— the e2e image (`server/Dockerfile` builds `cmd/s2-server`) — resolves
intra-repo deps from `proxy.golang.org`. It can only require the new
version once steps 1–2 have published the tags; bumping it earlier
breaks that resolution.

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

`go mod tidy` and the e2e Docker build (`server/Dockerfile`, which sets
`GOWORK=off`) both ignore `go.work` and resolve intra-repo deps through
`proxy.golang.org`. CI is configured to use `go test` without a
preceding `go mod tidy`, so the bump PR passes even before the new tags
are published. Running `go mod tidy` locally on the bump branch will
fail until the new tags exist — run it after pushing tags (that's what
step 3 does), or skip it entirely (CI will accept your branch).

GoReleaser's own build does **not** use `GOWORK=off` — it resolves via
`go.work` from local source, so it never depends on
`cmd/s2-server/go.mod`'s pins. Only the e2e Docker image and
`go install .../cmd/s2-server@vX.Y.Z` do, and neither gates `release`.

### Multi-tag push can miss webhooks

Pushing several tags in one `git push` has occasionally failed to
trigger the corresponding GitHub Actions workflow. Push the root `v*`
tag alone; non-trigger submodule tags can be batched.

### Incident (2026-07-02, v0.12.1 → v0.12.3)

`release` used to depend on `needs: [tests, e2e]`. e2e's `GOWORK=off`
Docker build always fails until step 3 bumps `cmd/s2-server` — which
lands in a later commit than the root tag by construction. So v0.12.1's
root tag made e2e fail as expected, `release` was silently *skipped*,
and no GitHub Release was published even though GoReleaser would have
built a correct one. Recovery cost a wasted patch version (v0.12.2, then
v0.12.3), since an un-released tag can't be reused.

Fix (#149): drop `e2e` from `release`'s `needs:`. It never protected the
artifact (GoReleaser doesn't use `GOWORK=off`) and the same commit
already passed e2e on its regular `main` push. That's why step 2 no
longer needs a decision gate.

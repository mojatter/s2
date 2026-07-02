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

### 2. Tag the five submodules, push (root tag not yet)

Before tagging anything, decide: does this release contain library
*code* (not just a dependency bump) that `cmd/s2-server`'s binary must
actually run — e.g. a fix under `fs/`, `server/`, or root package code?
If unsure, treat it as yes. This decides whether the root tag goes out
now (below) or after step 3 (see the decision gate at the end of this
section).

Create the five submodule tags at the merge commit (**not** root,
**not** `cmd/s2-server`):

```sh
git tag -a s3/v0.11.1     -m "Release s3/v0.11.1"
git tag -a gcs/v0.11.1    -m "Release gcs/v0.11.1"
git tag -a azblob/v0.11.1 -m "Release azblob/v0.11.1"
git tag -a s2env/v0.11.1  -m "Release s2env/v0.11.1"
git tag -a server/v0.11.1 -m "Release server/v0.11.1"
```

Push them, batched:

```sh
git push origin s3/v0.11.1 gcs/v0.11.1 azblob/v0.11.1 s2env/v0.11.1 server/v0.11.1
```

**Decision gate — root tag now, or after step 3?**

- **Pure dependency/security bump** (nothing under `cmd/s2-server`'s
  own linked library code changed in a way the binary must pick up) →
  push the root tag now:

  ```sh
  git tag -a v0.11.1 -m "Release v0.11.1"
  git push origin v0.11.1
  ```

  The root tag must be pushed alone — pushing multiple tags in a
  single `git push` has occasionally failed to fire GitHub Actions.
  This triggers GoReleaser. At this commit `cmd/s2-server` still
  requires the **previous** release's intra-repo versions (it is
  bumped in step 3), so the binary links the previous version of the
  library code. That is fine here: the dependency is pinned directly
  in `cmd/s2-server/go.mod` (e.g. `golang.org/x/net`), and MVS picks
  the higher version from `cmd/s2-server`'s own requires regardless of
  the library versions. Proceed to step 3 whenever.

- **Ships library code `cmd/s2-server`'s binary must include** → do
  **not** push the root tag yet. Go to step 3 first, merge it, and
  push the root tag from there instead. Pushing root here would make
  the release workflow's own e2e job build `cmd/s2-server` against the
  *old*, not-yet-bumped code, fail, and silently skip the GoReleaser
  job — producing no binary/Docker image for that tag. Tags are
  immutable once `proxy.golang.org` caches them, so recovering costs a
  whole extra patch version, not just a retry (see Gotchas below).

### 3. Bump `cmd/s2-server` (after the submodule tags are published)

`cmd/s2-server` has no `replace` directives, so its `GOWORK=off` build
— the e2e image (`server/Dockerfile` builds `cmd/s2-server`) and the
GoReleaser binary — resolves intra-repo deps from `proxy.golang.org`.
It can only require the new version once steps 1–2 have published the
submodule tags; bumping it earlier breaks the e2e build, which cannot
fetch the not-yet-published version.

Open a follow-up PR (the submodule tags now exist, so `go get`
resolves them):

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

Merge it, then tag `cmd/s2-server` at the merge commit and push
(publication-only, no workflow):

```sh
git tag -a cmd/s2-server/v0.11.1 -m "Release cmd/s2-server/v0.11.1"
git push origin cmd/s2-server/v0.11.1
```

If the root tag was deliberately deferred (library-code case in step
2), push it now, at this same merge commit, so the release workflow's
e2e job builds `cmd/s2-server` with the fix already in place:

```sh
git tag -a v0.11.1 -m "Release v0.11.1"
git push origin v0.11.1
```

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

**Incident (2026-07-02, v0.12.1/v0.12.2):** a bugfix release (the
`fs.Storage.Delete` no-op fix) pushed the root tag right after step 2,
before step 3. The release workflow's e2e job built `cmd/s2-server`
from the still-unbumped `go.mod` — pre-fix code — failed, and the
GoReleaser job (`needs: [tests, e2e]`) was silently *skipped*, not
failed. No GitHub Release or Docker image was published for v0.12.1,
and that tag couldn't be reused. Recovery required a full extra patch
version (v0.12.2): the `cmd/s2-server` bump PR was merged first, then
the root tag was cut at that later commit. The decision gate in step 2
above exists so this ordering mistake is caught before any tag is
pushed, not after.

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

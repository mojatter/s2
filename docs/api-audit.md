# s2 Public API Audit (towards v0.2.0)

This document inventories every exported symbol in package `s2` (the root
import path `github.com/mojatter/s2`) and assigns a v0.2 verdict to each.
It is the input for the breaking-change PR that will ship as **v0.2.0**;
no symbols outside that package are reviewed here.

**Scope note.** v1.0.0 is intentionally out of scope: the project is
days old with no announced users, so we will continue to use the v0.x
freedom-to-break window. v1 will be considered later, after the v0.2
shape has lived in real use for a while.

Verdict legend:

- **KEEP** — ship as-is.
- **DOC** — keep, but improve godoc / clarify contract.
- **CHANGE** — rename, retype, or merge into another symbol.
- **REMOVE** — drop from the public API.

---

## storage.go

### `type Storage interface`

| Method | Verdict | Notes |
|---|---|---|
| `Type() Type` | KEEP | Trivial introspection. |
| `Sub(ctx, prefix) (Storage, error)` | DOC | Clarify whether the returned storage shares the parent's lifetime, and whether `prefix` is normalized (trailing `/`, leading `/` etc.). |
| `List(ctx, prefix, limit) ([]Object, []string, error)` | **CHANGE** | See "List family collapse" below. |
| `ListAfter(ctx, prefix, limit, after) ([]Object, []string, error)` | **CHANGE** | Same. |
| `ListRecursive(ctx, prefix, limit) ([]Object, error)` | **CHANGE** | Same. |
| `ListRecursiveAfter(ctx, prefix, limit, after) ([]Object, error)` | **CHANGE** | Same. |
| `Get(ctx, name) (Object, error)` | DOC | Document that the returned `Object` carries metadata (vs. List results which may not). Sentinel error contract: must return `ErrNotExist`. |
| `Exists(ctx, name) (bool, error)` | KEEP | Convenience over `Get` + `errors.Is(err, ErrNotExist)`. Worth keeping. |
| `Put(ctx, obj Object) error` | DOC | State that Put is atomic per object (osfs already uses temp+rename) and that metadata on `obj` is persisted as part of Put. |
| `PutMetadata(ctx, name, metadata) error` | DOC | Keep. Improve godoc to spell out: not atomic with Put, intended for ETag/hash-style metadata that can only be computed after the body is written, replaces (not merges) the existing metadata. |
| `Copy(ctx, src, dst) error` | KEEP | S3-native, cheaper than Get+Put for `s3` backend. |
| `Move(ctx, src, dst) error` | **REMOVE** | Equivalent to `Copy + Delete`. Move it to a free function `s2.Move(ctx, s, src, dst)` so backends do not have to implement two operations. |
| `Delete(ctx, name) error` | DOC | Document idempotency: deleting a non-existent object is a no-op (verify per backend; align if inconsistent). |
| `DeleteRecursive(ctx, prefix) error` | KEEP | Mirrors S3 multi-object delete semantics. Document best-effort partial-failure behavior. |
| `SignedURL(ctx, name, ttl) (string, error)` | **CHANGE** | Cannot express GET vs PUT vs DELETE. Replace with `SignedURL(ctx, opts SignedURLOptions)` carrying `Method`, `Name`, `TTL`. |

#### List family collapse (CHANGE)

Four List methods exist as combinations of `(Recursive?, After?)`. Collapse to one
method with options:

```go
type ListOptions struct {
    Prefix    string
    After     string // continuation token; empty = first page
    Limit     int    // 0 = no limit
    Recursive bool
}

type ListResult struct {
    Objects        []Object
    CommonPrefixes []string // empty when Recursive == true
    NextAfter      string   // empty when exhausted
}

List(ctx context.Context, opts ListOptions) (ListResult, error)
```

Benefits:

- Backend implementations shrink from four methods to one.
- The currently-anonymous `[]string` second return value (common prefixes)
  becomes a named field.
- Pagination via continuation token aligns with S3 semantics directly.
- Adding new options later (e.g. `IncludeMetadata bool`) does not change the
  method signature.

#### SignedURL options (CHANGE)

```go
type SignedURLMethod string

const (
    SignedURLGet SignedURLMethod = "GET"
    SignedURLPut SignedURLMethod = "PUT"
)

type SignedURLOptions struct {
    Name   string
    Method SignedURLMethod // defaults to GET
    TTL    time.Duration
}

SignedURL(ctx context.Context, opts SignedURLOptions) (string, error)
```

#### PutMetadata (KEEP+DOC)

`PutMetadata` lets a caller update metadata without rewriting the body. The
S3 server in this repo uses it for ETag-style metadata that can only be
computed after the body is fully written. We keep it but make the
contract explicit:

- **Not atomic with `Put`.** A crash between `Put` and `PutMetadata` leaves
  the object on disk with whatever metadata `Put` itself wrote.
- **Replaces, not merges.** Callers wanting merge semantics must read,
  modify, and write back.
- **Intended use.** Hash/ETag computation, post-write tagging. For metadata
  known at write time, prefer passing it via `Object.Metadata()` to `Put`.

### `type NewStorageFunc`, `RegisterNewStorageFunc`, `UnregisterNewStorageFunc`, `NewStorage`

| Symbol | Verdict | Notes |
|---|---|---|
| `NewStorageFunc` | KEEP | Plugin contract. |
| `RegisterNewStorageFunc` | KEEP | Plugin registration. |
| `UnregisterNewStorageFunc` | KEEP | Useful for tests that swap backends. Document it as such. |
| `NewStorage` | KEEP | The plugin lookup entry point. |

---

## object.go

### `type Object interface`

| Method | Verdict | Notes |
|---|---|---|
| `Name() string` | KEEP | |
| `Open() (io.ReadCloser, error)` | DOC | Document that calling `Open` more than once is **not** supported on objects produced by `NewObjectReader` (single-use reader). |
| `OpenRange(offset, length uint64) (io.ReadCloser, error)` | DOC | Same single-use caveat. |
| `Length() uint64` | KEEP | |
| `LastModified() time.Time` | KEEP | |
| `Metadata() Metadata` | DOC | Document that List-returned objects may have empty metadata depending on the backend. |

### Constructors and options

| Symbol | Verdict | Notes |
|---|---|---|
| `type ObjectOption` | KEEP | Functional-options pattern. |
| `WithMetadata(md Metadata) ObjectOption` | KEEP | |
| `WithLastModified(t time.Time) ObjectOption` | KEEP | |
| `NewObject(ctx, name, opts...)` | **CHANGE** | Rename to `NewObjectFromFile`. The current name hides the fact that this is a local-FS `os.Stat`-backed constructor. |
| `NewObjectReader(name, body, length, opts...)` | KEEP | Streaming constructor. |
| `NewObjectBytes(name, body, opts...)` | KEEP | Bytes convenience. |

---

## metadata.go

The `Metadata` interface and its only implementation `MetadataMap` are
collapsed into a single named map type, mirroring `http.Header` and
`url.Values`:

```go
// Metadata holds object metadata as case-sensitive key/value pairs.
// The zero value is a usable empty Metadata.
type Metadata map[string]string

func (m Metadata) Get(key string) (string, bool)
func (m Metadata) Set(key, value string)
func (m Metadata) Delete(key string)
func (m Metadata) Clone() Metadata
```

| Symbol | Verdict | Notes |
|---|---|---|
| `type Metadata interface` | **REMOVE** | Single implementation; the interface adds nothing. |
| `Metadata.Len()` | **REMOVE** | Use `len(m)`. |
| `Metadata.Keys()` | **REMOVE** | Use `for k := range m`. |
| `Metadata.Get(key)` | KEEP | Wrapped on the new map type for `(value, ok)` ergonomics matching the previous interface. |
| `Metadata.Put(key, value)` | **CHANGE** | Renamed to `Set` to match `http.Header.Set`. |
| `Metadata.ToMap()` | **REMOVE** | The map *is* the value. |
| `type MetadataMap` | **REMOVE** | Becomes `type Metadata map[string]string`. |

---

## error.go

The struct-based errors are replaced with sentinel values, matching the
`io.EOF` / `os.ErrNotExist` idiom in the Go standard library.

```go
// ErrNotExist is returned when an operation targets an object that does
// not exist. Use errors.Is(err, s2.ErrNotExist) to detect.
var ErrNotExist = errors.New("s2: object not exist")

// ErrUnknownType is returned by NewStorage when no plugin is registered
// for the requested Type.
var ErrUnknownType = errors.New("s2: unknown storage type")
```

Backends wrap with `fmt.Errorf("%w: %s", ErrNotExist, name)` so callers get
both `errors.Is` matching and a useful error string.

| Symbol | Verdict | Notes |
|---|---|---|
| `type ErrNotExist struct{ Name string }` | **REMOVE** | Replaced by sentinel. |
| `(*ErrNotExist).Error()` | **REMOVE** | |
| `func IsNotExist(err) bool` | **REMOVE** | Callers use `errors.Is(err, s2.ErrNotExist)` directly. |
| `type ErrUnknownType struct{ Type Type }` | **REMOVE** | Replaced by sentinel. |
| `(*ErrUnknownType).Error()` | **REMOVE** | |
| `func IsUnknownType(err) bool` | **REMOVE** | Callers use `errors.Is(err, s2.ErrUnknownType)` directly. |

**Coverage**: add a backend-coverage test (probably in `s2test`) ensuring
that every backend's not-found path satisfies
`errors.Is(err, s2.ErrNotExist)`.

---

## config.go

| Symbol | Verdict | Notes |
|---|---|---|
| `type Type string` | KEEP | |
| `TypeOSFS`, `TypeMemFS`, `TypeS3` | KEEP | |
| `var Types []Type` | **CHANGE** | Replace with `func KnownTypes() []Type` returning a fresh copy. The mutable package-level slice can be appended to by anyone. |
| `type S3Config` | DOC | Document that `EndpointURL` overrides the SDK's resolved endpoint and is what enables S3-compatible servers like S2 itself. |
| `type Config` | DOC | The `SignedURL` field applies to `osfs`/`memfs` only — clarify what it means in each case. |

---

## utils.go

| Symbol | Verdict | Notes |
|---|---|---|
| `MustInt64(uint64) int64` | **REMOVE** | Implementation detail leaked into the public API. Move to an internal package. |
| `MustUint64(int64) uint64` | **REMOVE** | Same. |

---

## Cross-cutting items

### Atomicity matrix

For v0.2 publish a small table in `doc.go` saying which operations are
guaranteed atomic per backend. To be filled in during PR-2:

| Operation | osfs | memfs | s3 |
|---|---|---|---|
| `Put` | atomic (temp+rename) | atomic | atomic |
| `Copy` | TBD | TBD | atomic (server-side) |
| `Delete` | TBD | TBD | atomic |
| `DeleteRecursive` | non-atomic | non-atomic | non-atomic (best-effort) |

### Concurrency

Document that `Storage` instances are safe for concurrent use by multiple
goroutines. Verify per backend during PR-2.

### Context handling

Spot-check that every backend honors `ctx.Done()` for long-running
operations, especially streaming reads via `Open`. The `os.Open` path
in `osfs` does not honor context, which is acceptable for local FS but
worth documenting.

---

## Summary of v0.2.0 breaking changes

1. **`Storage`**: collapse 4 List methods → 1 `List(opts ListOptions) (ListResult, error)`.
2. **`Storage`**: remove `Move` from the interface; provide `s2.Move` free function.
3. **`Storage`**: change `SignedURL(ctx, name, ttl)` → `SignedURL(ctx, opts SignedURLOptions)`.
4. **`Object`**: rename `NewObject` → `NewObjectFromFile`.
5. **`Metadata`**: collapse `Metadata` interface + `MetadataMap` into `type Metadata map[string]string` with methods. Rename `Put` → `Set`. Remove `Len`/`Keys`/`ToMap`.
6. **Errors**: replace `*ErrNotExist`/`*ErrUnknownType` structs and their `Is*` helpers with sentinel `var ErrNotExist`/`var ErrUnknownType`. Backends wrap with `fmt.Errorf("%w: %s", …)`.
7. **`var Types`** → `func KnownTypes() []Type`.
8. **`MustInt64`/`MustUint64`**: removed from the public API (moved to `internal/`).

Non-breaking improvements bundled in the same release:

- godoc on every public symbol.
- atomicity / concurrency / context tables in a new `doc.go`.
- backend-coverage test ensuring `errors.Is(err, ErrNotExist)` for all
  not-found paths.
- Example tests (added in a separate follow-up PR).

---

## Out of scope for v0.2 audit

- Server packages (`server/`, `server/handlers/`, `server/middleware/`) —
  these are not part of the library import surface.
- CLI flag/env names — covered by separate user-facing compatibility.
- Web console templates and assets.
- v1.0.0 release planning (deferred until v0.2 has lived in use).

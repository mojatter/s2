package s2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	storageMux      sync.Mutex
	newStorageFuncs = map[Type]NewStorageFunc{}
)

// ListOptions controls a Storage.List call.
//
// All fields are optional. The zero value lists the first page of the
// storage's flat namespace.
type ListOptions struct {
	// Prefix restricts the listing to objects whose names begin with Prefix
	// when Recursive is true. A non-recursive listing treats it as a
	// directory, so every backend reads it as if it ended in "/".
	Prefix string
	// After is an opaque continuation token returned by a previous call as
	// ListResult.NextAfter; pass it to fetch the next page. Empty for the
	// first page. Its encoding is backend-specific: never construct one or
	// carry one across storages. After wins over StartAfter.
	After string
	// StartAfter is a key name picked by the caller, existing or not; the
	// listing resumes at the first entry, object or common prefix, sorting
	// after it, as S3's start-after does. Ignored when After is set.
	StartAfter string
	// Limit caps the number of returned entries, CommonPrefixes included, as
	// S3's max-keys does. Zero means the backend's default page size; follow
	// NextAfter for the rest.
	Limit int
	// Recursive, when true, walks subdirectories and returns no
	// CommonPrefixes; when false, the listing stops at the first "/" past
	// Prefix and "directory-like" entries are surfaced via CommonPrefixes.
	Recursive bool
}

// ListResult is the response from Storage.List.
type ListResult struct {
	// Objects are the objects matching the request, in lexicographic order.
	// Their metadata may be unset depending on the backend, and Metadata()
	// may report nil; use Storage.Get for full metadata and a writable map.
	Objects []Object
	// CommonPrefixes are the directory-like grouping prefixes (only populated
	// when ListOptions.Recursive is false). Each ends in "/", as S3 returns it,
	// and is listed once across the pages of a listing.
	CommonPrefixes []string
	// NextAfter is an opaque continuation token, set only when an entry
	// followed the page as it was read. When empty, the listing is exhausted.
	NextAfter string
}

// SignedURLMethod is the HTTP method that a presigned URL is authorized for.
type SignedURLMethod string

const (
	// SignedURLGet authorizes a GET request (download).
	SignedURLGet SignedURLMethod = "GET"
	// SignedURLPut authorizes a PUT request (upload).
	SignedURLPut SignedURLMethod = "PUT"
)

// SignedURLOptions controls a Storage.SignedURL call.
type SignedURLOptions struct {
	// Name is the object name to sign.
	Name string
	// Method is the HTTP method to authorize. Defaults to GET when empty.
	Method SignedURLMethod
	// TTL is how long the URL remains valid.
	TTL time.Duration
}

// Storage is a simple object storage abstraction. Implementations are
// expected to be safe for concurrent use by multiple goroutines.
//
// Errors that report a missing object wrap [ErrNotExist]; detect them with
// errors.Is(err, s2.ErrNotExist).
//
// Every method takes names exactly as stored -- see [ValidateName] and
// [ValidatePrefix] -- and wraps [ErrInvalidName] for anything else.
type Storage interface {
	// Type returns the type of the storage.
	Type() Type
	// Sub returns a new storage scoped to the given prefix. The returned
	// storage shares the parent's lifetime.
	Sub(ctx context.Context, prefix string) (Storage, error)
	// List returns the objects (and, when non-recursive, common prefixes)
	// matching opts.
	List(ctx context.Context, opts ListOptions) (ListResult, error)
	// Get returns the object identified by name, including its metadata.
	// The returned Object's Metadata map is non-nil and writable even when
	// the object carries none; a List result may report nil instead.
	// If no object exists at name, the returned error wraps ErrNotExist.
	Get(ctx context.Context, name string) (Object, error)
	// Exists reports whether anything is present at name. Backends that
	// expose a directory hierarchy (osfs, memfs) treat both regular
	// files and directories as "present"; flat key-value backends (s3)
	// only resolve to leaf objects since they have no directory
	// primitive of their own.
	Exists(ctx context.Context, name string) (bool, error)
	// Put writes obj to the storage atomically per object. Any metadata on
	// obj is persisted as part of the same call.
	Put(ctx context.Context, obj Object) error
	// PutMetadata replaces the metadata of an existing object without
	// rewriting its body. It is intended for hash- or ETag-style metadata
	// that can only be computed after the body is written. Note: PutMetadata
	// is NOT atomic with Put; a crash between the two leaves the object on
	// disk with whatever metadata Put itself wrote. Replaces (does not merge)
	// any existing metadata. A backend may fail the call when the object
	// changes concurrently; that error is the provider's own.
	PutMetadata(ctx context.Context, name string, metadata Metadata) error
	// Copy duplicates src to dst. The semantics are backend-defined: the s3
	// backend uses server-side copy, while file-backed backends stream the
	// body.
	Copy(ctx context.Context, src, dst string) error
	// Delete removes the object at name. Deleting a non-existent object is
	// a no-op and does not return an error.
	Delete(ctx context.Context, name string) error
	// DeleteRecursive removes every object whose name begins with prefix.
	// A prefix ending in "/" covers only that directory, not names that merely share it.
	// The operation is best-effort and not atomic across objects.
	DeleteRecursive(ctx context.Context, prefix string) error
	// SignedURL returns a presigned URL for the object identified by opts.
	// Backends that do not support presigning return an error.
	SignedURL(ctx context.Context, opts SignedURLOptions) (string, error)
}

// Mover is an optional interface that a Storage implementation may satisfy
// to provide a move operation that is more efficient — and possibly atomic —
// than the default Copy + Delete fallback. The osfs backend implements
// Mover via a filesystem rename, for example.
//
// Storage implementations are not required to satisfy Mover; the free
// function Move falls back to Copy + Delete when they do not.
//
// Move(k, k) must keep k, and Move of a missing key returns ErrNotExist;
// s2test.TestStorageCopyMove checks both.
type Mover interface {
	Move(ctx context.Context, src, dst string) error
}

// Move moves src to dst on s. If s implements Mover, its Move method is
// used (which may be atomic and is generally more efficient). Otherwise
// Move falls back to Copy followed by Delete; that fallback is NOT atomic:
// if Delete fails after a successful Copy, both objects exist.
func Move(ctx context.Context, s Storage, src, dst string) error {
	if mover, ok := s.(Mover); ok {
		return mover.Move(ctx, src, dst)
	}
	// Copy then Delete would delete a key moved onto itself (#457).
	if src == dst {
		_, err := s.Get(ctx, src)
		return err
	}
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}

// Uploader is an optional interface that a Storage implementation may satisfy
// to report what it stored. Put answers nothing, so a caller that needs the
// stored ETag otherwise has to read the object back; a backend already has the
// value in its own write response.
//
// Storage implementations are not required to satisfy Uploader; the free
// function Upload falls back to Put followed by Get when they do not.
//
// Upload must hold opts' conditions until the write lands, as [UploadOptions.Check]
// judges them, or fail a conditional call with an error wrapping [errors.ErrUnsupported].
type Uploader interface {
	Upload(ctx context.Context, obj Object, opts UploadOptions) (UploadResult, error)
}

// UploadOptions controls an Upload call.
type UploadOptions struct {
	// IfMatch, when set, writes only while the key holds an object with this ETag, in the quoted form Object.ETag reports.
	IfMatch string
	// IfNoneMatch, when "*", writes only while the key holds no object; any other value is unsupported.
	IfNoneMatch string
}

// Conditional reports whether opts carries a condition.
func (o UploadOptions) Conditional() bool {
	return o.IfMatch != "" || o.IfNoneMatch != ""
}

// Check judges opts against the object at name, whose ETag is etag when exists.
// It wraps ErrNotExist when IfMatch finds no object, ErrPreconditionFailed when a condition fails,
// and errors.ErrUnsupported for an IfNoneMatch other than "*".
func (o UploadOptions) Check(name, etag string, exists bool) error {
	if o.IfNoneMatch != "" && o.IfNoneMatch != "*" {
		return fmt.Errorf("s2: IfNoneMatch %q: %w", o.IfNoneMatch, errors.ErrUnsupported)
	}
	if o.IfMatch != "" {
		if !exists {
			return fmt.Errorf("%w: %s", ErrNotExist, name)
		}
		if o.IfMatch != "*" && o.IfMatch != etag {
			return fmt.Errorf("%w: %s", ErrPreconditionFailed, name)
		}
	}
	if o.IfNoneMatch != "" && exists {
		return fmt.Errorf("%w: %s", ErrPreconditionFailed, name)
	}
	return nil
}

// UploadResult reports what the backend stored. A field the backend does not
// know is left at its zero value.
type UploadResult struct {
	// ETag is the stored object's entity tag in quoted form, the same value a
	// following Get reports — including "" for a backend that has none, since
	// [Object.ETag] is itself "" when unknown.
	ETag string
}

// Upload stores obj on s and reports what was stored. If s implements Uploader,
// its Upload method is used and no read-back happens. Otherwise Upload falls
// back to Put followed by Get, which costs one more round trip and answers the
// same ETag. A capability that reports no ETag is read back the same way, so
// the result carries exactly what a following Get answers — which is "" for a
// backend that reports no ETag at all.
//
// A read-back failure is returned as the call's error even though the object
// was stored; it wraps [ErrUnknownETag] over the error Get gave. A caller
// mapping errors to a status must not turn that into "no such object" — the
// write succeeded — and can tell the case apart with errors.Is.
//
// The fallback is not atomic: a concurrent write to the same name can land
// between the Put and the read-back, and the ETag then describes that write
// rather than obj. Only a backend implementing Uploader is free of this.
// For the same reason the fallback fails a conditional call with errors.ErrUnsupported.
func Upload(ctx context.Context, s Storage, obj Object, opts UploadOptions) (UploadResult, error) {
	if u, ok := s.(Uploader); ok {
		res, err := u.Upload(ctx, obj, opts)
		if err != nil || res.ETag != "" {
			return res, err
		}
		// The capability reported no ETag; a read-back still can.
		res.ETag, err = readBackETag(ctx, s, obj.Name())
		if err != nil {
			return UploadResult{}, fmt.Errorf("%w: %w", ErrUnknownETag, err)
		}
		return res, nil
	}
	if opts.Conditional() {
		return UploadResult{}, fmt.Errorf("s2: conditional upload without Uploader: %w", errors.ErrUnsupported)
	}
	if err := s.Put(ctx, obj); err != nil {
		return UploadResult{}, err
	}
	etag, err := readBackETag(ctx, s, obj.Name())
	if err != nil {
		return UploadResult{}, fmt.Errorf("%w: %w", ErrUnknownETag, err)
	}
	return UploadResult{ETag: etag}, nil
}

// readBackETag reports the ETag a Get answers for name.
func readBackETag(ctx context.Context, s Storage, name string) (string, error) {
	got, err := s.Get(ctx, name)
	if err != nil {
		return "", err
	}
	if got == nil {
		return "", nil
	}
	return got.ETag(), nil
}

// Counter is an optional interface a Storage may satisfy to count faster than paging List; it must report what a recursive List of an unchanging, readable tree reports and may ignore UpTo, which Count caps.
type Counter interface {
	Count(ctx context.Context, opts CountOptions) (CountResult, error)
}

// CountOptions controls a Count call.
type CountOptions struct {
	// Prefix selects the names beginning with it, at any depth, as a recursive List does.
	Prefix string
	// UpTo stops the count once more than UpTo objects are seen; zero counts all.
	UpTo int64
}

// CountResult reports how many objects a Count call saw.
type CountResult struct {
	// Objects is the exact count, or UpTo when Truncated.
	Objects int64
	// Truncated reports that more than UpTo objects exist.
	Truncated bool
}

// countPageSize is the page size every bundled backend lists by default.
const countPageSize = 1000

// Count counts the objects on s matching opts, via Counter when s has it, else by paging a recursive List; a failure reports no partial count.
func Count(ctx context.Context, s Storage, opts CountOptions) (CountResult, error) {
	if opts.UpTo < 0 {
		return CountResult{}, fmt.Errorf("s2: negative CountOptions.UpTo: %d", opts.UpTo)
	}
	var (
		res CountResult
		err error
	)
	if c, ok := s.(Counter); ok {
		res, err = c.Count(ctx, opts)
	} else {
		res, err = countByList(ctx, s, opts)
	}
	if err != nil {
		return CountResult{}, err
	}
	if opts.UpTo > 0 && res.Objects > opts.UpTo {
		return CountResult{Objects: opts.UpTo, Truncated: true}, nil
	}
	return res, nil
}

// countByList pages through a recursive List, asking for one object past UpTo so the last page tells whether more exist.
func countByList(ctx context.Context, s Storage, opts CountOptions) (CountResult, error) {
	var res CountResult
	for after := ""; ; {
		lopts := ListOptions{Prefix: opts.Prefix, After: after, Recursive: true}
		if opts.UpTo > 0 && opts.UpTo-res.Objects < countPageSize {
			lopts.Limit = int(opts.UpTo - res.Objects + 1)
		}
		page, err := s.List(ctx, lopts)
		if err != nil {
			return CountResult{}, err
		}
		res.Objects += int64(len(page.Objects))
		if page.NextAfter == "" || (opts.UpTo > 0 && res.Objects > opts.UpTo) {
			return res, nil
		}
		if page.NextAfter == after {
			return CountResult{}, fmt.Errorf("s2: List returned a NextAfter that does not advance: %q", after)
		}
		after = page.NextAfter
	}
}

// NewStorageFunc is a function that creates a new storage.
type NewStorageFunc func(ctx context.Context, cfg Config) (Storage, error)

// RegisterNewStorageFunc registers a new storage function.
func RegisterNewStorageFunc(t Type, fn NewStorageFunc) {
	storageMux.Lock()
	defer storageMux.Unlock()

	newStorageFuncs[t] = fn
}

// UnregisterNewStorageFunc unregisters a storage function. Primarily useful
// in tests that swap backends.
func UnregisterNewStorageFunc(t Type) {
	storageMux.Lock()
	defer storageMux.Unlock()

	delete(newStorageFuncs, t)
}

// NewStorage creates a new storage from the given configuration. If no
// plugin is registered for cfg.Type, the returned error wraps ErrUnknownType.
func NewStorage(ctx context.Context, cfg Config) (Storage, error) {
	storageMux.Lock()
	defer storageMux.Unlock()

	if fn, ok := newStorageFuncs[cfg.Type]; ok {
		return fn(ctx, cfg)
	}
	return nil, fmt.Errorf("%w: %s", ErrUnknownType, cfg.Type)
}

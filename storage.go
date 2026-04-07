package s2

import (
	"context"
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
// All fields are optional. The zero value lists the entire flat namespace
// of the storage.
type ListOptions struct {
	// Prefix restricts the listing to objects whose names begin with Prefix.
	Prefix string
	// After is an opaque continuation token returned by a previous call as
	// ListResult.NextAfter; pass it to fetch the next page. Empty for the
	// first page.
	After string
	// Limit caps the number of returned Objects. Zero means no limit.
	Limit int
	// Recursive, when true, walks subdirectories and returns no
	// CommonPrefixes; when false, the listing stops at the first "/" past
	// Prefix and "directory-like" entries are surfaced via CommonPrefixes.
	Recursive bool
}

// ListResult is the response from Storage.List.
type ListResult struct {
	// Objects are the objects matching the request, in lexicographic order.
	// Their metadata may be unset depending on the backend; use Storage.Get
	// to fetch full metadata.
	Objects []Object
	// CommonPrefixes are the directory-like grouping prefixes (only populated
	// when ListOptions.Recursive is false).
	CommonPrefixes []string
	// NextAfter is an opaque continuation token. When empty, the listing is
	// exhausted.
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
	// If no object exists at name, the returned error wraps ErrNotExist.
	Get(ctx context.Context, name string) (Object, error)
	// Exists reports whether an object exists at name.
	Exists(ctx context.Context, name string) (bool, error)
	// Put writes obj to the storage atomically per object. Any metadata on
	// obj is persisted as part of the same call.
	Put(ctx context.Context, obj Object) error
	// PutMetadata replaces the metadata of an existing object without
	// rewriting its body. It is intended for hash- or ETag-style metadata
	// that can only be computed after the body is written. Note: PutMetadata
	// is NOT atomic with Put; a crash between the two leaves the object on
	// disk with whatever metadata Put itself wrote. Replaces (does not merge)
	// any existing metadata.
	PutMetadata(ctx context.Context, name string, metadata Metadata) error
	// Copy duplicates src to dst. The semantics are backend-defined: the s3
	// backend uses server-side copy, while file-backed backends stream the
	// body.
	Copy(ctx context.Context, src, dst string) error
	// Delete removes the object at name. Deleting a non-existent object is
	// a no-op and does not return an error.
	Delete(ctx context.Context, name string) error
	// DeleteRecursive removes every object whose name begins with prefix.
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
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
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

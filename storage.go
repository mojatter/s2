package s2

import (
	"context"
	"sync"
	"time"
)

var (
	storageMux      sync.Mutex
	newStorageFuncs = map[Type]NewStorageFunc{}
)

// Storage is an interface that represents a simple object storage.
//
// Note: Depending on the implementation (e.g., S3), objects returned by
// List operations may not contain metadata. Use Get to retrieve the
// complete metadata.
type Storage interface {
	// Type returns the type of the storage.
	Type() Type
	// Sub returns a new storage with the specified prefix.
	Sub(ctx context.Context, prefix string) (Storage, error)
	// List returns a list of objects in the storage with the specified prefix.
	List(ctx context.Context, prefix string, limit int) ([]Object, []string, error)
	// ListAfter returns a list of objects in the storage after the specified name.
	ListAfter(ctx context.Context, prefix string, limit int, after string) ([]Object, []string, error)
	// ListRecursive returns a list of objects in the storage recursively.
	ListRecursive(ctx context.Context, prefix string, limit int) ([]Object, error)
	// ListRecursiveAfter returns a list of objects in the storage recursively after the specified name.
	ListRecursiveAfter(ctx context.Context, prefix string, limit int, after string) ([]Object, error)
	// Get returns the specified object.
	Get(ctx context.Context, name string) (Object, error)
	// Exists returns true if the specified object exists.
	Exists(ctx context.Context, name string) (bool, error)
	// Put puts the specified object into the storage.
	Put(ctx context.Context, obj Object) error
	// PutMetadata puts the specified metadata for the specified object.
	PutMetadata(ctx context.Context, name string, metadata Metadata) error
	// Copy copies the source object to the destination.
	Copy(ctx context.Context, src, dst string) error
	// Move moves the source object to the destination.
	Move(ctx context.Context, src, dst string) error
	// Delete deletes the specified object.
	Delete(ctx context.Context, name string) error
	// DeleteRecursive deletes all objects with the specified prefix.
	DeleteRecursive(ctx context.Context, prefix string) error
	// SignedURL returns a signed URL for the specified object.
	SignedURL(ctx context.Context, name string, ttl time.Duration) (string, error)
}

// NewStorageFunc is a function that creates a new storage.
type NewStorageFunc func(ctx context.Context, cfg Config) (Storage, error)

// RegisterNewStorageFunc registers a new storage function.
func RegisterNewStorageFunc(t Type, fn NewStorageFunc) {
	storageMux.Lock()
	defer storageMux.Unlock()

	newStorageFuncs[t] = fn
}

// UnregisterNewStorageFunc unregisters a storage function.
func UnregisterNewStorageFunc(t Type) {
	storageMux.Lock()
	defer storageMux.Unlock()

	delete(newStorageFuncs, t)
}

// NewStorage creates a new storage from the given configuration.
func NewStorage(ctx context.Context, cfg Config) (Storage, error) {
	storageMux.Lock()
	defer storageMux.Unlock()

	if fn, ok := newStorageFuncs[cfg.Type]; ok {
		return fn(ctx, cfg)
	}
	return nil, &ErrUnknownType{Type: cfg.Type}
}

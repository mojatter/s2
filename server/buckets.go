package server

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/fs"
)

const keepFile = ".keep"

func isKeepFile(name string) bool {
	return path.Base(name) == keepFile
}

// FilterKeep removes .keep marker files from a list of objects.
func FilterKeep(objs []s2.Object) []s2.Object {
	filtered := make([]s2.Object, 0, len(objs))
	for _, o := range objs {
		if !isKeepFile(o.Name()) {
			filtered = append(filtered, o)
		}
	}
	return filtered
}

// ErrBucketNotFound is returned when a bucket does not exist.
type ErrBucketNotFound struct {
	Name string
}

func (e *ErrBucketNotFound) Error() string {
	return "bucket not found: " + e.Name
}

type Buckets struct {
	strg s2.Storage
}

func newBuckets(ctx context.Context, cfg *Config) (*Buckets, error) {
	strg, err := s2.NewStorage(ctx, cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}
	return &Buckets{strg: strg}, nil
}

func (bs *Buckets) Names() ([]string, error) {
	ctx := context.Background()
	_, prefixes, err := bs.strg.List(ctx, "", -1)
	if err != nil {
		return nil, err
	}
	return prefixes, nil
}

func (bs *Buckets) Get(ctx context.Context, name string) (s2.Storage, error) {
	exists, err := bs.Exists(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &ErrBucketNotFound{Name: name}
	}
	return bs.strg.Sub(ctx, name)
}

// CreatedAt returns the creation time of a bucket by reading the .keep marker file.
// If the marker is missing, the current time is returned as a fallback.
func (bs *Buckets) CreatedAt(ctx context.Context, name string) time.Time {
	sub, err := bs.strg.Sub(ctx, name)
	if err != nil {
		return time.Now()
	}
	obj, err := sub.Get(ctx, keepFile)
	if err != nil {
		return time.Now()
	}
	return obj.LastModified()
}

func (bs *Buckets) Exists(name string) (bool, error) {
	names, err := bs.Names()
	if err != nil {
		return false, err
	}
	for _, n := range names {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

func (bs *Buckets) Create(ctx context.Context, name string) error {
	obj := s2.NewObjectBytes(name+"/"+keepFile, []byte{})
	return bs.strg.Put(ctx, obj)
}

func (bs *Buckets) Delete(ctx context.Context, name string) error {
	return bs.strg.DeleteRecursive(ctx, name)
}

func (bs *Buckets) CreateFolder(ctx context.Context, bucket, key string) error {
	sub, err := bs.strg.Sub(ctx, bucket)
	if err != nil {
		return err
	}
	obj := s2.NewObjectBytes(key+"/"+keepFile, []byte{})
	return sub.Put(ctx, obj)
}


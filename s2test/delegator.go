package s2test

import (
	"context"
	"time"

	"github.com/mojatter/s2"
)

type StorageDelegator struct {
	TypeFunc               func() s2.Type
	SubFunc                func(ctx context.Context, prefix string) (s2.Storage, error)
	ListFunc               func(ctx context.Context, prefix string, limit int) ([]s2.Object, []string, error)
	ListAfterFunc          func(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, []string, error)
	ListRecursiveFunc      func(ctx context.Context, prefix string, limit int) ([]s2.Object, error)
	ListRecursiveAfterFunc func(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, error)
	GetFunc                func(ctx context.Context, name string) (s2.Object, error)
	ExistsFunc             func(ctx context.Context, name string) (bool, error)
	PutFunc                func(ctx context.Context, obj s2.Object) error
	PutMetadataFunc        func(ctx context.Context, name string, metadata s2.Metadata) error
	CopyFunc               func(ctx context.Context, src, dst string) error
	MoveFunc               func(ctx context.Context, src, dst string) error
	DeleteFunc             func(ctx context.Context, name string) error
	DeleteRecursiveFunc    func(ctx context.Context, prefix string) error
	SignedURLFunc          func(ctx context.Context, name string, ttl time.Duration) (string, error)
}

var _ s2.Storage = (*StorageDelegator)(nil)

func (d *StorageDelegator) Type() s2.Type {
	if d.TypeFunc != nil {
		return d.TypeFunc()
	}
	return ""
}

func (d *StorageDelegator) Sub(ctx context.Context, prefix string) (s2.Storage, error) {
	if d.SubFunc != nil {
		return d.SubFunc(ctx, prefix)
	}
	return nil, nil
}

func (d *StorageDelegator) List(ctx context.Context, prefix string, limit int) ([]s2.Object, []string, error) {
	if d.ListFunc != nil {
		return d.ListFunc(ctx, prefix, limit)
	}
	return nil, nil, nil
}

func (d *StorageDelegator) ListAfter(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, []string, error) {
	if d.ListAfterFunc != nil {
		return d.ListAfterFunc(ctx, prefix, limit, after)
	}
	return nil, nil, nil
}

func (d *StorageDelegator) ListRecursive(ctx context.Context, prefix string, limit int) ([]s2.Object, error) {
	if d.ListRecursiveFunc != nil {
		return d.ListRecursiveFunc(ctx, prefix, limit)
	}
	return nil, nil
}

func (d *StorageDelegator) ListRecursiveAfter(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, error) {
	if d.ListRecursiveAfterFunc != nil {
		return d.ListRecursiveAfterFunc(ctx, prefix, limit, after)
	}
	return nil, nil
}

func (d *StorageDelegator) Get(ctx context.Context, name string) (s2.Object, error) {
	if d.GetFunc != nil {
		return d.GetFunc(ctx, name)
	}
	return nil, nil
}

func (d *StorageDelegator) Put(ctx context.Context, obj s2.Object) error {
	if d.PutFunc != nil {
		return d.PutFunc(ctx, obj)
	}
	return nil
}

func (d *StorageDelegator) Exists(ctx context.Context, name string) (bool, error) {
	if d.ExistsFunc != nil {
		return d.ExistsFunc(ctx, name)
	}
	return false, nil
}

func (d *StorageDelegator) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	if d.PutMetadataFunc != nil {
		return d.PutMetadataFunc(ctx, name, metadata)
	}
	return nil
}

func (d *StorageDelegator) Copy(ctx context.Context, src, dst string) error {
	if d.CopyFunc != nil {
		return d.CopyFunc(ctx, src, dst)
	}
	return nil
}

func (d *StorageDelegator) Move(ctx context.Context, src, dst string) error {
	if d.MoveFunc != nil {
		return d.MoveFunc(ctx, src, dst)
	}
	return nil
}


func (d *StorageDelegator) Delete(ctx context.Context, name string) error {
	if d.DeleteFunc != nil {
		return d.DeleteFunc(ctx, name)
	}
	return nil
}

func (d *StorageDelegator) DeleteRecursive(ctx context.Context, prefix string) error {
	if d.DeleteRecursiveFunc != nil {
		return d.DeleteRecursiveFunc(ctx, prefix)
	}
	return nil
}

func (d *StorageDelegator) SignedURL(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if d.SignedURLFunc != nil {
		return d.SignedURLFunc(ctx, name, ttl)
	}
	return "", nil
}

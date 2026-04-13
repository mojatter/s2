package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

// gcsClient abstracts the GCS SDK so that tests can swap in a mock.
type gcsClient interface {
	bucket(name string) gcsBucket
}

type gcsBucket interface {
	object(name string) gcsObject
	objects(ctx context.Context, q *storage.Query) gcsObjectIterator
	signedURL(name string, opts *storage.SignedURLOptions) (string, error)
}

type gcsObject interface {
	attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	newReader(ctx context.Context) (io.ReadCloser, error)
	newRangeReader(ctx context.Context, offset, length int64) (io.ReadCloser, error)
	newWriter(ctx context.Context, metadata map[string]string) io.WriteCloser
	update(ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error)
	copyTo(ctx context.Context, dst gcsObject) error
	delete(ctx context.Context) error
}

type gcsObjectIterator interface {
	next() (*storage.ObjectAttrs, error)
}

// --- sdk implementations wrapping the GCS SDK ---

type sdkClient struct {
	c *storage.Client
}

func (c *sdkClient) bucket(name string) gcsBucket {
	return &sdkBucket{b: c.c.Bucket(name)}
}

type sdkBucket struct {
	b *storage.BucketHandle
}

func (b *sdkBucket) object(name string) gcsObject {
	return &sdkObject{obj: b.b.Object(name)}
}

func (b *sdkBucket) objects(ctx context.Context, q *storage.Query) gcsObjectIterator {
	return &sdkObjectIterator{it: b.b.Objects(ctx, q)}
}

func (b *sdkBucket) signedURL(name string, opts *storage.SignedURLOptions) (string, error) {
	return b.b.SignedURL(name, opts)
}

type sdkObject struct {
	obj *storage.ObjectHandle
}

func (o *sdkObject) attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return o.obj.Attrs(ctx)
}

func (o *sdkObject) newReader(ctx context.Context) (io.ReadCloser, error) {
	return o.obj.NewReader(ctx)
}

func (o *sdkObject) newRangeReader(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	return o.obj.NewRangeReader(ctx, offset, length)
}

func (o *sdkObject) newWriter(ctx context.Context, metadata map[string]string) io.WriteCloser {
	w := o.obj.NewWriter(ctx)
	if len(metadata) > 0 {
		w.Metadata = metadata
	}
	return w
}

func (o *sdkObject) update(ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
	return o.obj.Update(ctx, uattrs)
}

func (o *sdkObject) copyTo(ctx context.Context, dst gcsObject) error {
	// The caller (gcsStorage.Copy) always passes objects from the same
	// client, so dst is guaranteed to be *sdkObject.
	dstObj := dst.(*sdkObject)
	_, err := dstObj.obj.CopierFrom(o.obj).Run(ctx)
	return err
}

func (o *sdkObject) delete(ctx context.Context) error {
	return o.obj.Delete(ctx)
}

type sdkObjectIterator struct {
	it *storage.ObjectIterator
}

func (i *sdkObjectIterator) next() (*storage.ObjectAttrs, error) {
	return i.it.Next()
}

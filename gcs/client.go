package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	storagev1 "google.golang.org/api/storage/v1"
)

// objectPatch is one Objects.Patch: metadata keys to set, keys to delete, the Content-Type to set or clear, and what the object must still be.
type objectPatch struct {
	metadata         map[string]string
	deleteKeys       []string
	contentType      string
	clearContentType bool
	generation       int64
	metageneration   int64
}

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
	newWriter(ctx context.Context, metadata map[string]string, contentType string) io.WriteCloser
	patch(ctx context.Context, p objectPatch) error
	copyTo(ctx context.Context, dst gcsObject) error
	delete(ctx context.Context) error
}

type gcsObjectIterator interface {
	next() (*storage.ObjectAttrs, error)
	// setPageToken resumes the iteration at the given page token.
	setPageToken(token string)
	// setMaxSize caps how many items one page holds.
	setMaxSize(n int)
	// remaining reports how many items of the current page are still buffered.
	remaining() int
	// nextPageToken is the token for the page after the current one, empty
	// once the listing is exhausted.
	nextPageToken() string
}

// --- sdk implementations wrapping the GCS SDK ---

type sdkClient struct {
	c *storage.Client
	// svc is the JSON API client; only it can delete a single metadata key.
	svc *storagev1.Service
}

func (c *sdkClient) bucket(name string) gcsBucket {
	return &sdkBucket{b: c.c.Bucket(name), svc: c.svc, name: name}
}

type sdkBucket struct {
	b    *storage.BucketHandle
	svc  *storagev1.Service
	name string
}

func (b *sdkBucket) object(name string) gcsObject {
	return &sdkObject{obj: b.b.Object(name), svc: b.svc, bucket: b.name, key: name}
}

func (b *sdkBucket) objects(ctx context.Context, q *storage.Query) gcsObjectIterator {
	return &sdkObjectIterator{it: b.b.Objects(ctx, q)}
}

func (b *sdkBucket) signedURL(name string, opts *storage.SignedURLOptions) (string, error) {
	return b.b.SignedURL(name, opts)
}

type sdkObject struct {
	obj    *storage.ObjectHandle
	svc    *storagev1.Service
	bucket string
	key    string
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

func (o *sdkObject) newWriter(ctx context.Context, metadata map[string]string, contentType string) io.WriteCloser {
	w := o.obj.NewWriter(ctx)
	if len(metadata) > 0 {
		w.Metadata = metadata
	}
	w.ContentType = contentType
	// Without this the SDK sniffs the body; an unset type is guessed from the name on read instead.
	w.ForceEmptyContentType = contentType == ""
	return w
}

// patchObject renders p as a request body. NullFields carries the per-key deletes that ObjectAttrsToUpdate cannot express.
func patchObject(p objectPatch) *storagev1.Object {
	obj := &storagev1.Object{Metadata: p.metadata, ContentType: p.contentType}
	for _, k := range p.deleteKeys {
		obj.NullFields = append(obj.NullFields, "Metadata."+k)
	}
	if len(p.metadata) == 0 {
		// An empty map is dropped from the request body unless it is forced.
		obj.ForceSendFields = append(obj.ForceSendFields, "Metadata")
	}
	if p.clearContentType {
		obj.NullFields = append(obj.NullFields, "ContentType")
	}
	return obj
}

// patch applies p in one request, under whichever preconditions the caller could read.
func (o *sdkObject) patch(ctx context.Context, p objectPatch) error {
	call := o.svc.Objects.Patch(o.bucket, o.key, patchObject(p)).Context(ctx)
	// A zero generation would read as "the object must not exist"; an endpoint that omits it gets no precondition.
	if p.generation != 0 {
		call = call.IfGenerationMatch(p.generation)
	}
	if p.metageneration != 0 {
		call = call.IfMetagenerationMatch(p.metageneration)
	}
	_, err := call.Do()
	return err
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

func (i *sdkObjectIterator) setPageToken(token string) {
	i.it.PageInfo().Token = token
}

func (i *sdkObjectIterator) setMaxSize(n int) {
	i.it.PageInfo().MaxSize = n
}

func (i *sdkObjectIterator) remaining() int {
	return i.it.PageInfo().Remaining()
}

func (i *sdkObjectIterator) nextPageToken() string {
	return i.it.PageInfo().Token
}

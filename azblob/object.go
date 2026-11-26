package azblob

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/mojatter/s2"
)

type object struct {
	client      azblobClient
	container   string
	prefix      string
	name        string
	length      uint64
	modified    time.Time
	metadata    s2.Metadata
	contentType string
	etag        string
	// version is the native ETag Get or List saw; "" when unknown.
	version string
	class   s2.StorageClass
}

var _ s2.StorageClasser = (*object)(nil)

// StorageClass implements s2.StorageClasser.
func (o *object) StorageClass() s2.StorageClass {
	return o.class
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	return o.download(context.Background(), 0, 0)
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	if length == 0 {
		return o.openEmpty(context.Background())
	}
	return o.download(context.Background(), s2.MustInt64(offset), s2.MustInt64(length))
}

// openEmpty fails as download does but reads nothing, since a count of 0 means to the end.
func (o *object) openEmpty(ctx context.Context) (io.ReadCloser, error) {
	props, err := o.client.getProperties(ctx, o.container, o.key())
	if err != nil {
		return nil, mapNotExist(err, o.name)
	}
	// A listing reports Azure's ETag unquoted, the properties quoted.
	if o.version != "" && props.etag != "" && strings.Trim(props.etag, `"`) != strings.Trim(o.version, `"`) {
		return nil, fmt.Errorf("%w: %s", s2.ErrModified, o.name)
	}
	if props.archived() {
		return nil, errArchived(o.name)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

// download reads the version Get or List saw, or the current one when version is "".
func (o *object) download(ctx context.Context, offset, count int64) (io.ReadCloser, error) {
	rc, err := o.client.downloadStream(ctx, o.container, o.key(), offset, count, o.version)
	if bloberror.HasCode(err, bloberror.ConditionNotMet) {
		// Azurite answers If-Match on a missing blob with 412 rather than 404, so look before calling it modified.
		if _, perr := o.client.getProperties(ctx, o.container, o.key()); isBlobNotFound(perr) {
			return nil, fmt.Errorf("%w: %s", s2.ErrNotExist, o.name)
		}
		return nil, fmt.Errorf("%w: %s: %w", s2.ErrModified, o.name, err)
	}
	if bloberror.HasCode(err, bloberror.BlobArchived) {
		return nil, errArchived(o.name)
	}
	if err != nil {
		return nil, mapNotExist(err, o.name)
	}
	return rc, nil
}

func (o *object) Length() uint64 {
	return o.length
}

func (o *object) LastModified() time.Time {
	return o.modified
}

func (o *object) Metadata() s2.Metadata {
	return o.metadata
}

func (o *object) key() string {
	if o.prefix == "" {
		return o.name
	}
	return fmt.Sprintf("%s/%s", o.prefix, o.name)
}

func (o *object) ContentType() string {
	return o.contentType
}

func (o *object) ETag() string {
	return o.etag
}

// blobETag returns contentMD5 as a quoted hex string, else the quoted opaque etag.
func blobETag(contentMD5 []byte, etag string) string {
	if len(contentMD5) > 0 {
		return `"` + hex.EncodeToString(contentMD5) + `"`
	}
	if etag == "" || strings.HasPrefix(etag, `"`) {
		return etag
	}
	return `"` + etag + `"`
}

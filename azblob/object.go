package azblob

import (
	"context"
	"encoding/hex"
	"errors"
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
}

// errModified is what Open returns once the blob is another version than the one Get or List saw.
var errModified = errors.New("object modified since Get")

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	return o.download(0, 0)
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	return o.download(s2.MustInt64(offset), s2.MustInt64(length))
}

func (o *object) download(offset, count int64) (io.ReadCloser, error) {
	rc, err := o.client.downloadStream(context.Background(), o.container, o.key(), offset, count, o.version)
	if bloberror.HasCode(err, bloberror.ConditionNotMet) {
		// Azurite answers If-Match on a missing blob with 412 rather than 404, so look before calling it modified.
		if _, perr := o.client.getProperties(context.Background(), o.container, o.key()); isBlobNotFound(perr) {
			return nil, fmt.Errorf("%w: %s", s2.ErrNotExist, o.name)
		}
		return nil, fmt.Errorf("%w: %s: %w", errModified, o.name, err)
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

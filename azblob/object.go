package azblob

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

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
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	rc, err := o.client.downloadStream(context.Background(), o.container, o.key(), 0, 0)
	if err != nil {
		return nil, mapNotExist(err, o.name)
	}
	return rc, nil
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	rc, err := o.client.downloadStream(context.Background(), o.container, o.key(), s2.MustInt64(offset), s2.MustInt64(length))
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
	if o.metadata == nil {
		o.metadata = make(s2.Metadata)
	}
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

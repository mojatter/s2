package gcs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mojatter/s2"
)

type object struct {
	client       gcsClient
	bucket       string
	prefix       string
	name         string
	length       uint64
	lastModified time.Time
	metadata     s2.Metadata
	contentType  string
	etag         string
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	obj := o.client.bucket(o.bucket).object(o.key())
	rc, err := obj.newReader(context.Background())
	if err != nil {
		return nil, mapNotExist(err, o.name)
	}
	return rc, nil
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	obj := o.client.bucket(o.bucket).object(o.key())
	rc, err := obj.newRangeReader(context.Background(), s2.MustInt64(offset), s2.MustInt64(length))
	if err != nil {
		return nil, mapNotExist(err, o.name)
	}
	return rc, nil
}

func (o *object) Length() uint64 {
	return o.length
}

func (o *object) LastModified() time.Time {
	return o.lastModified
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

// objectETag returns the MD5 of attrs as a quoted hex string, else its opaque etag.
func objectETag(attrs *storage.ObjectAttrs) string {
	if len(attrs.MD5) > 0 {
		return `"` + hex.EncodeToString(attrs.MD5) + `"`
	}
	return quoteETag(attrs.Etag)
}

func quoteETag(etag string) string {
	if etag == "" || strings.HasPrefix(etag, `"`) {
		return etag
	}
	return `"` + etag + `"`
}

// Keys s2-server stored in provider user metadata before v0.18.
const (
	legacyETagKey        = "s2-etag"
	legacyContentTypeKey = "s2-content-type"
)

// liftLegacy returns md without s2-server's legacy keys, and the Content-Type they held.
func liftLegacy(md map[string]string) (s2.Metadata, string) {
	if md == nil {
		return nil, ""
	}
	out := make(s2.Metadata, len(md))
	var contentType string
	for k, v := range md {
		switch strings.ToLower(k) {
		case legacyContentTypeKey:
			contentType = v
		case legacyETagKey:
		default:
			out[k] = v
		}
	}
	return out, contentType
}

// objectMetadata returns attrs' user metadata and Content-Type, preferring a legacy s2-content-type.
func objectMetadata(attrs *storage.ObjectAttrs) (s2.Metadata, string) {
	md, contentType := liftLegacy(attrs.Metadata)
	if contentType == "" {
		contentType = attrs.ContentType
	}
	return md, contentType
}

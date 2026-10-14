package gcs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mojatter/s2"
	"google.golang.org/api/googleapi"
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
	// generation is the version Get or List saw; 0 when unknown.
	generation int64
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	return o.read(0, -1)
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	return o.read(s2.MustInt64(offset), s2.MustInt64(length))
}

// read reads the generation Get or List saw; the precondition spares the transfer, the check covers endpoints that ignore it.
func (o *object) read(offset, length int64) (io.ReadCloser, error) {
	obj := o.client.bucket(o.bucket).object(o.key())
	rc, generation, err := obj.newRangeReader(context.Background(), offset, length, o.generation)
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed {
		return nil, fmt.Errorf("%w: %s: %w", s2.ErrModified, o.name, err)
	}
	if err != nil {
		return nil, mapNotExist(err, o.name)
	}
	if o.generation != 0 && generation != 0 && generation != o.generation {
		_ = rc.Close()
		return nil, fmt.Errorf("%w: %s", s2.ErrModified, o.name)
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

// Keys s2-server stored in provider user metadata before v0.18; read until v1.0.
const (
	legacyETagKey        = "s2-etag"
	legacyContentTypeKey = "s2-content-type"
	// legacyDefaultContentType is what s2-server stored when a client sent no Content-Type.
	legacyDefaultContentType = "binary/octet-stream"
)

// liftLegacy returns md without the keys a pre-v0.18 s2-server wrote, the Content-Type it kept there, and whether it wrote md.
// Every such write carried s2-etag, so without it the s2-* keys are the client's own and stay.
func liftLegacy(md map[string]string) (s2.Metadata, string, bool) {
	if !hasLegacyETag(md) {
		return s2.Metadata(md), "", false
	}
	out := make(s2.Metadata, len(md))
	var contentType string
	for k, v := range md {
		switch strings.ToLower(k) {
		case legacyContentTypeKey:
			if v != legacyDefaultContentType {
				contentType = v
			}
		case legacyETagKey:
		default:
			out[k] = v
		}
	}
	return out, contentType, true
}

func hasLegacyETag(md map[string]string) bool {
	for k := range md {
		if strings.EqualFold(k, legacyETagKey) {
			return true
		}
	}
	return false
}

// objectMetadata returns attrs' user metadata and Content-Type; a pre-v0.18 object's native type was sniffed, so it is ignored.
func objectMetadata(attrs *storage.ObjectAttrs) (s2.Metadata, string) {
	md, contentType, legacy := liftLegacy(attrs.Metadata)
	if !legacy {
		contentType = attrs.ContentType
	}
	return md, contentType
}

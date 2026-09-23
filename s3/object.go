package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mojatter/s2"
)

type object struct {
	client       clientAPI
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

// key is the object's full key. Not path.Join: a listing hands out whatever
// the bucket holds, and joining would fold a name such as "photos/" onto the
// neighbouring object "photos".
func (o *object) key() string {
	if o.prefix == "" {
		return o.name
	}
	return o.prefix + "/" + o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	res, err := o.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key()),
	})
	if err != nil {
		var noSuchKeyErr *s3types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return nil, fmt.Errorf("%w: %s", s2.ErrNotExist, o.key())
		}
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	return res.Body, nil
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

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	res, err := o.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key()),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		var noSuchKeyErr *s3types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return nil, fmt.Errorf("%w: %s", s2.ErrNotExist, o.key())
		}
		return nil, fmt.Errorf("failed to get object range: %w", err)
	}
	return res.Body, nil
}

func (o *object) ContentType() string {
	return o.contentType
}

func (o *object) ETag() string {
	return o.etag
}

// Keys s2-server stored in provider user metadata before v0.18; read until v1.0.
const (
	legacyETagKey        = "s2-etag"
	legacyContentTypeKey = "s2-content-type"
	// legacyDefaultContentType is what s2-server stored when a client sent no Content-Type.
	legacyDefaultContentType = "binary/octet-stream"
)

// liftLegacy returns md without the keys a pre-v0.18 s2-server wrote, and the Content-Type it kept there.
// Every such write carried s2-etag, so without it the s2-* keys are the client's own and stay.
func liftLegacy(md map[string]string) (s2.Metadata, string) {
	if !hasLegacyETag(md) {
		return s2.Metadata(md), ""
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
	return out, contentType
}

func hasLegacyETag(md map[string]string) bool {
	for k := range md {
		if strings.EqualFold(k, legacyETagKey) {
			return true
		}
	}
	return false
}

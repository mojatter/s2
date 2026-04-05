package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
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
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	res, err := o.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(path.Join(o.prefix, o.name)),
	})
	if err != nil {
		var noSuchKeyErr *s3types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return nil, &s2.ErrNotExist{Name: path.Join(o.prefix, o.name)}
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
	if o.metadata == nil {
		o.metadata = make(s2.MetadataMap)
	}
	return o.metadata
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	res, err := o.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(path.Join(o.prefix, o.name)),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		var noSuchKeyErr *s3types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return nil, &s2.ErrNotExist{Name: path.Join(o.prefix, o.name)}
		}
		return nil, fmt.Errorf("failed to get object range: %w", err)
	}
	return res.Body, nil
}


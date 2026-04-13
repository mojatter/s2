package gcs

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/internal/numconv"
)

type object struct {
	client       gcsClient
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
	obj := o.client.bucket(o.bucket).object(o.key())
	rc, err := obj.newReader(context.Background())
	if err != nil {
		return nil, mapNotExist(err, o.name)
	}
	return rc, nil
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	obj := o.client.bucket(o.bucket).object(o.key())
	rc, err := obj.newRangeReader(context.Background(), numconv.MustInt64(offset), numconv.MustInt64(length))
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

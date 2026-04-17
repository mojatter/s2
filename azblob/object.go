package azblob

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/mojatter/s2"
)

type object struct {
	client    azblobClient
	container string
	prefix    string
	name      string
	length    uint64
	modified  time.Time
	metadata  s2.Metadata
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

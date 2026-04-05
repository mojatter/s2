package s2test

import (
	"bytes"
	"io"
	"time"

	"github.com/mojatter/s2"
)

// BytesObject is a test helper that implements s2.Object backed by a byte slice.
// Unlike s2.NewObjectBytes, Open() can be called multiple times and metadata
// can be set directly at construction.
type BytesObject struct {
	Name_     string
	Data      []byte
	Metadata_ s2.Metadata
}

var _ s2.Object = (*BytesObject)(nil)

func (o *BytesObject) Name() string { return o.Name_ }
func (o *BytesObject) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(o.Data)), nil
}
func (o *BytesObject) Length() uint64          { return uint64(len(o.Data)) }
func (o *BytesObject) LastModified() time.Time { return time.Now() }
func (o *BytesObject) Metadata() s2.Metadata   { return o.Metadata_ }
func (o *BytesObject) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	if offset+length > uint64(len(o.Data)) {
		return nil, io.EOF
	}
	return io.NopCloser(bytes.NewReader(o.Data[offset : offset+length])), nil
}

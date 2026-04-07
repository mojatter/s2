package s2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mojatter/s2/internal/numconv"
)

// Object is an interface that represents an object in a storage.
type Object interface {
	// Name returns the name of the object.
	Name() string
	// Open opens the object for reading and returns the reader stream.
	// The caller is responsible for closing the returned io.ReadCloser.
	Open() (io.ReadCloser, error)
	// OpenRange opens the object for reading the specified range and returns the reader stream.
	// The caller is responsible for closing the returned io.ReadCloser.
	OpenRange(offset, length uint64) (io.ReadCloser, error)
	// Length returns the length of the object in bytes.
	Length() uint64
	// LastModified returns the last modified time of the object.
	LastModified() time.Time
	// Metadata returns the metadata of the object.
	//
	// Note: Depending on the storage implementation (e.g., S3), objects
	// returned by List operations may not contain metadata. Use Storage.Get
	// to fetch the complete metadata.
	Metadata() Metadata
}

// ObjectOption is a functional option for configuring objects created by
// NewObject, NewObjectReader, and NewObjectBytes.
type ObjectOption func(*object)

// WithMetadata sets the metadata on the object.
func WithMetadata(md Metadata) ObjectOption {
	return func(o *object) {
		o.metadata = md
	}
}

// WithLastModified sets the last modified time on the object.
func WithLastModified(t time.Time) ObjectOption {
	return func(o *object) {
		o.lastModified = t
	}
}

// NewObjectFromFile creates a new Object backed by a file on the local
// filesystem. The file at name must exist and not be a directory; otherwise
// the returned error wraps ErrNotExist. The Object's Length and
// LastModified are populated from os.Stat; metadata can be supplied via
// WithMetadata.
//
// Reads via Open are performed lazily by re-opening the underlying file.
// The supplied ctx is currently unused but reserved for future cancellation.
func NewObjectFromFile(ctx context.Context, name string, opts ...ObjectOption) (Object, error) {
	info, err := os.Stat(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotExist, name)
		}
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrNotExist, name)
	}
	o := &object{
		name:         name,
		length:       numconv.MustUint64(info.Size()),
		lastModified: info.ModTime(),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o, nil
}

// NewObjectReader creates new object from io.ReadCloser.
func NewObjectReader(name string, body io.ReadCloser, length uint64, opts ...ObjectOption) Object {
	o := &object{
		name:         name,
		body:         body,
		length:       length,
		lastModified: time.Now(),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// NewObjectBytes creates new object from byte slice.
func NewObjectBytes(name string, body []byte, opts ...ObjectOption) Object {
	return NewObjectReader(name, io.NopCloser(bytes.NewReader(body)), uint64(len(body)), opts...)
}

type object struct {
	name         string
	body         io.ReadCloser
	length       uint64
	lastModified time.Time
	metadata     Metadata
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	if o.body != nil {
		return o.body, nil
	}
	return os.Open(o.name)
}

func (o *object) OpenRange(offset, length uint64) (io.ReadCloser, error) {
	rc, err := o.Open()
	if err != nil {
		return nil, err
	}
	if offset == 0 && length == o.length {
		return rc, nil
	}
	if seeker, ok := rc.(io.ReadSeeker); ok {
		if _, err := seeker.Seek(numconv.MustInt64(offset), io.SeekStart); err != nil {
			_ = rc.Close()
			return nil, err
		}
		return &limitReadCloser{
			Reader: io.LimitReader(seeker, numconv.MustInt64(length)),
			Closer: rc,
		}, nil
	}
	// Fallback for non-seeker
	if _, err := io.CopyN(io.Discard, rc, numconv.MustInt64(offset)); err != nil {
		_ = rc.Close()
		return nil, err
	}
	return &limitReadCloser{
		Reader: io.LimitReader(rc, numconv.MustInt64(length)),
		Closer: rc,
	}, nil
}

type limitReadCloser struct {
	io.Reader
	io.Closer
}

func (o *object) Length() uint64 {
	return o.length
}

func (o *object) LastModified() time.Time {
	return o.lastModified
}

func (o *object) Metadata() Metadata {
	if o.metadata == nil {
		o.metadata = make(Metadata)
	}
	return o.metadata
}

package s2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
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
	// Metadata returns the object's user metadata. Writing to the returned
	// map changes the in-memory Object, never the stored one.
	//
	// Storage.Get returns a writable map even when the object has none. A
	// List result reports whatever the backend holds, which may be nil: reads
	// are safe on a nil map, writes to one panic. On some backends (e.g. S3)
	// a listed object carries no metadata at all; use Storage.Get for it.
	Metadata() Metadata
	// ContentType returns the object's MIME type, or "" when unknown.
	// Objects returned by List may report "" (e.g., S3); use Storage.Get.
	ContentType() string
	// ETag returns the object's entity tag in quoted form, or "" when unknown.
	// Objects returned by List carry it without extra network round trips.
	ETag() string
}

// ObjectOption is a functional option for configuring objects created by
// NewObject, NewObjectReader, and NewObjectBytes.
type ObjectOption func(*object)

// WithMetadata sets the metadata on the object, keeping md by reference.
func WithMetadata(md Metadata) ObjectOption {
	return func(o *object) {
		o.metadata = md
	}
}

// WithContentType sets the content type on the object.
func WithContentType(contentType string) ObjectOption {
	return func(o *object) {
		o.contentType = contentType
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
		length:       MustUint64(info.Size()),
		lastModified: info.ModTime(),
	}
	for _, opt := range opts {
		opt(o)
	}
	o.initMetadata()
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
	o.initMetadata()
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
	contentType  string
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
		if _, err := seeker.Seek(MustInt64(offset), io.SeekStart); err != nil {
			_ = rc.Close()
			return nil, err
		}
		return &limitReadCloser{
			Reader: io.LimitReader(seeker, MustInt64(length)),
			Closer: rc,
		}, nil
	}
	// Fallback for non-seeker
	if _, err := io.CopyN(io.Discard, rc, MustInt64(offset)); err != nil {
		_ = rc.Close()
		return nil, err
	}
	return &limitReadCloser{
		Reader: io.LimitReader(rc, MustInt64(length)),
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
	return o.metadata
}

func (o *object) ContentType() string {
	return o.contentType
}

// ETag returns "": the entity tag is assigned by the storage that stores the object.
func (o *object) ETag() string {
	return ""
}

// initMetadata runs after the options, so WithMetadata(nil) still leaves a
// writable map for the obj.Metadata().Set(...) idiom.
func (o *object) initMetadata() {
	if o.metadata == nil {
		o.metadata = make(Metadata)
	}
}

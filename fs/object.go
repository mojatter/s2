package fs

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"path"
	"sync"
	"time"

	"github.com/mojatter/s2"
)

type object struct {
	fsys         fs.FS
	name         string
	length       uint64
	lastModified time.Time

	once    sync.Once
	m       meta
	loadErr error
}

func newObjectFileInfo(fsys fs.FS, name string, info fs.FileInfo) *object {
	return &object{
		fsys:         fsys,
		name:         name,
		length:       s2.MustUint64(info.Size()),
		lastModified: info.ModTime(),
	}
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	return o.fsys.Open(o.name)
}

// load reads the sidecar once; List results call it lazily, Get eagerly.
func (o *object) load() error {
	o.once.Do(func() {
		o.m, o.loadErr = loadMeta(o.fsys, o.name)
	})
	return o.loadErr
}

func (o *object) Length() uint64 {
	return o.length
}

func (o *object) LastModified() time.Time {
	return o.lastModified
}

func (o *object) Metadata() s2.Metadata {
	_ = o.load()
	if o.m.Metadata == nil {
		o.m.Metadata = make(s2.Metadata)
	}
	return o.m.Metadata
}

func (o *object) ContentType() string {
	_ = o.load()
	return o.m.ContentType
}

// ETag returns the stored MD5, or one derived from mtime and size when no sidecar holds it.
func (o *object) ETag() string {
	_ = o.load()
	if o.m.ETag != "" {
		return o.m.ETag
	}
	return fmt.Sprintf(`"%x-%x"`, o.lastModified.UnixNano(), o.length)
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
		if _, err := seeker.Seek(s2.MustInt64(offset), io.SeekStart); err != nil {
			_ = rc.Close()
			return nil, err
		}
		return &limitReadCloser{
			Reader: io.LimitReader(seeker, s2.MustInt64(length)),
			Closer: rc,
		}, nil
	}
	// Fallback for non-seeker
	if _, err := io.CopyN(io.Discard, rc, s2.MustInt64(offset)); err != nil {
		_ = rc.Close()
		return nil, err
	}
	return &limitReadCloser{
		Reader: io.LimitReader(rc, s2.MustInt64(length)),
		Closer: rc,
	}, nil
}

type limitReadCloser struct {
	io.Reader
	io.Closer
}

func (l *limitReadCloser) Read(p []byte) (n int, err error) {
	return l.Reader.Read(p)
}

// Keys s2-server stored in the flat sidecar format before v0.18.
const (
	legacyETagKey        = "s2-etag"
	legacyContentTypeKey = "s2-content-type"
	// legacyDefaultContentType is what s2-server stored when a client sent no Content-Type.
	legacyDefaultContentType = "binary/octet-stream"
)

// meta is the JSON sidecar of an object.
type meta struct {
	ETag        string      `json:"etag,omitempty"`
	ContentType string      `json:"content_type,omitempty"`
	Metadata    s2.Metadata `json:"metadata"`
}

func metaPath(name string) string {
	return path.Join(".meta", name)
}

func quotedMD5(h hash.Hash) string {
	return `"` + hex.EncodeToString(h.Sum(nil)) + `"`
}

func loadMeta(fsys fs.FS, name string) (meta, error) {
	f, err := fsys.Open(metaPath(name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return meta{}, nil
		}
		return meta{}, fmt.Errorf("failed to open meta file: %w", err)
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		return meta{}, fmt.Errorf("failed to read meta file: %w", err)
	}
	return parseMeta(data)
}

// parseMeta decodes a sidecar; one without a "metadata" object is the legacy flat map.
func parseMeta(data []byte) (meta, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return meta{}, fmt.Errorf("failed to decode meta file: %w", err)
	}
	var m meta
	if md, ok := raw["metadata"]; ok && bytes.HasPrefix(bytes.TrimSpace(md), []byte("{")) {
		if err := json.Unmarshal(data, &m); err != nil {
			return meta{}, fmt.Errorf("failed to decode meta file: %w", err)
		}
		return m, nil
	}
	if err := json.Unmarshal(data, &m.Metadata); err != nil {
		return meta{}, fmt.Errorf("failed to decode meta file: %w", err)
	}
	m.ETag = m.Metadata[legacyETagKey]
	if ct := m.Metadata[legacyContentTypeKey]; ct != legacyDefaultContentType {
		m.ContentType = ct
	}
	delete(m.Metadata, legacyETagKey)
	delete(m.Metadata, legacyContentTypeKey)
	return m, nil
}

func saveMeta(fsys fs.FS, name string, m meta) error {
	if m.Metadata == nil {
		m.Metadata = s2.Metadata{}
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(m); err != nil {
		return fmt.Errorf("failed to encode meta file: %w", err)
	}
	return atomicWrite(fsys, metaPath(name), &buf)
}

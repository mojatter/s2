package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
)

type object struct {
	fsys         fs.FS
	name         string
	length       uint64
	lastModified time.Time
	metadata     s2.Metadata
}

func newObjectFileInfo(fsys fs.FS, name string, info fs.FileInfo) *object {
	return &object{
		fsys:         fsys,
		name:         name,
		length:       uint64(info.Size()),
		lastModified: info.ModTime(),
	}
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Open() (io.ReadCloser, error) {
	return o.fsys.Open(o.name)
}

func (o *object) loadMetadata() error {
	md, err := loadMetadata(o.fsys, o.name)
	if err != nil {
		return err
	}
	o.metadata = md
	return nil
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
	rc, err := o.Open()
	if err != nil {
		return nil, err
	}
	if offset == 0 && length == o.length {
		return rc, nil
	}
	if seeker, ok := rc.(io.ReadSeeker); ok {
		if _, err := seeker.Seek(int64(offset), io.SeekStart); err != nil {
			rc.Close()
			return nil, err
		}
		return &limitReadCloser{
			Reader: io.LimitReader(seeker, int64(length)),
			Closer: rc,
		}, nil
	}
	// Fallback for non-seeker
	if _, err := io.CopyN(io.Discard, rc, int64(offset)); err != nil {
		rc.Close()
		return nil, err
	}
	return &limitReadCloser{
		Reader: io.LimitReader(rc, int64(length)),
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


func metaPath(name string) string {
	return filepath.Join(".meta", name)
}

func loadMetadata(fsys fs.FS, name string) (s2.Metadata, error) {
	metaFile, err := fsys.Open(metaPath(name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open meta file: %w", err)
	}
	defer func() { _ = metaFile.Close() }()

	var md s2.MetadataMap
	if err := json.NewDecoder(metaFile).Decode(&md); err != nil {
		return nil, fmt.Errorf("failed to decode meta file: %w", err)
	}
	return md, nil
}

func saveMetadata(fsys fs.FS, name string, md s2.Metadata) error {
	metaName := metaPath(name)
	if md.Len() == 0 {
		metaInfo, err := fs.Stat(fsys, metaName)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("failed to stat meta file: %w", err)
		}
		if metaInfo.IsDir() {
			return &s2.ErrNotExist{Name: metaName}
		}
		if err := wfs.RemoveFile(fsys, metaName); err != nil {
			return fmt.Errorf("failed to remove meta file: %w", err)
		}
		return nil
	}
	metaFile, err := wfs.CreateFile(fsys, metaName, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create meta file: %w", err)
	}
	defer func() { _ = metaFile.Close() }()

	if err := json.NewEncoder(metaFile).Encode(md); err != nil {
		return fmt.Errorf("failed to encode meta file: %w", err)
	}
	return nil
}

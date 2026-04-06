package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/memfs"
	"github.com/mojatter/wfs/osfs"
)

func init() {
	s2.RegisterNewStorageFunc(s2.TypeOSFS, NewStorage)
	s2.RegisterNewStorageFunc(s2.TypeMemFS, NewStorage)
}

func NewStorage(_ context.Context, cfg s2.Config) (s2.Storage, error) {
	if cfg.Type == s2.TypeMemFS {
		return NewStorageMem(cfg), nil
	}
	return NewStorageFS(cfg, osfs.DirFS(cfg.Root)), nil
}

func NewStorageFS(cfg s2.Config, fs fs.FS) s2.Storage {
	return &storage{
		cfg:  cfg,
		fsys: fs,
		typ:  cfg.Type,
	}
}

func NewStorageMem(cfg s2.Config) s2.Storage {
	return &storage{
		cfg:  cfg,
		fsys: memfs.New(),
		typ:  s2.TypeMemFS,
	}
}

func NewStorageDir(dir string) s2.Storage {
	return &storage{
		fsys: osfs.DirFS(dir),
		typ:  s2.TypeOSFS,
	}
}

type storage struct {
	cfg  s2.Config
	fsys fs.FS
	typ  s2.Type
}

func (s *storage) Type() s2.Type {
	if s.typ == "" {
		if _, ok := s.fsys.(*memfs.MemFS); ok {
			s.typ = s2.TypeMemFS
		} else {
			s.typ = s2.TypeOSFS
		}
	}
	return s.typ
}

func (s *storage) Sub(ctx context.Context, prefix string) (s2.Storage, error) {
	sub, err := fs.Sub(s.fsys, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to sub %q: %w", prefix, err)
	}
	return &storage{
		cfg:  s.cfg,
		fsys: sub,
		typ:  s.typ,
	}, nil
}

// isMetaDir reports whether a directory entry is the internal metadata directory.
func isMetaDir(name string) bool {
	return name == ".meta"
}

func (s *storage) List(ctx context.Context, prefix string, limit int) ([]s2.Object, []string, error) {
	return s.ListAfter(ctx, prefix, limit, "")
}

func (s *storage) ListAfter(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, []string, error) {
	if limit <= 0 {
		limit = 1000
	}
	// Normalize prefix into a directory path acceptable to fs.ReadDir.
	// S3 callers commonly pass a trailing slash (e.g. "dir/"), which fs.ValidPath rejects.
	dir := strings.TrimSuffix(prefix, "/")
	if dir == "" {
		dir = "."
	}
	entries, err := fs.ReadDir(s.fsys, dir)
	if err != nil {
		// A non-existent prefix is not an error in S3 semantics; return an empty result.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to read dir: %w", err)
	}
	prefixes := make([]string, 0)
	objs := make([]s2.Object, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if dir != "." {
			name = filepath.Join(dir, entry.Name())
		}
		if after != "" && name <= after {
			continue
		}
		if isMetaDir(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get info: %w", err)
		}
		if info.IsDir() {
			prefixes = append(prefixes, name)
			continue
		}
		objs = append(objs, newObjectFileInfo(s.fsys, name, info))
		limit--
		if limit <= 0 {
			break
		}
	}
	return objs, prefixes, nil
}

func (s *storage) ListRecursive(ctx context.Context, prefix string, limit int) ([]s2.Object, error) {
	return s.ListRecursiveAfter(ctx, prefix, limit, "")
}

func (s *storage) ListRecursiveAfter(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, error) {
	if limit <= 0 {
		limit = 1000
	}
	objs := make([]s2.Object, 0, limit)
	err := fs.WalkDir(s.fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if after != "" && name <= after {
			return nil
		}
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get info: %w", err)
		}
		if info.IsDir() {
			if isMetaDir(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		objs = append(objs, newObjectFileInfo(s.fsys, name, info))
		limit--
		if limit <= 0 {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return objs, nil
}

func (s *storage) Get(ctx context.Context, name string) (s2.Object, error) {
	info, err := fs.Stat(s.fsys, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &s2.ErrNotExist{Name: name}
		}
		return nil, fmt.Errorf("failed to stat: %w", err)
	}
	if info.IsDir() {
		return nil, &s2.ErrNotExist{Name: name}
	}
	obj := newObjectFileInfo(s.fsys, name, info)
	if err := obj.loadMetadata(); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *storage) Exists(ctx context.Context, name string) (bool, error) {
	info, err := fs.Stat(s.fsys, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat: %w", err)
	}
	return !info.IsDir(), nil
}


func (s *storage) Put(ctx context.Context, obj s2.Object) error {
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	out, err := wfs.CreateFile(s.fsys, obj.Name(), os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, rc); err != nil {
		return fmt.Errorf("failed to copy: %w", err)
	}
	return saveMetadata(s.fsys, obj.Name(), obj.Metadata())
}

func (s *storage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	if _, err := s.Get(ctx, name); err != nil {
		return err
	}
	return saveMetadata(s.fsys, name, metadata)
}

func (s *storage) Copy(ctx context.Context, src, dst string) error {
	srcObj, err := s.Get(ctx, src)
	if err != nil {
		return err
	}
	rc, err := srcObj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	out, err := wfs.CreateFile(s.fsys, dst, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, rc); err != nil {
		return fmt.Errorf("failed to copy: %w", err)
	}
	return saveMetadata(s.fsys, dst, srcObj.Metadata())
}

func (s *storage) Move(ctx context.Context, src, dst string) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}


func (s *storage) Delete(ctx context.Context, name string) error {
	// Ignore metadata deletion errors (file may not have metadata)
	_ = wfs.RemoveFile(s.fsys, metaPath(name))
	if err := wfs.RemoveFile(s.fsys, name); err != nil {
		return fmt.Errorf("failed to delete %q: %w", name, err)
	}
	return nil
}

func (s *storage) DeleteRecursive(ctx context.Context, prefix string) error {
	dirName := strings.TrimSuffix(prefix, "/")
	var dirs []string
	err := fs.WalkDir(s.fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if prefix != "" && !strings.HasPrefix(name, prefix) && name != dirName {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, name)
			return nil
		}
		return s.Delete(ctx, name)
	})
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		if err := wfs.RemoveAll(s.fsys, dir); err != nil {
			return fmt.Errorf("failed to remove dir %q: %w", dir, err)
		}
	}
	return nil
}

func (s *storage) SignedURL(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if _, err := s.Get(ctx, name); err != nil {
		return "", err
	}
	return url.JoinPath(s.cfg.SignedURL, name)
}

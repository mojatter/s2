package fs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"strings"

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

// defaultListLimit caps a List call when ListOptions.Limit is unset (0).
// It mirrors S3's default ListObjectsV2 page size.
const defaultListLimit = 1000

func (s *storage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if opts.Recursive {
		return s.listRecursive(opts.Prefix, opts.After, limit)
	}
	return s.listFlat(opts.Prefix, opts.After, limit)
}

func (s *storage) listFlat(prefix, after string, limit int) (s2.ListResult, error) {
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
			return s2.ListResult{}, nil
		}
		return s2.ListResult{}, fmt.Errorf("failed to read dir: %w", err)
	}
	res := s2.ListResult{
		Objects:        make([]s2.Object, 0, len(entries)),
		CommonPrefixes: make([]string, 0),
	}
	for _, entry := range entries {
		name := entry.Name()
		if dir != "." {
			name = path.Join(dir, entry.Name())
		}
		if after != "" && name <= after {
			continue
		}
		if isMetaDir(entry.Name()) || isTempFile(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return s2.ListResult{}, fmt.Errorf("failed to get info: %w", err)
		}
		if info.IsDir() {
			res.CommonPrefixes = append(res.CommonPrefixes, name)
			continue
		}
		res.Objects = append(res.Objects, newObjectFileInfo(s.fsys, name, info))
		limit--
		if limit <= 0 {
			break
		}
	}
	return res, nil
}

func (s *storage) listRecursive(prefix, after string, limit int) (s2.ListResult, error) {
	res := s2.ListResult{Objects: make([]s2.Object, 0, limit)}
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
		if isTempFile(d.Name()) {
			return nil
		}
		res.Objects = append(res.Objects, newObjectFileInfo(s.fsys, name, info))
		limit--
		if limit <= 0 {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		return s2.ListResult{}, err
	}
	return res, nil
}

func (s *storage) Get(ctx context.Context, name string) (s2.Object, error) {
	info, err := fs.Stat(s.fsys, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", s2.ErrNotExist, name)
		}
		return nil, fmt.Errorf("failed to stat: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%w: %s", s2.ErrNotExist, name)
	}
	obj := newObjectFileInfo(s.fsys, name, info)
	if err := obj.loadMetadata(); err != nil {
		return nil, err
	}
	return obj, nil
}

// Exists reports whether a path exists under the storage root. Both
// regular files and directories count as "present"; callers that need
// to distinguish the two should use Get (which rejects directories)
// or List (which only enumerates directories).
func (s *storage) Exists(ctx context.Context, name string) (bool, error) {
	_, err := fs.Stat(s.fsys, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat: %w", err)
	}
	return true, nil
}


func (s *storage) Put(ctx context.Context, obj s2.Object) error {
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	if err := atomicWrite(s.fsys, obj.Name(), rc); err != nil {
		return err
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

	if err := atomicWrite(s.fsys, dst, rc); err != nil {
		return err
	}
	return saveMetadata(s.fsys, dst, srcObj.Metadata())
}

func (s *storage) Move(ctx context.Context, src, dst string) error {
	// Prefer a direct rename on filesystems that support it: it's atomic and
	// avoids reading the object body twice.
	if _, ok := s.fsys.(wfs.RenameFS); ok {
		if _, err := s.Get(ctx, src); err != nil {
			return err
		}
		if err := wfs.Rename(s.fsys, src, dst); err != nil {
			return fmt.Errorf("failed to rename %q to %q: %w", src, dst, err)
		}
		// Move metadata too. If the source has no metadata, ignore ErrNotExist.
		srcMeta := metaPath(src)
		if _, err := fs.Stat(s.fsys, srcMeta); err == nil {
			if err := wfs.Rename(s.fsys, srcMeta, metaPath(dst)); err != nil {
				return fmt.Errorf("failed to rename metadata for %q: %w", src, err)
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to stat metadata for %q: %w", src, err)
		}
		return nil
	}
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

func (s *storage) SignedURL(ctx context.Context, opts s2.SignedURLOptions) (string, error) {
	if opts.Method != "" && opts.Method != s2.SignedURLGet {
		return "", fmt.Errorf("fs storage: unsupported signed URL method %q", opts.Method)
	}
	if _, err := s.Get(ctx, opts.Name); err != nil {
		return "", err
	}
	return url.JoinPath(s.cfg.SignedURL, opts.Name)
}

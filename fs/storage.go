package fs

import (
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is required for S3-compatible ETag
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"sort"
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
	if cfg.Root == "" {
		return nil, s2.ErrRequiredConfigRoot
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
	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}
	return s.sub(prefix)
}

// SubSidecar scopes strg to the directory holding object sidecars, which Sub
// refuses like any other spelling of it. It is how s2's own code reaches the
// state it keeps beside the objects; ok is false for any other storage.
func SubSidecar(strg s2.Storage) (sub s2.Storage, ok bool) {
	s, is := strg.(*storage)
	if !is {
		return nil, false
	}
	sub, err := s.sub(metaDir)
	if err != nil {
		return nil, false
	}
	return sub, true
}

// sub scopes the storage to prefix, which the caller has already vetted.
func (s *storage) sub(prefix string) (s2.Storage, error) {
	// A prefix selects rather than names, so it may be empty or end in "/";
	// io/fs.Sub takes neither.
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return &storage{cfg: s.cfg, fsys: s.fsys, typ: s.typ}, nil
	}
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
// metaDir holds the JSON sidecar of every object beside it.
const metaDir = ".meta"

func isMetaDir(name string) bool {
	return name == metaDir
}

// validateName is [s2.ValidateName] plus the metadata directory: ".meta/x" is
// the sidecar of the object "x", not an object of its own.
func validateName(name string) error {
	if err := s2.ValidateName(name); err != nil {
		return err
	}
	return rejectMetaDir(name)
}

// validatePrefix is [s2.ValidatePrefix] with the same exclusion.
func validatePrefix(prefix string) error {
	if err := s2.ValidatePrefix(prefix); err != nil {
		return err
	}
	return rejectMetaDir(prefix)
}

// rejectMetaDir rejects a name holding the metadata directory as any element.
// Any, not just the first: a Sub writes its own sidecars beside the names it
// scopes, and both listings hide the directory at every depth, so a name
// reaching through one would be stored and read but never listed.
func rejectMetaDir(name string) error {
	for elem := range strings.SplitSeq(name, "/") {
		if isMetaDir(elem) {
			return fmt.Errorf("%w: %s is reserved for object metadata", s2.ErrInvalidName, name)
		}
	}
	return nil
}

// defaultListLimit caps a List call when ListOptions.Limit is unset (0).
// It mirrors S3's default ListObjectsV2 page size.
const defaultListLimit = 1000

func (s *storage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
	if err := validatePrefix(opts.Prefix); err != nil {
		return s2.ListResult{}, err
	}
	// StartAfter is a key, and every backend joins it with the storage prefix.
	if err := validatePrefix(opts.StartAfter); err != nil {
		return s2.ListResult{}, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	// Both cursors are plain key names here, so they collapse into one
	// comparison; After wins.
	after := opts.After
	if after == "" {
		after = opts.StartAfter
	}
	if opts.Recursive {
		return s.listRecursive(opts.Prefix, after, limit)
	}
	return s.listFlat(opts.Prefix, after, limit)
}

// pastSubtree reports whether after sorts beyond every key under dir. A
// directory holding keys past after is still a common prefix.
func pastSubtree(dir, after string) bool {
	sub := dir + "/"
	return after >= sub && !strings.HasPrefix(after, sub)
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
		if isMetaDir(entry.Name()) || isTempFile(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return s2.ListResult{}, fmt.Errorf("failed to get info: %w", err)
		}
		if info.IsDir() {
			if pastSubtree(name, after) {
				continue
			}
			res.CommonPrefixes = append(res.CommonPrefixes, name)
			continue
		}
		if after != "" && name <= after {
			continue
		}
		res.Objects = append(res.Objects, newObjectFileInfo(s.fsys, name, info))
		limit--
		if limit <= 0 {
			res.NextAfter = name
			break
		}
	}
	return res, nil
}

func (s *storage) listRecursive(prefix, after string, limit int) (s2.ListResult, error) {
	// fs.WalkDir's visit order is pre-order by directory, not lexicographic
	// by full path (e.g. "backup/2024.log" is visited before sibling file
	// "backup-old.txt", even though "backup-old.txt" sorts first as a full
	// path). Collect everything first and sort below, rather than capping
	// at limit mid-walk, which could return objects out of order and,
	// combined with an after cursor, permanently skip unvisited ones.
	var objs []s2.Object
	err := fs.WalkDir(s.fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Ahead of the cursor: a cursor that sorts inside ".meta" -- the
		// directory name itself, or ".meta!" -- would otherwise let the walk
		// descend and report the sidecars as objects. A regular file by that
		// name is not an object either, and listFlat skips it too; SkipDir on
		// one would skip the rest of the directory holding it.
		// Not the walk root: a Sub of the metadata directory is how code that
		// keeps its own state there reaches it, and memfs reports that root's
		// name as ".meta" where osfs reports ".".
		if name != "." && isMetaDir(d.Name()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
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
			return nil
		}
		if isTempFile(d.Name()) {
			return nil
		}
		objs = append(objs, newObjectFileInfo(s.fsys, name, info))
		return nil
	})
	if err != nil {
		return s2.ListResult{}, err
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].Name() < objs[j].Name() })

	res := s2.ListResult{Objects: objs}
	if len(objs) > limit {
		res.Objects = objs[:limit]
		res.NextAfter = res.Objects[limit-1].Name()
	}
	return res, nil
}

func (s *storage) Get(ctx context.Context, name string) (s2.Object, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	return s.get(name)
}

func (s *storage) get(name string) (*object, error) {
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
	if err := obj.load(); err != nil {
		return nil, err
	}
	return obj, nil
}

// Exists reports whether a path exists under the storage root. Both
// regular files and directories count as "present"; callers that need
// to distinguish the two should use Get (which rejects directories)
// or List (which only enumerates directories).
func (s *storage) Exists(ctx context.Context, name string) (bool, error) {
	if err := validateName(name); err != nil {
		return false, err
	}
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
	if err := validateName(obj.Name()); err != nil {
		return err
	}
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	h := md5.New() // #nosec G401 -- MD5 is required for S3-compatible ETag
	if err := atomicWrite(s.fsys, obj.Name(), io.TeeReader(rc, h)); err != nil {
		return err
	}
	return s.saveMetaForNewBody(obj.Name(), meta{
		ETag:        quotedMD5(h),
		ContentType: obj.ContentType(),
		Metadata:    obj.Metadata(),
	})
}

// saveMetaForNewBody writes the sidecar of a body just written, dropping a stale one when the write fails.
func (s *storage) saveMetaForNewBody(name string, m meta) error {
	err := saveMeta(s.fsys, name, m)
	if err != nil {
		// A stale sidecar would describe the previous body; without one the ETag falls back to the synthetic form.
		_ = wfs.RemoveFile(s.fsys, metaPath(name))
	}
	return err
}

// PutMetadata replaces the user metadata and keeps the ETag and content type.
func (s *storage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	if err := validateName(name); err != nil {
		return err
	}
	obj, err := s.get(name)
	if err != nil {
		return err
	}
	m := obj.m
	m.Metadata = metadata
	return saveMeta(s.fsys, name, m)
}

func (s *storage) Copy(ctx context.Context, src, dst string) error {
	for _, name := range []string{src, dst} {
		if err := validateName(name); err != nil {
			return err
		}
	}
	srcObj, err := s.get(src)
	if err != nil {
		return err
	}
	rc, err := srcObj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	h := md5.New() // #nosec G401 -- MD5 is required for S3-compatible ETag
	if err := atomicWrite(s.fsys, dst, io.TeeReader(rc, h)); err != nil {
		return err
	}
	m := srcObj.m
	m.ETag = quotedMD5(h)
	return s.saveMetaForNewBody(dst, m)
}

func (s *storage) Move(ctx context.Context, src, dst string) error {
	for _, name := range []string{src, dst} {
		if err := validateName(name); err != nil {
			return err
		}
	}
	// Prefer a direct rename on filesystems that support it: it's atomic and
	// avoids reading the object body twice.
	if _, ok := s.fsys.(wfs.RenameFS); ok {
		if _, err := s.Get(ctx, src); err != nil {
			return err
		}
		if err := wfs.Rename(s.fsys, src, dst); err != nil {
			return fmt.Errorf("failed to rename %q to %q: %w", src, dst, err)
		}
		// Move the sidecar too; a source without one must not inherit dst's.
		srcMeta, dstMeta := metaPath(src), metaPath(dst)
		if _, err := fs.Stat(s.fsys, srcMeta); err == nil {
			if err := wfs.Rename(s.fsys, srcMeta, dstMeta); err != nil {
				return fmt.Errorf("failed to rename metadata for %q: %w", src, err)
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to stat metadata for %q: %w", src, err)
		} else if err := wfs.RemoveFile(s.fsys, dstMeta); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to remove metadata for %q: %w", dst, err)
		}
		return nil
	}
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}

func (s *storage) Delete(ctx context.Context, name string) error {
	if err := validateName(name); err != nil {
		return err
	}
	return s.delete(name)
}

func (s *storage) delete(name string) error {
	// Ignore metadata deletion errors (file may not have metadata)
	_ = wfs.RemoveFile(s.fsys, metaPath(name))
	if err := wfs.RemoveFile(s.fsys, name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to delete %q: %w", name, err)
	}
	return nil
}

func (s *storage) DeleteRecursive(ctx context.Context, prefix string) error {
	if err := validatePrefix(prefix); err != nil {
		return err
	}
	dirName := strings.TrimSuffix(prefix, "/")
	var dirs []string
	err := fs.WalkDir(s.fsys, ".", func(name string, d fs.DirEntry, err error) error {
		// A nil entry means the root could not be stat'd; a missing one is a no-op.
		if d == nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		// A ".meta" directory holds other objects' sidecars rather than
		// objects, at any depth: a Sub writes its own beside the names it
		// scopes. A prefix that merely starts its name -- ".met" -- must not
		// reach it, so it goes only when the directory holding it does. The
		// base name, because SkipDir on a file would skip the rest of the
		// directory holding that file. Not the walk root: this storage may
		// itself be a Sub of one, and memfs reports that root's name as
		// ".meta" where osfs reports ".".
		if d.IsDir() && name != "." && isMetaDir(d.Name()) {
			if prefix == "" || strings.HasPrefix(path.Dir(name)+"/", prefix) {
				dirs = append(dirs, name)
			}
			return fs.SkipDir
		}
		if prefix != "" && !strings.HasPrefix(name, prefix) && name != dirName {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, name)
			return nil
		}
		// These names came from the walk, so they are whatever the filesystem
		// already holds; validating them here would leave the rest behind.
		return s.delete(name)
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
	if err := validateName(opts.Name); err != nil {
		return "", err
	}
	if opts.Method != "" && opts.Method != s2.SignedURLGet {
		return "", fmt.Errorf("fs storage: unsupported signed URL method %q", opts.Method)
	}
	if _, err := s.Get(ctx, opts.Name); err != nil {
		return "", err
	}
	return url.JoinPath(s.cfg.SignedURL, opts.Name)
}

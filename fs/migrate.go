package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
)

// ErrMetaBlocked reports a file named .meta, a full directory, or a file that is not a metadata file where one goes.
var ErrMetaBlocked = errors.New("metadata file is blocked")

// MigrateMeta moves each metadata file v0.19.x kept under an ancestor's .meta beside its object; see docs/backends.md.
func MigrateMeta(ctx context.Context, strg s2.Storage) (ok bool, err error) {
	s, is := strg.(*storage)
	if !is {
		return false, nil
	}
	var blocked []string
	err = fs.WalkDir(s.fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			// A missing root holds nothing to migrate.
			if name == "." && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." || !d.IsDir() || !isMetaDir(d.Name()) {
			return nil
		}
		if err := s.migrateMetaDir(ctx, name, &blocked); err != nil {
			return err
		}
		return fs.SkipDir
	})
	switch {
	case err != nil || len(blocked) == 0:
	case len(blocked) == 1:
		err = fmt.Errorf("%w: %q left in place", ErrMetaBlocked, blocked[0])
	default:
		err = fmt.Errorf("%w: %q and %d more left in place", ErrMetaBlocked, blocked[0], len(blocked)-1)
	}
	return true, err
}

// migrateMetaDir moves the legacy metadata files under dir, a .meta; its subdirectories are all legacy.
func (s *storage) migrateMetaDir(ctx context.Context, dir string, blocked *[]string) error {
	owner := path.Dir(dir)
	var subdirs []string
	err := fs.WalkDir(s.fsys, dir, func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == dir {
			return nil
		}
		if d.IsDir() {
			subdirs = append(subdirs, name)
			return nil
		}
		rel := strings.TrimPrefix(name, dir+"/")
		if !strings.Contains(rel, "/") {
			return nil
		}
		key := path.Join(owner, rel)
		err = s.migrateMetaFile(name, key)
		if errors.Is(err, errBlocked) {
			*blocked = append(*blocked, name)
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	// Deepest first, so a parent is empty by the time it comes up.
	for _, sub := range slices.Backward(subdirs) {
		_ = wfs.RemoveFile(s.fsys, sub)
	}
	if len(subdirs) > 0 {
		// Only when it held nothing but legacy ones; RemoveFile keeps a non-empty directory.
		_ = wfs.RemoveFile(s.fsys, dir)
	}
	return nil
}

// migrateMetaFile moves legacy, the old metadata file of key, beside key; one without an object stays.
func (s *storage) migrateMetaFile(legacy, key string) error {
	dst := metaPath(key)
	// A key holding .meta predates v0.18.1 and cannot be told from a metadata file, so neither moves.
	if rejectMetaDir(key) != nil {
		return nil
	}
	body, err := fs.Stat(s.fsys, key)
	if err != nil && !isMissingMeta(err) {
		return fmt.Errorf("failed to stat %q: %w", key, err)
	}
	// Without an object it may be a pre-v0.18.1 key rather than ours, so it is left alone.
	if err != nil || body.IsDir() {
		return nil
	}
	// One that does not parse is no metadata file either, but possibly such a key; it stays, like an orphan.
	data, err := fs.ReadFile(s.fsys, legacy)
	if err != nil {
		return fmt.Errorf("failed to read %q: %w", legacy, err)
	}
	if _, err := parseMeta(data); err != nil {
		return nil
	}
	cur, err := fs.Stat(s.fsys, dst)
	if err != nil && !isMissingMeta(err) {
		return fmt.Errorf("failed to stat %q: %w", dst, err)
	}
	if err == nil && cur.IsDir() {
		// A directory another view left there is no metadata file; an empty one gives way, a full one blocks.
		if wfs.RemoveFile(s.fsys, dst) != nil {
			return errBlocked
		}
		err = fs.ErrNotExist
	}
	if err == nil {
		// Not a metadata file, so possibly a pre-v0.18.1 key: comparing it could delete either one.
		data, err := fs.ReadFile(s.fsys, dst)
		if err != nil {
			return fmt.Errorf("failed to read %q: %w", dst, err)
		}
		if _, err := parseMeta(data); err != nil {
			return errBlocked
		}
		old, err := fs.Stat(s.fsys, legacy)
		if err != nil {
			return fmt.Errorf("failed to stat %q: %w", legacy, err)
		}
		if !old.ModTime().After(cur.ModTime()) {
			return s.removeMetaFile(legacy)
		}
	}
	// errBlocked leaves legacy for the fallback to read.
	return s.moveMetaFile(legacy, dst)
}

// errBlocked marks a destination a file named .meta, a full directory or a file that is not a metadata file is in the way of.
var errBlocked = errors.New("metadata directory blocked")

// moveMetaFile renames src to dst, keeping its mtime; where rename is unavailable it copies, and dst gets a fresh one.
func (s *storage) moveMetaFile(src, dst string) error {
	if err := s.mkdirParent(dst); err != nil {
		if info, serr := fs.Stat(s.fsys, path.Dir(dst)); serr == nil && !info.IsDir() {
			return errBlocked
		}
		return err
	}
	if _, ok := s.fsys.(wfs.RenameFS); ok {
		if err := wfs.Rename(s.fsys, src, dst); err != nil {
			return fmt.Errorf("failed to move metadata file %q: %w", src, err)
		}
		return nil
	}
	data, err := fs.ReadFile(s.fsys, src)
	if err != nil {
		return fmt.Errorf("failed to read metadata file %q: %w", src, err)
	}
	if err := atomicWrite(s.fsys, dst, bytes.NewReader(data)); err != nil {
		return err
	}
	return s.removeMetaFile(src)
}

func (s *storage) removeMetaFile(name string) error {
	if err := wfs.RemoveFile(s.fsys, name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove metadata file %q: %w", name, err)
	}
	return nil
}

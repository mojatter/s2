package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
)

// A directory marker is a name ending in "/", such as "d/". fs keeps one as a metadata file with no body,
// at metaPath("d/") = "d/.meta/.meta", which no other name reaches since ".meta" is reserved.

// isMarker reports whether name is a directory marker.
func isMarker(name string) bool {
	return strings.HasSuffix(name, "/")
}

// emptyETag is the ETag of an empty body, the only body a marker has.
const emptyETag = `"d41d8cd98f00b204e9800998ecf8427e"`

// statKey stats what holds name, its body or a marker's metadata file; ok is false when the tree holds no key there.
func (s *storage) statKey(name string) (info fs.FileInfo, ok bool, err error) {
	p := name
	if isMarker(name) {
		p = metaPath(name)
	}
	info, err = fs.Stat(s.fsys, p)
	if err != nil {
		if s.noKey(path.Dir(name), err) || isMarker(name) && isMissingMeta(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to stat: %w", err)
	}
	// A directory there is what an older s2 left, not a marker.
	if isMarker(name) && info.IsDir() {
		return nil, false, nil
	}
	return info, true, nil
}

// markerInfo describes the metadata file of dir's marker, or is nil when dir holds none.
func (s *storage) markerInfo(dir string) (fs.FileInfo, error) {
	info, err := fs.Stat(s.fsys, metaPath(dir+"/"))
	if err != nil {
		if isMissingMeta(err) {
			return nil, nil // deleted since its directory was read
		}
		return nil, fmt.Errorf("failed to get info: %w", err)
	}
	if info.IsDir() {
		return nil, nil
	}
	return info, nil
}

// listedMarker is the marker dir+"/" as a listing reports it, or nil when dir holds none.
func (s *storage) listedMarker(dir string) (*object, error) {
	info, err := s.markerInfo(dir)
	if info == nil || err != nil {
		return nil, err
	}
	return newObjectFileInfo(s.fsys, dir+"/", info), nil
}

// writeMarker is write for a marker: what open yields must be an empty body.
func (s *storage) writeMarker(ctx context.Context, reads []string, name string, check func() error, open func() (io.Reader, meta, error)) (string, error) {
	var ran bool
	err := s.lockReading(ctx, reads, []string{name}, func() error {
		if err := check(); err != nil {
			return err
		}
		src, m, err := open()
		if err != nil {
			return err
		}
		ran = true
		return s.putMarker(name, src, m)
	})
	if err != nil {
		// Like write, prune only after a write that may have made directories.
		if ran {
			s.pruneIfEmpty(ctx, name)
		}
		return "", err
	}
	return emptyETag, nil
}

// emptyBody refuses a body for the marker name, declared by length or found in src.
func emptyBody(name string, length uint64, src io.Reader) error {
	if length > 0 {
		return errMarkerBody(name)
	}
	var b [1]byte
	switch _, err := io.ReadFull(src, b[:]); {
	case err == nil:
		return errMarkerBody(name)
	case !errors.Is(err, io.EOF):
		return err
	}
	return nil
}

func errMarkerBody(name string) error {
	return fmt.Errorf("%w: %s: fs keeps no body under a name ending in \"/\"", s2.ErrInvalidName, name)
}

// putMarker stores name's metadata file once src proves empty. Callers hold name's lock.
func (s *storage) putMarker(name string, src io.Reader, m meta) error {
	// Upload checked its body before the lock; Copy and Move read a local one.
	if err := emptyBody(name, 0, src); err != nil {
		return err
	}
	if err := writable(s.fsys, name); err != nil {
		return err
	}
	// The tree holds "d" as an object or a directory, never both.
	dir := strings.TrimSuffix(name, "/")
	info, err := fs.Stat(s.fsys, dir)
	switch {
	case err == nil && !info.IsDir():
		return fmt.Errorf("%w: %s is an object", s2.ErrInvalidName, dir)
	case err != nil && !errors.Is(err, fs.ErrNotExist) && belowObject(s.fsys, path.Dir(dir)):
		return fmt.Errorf("%w: %s lies under an object", s2.ErrInvalidName, name)
	}
	if err := s.prepareMeta(name); err != nil {
		return err
	}
	m.ETag = emptyETag
	if err := saveMeta(s.fsys, name, m, !s.noSync); err != nil {
		return err
	}
	return s.syncObject(name)
}

// deleteMarker removes name's metadata file, adding its directory to changed. Callers hold name's lock.
func (s *storage) deleteMarker(name string, changed *[]string) error {
	// As Exists answers: what an older s2 left there is no marker.
	if _, ok, err := s.statKey(name); err != nil || !ok {
		return err
	}
	p := metaPath(name)
	if err := wfs.RemoveFile(s.fsys, p); err != nil {
		if isMissingMeta(err) || s.noKey(path.Dir(name), err) {
			return nil
		}
		return fmt.Errorf("failed to delete %q: %w", name, err)
	}
	*changed = append(*changed, path.Dir(p))
	return nil
}

// sameMarker reports whether the markers src and dst are one metadata file by os.SameFile, which a filesystem hiding os.FileInfo never confirms.
func (s *storage) sameMarker(src, dst string) (bool, error) {
	di, err := fs.Stat(s.fsys, metaPath(dst))
	if isMissingMeta(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to stat %q: %w", dst, err)
	}
	si, err := fs.Stat(s.fsys, metaPath(src))
	if err != nil {
		return false, fmt.Errorf("failed to stat %q: %w", src, err)
	}
	return os.SameFile(si, di), nil
}

// moveMarker moves to or from a marker by copy and delete, as neither has a body file to rename.
func (s *storage) moveMarker(ctx context.Context, src, dst string) error {
	var ran bool
	err := s.lockNames(ctx, []string{src, dst}, func() error {
		srcObj, body, err := s.openSource(src)
		if err != nil {
			return err
		}
		if src == dst {
			_ = body.Close()
			// Refused as every write is where it could only write in place (#449).
			return writable(s.fsys, dst)
		}
		// One metadata file under both names, through a case-insensitive volume or a symlinked directory: writing dst then deleting src would lose it (#456).
		if isMarker(src) && isMarker(dst) {
			same, err := s.sameMarker(src, dst)
			if err != nil || same {
				_ = body.Close()
				if err != nil {
					return err
				}
				return writable(s.fsys, dst)
			}
		}
		ran = true
		if isMarker(dst) {
			err = s.putMarker(dst, body, srcObj.m)
		} else {
			_, err = s.uploadDirect(dst, body, srcObj.m)
		}
		// Closed before src is removed, which some filesystems refuse while it is open.
		_ = body.Close()
		if err != nil {
			return err
		}
		var changed []string
		if err := s.delete(src, &changed); err != nil {
			return err
		}
		return s.syncDirs(changed)
	})
	if err != nil {
		if ran {
			s.pruneIfEmpty(ctx, dst)
			s.pruneIfEmpty(ctx, src)
		}
		return err
	}
	s.pruneIfEmpty(ctx, src)
	return nil
}

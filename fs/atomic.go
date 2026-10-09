package fs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"path"
	"strings"

	"github.com/mojatter/wfs"
)

// tmpPrefix is the basename prefix of in-flight atomic-write temp files, which live in a .meta, so no listing shows a partial write.
const tmpPrefix = ".s2tmp-"

// atomicWrite writes src into name using a temp-file + Sync + Rename pattern
// when the filesystem implements wfs.RenameFS. Otherwise it falls back to
// directWrite, which is not crash-safe.
//
// The temp file lives in the .meta of name's directory, so the rename stays in one filesystem.
//
// The write order is Write -> Sync -> Close -> Rename. This order is load
// bearing: on wfs/memfs, buffered writes are not published to the store
// until Close, so Rename must run after Close.
//
// Callers hold name's lock, except MigrateMeta, which runs without writers.
func atomicWrite(fsys iofs.FS, name string, src io.Reader) error {
	if _, ok := fsys.(wfs.RenameFS); !ok {
		return directWrite(fsys, name, src)
	}
	t, err := createTemp(fsys, name)
	if err != nil {
		return err
	}
	defer t.discard()

	if err := t.write(src); err != nil {
		return err
	}
	return t.publish()
}

// tempFile is an in-flight write: created and published under the lock, filled outside it.
type tempFile struct {
	fsys      iofs.FS
	name, tmp string
	f         wfs.WriterFile
	written   bool // write ran, and closed f
	published bool
}

// createTemp creates the temp file that will replace name. Callers hold name's lock.
func createTemp(fsys iofs.FS, name string) (*tempFile, error) {
	tmp, err := tempName(name)
	if err != nil {
		return nil, err
	}
	f, err := wfs.CreateFile(fsys, tmp, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	return &tempFile{fsys: fsys, name: name, tmp: tmp, f: f}, nil
}

// write copies src into the temp file, syncs and closes it; it closes on error too.
func (t *tempFile) write(src io.Reader) error {
	t.written = true
	if _, err := io.Copy(t.f, src); err != nil {
		_ = t.f.Close()
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	if sf, ok := t.f.(wfs.SyncWriterFile); ok {
		if err := sf.Sync(); err != nil {
			_ = t.f.Close()
			return fmt.Errorf("failed to sync temp file: %w", err)
		}
	}
	if err := t.f.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}
	return nil
}

// publish renames the temp file over name. Callers hold name's lock.
func (t *tempFile) publish() error {
	if err := wfs.Rename(t.fsys, t.tmp, t.name); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	t.published = true
	return nil
}

// discard closes the temp file if write never did and removes it unless it was published. Callers hold name's lock.
func (t *tempFile) discard() {
	if !t.written {
		_ = t.f.Close()
	}
	if !t.published {
		_ = wfs.RemoveFile(t.fsys, t.tmp)
	}
}

// directWrite copies src over name once read in full into a temp file, so a failed src leaves name as it was; without RemoveFileFS it writes in place.
func directWrite(fsys iofs.FS, name string, src io.Reader) error {
	if _, ok := fsys.(wfs.RemoveFileFS); !ok {
		// A temp file that cannot be removed would pile up, so write in place.
		return copyTo(fsys, name, src)
	}
	t, err := createTemp(fsys, name)
	if err != nil {
		return err
	}
	defer t.discard()

	if err := t.write(src); err != nil {
		return err
	}
	f, err := fsys.Open(t.tmp)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %w", err)
	}
	defer func() { _ = f.Close() }()

	err = copyTo(fsys, name, f)
	if errors.Is(err, errTruncated) {
		_ = wfs.RemoveFile(fsys, name)
	}
	return err
}

// errTruncated marks a write that failed after truncating its target, which then holds a partial body.
var errTruncated = errors.New("target truncated")

// truncatedError is a write failure past truncation; it is errTruncated but reads as its cause.
type truncatedError struct{ error }

func (e truncatedError) Unwrap() error { return e.error }

func (truncatedError) Is(target error) bool { return target == errTruncated }

// copyTo truncates name and copies src into it, joining a Close error with a copy error.
func copyTo(fsys iofs.FS, name string, src io.Reader) error {
	f, err := wfs.CreateFile(fsys, name, 0o666)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	if _, err := io.Copy(f, src); err != nil {
		return truncatedError{errors.Join(fmt.Errorf("failed to write file: %w", err), closeErr(f))}
	}
	if err := closeErr(f); err != nil {
		return truncatedError{err}
	}
	return nil
}

// closeErr closes f and describes a failure.
func closeErr(f io.Closer) error {
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	return nil
}

// tempName returns a unique temp file path in the .meta of name's directory, such as "images/.meta/.s2tmp-a.png.ab12cd34"; a metadata file's stays in its own.
func tempName(name string) (string, error) {
	dir, base := path.Split(name)
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("failed to generate temp name: %w", err)
	}
	tmpBase := tmpPrefix + base + "." + hex.EncodeToString(buf[:])
	dir = strings.TrimSuffix(dir, "/")
	if path.Base(dir) != metaDir {
		dir = path.Join(dir, metaDir)
	}
	return path.Join(dir, tmpBase), nil
}

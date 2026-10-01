package fs

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"strings"

	"github.com/mojatter/wfs"
)

// tmpPrefix is the basename prefix used for in-flight atomic-write files.
// Entries with this prefix are hidden from listings so partial writes are
// never observable through Storage.List (in either flat or recursive mode).
const tmpPrefix = ".s2tmp-"

// atomicWrite writes src into name using a temp-file + Sync + Rename pattern
// when the filesystem implements wfs.RenameFS. Otherwise it falls back to a
// direct write (which is not crash-safe but keeps behavior well-defined for
// non-osfs backends that don't implement rename).
//
// The temp file is created in the same directory as name so that the final
// rename is atomic on POSIX-backed filesystems.
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
	f, err := wfs.CreateFile(fsys, tmp, os.ModePerm)
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

// directWrite writes src to name without atomicity, used as a fallback for
// filesystems that do not implement wfs.RenameFS.
func directWrite(fsys iofs.FS, name string, src io.Reader) error {
	f, err := wfs.CreateFile(fsys, name, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, src); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

// tempName returns a sibling path suitable for a unique in-flight temp file.
// For "images/a.png" it returns something like "images/.s2tmp-a.png.ab12cd34".
func tempName(name string) (string, error) {
	dir, base := path.Split(name)
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("failed to generate temp name: %w", err)
	}
	tmpBase := tmpPrefix + base + "." + hex.EncodeToString(buf[:])
	if dir == "" {
		return tmpBase, nil
	}
	return path.Join(dir, tmpBase), nil
}

// isTempFile reports whether a basename is an in-flight atomic-write temp
// file that should be hidden from listings.
func isTempFile(basename string) bool {
	return strings.HasPrefix(basename, tmpPrefix)
}

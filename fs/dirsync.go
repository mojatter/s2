package fs

import (
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"path"
	"runtime"
	"syscall"

	"github.com/mojatter/wfs/osfs"
)

// fsyncDir syncs an open directory; tests replace it to record what was synced.
var fsyncDir = func(d *os.File) error { return d.Sync() }

// syncDir makes dir's entries durable on osfs, where a rename lasts a crash only once its directory is synced (#493).
// Other filesystems need none, and Windows cannot sync a directory.
func syncDir(fsys iofs.FS, dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	switch fsys.(type) {
	case *osfs.OSFS, *rootFS:
	default:
		return nil
	}
	f, err := fsys.Open(dir)
	// A directory the process may write but not read cannot be synced, and writes into it worked before.
	if errors.Is(err, iofs.ErrPermission) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to open directory %q to sync: %w", dir, err)
	}
	defer func() { _ = f.Close() }()

	d, ok := f.(*os.File)
	if !ok {
		return nil
	}
	err = fsyncDir(d)
	// A filesystem that cannot sync a directory gains nothing from a failed write.
	if err == nil || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP) {
		return nil
	}
	return fmt.Errorf("failed to sync directory %q: %w", dir, err)
}

// objectDirs lists the directories holding name and its metadata file.
func objectDirs(name string) []string {
	return []string{path.Dir(name), path.Dir(metaPath(name))}
}

// syncObject syncs the directories holding name and its metadata file, which a write has just made sure exist.
func syncObject(fsys iofs.FS, name string) error {
	for _, dir := range objectDirs(name) {
		if err := syncDir(fsys, dir); err != nil {
			return err
		}
	}
	return nil
}

// syncDirs syncs each of dirs once; one gone since gives way to its nearest existing parent, whose sync makes the removal last.
func syncDirs(fsys iofs.FS, dirs []string) error {
	seen := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		for !seen[dir] {
			seen[dir] = true
			err := syncDir(fsys, dir)
			// One under what is now a file is skipped: the write that put the file there synced its directory.
			if err == nil || errors.Is(err, syscall.ENOTDIR) {
				break
			}
			if !errors.Is(err, iofs.ErrNotExist) {
				return err
			}
			dir = path.Dir(dir)
		}
	}
	return nil
}

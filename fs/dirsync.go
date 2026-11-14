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

// syncObject syncs the directories holding name and its metadata file.
func syncObject(fsys iofs.FS, name string) error {
	if err := syncDir(fsys, path.Dir(name)); err != nil {
		return err
	}
	return syncDir(fsys, path.Dir(metaPath(name)))
}

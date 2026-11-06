package fs

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/osfs"
)

// WithConfinedRoot resolves every path of an osfs storage through os.Root, so a symlink leading outside the root, or an absolute one, leads nowhere.
// It leaves a storage over any other filesystem as it is.
func WithConfinedRoot() Option {
	return func(s *storage) {
		if o, ok := s.fsys.(*osfs.OSFS); ok {
			s.fsys = newRootFS(o.Dir)
		}
	}
}

// rootFS is an osfs directory resolved through os.Root; an error opening it fails every call.
type rootFS struct {
	r   *os.Root
	rfs fs.FS  // r.FS()
	err error  // from opening r
	dir string // of this filesystem under r, "." for r itself
}

var (
	_ fs.ReadDirFS     = (*rootFS)(nil)
	_ fs.ReadFileFS    = (*rootFS)(nil)
	_ fs.ReadLinkFS    = (*rootFS)(nil)
	_ fs.StatFS        = (*rootFS)(nil)
	_ fs.SubFS         = (*rootFS)(nil)
	_ wfs.WriteFileFS  = (*rootFS)(nil)
	_ wfs.RemoveFileFS = (*rootFS)(nil)
	_ wfs.RenameFS     = (*rootFS)(nil)
)

// errEscapes is the unexported error os.Root gives a path leading outside it, nil until a root opens.
var (
	errEscapes     error
	errEscapesOnce sync.Once
)

func newRootFS(dir string) *rootFS {
	// os.OpenRoot needs the directory, which osfs creates on the first write.
	if err := os.MkdirAll(dir, fs.ModePerm); err != nil {
		return &rootFS{err: err}
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		return &rootFS{err: err}
	}
	errEscapesOnce.Do(func() {
		var pathErr *fs.PathError
		if file, err := r.Open(".."); err == nil {
			_ = file.Close()
		} else if errors.As(err, &pathErr) {
			errEscapes = pathErr.Err
		}
	})
	return &rootFS{r: r, rfs: r.FS(), dir: "."}
}

// escapedError reports a path leading outside the root as fs.ErrNotExist, as a link leading nowhere does.
type escapedError struct{ error }

func (e escapedError) Is(target error) bool { return target == fs.ErrNotExist }

func (e escapedError) Unwrap() error { return e.error }

func notExist(err error) error {
	if err != nil && errEscapes != nil && errors.Is(err, errEscapes) {
		return escapedError{err}
	}
	return err
}

// join names name under r, after the checks every call makes.
func (f *rootFS) join(op, name string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	if invalidName(runtime.GOOS, name) {
		return "", &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return path.Join(f.dir, name), nil
}

// invalidName rejects what osfs does: os.Root reads `\` as a separator on Windows outside r.FS().
func invalidName(goos, name string) bool {
	return !fs.ValidPath(name) || goos == "windows" && strings.ContainsAny(name, `\:`)
}

func (f *rootFS) Open(name string) (fs.File, error) {
	full, err := f.join("open", name)
	if err != nil {
		return nil, err
	}
	file, err := f.rfs.Open(full)
	if err != nil {
		return nil, notExist(err)
	}
	return file, nil
}

func (f *rootFS) ReadDir(name string) ([]fs.DirEntry, error) {
	full, err := f.join("readdir", name)
	if err != nil {
		return nil, err
	}
	entries, err := fs.ReadDir(f.rfs, full)
	return entries, notExist(err)
}

func (f *rootFS) ReadFile(name string) ([]byte, error) {
	full, err := f.join("readfile", name)
	if err != nil {
		return nil, err
	}
	b, err := fs.ReadFile(f.rfs, full)
	return b, notExist(err)
}

func (f *rootFS) Stat(name string) (fs.FileInfo, error) {
	full, err := f.join("stat", name)
	if err != nil {
		return nil, err
	}
	info, err := fs.Stat(f.rfs, full)
	return info, notExist(err)
}

func (f *rootFS) Lstat(name string) (fs.FileInfo, error) {
	full, err := f.join("lstat", name)
	if err != nil {
		return nil, err
	}
	info, err := fs.Lstat(f.rfs, full)
	return info, notExist(err)
}

func (f *rootFS) ReadLink(name string) (string, error) {
	full, err := f.join("readlink", name)
	if err != nil {
		return "", err
	}
	target, err := fs.ReadLink(f.rfs, full)
	return target, notExist(err)
}

// Sub shares r, so a link under dir may still lead anywhere under r.
func (f *rootFS) Sub(dir string) (fs.FS, error) {
	full, err := f.join("sub", dir)
	if err != nil {
		return nil, err
	}
	return &rootFS{r: f.r, rfs: f.rfs, dir: full}, nil
}

func (f *rootFS) MkdirAll(dir string, mode fs.FileMode) error {
	full, err := f.join("mkdir", dir)
	if err != nil {
		return err
	}
	return f.mkdirAll(full, mode)
}

// mkdirAll is r.MkdirAll failing, as os.MkdirAll does, on a link leading nowhere, whose target r.MkdirAll would create.
func (f *rootFS) mkdirAll(full string, mode fs.FileMode) error {
	// A directory reached inside the root leaves r.MkdirAll nothing to create.
	if info, err := f.r.Stat(full); err == nil && info.IsDir() {
		return nil
	}
	if full != "." {
		parts := strings.Split(full, "/")
		for i := range parts {
			p := strings.Join(parts[:i+1], "/")
			info, err := f.r.Lstat(p)
			if errors.Is(notExist(err), fs.ErrNotExist) {
				break // nothing below can be a link
			}
			if err != nil {
				return err
			}
			if info.Mode()&fs.ModeSymlink == 0 {
				continue
			}
			if _, err := f.r.Stat(p); errors.Is(notExist(err), fs.ErrNotExist) {
				return &fs.PathError{Op: "mkdir", Path: p, Err: fs.ErrExist}
			} else if err != nil {
				return err
			}
		}
	}
	return notExist(f.r.MkdirAll(full, mode))
}

// CreateFile creates or truncates the named file with mode, and missing parents with fs.ModePerm, as osfs does.
func (f *rootFS) CreateFile(name string, mode fs.FileMode) (wfs.WriterFile, error) {
	full, err := f.join("open", name)
	if err != nil {
		return nil, err
	}
	if err := f.mkdirAll(path.Dir(full), fs.ModePerm); err != nil {
		return nil, err
	}
	file, err := f.r.OpenFile(full, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return nil, notExist(err)
	}
	return file, nil
}

func (f *rootFS) WriteFile(name string, p []byte, mode fs.FileMode) (int, error) {
	file, err := f.CreateFile(name, mode)
	if err != nil {
		return 0, err
	}
	n, err := file.Write(p)
	if cerr := file.Close(); cerr != nil {
		err = errors.Join(err, cerr)
	}
	return n, err
}

func (f *rootFS) RemoveFile(name string) error {
	full, err := f.join("remove", name)
	if err != nil {
		return err
	}
	return notExist(f.r.Remove(full))
}

// RemoveAll of the root itself empties it and keeps it, since r holds it open and os.Root cannot remove ".".
func (f *rootFS) RemoveAll(name string) error {
	full, err := f.join("removeall", name)
	if err != nil {
		return err
	}
	if full != "." {
		return notExist(f.r.RemoveAll(full))
	}
	entries, err := fs.ReadDir(f.rfs, ".")
	if err != nil {
		return notExist(err)
	}
	// Like os.RemoveAll, it removes what it can and returns the first error.
	var first error
	for _, e := range entries {
		if err := f.r.RemoveAll(e.Name()); err != nil && first == nil {
			first = notExist(err)
		}
	}
	return first
}

func (f *rootFS) Rename(oldpath, newpath string) error {
	oldFull, err := f.join("rename", oldpath)
	if err != nil {
		return err
	}
	newFull, err := f.join("rename", newpath)
	if err != nil {
		return err
	}
	return notExist(f.r.Rename(oldFull, newFull))
}

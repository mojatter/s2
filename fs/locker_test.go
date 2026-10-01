package fs

import (
	"context"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/memfs"
	"github.com/mojatter/wfs/osfs"
	"github.com/stretchr/testify/require"
)

// spyLocker is a rootLocker that reports whether it is held.
type spyLocker struct {
	*rootLocker
	held atomic.Bool
}

func newSpyLocker() *spyLocker {
	return &spyLocker{rootLocker: newRootLocker()}
}

func (l *spyLocker) Lock(ctx context.Context, name string) (func(), error) {
	unlock, err := l.rootLocker.Lock(ctx, name)
	if err != nil {
		return nil, err
	}
	l.held.Store(true)
	return func() {
		l.held.Store(false)
		unlock()
	}, nil
}

func TestRootKey(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	require.NoError(t, os.Mkdir(real, 0o755))
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink(real, link))

	testCases := []struct {
		caseName string
		a, b     string
		same     bool
	}{
		{caseName: "same spelling", a: real, b: real, same: true},
		{caseName: "symlinked", a: link, b: real, same: true},
		{caseName: "not created yet", a: filepath.Join(link, "new"), b: filepath.Join(real, "new"), same: true},
		{caseName: "untidy spelling", a: real + "/./", b: real, same: true},
		{caseName: "nested", a: real, b: filepath.Join(real, "b"), same: false},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			require.Equal(t, tc.same, dirLockerFor(tc.a) == dirLockerFor(tc.b))
		})
	}
}

func TestLockerSharing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	spy := newSpyLocker()
	lockerOf := func(strg s2.Storage) NameLocker { return strg.(*storage).lk }
	sub := func(strg s2.Storage) s2.Storage {
		sub, err := strg.Sub(ctx, "b")
		require.NoError(t, err)
		return sub
	}
	sidecar := func(strg s2.Storage) s2.Storage {
		sub, ok := SubSidecar(strg)
		require.True(t, ok)
		return sub
	}
	mem := NewStorageMem(s2.Config{})
	literal := &storage{lk: newRootLocker(), fsys: memfs.New()}

	testCases := []struct {
		caseName string
		a, b     s2.Storage
		same     bool
	}{
		{caseName: "dir twice", a: NewStorageDir(dir), b: NewStorageDir(dir), same: true},
		{caseName: "dir and config", a: NewStorageDir(dir), b: NewStorageFS(s2.Config{Type: s2.TypeOSFS}, osfs.DirFS(dir)), same: true},
		{caseName: "sub", a: NewStorageDir(dir), b: sub(NewStorageDir(dir)), same: true},
		{caseName: "sidecar", a: NewStorageDir(dir), b: sidecar(NewStorageDir(dir)), same: true},
		{caseName: "memfs sub", a: mem, b: sub(mem), same: true},
		{caseName: "memfs twice", a: NewStorageMem(s2.Config{}), b: NewStorageMem(s2.Config{}), same: false},
		{caseName: "wrapped fs", a: NewStorageDir(dir), b: NewStorageFS(s2.Config{}, wfs.DelegateFS(osfs.DirFS(dir))), same: false},
		{caseName: "literal sub", a: literal, b: sub(literal), same: true},
		{caseName: "injected", a: NewStorageDir(dir, WithNameLocker(spy)), b: sub(NewStorageDir(dir, WithNameLocker(spy))), same: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			require.Equal(t, tc.same, lockerOf(tc.a) == lockerOf(tc.b))
		})
	}
	require.Equal(t, NameLocker(spy), lockerOf(NewStorageDir(dir, WithNameLocker(spy))))
}

func TestRootLockerCancel(t *testing.T) {
	l := newRootLocker()
	unlock, err := l.Lock(context.Background(), ".")
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = l.Lock(ctx, ".")
	require.ErrorIs(t, err, context.Canceled)
}

// TestWritesHoldTheLock checks every change an operation makes to the filesystem happens under the lock.
func TestWritesHoldTheLock(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		op       func(context.Context, s2.Storage) error
	}{
		{caseName: "put metadata", op: func(ctx context.Context, strg s2.Storage) error {
			return strg.PutMetadata(ctx, "a/b/x", s2.Metadata{"k": "v"})
		}},
		{caseName: "move", op: func(ctx context.Context, strg s2.Storage) error { return s2.Move(ctx, strg, "a/b/x", "c/y") }},
		{caseName: "delete", op: func(ctx context.Context, strg s2.Storage) error { return strg.Delete(ctx, "a/b/x") }},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			spy := newSpyLocker()
			var recording, unlocked atomic.Bool
			check := func() {
				if recording.Load() && !spy.held.Load() {
					unlocked.Store(true)
				}
			}
			base := memfs.New()
			fsys := wfs.DelegateFS(base)
			fsys.CreateFileFunc = func(name string, mode iofs.FileMode) (wfs.WriterFile, error) {
				check()
				return base.CreateFile(name, mode)
			}
			fsys.RenameFunc = func(oldpath, newpath string) error { check(); return base.Rename(oldpath, newpath) }
			fsys.RemoveFileFunc = func(name string) error { check(); return base.RemoveFile(name) }
			fsys.MkdirAllFunc = func(dir string, mode iofs.FileMode) error { check(); return base.MkdirAll(dir, mode) }
			strg := NewStorageFS(s2.Config{Type: s2.TypeMemFS}, fsys, WithNameLocker(spy))
			require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("a/b/x", []byte("x"))))

			recording.Store(true)
			require.NoError(t, tc.op(ctx, strg))
			require.False(t, unlocked.Load(), "changed the filesystem without the lock")

			// The lock is taken, not only held: with it held elsewhere the operation waits.
			require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("a/b/x", []byte("x"))))
			unlock, err := spy.Lock(ctx, ".")
			require.NoError(t, err)
			defer unlock()

			waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()

			require.ErrorIs(t, tc.op(waitCtx, strg), context.DeadlineExceeded)
		})
	}
}

// TestDirLockerLeavesNoEntry checks a directory stays registered only while its lock is held or awaited.
func TestDirLockerLeavesNoEntry(t *testing.T) {
	ctx := context.Background()
	registered := func(l dirLocker) bool {
		dirLockers.mu.Lock()
		defer dirLockers.mu.Unlock()

		_, ok := dirLockers.m[l.dir]
		return ok
	}
	testCases := []struct {
		caseName string
		run      func(t *testing.T, l dirLocker, strg s2.Storage)
	}{
		{caseName: "after operations", run: func(t *testing.T, _ dirLocker, strg s2.Storage) {
			require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("a/x", []byte("x"))))
			require.NoError(t, strg.PutMetadata(ctx, "a/x", s2.Metadata{"k": "v"}))
			require.NoError(t, strg.Delete(ctx, "a/x"))
		}},
		{caseName: "while held", run: func(t *testing.T, l dirLocker, _ s2.Storage) {
			unlock, err := l.Lock(ctx, ".")
			require.NoError(t, err)
			require.True(t, registered(l))
			unlock()
		}},
		{caseName: "after a cancelled wait", run: func(t *testing.T, l dirLocker, _ s2.Storage) {
			unlock, err := l.Lock(ctx, ".")
			require.NoError(t, err)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = l.Lock(cancelled, ".")
			require.ErrorIs(t, err, context.Canceled)
			unlock()
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			dir := t.TempDir()
			l := dirLockerFor(dir)
			tc.run(t, l, NewStorageDir(dir))
			require.False(t, registered(l))
		})
	}
}

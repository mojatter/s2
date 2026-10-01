package fs

import (
	"context"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/memfs"
	"github.com/mojatter/wfs/osfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spyLocker is a nameLocker that reports whether it is held.
type spyLocker struct {
	*nameLocker
	held atomic.Bool
}

func newSpyLocker() *spyLocker {
	return &spyLocker{nameLocker: newNameLocker()}
}

func (l *spyLocker) Lock(ctx context.Context, name string) (func(), error) {
	return l.track(l.nameLocker.Lock(ctx, name))
}

func (l *spyLocker) SLock(ctx context.Context, name string) (func(), error) {
	return l.track(l.nameLocker.SLock(ctx, name))
}

func (l *spyLocker) track(unlock func(), err error) (func(), error) {
	if err != nil {
		return nil, err
	}
	l.held.Store(true)
	return func() {
		l.held.Store(false)
		unlock()
	}, nil
}

// lockReq is one lock call of a locker test.
type lockReq struct {
	name   string
	shared bool
}

func (r lockReq) take(ctx context.Context, l *nameLocker) (func(), error) {
	if r.shared {
		return l.SLock(ctx, r.name)
	}
	return l.Lock(ctx, r.name)
}

// queued reports how many calls await name.
func queued(l *nameLocker, name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if e, ok := l.m[name]; ok {
		return len(e.queue)
	}
	return 0
}

func TestNameLocker(t *testing.T) {
	ctx := context.Background()
	x := func(name string) lockReq { return lockReq{name: name} }
	s := func(name string) lockReq { return lockReq{name: name, shared: true} }

	testCases := []struct {
		caseName string
		held     []lockReq
		queued   []lockReq // wait behind held
		then     lockReq
		waits    bool
	}{
		{caseName: "shared x shared", held: []lockReq{s("a")}, then: s("a")},
		{caseName: "shared x exclusive", held: []lockReq{s("a")}, then: x("a"), waits: true},
		{caseName: "exclusive x shared", held: []lockReq{x("a")}, then: s("a"), waits: true},
		{caseName: "exclusive x exclusive", held: []lockReq{x("a")}, then: x("a"), waits: true},
		{caseName: "other name", held: []lockReq{x("a")}, then: x("b")},
		{caseName: "root and a name", held: []lockReq{x(".")}, then: x("a")},
		{caseName: "reader behind a waiting writer", held: []lockReq{s("a")}, queued: []lockReq{x("a")}, then: s("a"), waits: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			l := newNameLocker()
			var unlocks []func()
			for _, r := range tc.held {
				unlock, err := r.take(ctx, l)
				require.NoError(t, err)
				unlocks = append(unlocks, unlock)
			}
			granted := make(chan struct{}, len(tc.queued))
			for _, r := range tc.queued {
				go func() {
					if unlock, err := r.take(ctx, l); assert.NoError(t, err) {
						unlock()
					}
					granted <- struct{}{}
				}()
				require.Eventually(t, func() bool { return queued(l, r.name) > 0 }, time.Second, time.Millisecond)
			}

			waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()

			unlock, err := tc.then.take(waitCtx, l)
			if tc.waits {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err)
				unlock()
			}
			for _, unlock := range unlocks {
				unlock()
			}
			for range tc.queued {
				<-granted
			}
			require.Empty(t, l.m)
		})
	}
}

func TestNameLockerCancel(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		run      func(t *testing.T, l *nameLocker)
	}{
		{caseName: "cancelled before the call", run: func(t *testing.T, l *nameLocker) {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			_, err := l.Lock(cancelled, "a")
			require.ErrorIs(t, err, context.Canceled)
		}},
		{caseName: "cancelled while queued", run: func(t *testing.T, l *nameLocker) {
			unlock, err := l.Lock(ctx, "a")
			require.NoError(t, err)
			waitCtx, cancel := context.WithCancel(ctx)
			errs := make(chan error, 1)
			go func() {
				_, err := l.Lock(waitCtx, "a")
				errs <- err
			}()
			require.Eventually(t, func() bool { return queued(l, "a") == 1 }, time.Second, time.Millisecond)
			cancel()
			require.ErrorIs(t, <-errs, context.Canceled)
			require.Zero(t, queued(l, "a"))
			unlock()
		}},
		{caseName: "reader behind a cancelled writer", run: func(t *testing.T, l *nameLocker) {
			unlock, err := l.SLock(ctx, "a")
			require.NoError(t, err)
			waitCtx, cancel := context.WithCancel(ctx)
			errs := make(chan error, 1)
			go func() {
				_, err := l.Lock(waitCtx, "a")
				errs <- err
			}()
			require.Eventually(t, func() bool { return queued(l, "a") == 1 }, time.Second, time.Millisecond)
			granted := make(chan func(), 1)
			go func() {
				unlock, err := l.SLock(ctx, "a")
				assert.NoError(t, err)
				granted <- unlock
			}()
			require.Eventually(t, func() bool { return queued(l, "a") == 2 }, time.Second, time.Millisecond)
			cancel()
			require.ErrorIs(t, <-errs, context.Canceled)
			(<-granted)()
			unlock()
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			l := newNameLocker()
			tc.run(t, l)
			require.Empty(t, l.m)
		})
	}
}

func TestFoldName(t *testing.T) {
	testCases := []struct {
		caseName string
		in, want string
	}{
		{caseName: "ascii", in: "Photo.jpg", want: "PHOTO.JPG"}, // the least rune of a case orbit is the capital
		{caseName: "already folded", in: "A/B/X", want: "A/B/X"},
		{caseName: "orbit of three", in: "\u01c5", want: "\u01c4"}, // U+01C5 folds past U+01C6 to U+01C4
		{caseName: "kelvin sign", in: "\u212a", want: "K"},
		{caseName: "sharp s", in: "\u00df", want: "\u00df"}, // only full folding maps it to SS
		{caseName: "invalid utf-8", in: "A\xffB", want: "A\ufffdB"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			require.Equal(t, tc.want, foldName(tc.in))
		})
	}
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
	literal := &storage{lk: newNameLocker(), fsys: memfs.New()}

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
		{caseName: "delete recursive", op: func(ctx context.Context, strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/") }},
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
			fsys.RemoveAllFunc = func(dir string) error { check(); return base.RemoveAll(dir) }
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

		for key := range dirLockers.m {
			if strings.HasPrefix(key, l.key("")) {
				return true
			}
		}
		return false
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
		{caseName: "while a name is held", run: func(t *testing.T, l dirLocker, _ s2.Storage) {
			unlock, err := l.Lock(ctx, "a/x")
			require.NoError(t, err)
			require.True(t, registered(l))
			unlock()
		}},
		{caseName: "while a name is shared", run: func(t *testing.T, l dirLocker, _ s2.Storage) {
			unlock, err := l.SLock(ctx, ".")
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

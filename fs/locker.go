package fs

import (
	"context"
	"path/filepath"
	"sync"
)

// NameLocker locks names relative to the storage root; "." is the root itself.
// s2 locks only "." for now; an implementation must treat each name as its own lock.
type NameLocker interface {
	// Lock takes name exclusively.
	Lock(ctx context.Context, name string) (unlock func(), err error)
}

// WithNameLocker makes the storage, and every Sub of it, serialize writes through l.
func WithNameLocker(l NameLocker) Option {
	return func(s *storage) { s.lk = l }
}

// rootLocker is the default: one exclusive lock for every name.
type rootLocker struct{ ch chan struct{} }

func newRootLocker() *rootLocker {
	return &rootLocker{ch: make(chan struct{}, 1)}
}

func (l *rootLocker) Lock(ctx context.Context, _ string) (func(), error) {
	select {
	case l.ch <- struct{}{}:
		return func() { <-l.ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dirLocker is the rootLocker of an osfs directory, shared by every storage opened on it in this process.
type dirLocker struct{ dir string } // as rootKey normalized it

// dirLockers holds a directory's rootLocker only while someone holds or awaits it.
var dirLockers = struct {
	mu sync.Mutex
	m  map[string]*lockEntry
}{m: map[string]*lockEntry{}}

type lockEntry struct {
	*rootLocker
	refs int
}

func dirLockerFor(dir string) dirLocker {
	return dirLocker{dir: rootKey(dir)}
}

func (l dirLocker) Lock(ctx context.Context, name string) (func(), error) {
	e := l.enter()
	unlock, err := e.Lock(ctx, name)
	if err != nil {
		l.leave(e)
		return nil, err
	}
	return func() {
		unlock()
		l.leave(e)
	}, nil
}

func (l dirLocker) enter() *lockEntry {
	dirLockers.mu.Lock()
	defer dirLockers.mu.Unlock()

	e, ok := dirLockers.m[l.dir]
	if !ok {
		e = &lockEntry{rootLocker: newRootLocker()}
		dirLockers.m[l.dir] = e
	}
	e.refs++
	return e
}

func (l dirLocker) leave(e *lockEntry) {
	dirLockers.mu.Lock()
	defer dirLockers.mu.Unlock()

	if e.refs--; e.refs == 0 {
		delete(dirLockers.m, l.dir)
	}
}

// rootKey resolves symlinks in the part of dir that exists, so a root created later keeps its key.
func rootKey(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return dir
	}
	rest := ""
	for p := abs; ; p = filepath.Dir(p) {
		if real, err := filepath.EvalSymlinks(p); err == nil {
			return filepath.Join(real, rest)
		}
		if filepath.Dir(p) == p {
			return abs
		}
		rest = filepath.Join(filepath.Base(p), rest)
	}
}

// locked runs fn under the root lock; it is not reentrant, so nothing inside fn may call it.
func (s *storage) locked(ctx context.Context, fn func() error) error {
	unlock, err := s.lk.Lock(ctx, ".")
	if err != nil {
		return err
	}
	defer unlock()

	return fn()
}

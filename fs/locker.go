package fs

import (
	"context"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unicode"
)

// NameLocker locks names relative to the storage root; "." is the root itself.
// Without SLock, s2 locks only "."; with it, each name is its own lock.
type NameLocker interface {
	// Lock takes name exclusively.
	Lock(ctx context.Context, name string) (unlock func(), err error)
}

// SharedNameLocker is a NameLocker whose names can also be held shared; docs/backends.md lists which operation takes which.
type SharedNameLocker interface {
	NameLocker
	// SLock takes name shared: it excludes Lock holders, not other SLock holders.
	SLock(ctx context.Context, name string) (unlock func(), err error)
}

// WithNameLocker makes the storage, and every Sub of it, serialize writes through l.
func WithNameLocker(l NameLocker) Option {
	return func(s *storage) { s.lk = l }
}

// nameLocker is the default: a FIFO read-write lock per name, kept only while held or awaited.
type nameLocker struct {
	mu sync.Mutex
	m  map[string]*lockEntry
}

// lockEntry is one name's lock; it exists only while held or awaited.
type lockEntry struct {
	readers int
	writer  bool
	queue   []*waiter
}

// waiter is a queued lock call; ch closes once it is granted.
type waiter struct {
	shared bool
	ch     chan struct{}
}

func newNameLocker() *nameLocker {
	return &nameLocker{m: map[string]*lockEntry{}}
}

func (l *nameLocker) Lock(ctx context.Context, name string) (func(), error) {
	return l.lock(ctx, name, false)
}

func (l *nameLocker) SLock(ctx context.Context, name string) (func(), error) {
	return l.lock(ctx, name, true)
}

func (l *nameLocker) lock(ctx context.Context, name string, shared bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	e, ok := l.m[name]
	if !ok {
		e = &lockEntry{}
		l.m[name] = e
	}
	w := &waiter{shared: shared, ch: make(chan struct{})}
	e.queue = append(e.queue, w)
	e.grant()
	l.mu.Unlock()

	select {
	case <-w.ch:
		return func() { l.release(name, e, shared) }, nil
	case <-ctx.Done():
	}
	l.mu.Lock()
	i := slices.Index(e.queue, w)
	if i < 0 {
		l.mu.Unlock()
		l.release(name, e, shared) // granted meanwhile
		return nil, ctx.Err()
	}
	e.queue = slices.Delete(e.queue, i, i+1)
	e.grant()
	l.drop(name, e)
	l.mu.Unlock()
	return nil, ctx.Err()
}

// grant hands the lock to the waiters at the head of the queue; a writer there holds later readers back.
func (e *lockEntry) grant() {
	for len(e.queue) > 0 && !e.writer {
		w := e.queue[0]
		if !w.shared && e.readers > 0 {
			return
		}
		if w.shared {
			e.readers++
		} else {
			e.writer = true
		}
		e.queue = e.queue[1:]
		close(w.ch)
	}
}

func (l *nameLocker) release(name string, e *lockEntry, shared bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if shared {
		e.readers--
	} else {
		e.writer = false
	}
	e.grant()
	l.drop(name, e)
}

// drop forgets the entry once nothing holds or awaits it. Callers hold mu.
func (l *nameLocker) drop(name string, e *lockEntry) {
	if e.readers == 0 && !e.writer && len(e.queue) == 0 {
		delete(l.m, name)
	}
}

// dirLocker locks the names of an osfs directory in dirLockers, shared by every storage opened on it in this process.
type dirLocker struct{ dir string } // as rootKey normalized and foldName folded it

// dirLockers holds every osfs directory's names, qualified by the directory.
var dirLockers = newNameLocker()

func dirLockerFor(dir string) dirLocker {
	return dirLocker{dir: foldName(rootKey(dir))}
}

func (l dirLocker) Lock(ctx context.Context, name string) (func(), error) {
	return dirLockers.lock(ctx, l.key(name), false)
}

func (l dirLocker) SLock(ctx context.Context, name string) (func(), error) {
	return dirLockers.lock(ctx, l.key(name), true)
}

// key qualifies name by the directory; the separator keeps nested roots apart.
func (l dirLocker) key(name string) string {
	return l.dir + "\x00" + name
}

// foldName maps each rune to the smallest in its case orbit, so spellings differing only by case lock as one.
func foldName(name string) string {
	return strings.Map(func(r rune) rune {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			least = min(least, f)
		}
		return least
	}, name)
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

// lockRoot runs fn with the root held exclusively; nothing nests, so nothing inside fn may lock.
func (s *storage) lockRoot(ctx context.Context, fn func() error) error {
	unlock, err := s.lk.Lock(ctx, ".")
	if err != nil {
		return err
	}
	defer unlock()

	return fn()
}

// lockNames runs fn with the root shared and names held exclusively; a locker without SLock holds the root alone.
func (s *storage) lockNames(ctx context.Context, names []string, fn func() error) error {
	sl, ok := s.lk.(SharedNameLocker)
	if !ok {
		return s.lockRoot(ctx, fn)
	}
	unlock, err := sl.SLock(ctx, ".")
	if err != nil {
		return err
	}
	defer unlock()

	for _, key := range s.qualifiedNames(names) {
		unlock, err := sl.Lock(ctx, key)
		if err != nil {
			return err
		}
		defer unlock()
	}
	return fn()
}

// qualifiedNames qualifies names by the storage's prefix and folds them, sorted and without duplicates.
func (s *storage) qualifiedNames(names []string) []string {
	keys := make([]string, 0, len(names))
	for _, name := range names {
		keys = append(keys, foldName(path.Join(s.prefix, name)))
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
	"github.com/stretchr/testify/require"
)

// stubDirSync replaces fsyncDir with fn for t, which wraps the original.
func stubDirSync(t *testing.T, fn func(orig func(*os.File) error, d *os.File) error) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Windows syncs no directory")
	}
	orig := fsyncDir
	fsyncDir = func(d *os.File) error { return fn(orig, d) }
	t.Cleanup(func() { fsyncDir = orig })
}

var errLock = errors.New("lock failed")

// failingLocker fails its failAt-th Lock, counting from 1; zero never fails.
type failingLocker struct{ calls, failAt int }

func (l *failingLocker) Lock(context.Context, string) (func(), error) {
	l.calls++
	if l.calls == l.failAt {
		return nil, errLock
	}
	return func() {}, nil
}

// recordDirSyncs records each directory synced for t, relative to root.
func recordDirSyncs(t *testing.T, root string) *[]string {
	t.Helper()
	var synced []string
	stubDirSync(t, func(orig func(*os.File) error, d *os.File) error {
		name := d.Name()
		if filepath.IsAbs(name) {
			rel, err := filepath.Rel(root, name)
			require.NoError(t, err)
			name = rel
		}
		synced = append(synced, filepath.ToSlash(name))
		return orig(d)
	})
	return &synced
}

func TestDirSync(t *testing.T) {
	ctx := context.Background()
	put := func(name string) func(s2.Storage) error {
		return func(strg s2.Storage) error { return strg.Put(ctx, s2.NewObjectBytes(name, []byte("x"))) }
	}
	moveDirect := func(dropMeta bool) func(s2.Storage) error {
		return func(strg s2.Storage) error {
			st := strg.(*storage)
			if dropMeta {
				require.NoError(t, wfs.RemoveAll(st.fsys, "a/.meta"))
			}
			var ran bool
			return st.moveDirect("a/y.txt", "c/z.txt", &ran)
		}
	}
	testCases := []struct {
		caseName string
		confined bool
		seed     []string
		sub      string
		readOnly string // made read-only before op, so op fails partway
		lockErr  int    // the Lock during op that fails, counting from 1
		op       func(s2.Storage) error
		wantErr  bool
		want     []string
	}{
		{
			caseName: "put into a new prefix syncs the parent of each new directory",
			op:       put("a/b/c.txt"),
			want:     []string{"a/b", "a", ".", "a/b", "a/b/.meta"},
		},
		{
			caseName: "put into an existing prefix",
			seed:     []string{"a/y.txt"},
			op:       put("a/x.txt"),
			want:     []string{"a", "a/.meta"},
		},
		{
			caseName: "put at the top of a confined root",
			confined: true,
			op:       put("x.txt"),
			want:     []string{".", ".", ".meta"},
		},
		{
			caseName: "copy",
			seed:     []string{"a/y.txt"},
			op:       func(strg s2.Storage) error { return strg.Copy(ctx, "a/y.txt", "b/z.txt") },
			want:     []string{"b", ".", "b", "b/.meta"},
		},
		{
			caseName: "move syncs dst, then src, then prunes",
			seed:     []string{"a/y.txt"},
			op:       func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/y.txt", "c/z.txt") },
			want:     []string{".", "c", "c", "c/.meta", "a", "a/.meta", "."},
		},
		{
			caseName: "move within a directory syncs it once",
			seed:     []string{"a/x.txt", "a/y.txt"},
			op:       func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/y.txt", "a/z.txt") },
			want:     []string{"a", "a/.meta"},
		},
		{
			caseName: "cross-device move syncs dst, then src",
			seed:     []string{"a/y.txt"},
			op:       moveDirect(false),
			want:     []string{"c", ".", "c", "c/.meta", "a", "a/.meta"},
		},
		{
			caseName: "cross-device move of a key without a metadata file",
			seed:     []string{"a/y.txt"},
			op:       moveDirect(true),
			want:     []string{"c", ".", "c", "c/.meta", "a"},
		},
		{
			caseName: "delete from a directory that stays",
			seed:     []string{"a/x.txt", "a/y.txt"},
			op:       func(strg s2.Storage) error { return strg.Delete(ctx, "a/x.txt") },
			want:     []string{"a", "a/.meta"},
		},
		{
			caseName: "delete syncs the directory left holding what it pruned",
			seed:     []string{"a/b/x.txt"},
			op:       func(strg s2.Storage) error { return strg.Delete(ctx, "a/b/x.txt") },
			want:     []string{"a/b", "a/b/.meta", "."},
		},
		{
			caseName: "delete of a missing key syncs nothing",
			seed:     []string{"a/x.txt"},
			op:       func(strg s2.Storage) error { return strg.Delete(ctx, "a/y.txt") },
		},
		{
			caseName: "delete recursive syncs once what holds the removed tree",
			seed:     []string{"a/b/x.txt", "a/b/y.txt", "a/c.txt", "k.txt"},
			op:       func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/") },
			want:     []string{"."},
		},
		{
			caseName: "delete recursive by a partial name syncs each directory once",
			seed:     []string{"a/b1.txt", "a/b2.txt", "a/c.txt"},
			op:       func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/b") },
			want:     []string{"a", "a/.meta"},
		},
		{
			caseName: "put metadata",
			seed:     []string{"a/y.txt"},
			op:       func(strg s2.Storage) error { return strg.PutMetadata(ctx, "a/y.txt", s2.Metadata{"k": "v"}) },
			want:     []string{"a", "a/.meta"},
		},
		{
			caseName: "failed delete recursive syncs what it deleted",
			seed:     []string{"a/x.txt", "b/y.txt"},
			readOnly: "b",
			op:       func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "") },
			wantErr:  true,
			want:     []string{"a", "a/.meta"},
		},
		{
			caseName: "delete recursive whose last lock fails syncs what it deleted",
			seed:     []string{"a/x.txt", "k.txt"},
			lockErr:  4,
			op:       func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/") },
			wantErr:  true,
			want:     []string{"."},
		},
		{
			caseName: "delete after single entries still adds the metadata directory",
			seed:     []string{"a/x.txt", "a/y.txt"},
			op: func(strg s2.Storage) error {
				st := strg.(*storage)
				changed := []string{"a", "a"}
				require.NoError(t, st.delete("a/x.txt", &changed))
				return syncDirs(st.fsys, changed)
			},
			want: []string{"a", "a/.meta"},
		},
		{
			caseName: "delete recursive syncs the parent of a directory pruned meanwhile",
			seed:     []string{"a/b/x.txt", "a/k.txt"},
			op: func(strg s2.Storage) error {
				st := strg.(*storage)
				var changed []string
				require.NoError(t, st.delete("a/b/x.txt", &changed))
				pruneEmptyParents(st.fsys, "a/b/x.txt")
				return syncDirs(st.fsys, changed)
			},
			want: []string{"a"},
		},
		{
			caseName: "put into a new sub syncs up to the root",
			sub:      "b/u1",
			op:       put("meta"),
			want:     []string{"b/u1", "b", ".", "b/u1", "b/u1/.meta"},
		},
		{
			caseName: "put into a new sub of a confined root",
			confined: true,
			sub:      "b/u1",
			op:       put("meta"),
			want:     []string{"b/u1", "b", ".", "b/u1", "b/u1/.meta"},
		},
		{
			caseName: "put into an existing sub",
			seed:     []string{"b/u1/y"},
			sub:      "b/u1",
			op:       put("meta"),
			want:     []string{"b/u1", "b/u1/.meta"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			root := t.TempDir()
			var opts []Option
			if tc.confined {
				opts = append(opts, WithConfinedRoot())
			}
			lk := &failingLocker{}
			if tc.lockErr != 0 {
				opts = append(opts, WithNameLocker(lk))
			}
			strg := NewStorageDir(root, opts...)
			for _, name := range tc.seed {
				require.NoError(t, put(name)(strg))
			}
			lk.calls, lk.failAt = 0, tc.lockErr
			target := strg
			if tc.sub != "" {
				var err error
				target, err = strg.Sub(ctx, tc.sub)
				require.NoError(t, err)
			}
			if tc.readOnly != "" {
				if os.Geteuid() == 0 {
					t.Skip("root writes any directory")
				}
				dir := filepath.Join(root, tc.readOnly)
				require.NoError(t, os.Chmod(dir, 0o555))
				t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
			}
			synced := recordDirSyncs(t, root)

			err := tc.op(target)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, *synced)
		})
	}
}

func TestDirSyncError(t *testing.T) {
	ctx := context.Background()
	errSync := errors.New("sync failed")
	put := func(strg s2.Storage) error { return strg.Put(ctx, s2.NewObjectBytes("a/x.txt", []byte("new"))) }
	testCases := []struct {
		caseName   string
		op         func(s2.Storage) error
		seed       []string
		syncErr    error
		wantErr    error
		gone       string // removed by the prune after the failure
		kept       string
		keptDir    string
		unreadable string // made write-only before op
	}{
		{caseName: "put", op: put, syncErr: errSync, wantErr: errSync},
		{caseName: "move", op: func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/x.txt", "a/z.txt") }, syncErr: errSync, wantErr: errSync},
		{caseName: "put metadata", op: func(strg s2.Storage) error { return strg.PutMetadata(ctx, "a/x.txt", s2.Metadata{"k": "v"}) }, syncErr: errSync, wantErr: errSync},
		{caseName: "delete still prunes", op: func(strg s2.Storage) error { return strg.Delete(ctx, "a/x.txt") }, syncErr: errSync, wantErr: errSync, gone: "a"},
		{
			caseName: "delete that never took its lock prunes nothing",
			op: func(strg s2.Storage) error {
				require.NoError(t, wfs.MkdirAll(strg.(*storage).fsys, "e", 0o755))
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				return strg.Delete(cancelled, "e/x.txt")
			},
			wantErr: context.Canceled,
			keptDir: "e",
		},
		{caseName: "delete recursive", op: func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/") }, syncErr: errSync, wantErr: errSync, gone: "a"},
		{caseName: "EINVAL is no failure", op: put, syncErr: &os.PathError{Op: "sync", Err: syscall.EINVAL}},
		{caseName: "ENOTSUP is no failure", op: put, syncErr: &os.PathError{Op: "sync", Err: syscall.ENOTSUP}},
		{caseName: "EOPNOTSUPP is no failure", op: put, syncErr: &os.PathError{Op: "sync", Err: syscall.EOPNOTSUPP}},
		{
			caseName: "move out of a directory prunes it",
			seed:     []string{"b/y.txt"},
			op:       func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/x.txt", "b/z.txt") },
			syncErr:  errSync,
			wantErr:  errSync,
			gone:     "a",
			kept:     "b/z.txt",
		},
		{
			caseName: "move into a new directory prunes it",
			op:       func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/x.txt", "c/z.txt") },
			syncErr:  errSync,
			wantErr:  errSync,
			gone:     "c",
			kept:     "a/x.txt",
		},
		{
			caseName: "put into a new directory prunes it",
			op:       func(strg s2.Storage) error { return strg.Put(ctx, s2.NewObjectBytes("n/m/x.txt", []byte("new"))) },
			syncErr:  errSync,
			wantErr:  errSync,
			gone:     "n",
		},
		{
			caseName: "copy into a new directory prunes it",
			op:       func(strg s2.Storage) error { return strg.Copy(ctx, "a/x.txt", "n/m/z.txt") },
			syncErr:  errSync,
			wantErr:  errSync,
			gone:     "n",
			kept:     "a/x.txt",
		},
		{caseName: "an unreadable directory is no failure", op: put, unreadable: "a"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			root := t.TempDir()
			strg := NewStorageDir(root)
			for _, name := range append([]string{"a/x.txt"}, tc.seed...) {
				require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(name, []byte("old"))))
			}
			if tc.unreadable != "" {
				if os.Geteuid() == 0 {
					t.Skip("root reads any directory")
				}
				dir := filepath.Join(root, tc.unreadable)
				require.NoError(t, os.Chmod(dir, 0o333))
				t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
			}
			stubDirSync(t, func(orig func(*os.File) error, d *os.File) error {
				if tc.syncErr == nil {
					return orig(d)
				}
				return tc.syncErr
			})

			err := tc.op(strg)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
			if tc.gone != "" {
				require.NoDirExists(t, filepath.Join(root, tc.gone))
			}
			if tc.kept != "" {
				require.FileExists(t, filepath.Join(root, tc.kept))
			}
			if tc.keptDir != "" {
				require.DirExists(t, filepath.Join(root, tc.keptDir))
			}
		})
	}
}

func TestSyncDirs(t *testing.T) {
	testCases := []struct {
		caseName string
		dirs     []string
		want     []string
	}{
		{caseName: "a directory gone since syncs its nearest parent", dirs: []string{"a", "gone"}, want: []string{"a", "."}},
		{caseName: "a directory gone two levels deep climbs to a parent synced once", dirs: []string{"a/gone/deeper", "a"}, want: []string{"a"}},
		{caseName: "a directory under what is now a file is skipped", dirs: []string{"f/x"}},
		{caseName: "a duplicate is synced once", dirs: []string{"a", "a/b", "a"}, want: []string{"a", "a/b"}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(root, "a", "b"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(root, "f"), []byte("x"), 0o644))
			fsys := NewStorageDir(root).(*storage).fsys
			synced := recordDirSyncs(t, root)

			require.NoError(t, syncDirs(fsys, tc.dirs))
			require.Equal(t, tc.want, *synced)
		})
	}
}

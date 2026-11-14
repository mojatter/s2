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
	testCases := []struct {
		caseName string
		confined bool
		seed     []string
		sub      string
		op       func(s2.Storage) error
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
			caseName: "move syncs dst only",
			seed:     []string{"a/y.txt"},
			op:       func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/y.txt", "c/z.txt") },
			want:     []string{".", "c", "c", "c/.meta"},
		},
		{
			caseName: "put metadata",
			seed:     []string{"a/y.txt"},
			op:       func(strg s2.Storage) error { return strg.PutMetadata(ctx, "a/y.txt", s2.Metadata{"k": "v"}) },
			want:     []string{"a", "a/.meta"},
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
			strg := NewStorageDir(root, opts...)
			for _, name := range tc.seed {
				require.NoError(t, put(name)(strg))
			}
			target := strg
			if tc.sub != "" {
				var err error
				target, err = strg.Sub(ctx, tc.sub)
				require.NoError(t, err)
			}
			synced := recordDirSyncs(t, root)

			require.NoError(t, tc.op(target))
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
		unreadable string // made write-only before op
	}{
		{caseName: "put", op: put, syncErr: errSync, wantErr: errSync},
		{caseName: "move", op: func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/x.txt", "a/z.txt") }, syncErr: errSync, wantErr: errSync},
		{caseName: "put metadata", op: func(strg s2.Storage) error { return strg.PutMetadata(ctx, "a/x.txt", s2.Metadata{"k": "v"}) }, syncErr: errSync, wantErr: errSync},
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
		})
	}
}

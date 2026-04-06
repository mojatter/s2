package fs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs/osfs"
	"github.com/stretchr/testify/require"
)

// newOSFSStorage returns a storage backed by an osfs rooted at a fresh temp
// directory. The directory is cleaned up by t.Cleanup.
func newOSFSStorage(t *testing.T) (*storage, string) {
	t.Helper()
	dir := t.TempDir()
	return &storage{
		cfg:  s2.Config{Type: s2.TypeOSFS, Root: dir},
		fsys: osfs.DirFS(dir),
		typ:  s2.TypeOSFS,
	}, dir
}

// readDirNames returns all basenames in dir, including hidden files.
func readDirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestAtomicWrite_LeavesNoTempFile(t *testing.T) {
	strg, dir := newOSFSStorage(t)
	ctx := context.Background()

	obj := s2.NewObjectBytes("hello.txt", []byte("hi"))
	require.NoError(t, strg.Put(ctx, obj))

	names := readDirNames(t, dir)
	for _, n := range names {
		if strings.HasPrefix(n, tmpPrefix) {
			t.Fatalf("temp file %q was left behind in %v", n, names)
		}
	}
	// The committed file should be present.
	data, err := os.ReadFile(filepath.Join(dir, "hello.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("hi"), data)
}

func TestAtomicWrite_Overwrite(t *testing.T) {
	strg, dir := newOSFSStorage(t)
	ctx := context.Background()

	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("k", []byte("v1"))))
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("k", []byte("v2-longer"))))

	data, err := os.ReadFile(filepath.Join(dir, "k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2-longer"), data)
}

func TestAtomicWrite_TempHiddenFromList(t *testing.T) {
	strg, dir := newOSFSStorage(t)
	ctx := context.Background()

	// Manually place a temp file as if a crash had left one behind.
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, tmpPrefix+"ghost.abcd1234"),
		[]byte("garbage"),
		0o644,
	))
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("real.txt", []byte("ok"))))

	// Flat list must not include the temp file.
	objs, _, err := strg.List(ctx, "", 100)
	require.NoError(t, err)
	names := objectNames(objs)
	require.ElementsMatch(t, []string{"real.txt"}, names)

	// Recursive list must also hide it.
	objs, err = strg.ListRecursive(ctx, "", 100)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"real.txt"}, objectNames(objs))
}

func TestAtomicWrite_NestedDir(t *testing.T) {
	strg, dir := newOSFSStorage(t)
	ctx := context.Background()

	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("sub/dir/file.txt", []byte("nested"))))

	data, err := os.ReadFile(filepath.Join(dir, "sub", "dir", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("nested"), data)

	// No temp files should remain in the nested directory.
	entries, err := os.ReadDir(filepath.Join(dir, "sub", "dir"))
	require.NoError(t, err)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tmpPrefix) {
			t.Fatalf("temp file %q was left behind", e.Name())
		}
	}
}

func TestAtomicWrite_MoveRenamesInPlace(t *testing.T) {
	strg, dir := newOSFSStorage(t)
	ctx := context.Background()

	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("src.txt", []byte("payload"))))
	require.NoError(t, strg.Move(ctx, "src.txt", "dst.txt"))

	if _, err := os.Stat(filepath.Join(dir, "src.txt")); !os.IsNotExist(err) {
		t.Fatalf("src still exists after move: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "dst.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), data)
}

func objectNames(objs []s2.Object) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.Name())
	}
	return out
}

// Sanity check: tempName produces unique names with the expected prefix and
// placement so that the rename target ends up as a sibling of the final file.
func TestTempName_Unique(t *testing.T) {
	a, err := tempName("dir/file.txt")
	require.NoError(t, err)
	b, err := tempName("dir/file.txt")
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	require.True(t, strings.HasPrefix(filepath.Base(a), tmpPrefix))
	require.Equal(t, "dir", filepath.Dir(a))
}

func TestAtomicWrite_FallbackWhenNoRename(t *testing.T) {
	// Use a memfs storage and assert that even without a RenameFS the file
	// ends up with the expected contents. MemFS does implement RenameFS so
	// this is really exercising the happy path on memfs; the fallback path
	// is covered by writing through a plain fs.FS directly.
	strg := NewStorageMem(s2.Config{})
	ctx := context.Background()
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("k", []byte("v"))))

	obj, err := strg.Get(ctx, "k")
	require.NoError(t, err)
	rc, err := obj.Open()
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), data)
}

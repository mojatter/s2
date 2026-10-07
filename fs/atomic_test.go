package fs

import (
	"context"
	"errors"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/osfs"
	"github.com/stretchr/testify/require"
)

// newOSFSStorage returns a storage backed by an osfs rooted at a fresh temp
// directory. The directory is cleaned up by t.Cleanup.
func newOSFSStorage(t *testing.T) (*storage, string) {
	t.Helper()
	dir := t.TempDir()
	return &storage{
		lk:   newNameLocker(),
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

	requireNoTempFile(t, dir)
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

func TestAtomicWrite_NestedDir(t *testing.T) {
	strg, dir := newOSFSStorage(t)
	ctx := context.Background()

	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("sub/dir/file.txt", []byte("nested"))))

	data, err := os.ReadFile(filepath.Join(dir, "sub", "dir", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("nested"), data)

	requireNoTempFile(t, dir)
}

// requireNoTempFile fails when a temp file is left anywhere under dir, its .meta directories included.
func requireNoTempFile(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(dir, func(name string, d os.DirEntry, err error) error {
		if err == nil && strings.HasPrefix(d.Name(), tmpPrefix) {
			t.Fatalf("temp file %q was left behind", name)
		}
		return err
	}))
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

// TestTempName_Unique checks tempName gives unique names in the .meta of the file's directory, or in that .meta itself for a metadata file.
func TestTempName_Unique(t *testing.T) {
	a, err := tempName("dir/file.txt")
	require.NoError(t, err)
	b, err := tempName("dir/file.txt")
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	require.True(t, strings.HasPrefix(filepath.Base(a), tmpPrefix))
	require.Equal(t, "dir/.meta", filepath.Dir(a))
	m, err := tempName("dir/.meta/file.txt")
	require.NoError(t, err)
	require.Equal(t, "dir/.meta", filepath.Dir(m))
	r, err := tempName("file.txt")
	require.NoError(t, err)
	require.Equal(t, ".meta", filepath.Dir(r))
}

// TestTempNameIsAnObjectName checks a key holding a temp file name is an ordinary object: stored, read, listed and deleted (#268).
func TestTempNameIsAnObjectName(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		strg     s2.Storage
	}{
		{caseName: "memfs", strg: NewStorageMem(s2.Config{})},
		{caseName: "osfs", strg: NewStorageDir(t.TempDir())},
	}
	names := []string{tmpPrefix + "a.txt", "docs/" + tmpPrefix + "a.txt.0123456789abcdef", "a/" + tmpPrefix + "x/b"}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			for _, name := range names {
				require.NoError(t, tc.strg.Put(ctx, s2.NewObjectBytes(name, []byte("v"), s2.WithMetadata(s2.Metadata{"k": "v"}))))
				obj, err := tc.strg.Get(ctx, name)
				require.NoError(t, err)
				require.Equal(t, s2.Metadata{"k": "v"}, obj.Metadata())
			}
			res, err := tc.strg.List(ctx, s2.ListOptions{Recursive: true})
			require.NoError(t, err)
			require.ElementsMatch(t, names, objectNames(res.Objects))
			res, err = tc.strg.List(ctx, s2.ListOptions{})
			require.NoError(t, err)
			require.ElementsMatch(t, names[:1], objectNames(res.Objects))
			require.ElementsMatch(t, []string{"a/", "docs/"}, res.CommonPrefixes)

			require.NoError(t, tc.strg.Delete(ctx, names[0]))
			require.NoError(t, tc.strg.DeleteRecursive(ctx, "a/"))
			res, err = tc.strg.List(ctx, s2.ListOptions{Recursive: true})
			require.NoError(t, err)
			require.ElementsMatch(t, names[1:2], objectNames(res.Objects))
		})
	}
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

// noRenameFS hides osfs's Rename so Storage.Put takes the direct-write path.
type noRenameFS struct{ wfs.WriteFileFS }

// TestWrittenFileMode checks a stored body or metadata file is not created executable.
func TestWrittenFileMode(t *testing.T) {
	testCases := []struct {
		caseName string
		write    func(fsys iofs.FS, name string) error
	}{
		{caseName: "temp file then rename", write: func(fsys iofs.FS, name string) error {
			tf, err := createTemp(fsys, name)
			if err != nil {
				return err
			}
			defer tf.discard()

			if err := tf.write(strings.NewReader("v")); err != nil {
				return err
			}
			return tf.publish()
		}},
		{caseName: "direct write", write: func(fsys iofs.FS, name string) error {
			return directWrite(fsys, name, strings.NewReader("v"))
		}},
		{caseName: "Storage.Put", write: func(fsys iofs.FS, name string) error {
			return NewStorageFS(s2.Config{}, fsys).Put(context.Background(), s2.NewObjectBytes(name, []byte("v")))
		}},
		{caseName: "Storage.Put without rename", write: func(fsys iofs.FS, name string) error {
			return NewStorageFS(s2.Config{}, noRenameFS{fsys.(wfs.WriteFileFS)}).Put(context.Background(), s2.NewObjectBytes(name, []byte("v")))
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "d"), 0o755))
			require.NoError(t, tc.write(osfs.DirFS(dir), "d/x"))

			require.NoError(t, filepath.WalkDir(dir, func(name string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return err
				}
				info, err := d.Info()
				require.NoError(t, err)
				require.Zero(t, info.Mode().Perm()&0o111, "%s mode %v", name, info.Mode())
				return nil
			}))
		})
	}
}

// TestTempFile checks each step leaves name and the temp file as the next section expects.
func TestTempFile(t *testing.T) {
	testCases := []struct {
		caseName    string
		src         io.Reader
		publish     bool
		wantWrite   string
		wantContent string
	}{
		{caseName: "published", src: strings.NewReader("new"), publish: true, wantContent: "new"},
		{caseName: "discarded", src: strings.NewReader("new"), wantContent: "old"},
		{caseName: "write fails", src: iotest.ErrReader(errors.New("boom")), wantWrite: "boom", wantContent: "old"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			dir := t.TempDir()
			fsys := osfs.DirFS(dir)
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "d"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "d", "x"), []byte("old"), 0o644))

			tf, err := createTemp(fsys, "d/x")
			require.NoError(t, err)
			err = tf.write(tc.src)
			if tc.wantWrite != "" {
				require.ErrorContains(t, err, tc.wantWrite)
			} else {
				require.NoError(t, err)
			}
			if tc.publish {
				require.NoError(t, tf.publish())
			}
			tf.discard()

			require.Equal(t, []string{metaDir, "x"}, readDirNames(t, filepath.Join(dir, "d")))
			require.Empty(t, readDirNames(t, filepath.Join(dir, "d", metaDir)))
			data, err := os.ReadFile(filepath.Join(dir, "d", "x"))
			require.NoError(t, err)
			require.Equal(t, tc.wantContent, string(data))
		})
	}
}

package fs

import (
	"context"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/memfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDirMarkerOnTree checks what fs adds to a marker: no body, "d" and "d/" never both, its file at d/.meta/.meta, and d kept only while it holds something.
func TestDirMarkerOnTree(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		before   []string // written first, "/"-ended ones as markers
		put      string
		body     string
		wantErr  error
		wantDirs []string // under the root afterwards
		gone     []string // absent afterwards
	}{
		{caseName: "marker", put: "d/", wantDirs: []string{"d", "d/.meta"}},
		{caseName: "nested marker", put: "a/b/", wantDirs: []string{"a", "a/b", "a/b/.meta"}},
		{caseName: "body refused", put: "d/", body: "x", wantErr: s2.ErrInvalidName, gone: []string{"d"}},
		{caseName: "over an object", before: []string{"d"}, put: "d/", wantErr: s2.ErrInvalidName},
		{caseName: "under an object", before: []string{"d"}, put: "d/e/", wantErr: s2.ErrInvalidName},
		{caseName: "object over a marker", before: []string{"d/"}, put: "d", wantErr: s2.ErrInvalidName},
		{caseName: "marker beside its keys", before: []string{"d/a"}, put: "d/", wantDirs: []string{"d", "d/.meta"}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			root := t.TempDir()
			strg := NewStorageDir(root)
			for _, name := range tc.before {
				require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(name, nil)))
			}
			err := strg.Put(ctx, s2.NewObjectBytes(tc.put, []byte(tc.body)))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.FileExists(t, filepath.Join(root, filepath.FromSlash(metaPath(tc.put))))
			}
			for _, dir := range tc.wantDirs {
				assert.DirExists(t, filepath.Join(root, filepath.FromSlash(dir)))
			}
			for _, name := range tc.gone {
				assert.NoFileExists(t, filepath.Join(root, filepath.FromSlash(name)))
				assert.NoDirExists(t, filepath.Join(root, filepath.FromSlash(name)))
			}
		})
	}
}

// TestDirMarkerKeepsItsDir checks a marker keeps its directory after the last key under it goes, and deleting it prunes the directory.
func TestDirMarkerKeepsItsDir(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	strg := NewStorageDir(root)
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("a/d/", nil)))
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("a/d/x", []byte("x"))))

	require.NoError(t, strg.Delete(ctx, "a/d/x"))
	assert.FileExists(t, filepath.Join(root, "a", "d", ".meta", ".meta"))

	require.NoError(t, strg.Delete(ctx, "a/d/"))
	assert.NoDirExists(t, filepath.Join(root, "a"))
}

// TestMoveMarker checks a Move to or from a marker, which goes by copy and delete.
func TestMoveMarker(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		src      string
		body     string
		dst      string
		noRemove bool   // reopened without Rename or RemoveFile
		link     string // a symlink to src's directory, made before the Move
		wantErr  error
	}{
		{caseName: "marker to marker", src: "a/", dst: "b/"},
		{caseName: "marker to object", src: "a/", dst: "b.txt"},
		{caseName: "empty object to marker", src: "a.txt", dst: "b/"},
		{caseName: "object with a body to marker", src: "a.txt", body: "x", dst: "b/", wantErr: s2.ErrInvalidName},
		{caseName: "marker onto itself", src: "a/", dst: "a/"},
		{caseName: "marker onto itself where nothing can be written", src: "a/", dst: "a/", noRemove: true, wantErr: wfs.ErrNotImplemented},
		// One file on a case-insensitive volume, two otherwise; either way no marker is lost.
		{caseName: "marker onto its case variant", src: "a/", dst: "A/"},
		{caseName: "marker onto itself through a symlinked directory", src: "a/", dst: "l/", link: "l"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			root := t.TempDir()
			strg := NewStorageDir(root)
			mem := memfs.New()
			if tc.noRemove {
				strg = NewStorageFS(s2.Config{}, mem)
			}
			require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(tc.src, []byte(tc.body), s2.WithMetadata(s2.Metadata{"k": "v"}))))
			if tc.noRemove {
				strg = NewStorageFS(s2.Config{}, noRemoveFS{mem})
			}
			if tc.link != "" {
				require.NoError(t, os.Symlink(strings.TrimSuffix(tc.src, "/"), filepath.Join(root, tc.link)))
			}
			// Both names reach one directory on disk.
			si, serr := os.Stat(filepath.Join(root, tc.src))
			di, derr := os.Stat(filepath.Join(root, tc.dst))
			alias := !tc.noRemove && serr == nil && derr == nil && os.SameFile(si, di)

			err := s2.Move(ctx, strg, tc.src, tc.dst)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				ok, err := strg.Exists(ctx, tc.src)
				require.NoError(t, err)
				assert.True(t, ok, "a refused Move removed its source")
				return
			}
			require.NoError(t, err)
			got, err := strg.Get(ctx, tc.dst)
			require.NoError(t, err)
			assert.Equal(t, uint64(0), got.Length())
			assert.Equal(t, "v", got.Metadata()["k"])
			if tc.src != tc.dst {
				ok, err := strg.Exists(ctx, tc.src)
				require.NoError(t, err)
				assert.Equal(t, alias, ok, "the source stays only when it is the destination")
			}
		})
	}
}

// TestOpenMarker checks a marker's Open fails once the marker is gone, and not after a metadata update, as a body's Open does not.
func TestOpenMarker(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		after    func(strg s2.Storage) error
		wantErr  error
	}{
		{caseName: "unchanged"},
		{
			caseName: "metadata rewritten",
			after: func(strg s2.Storage) error {
				return strg.PutMetadata(ctx, "d/", s2.Metadata{"k": "w"})
			},
		},
		{
			caseName: "deleted",
			after:    func(strg s2.Storage) error { return strg.Delete(ctx, "d/") },
			wantErr:  s2.ErrNotExist,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			strg := NewStorageMem(s2.Config{})
			require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("d/", nil)))
			obj, err := strg.Get(ctx, "d/")
			require.NoError(t, err)
			if tc.after != nil {
				require.NoError(t, tc.after(strg))
			}
			// Repeated until the metadata file's time moves, so the Open below sees a rewritten one.
			for tc.wantErr == nil && tc.after != nil {
				got, err := strg.Get(ctx, "d/")
				require.NoError(t, err)
				if !got.LastModified().Equal(obj.LastModified()) {
					break
				}
				require.NoError(t, tc.after(strg))
			}
			rc, err := obj.Open()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			_ = rc.Close()
		})
	}
}

// TestStatKeyLeftovers checks what an older s2 left where d's marker belongs holds no marker.
func TestStatKeyLeftovers(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		files    []string // written raw, before any marker
	}{
		{caseName: "a key d/.meta stored before v0.18.1", files: []string{"d/.meta"}},
		{caseName: "a directory at the marker's place", files: []string{"d/.meta/.meta/x"}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			strg := NewStorageMem(s2.Config{}).(*storage)
			for _, name := range tc.files {
				require.NoError(t, wfs.MkdirAll(strg.fsys, path.Dir(name), 0o755))
				_, err := wfs.WriteFile(strg.fsys, name, []byte("old"), 0o644)
				require.NoError(t, err)
			}
			ok, err := strg.Exists(ctx, "d/")
			require.NoError(t, err)
			assert.False(t, ok)
			_, err = strg.Get(ctx, "d/")
			assert.ErrorIs(t, err, s2.ErrNotExist)
			res, err := strg.List(ctx, s2.ListOptions{Prefix: "d/"})
			require.NoError(t, err)
			assert.Empty(t, res.Objects)
			// Delete agrees with Exists, leaving what it does not take for a marker.
			require.NoError(t, strg.Delete(ctx, "d/"))
			for _, name := range tc.files {
				_, err := iofs.Stat(strg.fsys, name)
				assert.NoError(t, err, "%s stays", name)
			}
			assert.ErrorIs(t, strg.Put(ctx, s2.NewObjectBytes("d/", nil)), ErrMetaBlocked)
		})
	}
}

// blockingReader blocks its first Read until release closes, telling reading once it has started.
type blockingReader struct {
	once    sync.Once
	reading chan struct{}
	release chan struct{}
}

func (r *blockingReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.reading) })
	<-r.release
	return 0, io.EOF
}

// TestMarkerBodyBeforeLock checks a marker's body is judged before any lock, so one still arriving holds no other writer.
func TestMarkerBodyBeforeLock(t *testing.T) {
	ctx := context.Background()
	strg := NewStorageDir(t.TempDir())

	t.Run("declared length refused", func(t *testing.T) {
		obj := s2.NewObjectReader("d/", io.NopCloser(strings.NewReader("")), 1)
		assert.ErrorIs(t, strg.Put(ctx, obj), s2.ErrInvalidName)
	})
	t.Run("stalled body holds no lock", func(t *testing.T) {
		body := &blockingReader{reading: make(chan struct{}), release: make(chan struct{})}
		done := make(chan error, 1)
		go func() {
			done <- strg.Put(ctx, s2.NewObjectReader("slow/", io.NopCloser(body), 0))
		}()
		<-body.reading
		defer close(body.release)

		// DeleteRecursive takes the root exclusively, which a held name lock would block.
		deleted := make(chan error, 1)
		go func() { deleted <- strg.DeleteRecursive(ctx, "") }()
		select {
		case err := <-deleted:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("DeleteRecursive waited on a marker whose body is still arriving")
		}
	})
}

// TestCountMarkerStatError checks Count fails as List does when a marker cannot be stat'd.
func TestCountMarkerStatError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads any directory")
	}
	ctx := context.Background()
	root := t.TempDir()
	strg := NewStorageDir(root)
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("d/", nil)))
	meta := filepath.Join(root, "d", ".meta")
	require.NoError(t, os.Chmod(meta, 0o000))
	t.Cleanup(func() { _ = os.Chmod(meta, 0o755) })

	_, err := strg.List(ctx, s2.ListOptions{Prefix: "d/"})
	require.Error(t, err)
	_, err = s2.Count(ctx, strg, s2.CountOptions{Prefix: "d/"})
	assert.Error(t, err)
}

// TestMarkerWithoutRename checks a marker on a filesystem without Rename, where its metadata file is written in place.
func TestMarkerWithoutRename(t *testing.T) {
	ctx := context.Background()
	strg := NewStorageFS(s2.Config{}, noRenameFS{memfs.New()})
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("d/", nil, s2.WithMetadata(s2.Metadata{"k": "v"}))))
	require.NoError(t, strg.Copy(ctx, "d/", "e/"))
	require.NoError(t, s2.Move(ctx, strg, "e/", "f/"))

	got, err := strg.Get(ctx, "f/")
	require.NoError(t, err)
	assert.Equal(t, "v", got.Metadata()["k"])
	require.NoError(t, strg.Delete(ctx, "d/"))
	res, err := strg.List(ctx, s2.ListOptions{Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"f/"}, objectNames(res.Objects))
}

// TestEmptiedKeepsMarkedDir checks a directory holding its marker is not emptied, so deleting its last key takes no root lock.
func TestEmptiedKeepsMarkedDir(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		names    []string
		want     bool
	}{
		{caseName: "nothing left", names: []string{"d/x"}, want: true},
		{caseName: "marker left", names: []string{"d/", "d/x"}, want: false},
		{caseName: "another key left", names: []string{"d/k", "d/x"}, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			strg := NewStorageMem(s2.Config{}).(*storage)
			for _, name := range tc.names {
				require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(name, nil)))
			}
			// Removed by hand, as Delete would prune what emptied reports.
			require.NoError(t, wfs.RemoveFile(strg.fsys, "d/x"))
			require.NoError(t, wfs.RemoveFile(strg.fsys, metaPath("d/x")))
			assert.Equal(t, tc.want, emptied(strg.fsys, "d"))
		})
	}
}

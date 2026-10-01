package fs

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501 -- the ETag is an MD5
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/iotest"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/memfs"
	"github.com/mojatter/wfs/osfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// errReadDirFS wraps an fs.FS and injects a ReadDir error for a specific path.
type errReadDirFS struct {
	fs.FS
	errorPath string
}

func (e *errReadDirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == e.errorPath {
		return nil, fmt.Errorf("injected ReadDir error for %q", name)
	}
	return fs.ReadDir(e.FS, name)
}

func (e *errReadDirFS) Open(name string) (fs.File, error) {
	return e.FS.Open(name)
}

// goneStatFS answers ErrNotExist for one path's Stat while ReadDir still sees
// the tree, standing in for an object deleted between the two calls.
type goneStatFS struct {
	fs.FS
	gonePath string
}

func (g *goneStatFS) Stat(name string) (fs.FileInfo, error) {
	if name == g.gonePath {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return fs.Stat(g.FS, name)
}

// vanishFS lists a file and a directory in every ReadDir that are gone by the time they are read.
type vanishFS struct {
	fs.FS
}

func (v *vanishFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if path.Base(name) == "gone-dir" {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	entries, err := fs.ReadDir(v.FS, name)
	if err != nil {
		return nil, err
	}
	entries = append(entries, goneEntry{name: "gone-dir", dir: true}, goneEntry{name: "gone.txt"})
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return entries, nil
}

type goneEntry struct {
	name string
	dir  bool
}

func (e goneEntry) Name() string { return e.name }
func (e goneEntry) IsDir() bool  { return e.dir }
func (e goneEntry) Type() fs.FileMode {
	if e.dir {
		return fs.ModeDir
	}
	return 0
}
func (e goneEntry) Info() (fs.FileInfo, error) {
	return nil, &fs.PathError{Op: "lstat", Path: e.name, Err: fs.ErrNotExist}
}

type StorageTestSuite struct {
	suite.Suite
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, &StorageTestSuite{})
}

func (s *StorageTestSuite) TestNewStorage() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		cfg      s2.Config
		wantType any
	}{
		{
			caseName: "osfs",
			cfg:      s2.Config{Type: s2.TypeOSFS, Root: s.T().TempDir()},
			wantType: &storage{},
		},
		{
			caseName: "memfs",
			cfg:      s2.Config{Type: s2.TypeMemFS},
			wantType: &storage{},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got, err := NewStorage(tc.ctx, tc.cfg)
			s.Require().NoError(err)
			s.IsType(tc.wantType, got)
		})
	}
}

func (s *StorageTestSuite) TestNewStorageOSFSEmptyRoot() {
	_, err := NewStorage(context.Background(), s2.Config{Type: s2.TypeOSFS})
	s.Require().ErrorIs(err, s2.ErrRequiredConfigRoot)
}

func (s *StorageTestSuite) TestNewStorageDir() {
	tempDir := s.T().TempDir()
	got := NewStorageDir(tempDir)
	s.Require().NotNil(got)
	s.Equal(s2.TypeOSFS, got.Type())
}

func (s *StorageTestSuite) TestType() {
	s.Run("memfs", func() {
		strg := NewStorageMem(s2.Config{})
		s.Equal(s2.TypeMemFS, strg.Type())
	})
	s.Run("osfs", func() {
		tempDir := s.T().TempDir()
		strg := NewStorageFS(s2.Config{Type: s2.TypeOSFS}, osfs.DirFS(tempDir))
		s.Equal(s2.TypeOSFS, strg.Type())
	})
}

func (s *StorageTestSuite) testMemFS() fs.FS {
	return s.fillTestFS(memfs.New())
}

func (s *StorageTestSuite) testOSFS() fs.FS {
	return s.fillTestFS(osfs.DirFS(s.T().TempDir()))
}

func (s *StorageTestSuite) fillTestFS(fsys fs.FS) fs.FS {
	files := map[string][]byte{
		"a.txt":     []byte("a"),
		"b.txt":     []byte("b"),
		"cc/c1.txt": []byte("c1"),
		"cc/c2.txt": []byte("c2"),
	}
	for name, b := range files {
		_, err := wfs.WriteFile(fsys, name, b, fs.ModePerm)
		s.Require().NoError(err)
	}
	return fsys
}

func (s *StorageTestSuite) TestS2TestList() {
	strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}
	ctx := context.Background()

	err := s2test.TestStorageListRecursive(ctx, strg, "a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageListRecursivePrefix(ctx, strg)
	s.Require().NoError(err)

	err = s2test.TestStorageListWithPrefixes(ctx, strg, "", []string{"cc/"}, "a.txt", "b.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "cc", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)

	s.Require().NoError(s2test.TestStorageListDefaultPage(ctx, strg))
}

func (s *StorageTestSuite) TestS2TestListPaging() {
	testCases := []struct {
		caseName string
		newFS    func() fs.FS
	}{
		{caseName: "memfs", newFS: func() fs.FS { return memfs.New() }},
		{caseName: "osfs", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			strg := NewStorageFS(s2.Config{}, tc.newFS())
			s.Require().NoError(s2test.TestStorageListPaging(context.Background(), strg))
		})
	}
}

func (s *StorageTestSuite) TestS2TestGetPut() {
	strg := NewStorageMem(s2.Config{})
	s.Require().NoError(s2test.TestStorageGetPut(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestGetNotExist() {
	strg := NewStorageMem(s2.Config{})
	s.Require().NoError(s2test.TestStorageGetNotExist(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestExists() {
	strg := NewStorageMem(s2.Config{})
	s.Require().NoError(s2test.TestStorageExists(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestCopyMove() {
	strg := NewStorageMem(s2.Config{})
	s.Require().NoError(s2test.TestStorageCopyMove(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestDelete() {
	strg := NewStorageMem(s2.Config{})
	s.Require().NoError(s2test.TestStorageDelete(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestNameEscape() {
	strg := NewStorageMem(s2.Config{})
	s.Require().NoError(s2test.TestStorageNameEscape(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestPutMetadata() {
	strg := NewStorageMem(s2.Config{})
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), strg))
}

func (s *StorageTestSuite) TestList() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		prefix   string
		limit    int
		want     []s2.Object
		wantErr  string
	}{
		{
			caseName: "typical",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "",
			limit:    10,
			want: []s2.Object{
				s2.NewObjectBytes("a.txt", []byte("a")),
				s2.NewObjectBytes("b.txt", []byte("b")),
			},
		},
		{
			caseName: "prefix",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "cc",
			limit:    10,
			want: []s2.Object{
				s2.NewObjectBytes("cc/c1.txt", []byte("c1")),
				s2.NewObjectBytes("cc/c2.txt", []byte("c2")),
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			res, err := tc.strg.List(tc.ctx, s2.ListOptions{Prefix: tc.prefix, Limit: tc.limit})
			got := res.Objects
			_ = got
			if tc.wantErr != "" {
				s.EqualError(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Require().Equal(len(tc.want), len(got))
			for i, w := range tc.want {
				g := got[i]
				func() {
					wrc, err := w.Open()
					s.Require().NoError(err)
					defer wrc.Close()
					grc, err := g.Open()
					s.Require().NoError(err)
					defer grc.Close()

					s.Equal(w.Name(), g.Name())
					s.Equal(w.Length(), g.Length())
					// A List result reports nil where the fixture holds an
					// empty map; maps.Equal treats the two as equal.
					s.Truef(maps.Equal(w.Metadata(), g.Metadata()),
						"Metadata() = %v, want %v", g.Metadata(), w.Metadata())
					wb, err := io.ReadAll(wrc)
					s.Require().NoError(err)
					gb, err := io.ReadAll(grc)
					s.Require().NoError(err)
					s.Equal(wb, gb)
				}()
			}
		})
	}
}

func (s *StorageTestSuite) TestListRecursive() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		prefix   string
		limit    int
		want     []s2.Object
		wantErr  string
	}{
		{
			caseName: "typical",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "",
			limit:    10,
			want: []s2.Object{
				s2.NewObjectBytes("a.txt", []byte("a")),
				s2.NewObjectBytes("b.txt", []byte("b")),
				s2.NewObjectBytes("cc/c1.txt", []byte("c1")),
				s2.NewObjectBytes("cc/c2.txt", []byte("c2")),
			},
		},
		{
			caseName: "prefix",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "c",
			limit:    10,
			want: []s2.Object{
				s2.NewObjectBytes("cc/c1.txt", []byte("c1")),
				s2.NewObjectBytes("cc/c2.txt", []byte("c2")),
			},
		},
		{
			caseName: "limit",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "",
			limit:    3,
			want: []s2.Object{
				s2.NewObjectBytes("a.txt", []byte("a")),
				s2.NewObjectBytes("b.txt", []byte("b")),
				s2.NewObjectBytes("cc/c1.txt", []byte("c1")),
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			res, err := tc.strg.List(tc.ctx, s2.ListOptions{Prefix: tc.prefix, Limit: tc.limit, Recursive: true})
			got := res.Objects
			if tc.wantErr != "" {
				s.EqualError(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Require().Equal(len(tc.want), len(got))
			for i, w := range tc.want {
				g := got[i]
				func() {
					wrc, err := w.Open()
					s.Require().NoError(err)
					defer wrc.Close()
					grc, err := g.Open()
					s.Require().NoError(err)
					defer grc.Close()

					s.Equal(w.Name(), g.Name())
					s.Equal(w.Length(), g.Length())
					// A List result reports nil where the fixture holds an
					// empty map; maps.Equal treats the two as equal.
					s.Truef(maps.Equal(w.Metadata(), g.Metadata()),
						"Metadata() = %v, want %v", g.Metadata(), w.Metadata())
					wantBody, err := io.ReadAll(wrc)
					s.Require().NoError(err)
					gotBody, err := io.ReadAll(grc)
					s.Require().NoError(err)
					s.Equal(wantBody, gotBody)
				}()
			}
		})
	}
}

func (s *StorageTestSuite) TestListAfter() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		prefix   string
		after    string
		limit    int
		want     []string
	}{
		{
			caseName: "typical",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "",
			after:    "a.txt",
			limit:    10,
			want:     []string{"b.txt"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			res, err := tc.strg.List(tc.ctx, s2.ListOptions{Prefix: tc.prefix, Limit: tc.limit, After: tc.after})
			got := res.Objects
			s.Require().NoError(err)
			s.Require().Equal(len(tc.want), len(got))
			for i, w := range tc.want {
				s.Equal(w, got[i].Name())
			}
		})
	}
}

func (s *StorageTestSuite) TestListStartAfter() {
	testCases := []struct {
		caseName     string
		prefix       string
		after        string
		startAfter   string
		recursive    bool
		want         []string
		wantPrefixes []string
	}{
		{
			caseName:   "start after key",
			startAfter: "a.txt",
			want:       []string{"b.txt"},
		},
		{
			caseName:   "start after a key that does not exist",
			startAfter: "a0.txt",
			want:       []string{"b.txt"},
		},
		{
			caseName:   "after wins over start after",
			after:      "a.txt",
			startAfter: "b.txt",
			want:       []string{"b.txt"},
		},
		{
			caseName:   "recursive",
			startAfter: "b.txt",
			recursive:  true,
			want:       []string{"cc/c1.txt", "cc/c2.txt"},
		},
		{
			caseName:   "with prefix",
			prefix:     "cc",
			startAfter: "cc/c1.txt",
			want:       []string{"cc/c2.txt"},
		},
		{
			caseName:     "keeps the prefix holding the key",
			startAfter:   "cc/c1.txt",
			want:         []string{},
			wantPrefixes: []string{"cc/"},
		},
		{
			caseName:     "drops a prefix sorting before the key",
			startAfter:   "d",
			want:         []string{},
			wantPrefixes: []string{},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}
			res, err := strg.List(context.Background(), s2.ListOptions{
				Prefix:     tc.prefix,
				After:      tc.after,
				StartAfter: tc.startAfter,
				Recursive:  tc.recursive,
			})
			s.Require().NoError(err)
			got := make([]string, 0, len(res.Objects))
			for _, obj := range res.Objects {
				got = append(got, obj.Name())
			}
			s.Equal(tc.want, got)
			if tc.wantPrefixes != nil {
				s.Equal(tc.wantPrefixes, res.CommonPrefixes)
			}
		})
	}
}

func (s *StorageTestSuite) TestListRecursiveAfter() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		prefix   string
		after    string
		limit    int
		want     []string
	}{
		{
			caseName: "typical",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "",
			after:    "b.txt",
			limit:    10,
			want:     []string{"cc/c1.txt", "cc/c2.txt"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			res, err := tc.strg.List(tc.ctx, s2.ListOptions{Prefix: tc.prefix, Limit: tc.limit, After: tc.after, Recursive: true})
			got := res.Objects
			s.Require().NoError(err)
			s.Require().Equal(len(tc.want), len(got))
			for i, w := range tc.want {
				s.Equal(w, got[i].Name())
			}
		})
	}
}

// TestListNextAfter verifies that List sets NextAfter when a Limit
// truncates the result, for both flat and recursive listings, and that
// following NextAfter across pages yields every object exactly once with
// no duplicates or gaps.
func (s *StorageTestSuite) TestListNextAfter() {
	testCases := []struct {
		caseName  string
		recursive bool
		want      []string
	}{
		{caseName: "flat", recursive: false, want: []string{"a.txt", "b.txt"}},
		{caseName: "recursive", recursive: true, want: []string{"a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt"}},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}
			ctx := context.Background()

			// Limit: 1 forces a new page per object, so a flat listing's
			// final page (which only has the "cc/" CommonPrefix left, no
			// more Objects) is expected to come back empty before
			// NextAfter finally goes empty too.
			var got []string
			after := ""
			pages := 0
			for {
				res, err := strg.List(ctx, s2.ListOptions{Limit: 1, After: after, Recursive: tc.recursive})
				s.Require().NoError(err)
				for _, obj := range res.Objects {
					got = append(got, obj.Name())
				}
				pages++
				s.Require().Less(pages, 10, "pagination did not terminate")
				if res.NextAfter == "" {
					break
				}
				after = res.NextAfter
			}
			s.Equal(tc.want, got)
		})
	}
}

// TestListRecursivePaginationOrder guards against fs.WalkDir's traversal
// order being mistaken for lexicographic order: WalkDir descends into a
// directory's full subtree before moving to the next sibling, so a
// directory name that sorts before a sibling file (e.g. "backup" <
// "backup-old.txt") gets its entire contents visited before that sibling,
// even though as full paths the sibling sorts first ("backup-old.txt" <
// "backup/2024-01-01.log", since '-' < '/'). Pagination that trusted
// WalkDir's raw visit order for NextAfter would permanently skip
// "backup-old.txt" once the walk reached "backup"'s contents first.
func (s *StorageTestSuite) TestListRecursivePaginationOrder() {
	fsys := memfs.New()
	_, err := fsys.WriteFile("backup-old.txt", []byte("x"), fs.ModePerm)
	s.Require().NoError(err)
	_, err = fsys.WriteFile("backup/2024-01-01.log", []byte("y"), fs.ModePerm)
	s.Require().NoError(err)

	strg := &storage{lk: newNameLocker(), fsys: fsys}
	ctx := context.Background()

	var got []string
	after := ""
	pages := 0
	for {
		res, err := strg.List(ctx, s2.ListOptions{Limit: 1, After: after, Recursive: true})
		s.Require().NoError(err)
		for _, obj := range res.Objects {
			got = append(got, obj.Name())
		}
		pages++
		s.Require().Less(pages, 10, "pagination did not terminate")
		if res.NextAfter == "" {
			break
		}
		after = res.NextAfter
	}
	s.Equal([]string{"backup-old.txt", "backup/2024-01-01.log"}, got)
}

// TestListRecursiveAfter_WalkDirError verifies that an error passed to the
// WalkDir callback (e.g. a failed ReadDir on a subdirectory) is propagated
// instead of being silently ignored. This covers the err != nil guard added
// at the top of the WalkDir callback in ListRecursiveAfter.
func (s *StorageTestSuite) TestListRecursiveAfter_WalkDirError() {
	base := s.testMemFS()
	strg := &storage{lk: newNameLocker(), fsys: &errReadDirFS{FS: base, errorPath: "cc"}}
	ctx := context.Background()

	_, err := strg.List(ctx, s2.ListOptions{Limit: 10, Recursive: true})
	s.Error(err)
	s.ErrorContains(err, "injected ReadDir error")
}

func (s *StorageTestSuite) TestGet() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		name     string
		wantErr  string
	}{
		{
			caseName: "found",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			name:     "a.txt",
		},
		{
			caseName: "not found",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			name:     "not-found.txt",
			wantErr:  "not exist: not-found.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got, err := tc.strg.Get(tc.ctx, tc.name)
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.name, got.Name())
			// The fixture writes no sidecar, so this covers Get's guarantee
			// of a writable map for an object s2 did not write.
			s.NotNil(got.Metadata())
			// NOTE: memfs might return zero time if not set correctly or supported
			// s.NotZero(got.LastModified())
		})
	}
}

func (s *StorageTestSuite) TestPut() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		obj      s2.Object
		wantBody string
	}{
		{
			caseName: "new-file",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			obj: &s2test.BytesObject{
				Name_:     "new.txt",
				Data:      []byte("new content"),
				Metadata_: s2.Metadata{"key": "val"},
			},
			wantBody: "new content",
		},
		{
			caseName: "empty-metadata-deletion",
			strg: func() s2.Storage {
				strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}
				o := &s2test.BytesObject{
					Name_:     "empty-meta.txt",
					Data:      []byte("content"),
					Metadata_: s2.Metadata{"key": "val"},
				}
				_ = strg.Put(context.Background(), o)
				return strg
			}(),
			ctx: context.Background(),
			obj: &s2test.BytesObject{
				Name_:     "empty-meta.txt",
				Data:      []byte("updated"),
				Metadata_: s2.Metadata{},
			},
			wantBody: "updated",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			err := tc.strg.Put(tc.ctx, tc.obj)
			s.Require().NoError(err)

			got, err := tc.strg.Get(tc.ctx, tc.obj.Name())
			s.Require().NoError(err)
			rc, err := got.Open()
			s.Require().NoError(err)
			defer rc.Close()

			body, _ := io.ReadAll(rc)
			s.Equal(tc.wantBody, string(body))
			if len(tc.obj.Metadata()) > 0 {
				v, _ := got.Metadata().Get("key")
				s.Equal("val", v)
			} else {
				s.Equal(0, len(got.Metadata()))
			}
		})
	}
}

func (s *StorageTestSuite) TestDelete() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		name     string
	}{
		{
			caseName: "typical",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			name:     "a.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			err := tc.strg.Delete(tc.ctx, tc.name)
			s.Require().NoError(err)

			_, err = tc.strg.Get(tc.ctx, tc.name)
			s.Error(err)
		})
	}
}

// Removing an object prunes the directories it leaves empty, with their .meta, and nothing else.
func (s *StorageTestSuite) TestDeletePrunesEmptyDirs() {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		puts     []string
		// raws are files made around the storage, such as by an older s2.
		raws     []string
		remove   func(strg s2.Storage) error
		wantLeft []string
	}{
		{
			caseName: "last object",
			puts:     []string{"a/b/c.txt"},
			remove:   func(strg s2.Storage) error { return strg.Delete(ctx, "a/b/c.txt") },
		},
		{
			caseName: "a sibling keeps the parent",
			puts:     []string{"a/x.txt", "a/b/c.txt"},
			remove:   func(strg s2.Storage) error { return strg.Delete(ctx, "a/b/c.txt") },
			wantLeft: []string{"a", "a/.meta", "a/.meta/x.txt", "a/x.txt"},
		},
		{
			caseName: "a folder marker keeps the folder",
			puts:     []string{"a/.keep", "a/b/c.txt"},
			remove:   func(strg s2.Storage) error { return strg.Delete(ctx, "a/b/c.txt") },
			wantLeft: []string{"a", "a/.keep", "a/.meta", "a/.meta/.keep"},
		},
		{
			caseName: "a .meta file is a key, not a directory to prune",
			puts:     []string{"a/b/c.txt"},
			raws:     []string{"a/.meta"},
			remove:   func(strg s2.Storage) error { return strg.Delete(ctx, "a/b/c.txt") },
			wantLeft: []string{"a", "a/.meta"},
		},
		{
			caseName: "missing key",
			raws:     []string{"a/x.txt"},
			remove:   func(strg s2.Storage) error { return strg.Delete(ctx, "a/b/c.txt") },
			wantLeft: []string{"a", "a/x.txt"},
		},
		{
			caseName: "a prefix's name is not a key", // nor a directory to remove, which only the root lock may
			puts:     []string{"a/b/c.txt"},
			remove:   func(strg s2.Storage) error { return strg.Delete(ctx, "a/b") },
			wantLeft: []string{"a", "a/b", "a/b/.meta", "a/b/.meta/c.txt", "a/b/c.txt"},
		},
		{
			caseName: "delete recursive",
			puts:     []string{"a/b/c.txt", "a/b/d/e.txt"},
			remove:   func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/b/") },
		},
		{
			caseName: "delete recursive keeps a non-empty parent",
			puts:     []string{"a/x.txt", "a/b/c.txt"},
			remove:   func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/b/") },
			wantLeft: []string{"a", "a/.meta", "a/.meta/x.txt", "a/x.txt"},
		},
		{
			caseName: "through a Sub, its root stays",
			puts:     []string{"a/b/c.txt"},
			remove: func(strg s2.Storage) error {
				sub, err := strg.Sub(ctx, "a")
				if err != nil {
					return err
				}
				return sub.Delete(ctx, "b/c.txt")
			},
			wantLeft: []string{"a"},
		},
		{
			caseName: "move",
			puts:     []string{"a/b/c.txt"},
			remove:   func(strg s2.Storage) error { return s2.Move(ctx, strg, "a/b/c.txt", "d.txt") },
			wantLeft: []string{".meta", ".meta/d.txt", "d.txt"},
		},
	}
	fsyses := []struct {
		caseName string
		newFS    func() fs.FS
	}{
		{caseName: "memfs", newFS: func() fs.FS { return memfs.New() }},
		{caseName: "osfs", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }},
	}
	for _, fc := range fsyses {
		for _, tc := range testCases {
			s.Run(fc.caseName+"/"+tc.caseName, func() {
				fsys := fc.newFS()
				strg := NewStorageFS(s2.Config{}, fsys)
				for _, name := range tc.puts {
					s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(name, []byte("x"))))
				}
				for _, name := range tc.raws {
					_, err := wfs.WriteFile(fsys, name, []byte("x"), fs.ModePerm)
					s.Require().NoError(err)
				}

				s.Require().NoError(tc.remove(strg))

				var left []string
				s.Require().NoError(fs.WalkDir(fsys, ".", func(name string, _ fs.DirEntry, err error) error {
					if name != "." {
						left = append(left, name)
					}
					return err
				}))
				s.Equal(tc.wantLeft, left)
			})
		}
	}
}

func (s *StorageTestSuite) TestDeleteRecursive() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		prefix   string
		wantErr  string
		wantLeft []string
	}{
		{
			caseName: "typical",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "c",
			wantLeft: []string{"a.txt", "b.txt"},
		},
		{
			caseName: "memfs object named like the prefix",
			strg:     &storage{lk: newNameLocker(), fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "a.txt/",
			wantLeft: []string{"a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt"},
		},
		{
			caseName: "osfs object named like the prefix",
			strg:     &storage{lk: newNameLocker(), fsys: s.testOSFS()},
			ctx:      context.Background(),
			prefix:   "a.txt/",
			wantLeft: []string{"a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			err := tc.strg.DeleteRecursive(tc.ctx, tc.prefix)
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)

			res, err := tc.strg.List(tc.ctx, s2.ListOptions{Limit: 10, Recursive: true})
			s.Require().NoError(err)
			objs := res.Objects
			s.Equal(len(tc.wantLeft), len(objs))
			for i, w := range tc.wantLeft {
				s.Equal(w, objs[i].Name())
			}
		})
	}
}

// A prefix naming an object selects no key, as on every other backend: the
// listing is empty, not an error.
func (s *StorageTestSuite) TestListWhenThePrefixNamesAnObject() {
	testCases := []struct {
		caseName string
		newFS    func() fs.FS
		typ      s2.Type
		prefix   string
	}{
		{caseName: "osfs", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }, typ: s2.TypeOSFS, prefix: "a.txt"},
		{caseName: "osfs trailing slash", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }, typ: s2.TypeOSFS, prefix: "a.txt/"},
		{caseName: "osfs below the object", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }, typ: s2.TypeOSFS, prefix: "a.txt/sub"},
		{caseName: "memfs", newFS: func() fs.FS { return memfs.New() }, typ: s2.TypeMemFS, prefix: "a.txt"},
		{caseName: "memfs trailing slash", newFS: func() fs.FS { return memfs.New() }, typ: s2.TypeMemFS, prefix: "a.txt/"},
		{caseName: "memfs below the object", newFS: func() fs.FS { return memfs.New() }, typ: s2.TypeMemFS, prefix: "a.txt/sub"},
		// NewStorageFS takes any fs.FS, and one that is not backed by a
		// syscall reports something else: fstest.MapFS says "not implemented".
		{caseName: "mapfs", newFS: func() fs.FS { return fstest.MapFS{"a.txt": {Data: []byte("a")}} }, typ: s2.TypeMemFS, prefix: "a.txt"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			fsys := tc.newFS()
			strg := NewStorageFS(s2.Config{Type: tc.typ}, fsys)
			if _, ok := fsys.(fstest.MapFS); !ok {
				s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("a.txt", []byte("a"))))
			}

			res, err := strg.List(ctx, s2.ListOptions{Prefix: tc.prefix})
			s.Require().NoError(err)
			s.Empty(res.Objects)
			s.Empty(res.CommonPrefixes)
		})
	}
}

// A root that is not a directory is a misconfiguration, not an empty bucket:
// both listing paths must still say so.
func (s *StorageTestSuite) TestListSaysSoWhenTheRootIsNotADirectory() {
	testCases := []struct {
		caseName string
		newStrg  func(dir, file string) s2.Storage
	}{
		// What a typo in S2_SERVER_ROOT produces.
		{
			caseName: "a DirFS of a file",
			newStrg: func(_, file string) s2.Storage {
				return NewStorageFS(s2.Config{Type: s2.TypeOSFS}, osfs.DirFS(file))
			},
		},
		{
			caseName: "a Sub of an object",
			newStrg: func(dir, _ string) s2.Storage {
				root := NewStorageFS(s2.Config{Type: s2.TypeOSFS}, osfs.DirFS(dir))
				sub, err := root.Sub(context.Background(), "notes")
				s.Require().NoError(err)
				return sub
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			dir := s.T().TempDir()
			file := filepath.Join(dir, "notes")
			s.Require().NoError(os.WriteFile(file, []byte("x"), 0o600))
			strg := tc.newStrg(dir, file)

			for _, recursive := range []bool{false, true} {
				_, err := strg.List(context.Background(), s2.ListOptions{Recursive: recursive})
				s.Require().Errorf(err, "recursive=%v", recursive)
			}
			// Reads stay errors too, not a missing key.
			_, err := strg.Get(context.Background(), "x")
			s.Require().Error(err)
			s.NotErrorIs(err, s2.ErrNotExist)
		})
	}
}

// An object deleted between the failed ReadDir and the walk leaves nothing to
// select either way, so the listing is empty rather than the raw walk error.
func (s *StorageTestSuite) TestListWhenTheObjectVanishesMidWalk() {
	dir := s.T().TempDir()
	base := &storage{lk: newNameLocker(), fsys: osfs.DirFS(dir), typ: s2.TypeOSFS}
	ctx := context.Background()
	s.Require().NoError(base.Put(ctx, s2.NewObjectBytes("a.txt", []byte("a"))))
	strg := NewStorageFS(s2.Config{Type: s2.TypeOSFS}, &goneStatFS{FS: osfs.DirFS(dir), gonePath: "a.txt"})

	res, err := strg.List(ctx, s2.ListOptions{Prefix: "a.txt/sub"})
	s.Require().NoError(err)
	s.Empty(res.Objects)
}

// An entry deleted after its directory was read is skipped, as a concurrent Delete prunes directories.
func (s *StorageTestSuite) TestListSkipsVanishedEntries() {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		opts     s2.ListOptions
		want     []string
	}{
		{caseName: "flat", opts: s2.ListOptions{Prefix: "a/"}, want: []string{"a/x.txt"}},
		{caseName: "recursive", opts: s2.ListOptions{Recursive: true}, want: []string{"a/b/y.txt", "a/x.txt"}},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			fsys := memfs.New()
			base := NewStorageFS(s2.Config{}, fsys)
			for _, name := range []string{"a/x.txt", "a/b/y.txt"} {
				s.Require().NoError(base.Put(ctx, s2.NewObjectBytes(name, []byte("x"))))
			}
			strg := NewStorageFS(s2.Config{}, &vanishFS{FS: fsys})

			res, err := strg.List(ctx, tc.opts)
			s.Require().NoError(err)
			var got []string
			for _, obj := range res.Objects {
				got = append(got, obj.Name())
			}
			s.Equal(tc.want, got)
		})
	}
}

// A Sub whose root a Delete through its parent pruned lists as empty, recursive as well as flat.
func (s *StorageTestSuite) TestListOfPrunedSubRoot() {
	ctx := context.Background()
	for _, fsys := range []fs.FS{memfs.New(), osfs.DirFS(s.T().TempDir())} {
		root := NewStorageFS(s2.Config{}, fsys)
		sub, err := root.Sub(ctx, "users/42/")
		s.Require().NoError(err)
		s.Require().NoError(root.Put(ctx, s2.NewObjectBytes("users/42/x.txt", []byte("x"))))
		s.Require().NoError(root.Delete(ctx, "users/42/x.txt"))
		for _, recursive := range []bool{false, true} {
			res, err := sub.List(ctx, s2.ListOptions{Recursive: recursive})
			s.NoError(err)
			s.Empty(res.Objects)
		}
	}
}

// Emptying a Sub takes its sidecars with it: they live inside it, so nothing
// of the deleted names is left for the next caller of that prefix.
func (s *StorageTestSuite) TestDeleteRecursiveDropsSidecars() {
	testCases := []struct {
		caseName string
		newFS    func() fs.FS
		typ      s2.Type
	}{
		{caseName: "osfs", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }, typ: s2.TypeOSFS},
		{caseName: "memfs", newFS: func() fs.FS { return memfs.New() }, typ: s2.TypeMemFS},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			fsys := tc.newFS()
			root := &storage{lk: newNameLocker(), fsys: fsys, typ: tc.typ}
			sub, err := root.Sub(ctx, "up")
			s.Require().NoError(err)
			s.Require().NoError(sub.Put(ctx, s2.NewObjectBytes("1", []byte("a"))))
			s.Require().NoError(sub.Put(ctx, s2.NewObjectBytes("2", []byte("b"))))
			_, err = fs.Stat(fsys, path.Join("up", metaPath("1")))
			s.Require().NoError(err)

			s.Require().NoError(sub.DeleteRecursive(ctx, ""))

			for _, name := range []string{"1", metaPath("1"), "2", metaPath("2")} {
				_, err := fs.Stat(fsys, path.Join("up", name))
				s.ErrorIsf(err, fs.ErrNotExist, "up/%s", name)
			}
		})
	}
}

// A missing root is a no-op, as Delete is on a missing name.
func (s *StorageTestSuite) TestDeleteRecursiveMissingRoot() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
	}{
		{caseName: "osfs", strg: NewStorageDir(s.T().TempDir())},
		{caseName: "memfs", strg: NewStorageMem(s2.Config{})},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			sub, err := tc.strg.Sub(ctx, "missing")
			s.Require().NoError(err)
			s.NoError(sub.DeleteRecursive(ctx, ""))
		})
	}
}

// errStatMemFS is a writable memfs whose root cannot be stat'd.
type errStatMemFS struct {
	*memfs.MemFS
}

func (e *errStatMemFS) Stat(name string) (fs.FileInfo, error) {
	if name == "." {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrPermission}
	}
	return e.MemFS.Stat(name)
}

// An unreadable root is reported, not dereferenced.
func (s *StorageTestSuite) TestDeleteRecursiveUnreadableRoot() {
	strg := &storage{lk: newNameLocker(), fsys: &errStatMemFS{memfs.New()}}

	s.ErrorIs(strg.DeleteRecursive(context.Background(), ""), fs.ErrPermission)
}

func (s *StorageTestSuite) TestSub() {
	strg := &storage{lk: newNameLocker(), fsys: s.testMemFS(), typ: s2.TypeMemFS}

	s.Run("typical", func() {
		sub, err := strg.Sub(context.Background(), "cc")
		s.Require().NoError(err)
		s.Equal(s2.TypeMemFS, sub.Type())

		res, err := sub.List(context.Background(), s2.ListOptions{Limit: 10, Recursive: true})
		s.Require().NoError(err)
		s.Len(res.Objects, 2)
	})

	// A prefix selects rather than names, so these are the whole storage and
	// the same directory; io/fs.Sub rejects both on its own.
	testCases := []struct {
		caseName string
		prefix   string
		want     int
	}{
		{caseName: "empty", prefix: "", want: 4},
		{caseName: "trailing slash", prefix: "cc/", want: 2},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			sub, err := strg.Sub(context.Background(), tc.prefix)
			s.Require().NoError(err)
			res, err := sub.List(context.Background(), s2.ListOptions{Limit: 10, Recursive: true})
			s.Require().NoError(err)
			s.Len(res.Objects, tc.want)
		})
	}

	// The metadata directory is closed to Sub as it is to object names; the
	// package reaches it through sub, which does not validate. The walk root
	// is that directory, and memfs reports its name as ".meta".
	s.Run("the metadata directory", func() {
		ctx := context.Background()
		root := &storage{lk: newNameLocker(), fsys: memfs.New(), typ: s2.TypeMemFS}
		s.Require().NoError(root.Put(ctx, s2.NewObjectBytes("a.txt", []byte("a"))))

		_, err := root.Sub(ctx, ".meta")
		s.Require().ErrorIs(err, s2.ErrInvalidName)
		_, err = root.Sub(ctx, "docs/.meta/")
		s.Require().ErrorIs(err, s2.ErrInvalidName)

		meta, err := root.sub(metaDir)
		s.Require().NoError(err)
		s.Require().NoError(meta.Put(ctx, s2.NewObjectBytes("bucket1", []byte{})))

		for _, recursive := range []bool{true, false} {
			res, err := meta.List(ctx, s2.ListOptions{Recursive: recursive})
			s.Require().NoError(err)
			s.Lenf(res.Objects, 2, "recursive=%v", recursive)
		}

		// And it deletes within itself: the walk root is that directory, so a
		// guard that matches it by name aborts the whole walk silently.
		s.Require().NoError(meta.Put(ctx, s2.NewObjectBytes("gen/b1", []byte{})))
		s.Require().NoError(meta.DeleteRecursive(ctx, "gen/"))
		res, err := meta.List(ctx, s2.ListOptions{Recursive: true})
		s.Require().NoError(err)
		s.Len(res.Objects, 2)
	})
}

func (s *StorageTestSuite) TestExists() {
	strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}

	testCases := []struct {
		caseName string
		name     string
		want     bool
	}{
		{caseName: "file exists", name: "a.txt", want: true},
		{caseName: "not found", name: "not-found.txt", want: false},
		{caseName: "directory counts as existing", name: "cc", want: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got, err := strg.Exists(context.Background(), tc.name)
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
}

// errMetaRenameMemFS is a writable memfs that fails to rename into any .meta.
type errMetaRenameMemFS struct {
	*memfs.MemFS
}

func (e *errMetaRenameMemFS) Rename(oldpath, newpath string) error {
	if slices.Contains(strings.Split(newpath, "/"), metaDir) {
		return &fs.PathError{Op: "rename", Path: newpath, Err: fs.ErrPermission}
	}
	return e.MemFS.Rename(oldpath, newpath)
}

// A failed sidecar write must not leave the previous body's sidecar behind.
func (s *StorageTestSuite) TestSidecarWriteFails() {
	testCases := []struct {
		caseName string
		name     string
		legacy   bool
		write    func(ctx context.Context, strg *storage, name string) error
	}{
		{
			caseName: "put over an object",
			name:     "a.txt",
			write: func(ctx context.Context, strg *storage, name string) error {
				return strg.Put(ctx, s2.NewObjectBytes(name, []byte("new body")))
			},
		},
		{
			caseName: "copy over an object",
			name:     "a.txt",
			write: func(ctx context.Context, strg *storage, name string) error {
				return strg.Copy(ctx, "src.txt", name)
			},
		},
		{
			caseName: "put over a nested object",
			name:     "docs/a.txt",
			write: func(ctx context.Context, strg *storage, name string) error {
				return strg.Put(ctx, s2.NewObjectBytes(name, []byte("new body")))
			},
		},
		{
			caseName: "copy over a nested object",
			name:     "docs/a.txt",
			write: func(ctx context.Context, strg *storage, name string) error {
				return strg.Copy(ctx, "src.txt", name)
			},
		},
		{
			caseName: "put over a nested object with a legacy metadata file",
			name:     "docs/a.txt",
			legacy:   true,
			write: func(ctx context.Context, strg *storage, name string) error {
				return strg.Put(ctx, s2.NewObjectBytes(name, []byte("new body")))
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			mem := memfs.New()
			seed := NewStorageFS(s2.Config{}, mem)
			s.Require().NoError(seed.Put(ctx, s2.NewObjectBytes(tc.name, []byte("old"), s2.WithContentType("text/plain"))))
			if tc.legacy {
				// Where v0.19.1 kept it, with the body's MD5, so only its removal makes the ETag synthetic.
				data, err := fs.ReadFile(mem, metaPath(tc.name))
				s.Require().NoError(err)
				_, err = mem.WriteFile(path.Join(metaDir, tc.name), data, fs.ModePerm)
				s.Require().NoError(err)
				s.Require().NoError(mem.RemoveFile(metaPath(tc.name)))
			}
			s.Require().NoError(seed.Put(ctx, s2.NewObjectBytes("src.txt", []byte("new body"), s2.WithContentType("text/csv"))))
			strg := &storage{lk: newNameLocker(), fsys: &errMetaRenameMemFS{mem}}

			s.ErrorIs(tc.write(ctx, strg, tc.name), fs.ErrPermission)

			got, err := strg.Get(ctx, tc.name)
			s.Require().NoError(err)
			s.Empty(got.ContentType())
			s.Contains(got.ETag(), "-", "synthetic ETag, not the old body's MD5")
		})
	}
}

func (s *StorageTestSuite) TestPutMetadata() {
	strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}
	ctx := context.Background()

	s.Run("typical", func() {
		err := strg.PutMetadata(ctx, "a.txt", s2.Metadata{"key": "val"})
		s.Require().NoError(err)

		obj, err := strg.Get(ctx, "a.txt")
		s.Require().NoError(err)
		v, ok := obj.Metadata().Get("key")
		s.True(ok)
		s.Equal("val", v)
	})

	s.Run("not found", func() {
		err := strg.PutMetadata(ctx, "not-found.txt", s2.Metadata{"key": "val"})
		s.Error(err)
	})
}

func (s *StorageTestSuite) TestCopy() {
	strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}
	ctx := context.Background()

	s.Run("typical", func() {
		err := strg.Copy(ctx, "a.txt", "a-copy.txt")
		s.Require().NoError(err)

		got, err := strg.Get(ctx, "a-copy.txt")
		s.Require().NoError(err)
		rc, err := got.Open()
		s.Require().NoError(err)
		defer rc.Close()
		body, _ := io.ReadAll(rc)
		s.Equal("a", string(body))
	})

	s.Run("not found", func() {
		err := strg.Copy(ctx, "not-found.txt", "dst.txt")
		s.Error(err)
	})
}

func (s *StorageTestSuite) TestMove() {
	strg := &storage{lk: newNameLocker(), fsys: s.testMemFS()}
	ctx := context.Background()

	err := strg.Move(ctx, "a.txt", "moved.txt")
	s.Require().NoError(err)

	// Source is gone
	_, err = strg.Get(ctx, "a.txt")
	s.Error(err)

	// Destination exists
	got, err := strg.Get(ctx, "moved.txt")
	s.Require().NoError(err)
	rc, err := got.Open()
	s.Require().NoError(err)
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	s.Equal("a", string(body))
}

func (s *StorageTestSuite) TestMoveNestedDst() {
	newOSFS := func() fs.FS { return osfs.New(s.T().TempDir()) }
	newMemFS := func() fs.FS { return memfs.New() }
	testCases := []struct {
		caseName string
		fsys     func() fs.FS
		withMeta bool
		staleDst bool
	}{
		{caseName: "osfs with sidecar", fsys: newOSFS, withMeta: true},
		{caseName: "osfs without sidecar", fsys: newOSFS},
		{caseName: "osfs without sidecar over stale dst", fsys: newOSFS, staleDst: true},
		{caseName: "osfs with sidecar over stale dst", fsys: newOSFS, withMeta: true, staleDst: true},
		{caseName: "memfs with sidecar", fsys: newMemFS, withMeta: true},
		{caseName: "memfs without sidecar", fsys: newMemFS},
		{caseName: "memfs without sidecar over stale dst", fsys: newMemFS, staleDst: true},
		{caseName: "memfs with sidecar over stale dst", fsys: newMemFS, withMeta: true, staleDst: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			fsys := tc.fsys()
			strg := &storage{lk: newNameLocker(), fsys: fsys}
			ctx := context.Background()
			src, dst := "a.txt", "dir/sub/a.txt"
			if tc.staleDst {
				s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(dst, []byte("old"), s2.WithContentType("application/octet-stream"))))
			}
			if tc.withMeta {
				s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(src, []byte("a"), s2.WithContentType("text/plain"))))
			} else {
				_, err := wfs.WriteFile(fsys, src, []byte("a"), fs.ModePerm)
				s.Require().NoError(err)
			}

			s.Require().NoError(strg.Move(ctx, src, dst))

			got, err := strg.Get(ctx, dst)
			s.Require().NoError(err)
			rc, err := got.Open()
			s.Require().NoError(err)
			body, err := io.ReadAll(rc)
			_ = rc.Close()
			s.Require().NoError(err)
			s.Equal("a", string(body))
			if tc.withMeta {
				s.Equal("text/plain", got.ContentType())
			} else {
				// A stale dst sidecar must not survive a move from a source without one.
				s.Empty(got.ContentType())
				_, err = fs.Stat(fsys, metaPath(dst))
				s.ErrorIs(err, fs.ErrNotExist)
			}
			ok, err := strg.Exists(ctx, src)
			s.Require().NoError(err)
			s.False(ok)
			_, err = fs.Stat(fsys, metaPath(src))
			s.ErrorIs(err, fs.ErrNotExist)
		})
	}
}

// renameOnlyFS renames but cannot create directories.
type renameOnlyFS struct {
	fs.FS
	mem *memfs.MemFS
}

func (r *renameOnlyFS) Rename(oldpath, newpath string) error {
	return r.mem.Rename(oldpath, newpath)
}

// Moving into an existing directory must not need WriteFileFS.
func (s *StorageTestSuite) TestMoveRenameOnlyFS() {
	mem := memfs.New()
	ctx := context.Background()
	memStrg := &storage{lk: newNameLocker(), fsys: mem}
	s.Require().NoError(memStrg.Put(ctx, s2.NewObjectBytes("a.txt", []byte("a"), s2.WithContentType("text/plain"))))
	s.Require().NoError(memStrg.Put(ctx, s2.NewObjectBytes("dir/b.txt", []byte("b"), s2.WithContentType("text/plain"))))
	strg := &storage{lk: newNameLocker(), fsys: &renameOnlyFS{FS: mem, mem: mem}}

	s.Require().NoError(strg.Move(ctx, "a.txt", "dir/a.txt"))

	got, err := strg.Get(ctx, "dir/a.txt")
	s.Require().NoError(err)
	s.Equal("text/plain", got.ContentType())
}

// A parent that cannot be created must fail the Move before anything is renamed.
func (s *StorageTestSuite) TestMoveBlockedParentLeavesSrc() {
	mem := memfs.New()
	strg := &storage{lk: newNameLocker(), fsys: mem}
	ctx := context.Background()
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("a.txt", []byte("a"), s2.WithContentType("text/plain"))))
	// A file where the sidecar's parent directory must go.
	_, err := mem.WriteFile(path.Dir(metaPath("new/a.txt")), []byte("{}"), fs.ModePerm)
	s.Require().NoError(err)

	s.Require().Error(strg.Move(ctx, "a.txt", "new/a.txt"))

	got, err := strg.Get(ctx, "a.txt")
	s.Require().NoError(err)
	s.Equal("text/plain", got.ContentType())
	rc, err := got.Open()
	s.Require().NoError(err)
	defer rc.Close()

	body, err := io.ReadAll(rc)
	s.Require().NoError(err)
	s.Equal("a", string(body))
}

// A moved file without a sidecar must not keep the destination's old one.
func (s *StorageTestSuite) TestMoveDropsStaleSidecar() {
	fsys := memfs.New()
	strg := &storage{lk: newNameLocker(), fsys: fsys}
	ctx := context.Background()
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("dst.txt", []byte("old"), s2.WithContentType("text/plain"))))
	_, err := fsys.WriteFile("src.txt", []byte("new"), fs.ModePerm)
	s.Require().NoError(err)

	s.Require().NoError(strg.Move(ctx, "src.txt", "dst.txt"))

	got, err := strg.Get(ctx, "dst.txt")
	s.Require().NoError(err)
	s.Empty(got.ContentType())
	s.Contains(got.ETag(), "-")
	_, err = fs.Stat(fsys, metaPath("dst.txt"))
	s.ErrorIs(err, fs.ErrNotExist)
}

func (s *StorageTestSuite) TestSignedURL() {
	testCases := []struct {
		caseName string
		strg     s2.Storage
		ctx      context.Context
		name     string
		ttl      time.Duration
		want     string
		wantErr  string
	}{
		{
			caseName: "typical",
			strg: &storage{
				lk:   newNameLocker(),
				cfg:  s2.Config{},
				fsys: s.testMemFS(),
			},
			ctx:  context.Background(),
			name: "a.txt",
			ttl:  time.Hour,
			want: "a.txt",
		},
		{
			caseName: "with signed url",
			strg: &storage{
				lk:   newNameLocker(),
				cfg:  s2.Config{SignedURL: "http://localhost"},
				fsys: s.testMemFS(),
			},
			ctx:  context.Background(),
			name: "a.txt",
			ttl:  time.Hour,
			want: "http://localhost/a.txt",
		},
		{
			caseName: "not found",
			strg: &storage{
				lk:   newNameLocker(),
				cfg:  s2.Config{SignedURL: "http://localhost"},
				fsys: s.testMemFS(),
			},
			ctx:     context.Background(),
			name:    "not-found.txt",
			ttl:     time.Hour,
			wantErr: "not exist: not-found.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got, err := tc.strg.SignedURL(tc.ctx, s2.SignedURLOptions{Name: tc.name, TTL: tc.ttl})
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
}

// --- Benchmarks ---
//
// Storage-layer benchmarks against the osfs and memfs backends with a
// 1 KiB payload. PUT rotates the key across a small alphabet so each
// iteration writes to a distinct path (stressing directory creation
// and rename); GET repeatedly reads a single pre-populated key
// (stressing open/stat/read). Note that s2's atomicWrite always fsyncs
// the payload before rename, so PUT here pays the durability cost on
// every iteration — when comparing against benchmarks from other S3
// implementations, verify that they are also running with fsync on
// before drawing conclusions.

// newBenchStorage creates an empty Storage for the given backend type.
// osfs is rooted at b.TempDir() so iteration state is isolated from
// other tests/benches in the package; memfs is a pristine in-memory
// filesystem. Used to share the bench bodies between osfs and memfs
// variants.
func newBenchStorage(b *testing.B, typ s2.Type) s2.Storage {
	b.Helper()
	cfg := s2.Config{Type: typ}
	if typ == s2.TypeOSFS {
		cfg.Root = b.TempDir()
	}
	strg, err := s2.NewStorage(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	return strg
}

func benchPutObject(b *testing.B, typ s2.Type) {
	ctx := context.Background()
	strg := newBenchStorage(b, typ)
	content := bytes.Repeat([]byte("a"), 1024) // 1 KiB

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := path.Join("test", string(rune(i%26+97)), "file.txt")
		if err := strg.Put(ctx, s2.NewObjectBytes(key, content)); err != nil {
			b.Fatal(err)
		}
	}
}

func benchGetObject(b *testing.B, typ s2.Type) {
	ctx := context.Background()
	strg := newBenchStorage(b, typ)
	content := bytes.Repeat([]byte("a"), 1024) // 1 KiB
	if err := strg.Put(ctx, s2.NewObjectBytes("test.txt", content)); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		obj, err := strg.Get(ctx, "test.txt")
		if err != nil {
			b.Fatal(err)
		}
		rc, err := obj.Open()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			b.Fatal(err)
		}
		_ = rc.Close()
	}
}

func benchList(b *testing.B, typ s2.Type, readETag bool) {
	ctx := context.Background()
	strg := newBenchStorage(b, typ)
	for i := range 1000 {
		if err := strg.Put(ctx, s2.NewObjectBytes(fmt.Sprintf("obj-%04d.txt", i), []byte("a"))); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := strg.List(ctx, s2.ListOptions{Limit: 1000})
		if err != nil {
			b.Fatal(err)
		}
		if readETag {
			for _, obj := range res.Objects {
				_ = obj.ETag()
			}
		}
	}
}

// BenchmarkList lists a 1000-object page; the ETag variants add the lazy sidecar reads.
func BenchmarkList(b *testing.B)          { benchList(b, s2.TypeOSFS, false) }
func BenchmarkListETag(b *testing.B)      { benchList(b, s2.TypeOSFS, true) }
func BenchmarkListMemFS(b *testing.B)     { benchList(b, s2.TypeMemFS, false) }
func BenchmarkListETagMemFS(b *testing.B) { benchList(b, s2.TypeMemFS, true) }

// BenchmarkPutObject covers the osfs backend with a 1 KiB payload and
// rotating-key writes. s2's atomicWrite always fsyncs before rename;
// see BenchmarkPutObjectMemFS for numbers that exclude the durability
// cost.
func BenchmarkPutObject(b *testing.B)      { benchPutObject(b, s2.TypeOSFS) }
func BenchmarkGetObject(b *testing.B)      { benchGetObject(b, s2.TypeOSFS) }
func BenchmarkPutObjectMemFS(b *testing.B) { benchPutObject(b, s2.TypeMemFS) }
func BenchmarkGetObjectMemFS(b *testing.B) { benchGetObject(b, s2.TypeMemFS) }

func (s *StorageTestSuite) TestListCursorDoesNotReachSidecars() {
	ctx := context.Background()
	strg := &storage{lk: newNameLocker(), fsys: memfs.New(), typ: s2.TypeMemFS}
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("private/secret.txt", []byte("x"),
		s2.WithContentType("text/plain"))))

	// A cursor that sorts inside ".meta" must not let the walk descend into
	// it: the sidecars would be reported as objects, and opening one returns
	// the metadata of an object the caller never listed.
	testCases := []struct {
		caseName string
		opts     s2.ListOptions
	}{
		{caseName: "no cursor", opts: s2.ListOptions{Recursive: true}},
		{caseName: "the dir itself", opts: s2.ListOptions{Recursive: true, After: ".meta"}},
		{caseName: "just past the dir", opts: s2.ListOptions{Recursive: true, After: ".meta!"}},
		{caseName: "start after", opts: s2.ListOptions{Recursive: true, StartAfter: ".meta!"}},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			res, err := strg.List(ctx, tc.opts)
			s.Require().NoError(err)
			for _, obj := range res.Objects {
				s.NotContains(obj.Name(), ".meta/")
			}
		})
	}
}

func (s *StorageTestSuite) TestMetaDirIsNotAnObjectName() {
	ctx := context.Background()
	strg := &storage{lk: newNameLocker(), fsys: memfs.New(), typ: s2.TypeMemFS}
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("a.txt", []byte("a"),
		s2.WithContentType("text/plain"), s2.WithMetadata(s2.Metadata{"k": "v"}))))

	// Every name below is the sidecar of a.txt, not an object of its own.
	testCases := []struct {
		caseName string
		call     func(name string) error
	}{
		{"get", func(name string) error { _, err := strg.Get(ctx, name); return err }},
		{"exists", func(name string) error { _, err := strg.Exists(ctx, name); return err }},
		{"put", func(name string) error {
			return strg.Put(ctx, s2.NewObjectBytes(name, []byte(`{"content_type":"evil/x"}`)))
		}},
		{"put metadata", func(name string) error { return strg.PutMetadata(ctx, name, s2.Metadata{"k": "w"}) }},
		{"copy", func(name string) error { return strg.Copy(ctx, name, "copy.txt") }},
		{"move", func(name string) error { return strg.Move(ctx, name, "moved.txt") }},
		{"delete", func(name string) error { return strg.Delete(ctx, name) }},
		{"delete recursive", func(name string) error { return strg.DeleteRecursive(ctx, name+"/") }},
		{"list", func(name string) error { _, err := strg.List(ctx, s2.ListOptions{Prefix: name}); return err }},
		{"signed url", func(name string) error {
			_, err := strg.SignedURL(ctx, s2.SignedURLOptions{Name: name})
			return err
		}},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			// Nested too: every directory keeps its metadata files in one, and
			// neither listing would ever show the name.
			for _, name := range []string{".meta/a.txt", "docs/.meta/a.txt", "a/.meta"} {
				s.ErrorIsf(tc.call(name), s2.ErrInvalidName, "name %q", name)
			}
		})
	}

	// ".met" is a prefix of ".meta" without naming it.
	s.Run("a prefix that merely starts the meta dir", func() {
		s.Require().NoError(strg.DeleteRecursive(ctx, ".met"))
	})

	// The same one level down: a Sub keeps its sidecars beside the names it
	// scopes, so every bucket in a server root has one.
	s.Run("a prefix that merely starts a nested meta dir", func() {
		root := &storage{lk: newNameLocker(), fsys: memfs.New(), typ: s2.TypeMemFS}
		sub, err := root.Sub(ctx, "photos")
		s.Require().NoError(err)
		s.Require().NoError(sub.Put(ctx, s2.NewObjectBytes("a.txt", []byte("body"),
			s2.WithContentType("text/plain"), s2.WithMetadata(s2.Metadata{"k": "v"}))))

		s.Require().NoError(root.DeleteRecursive(ctx, "photos/.met"))

		got, err := sub.Get(ctx, "a.txt")
		s.Require().NoError(err)
		s.Equal("text/plain", got.ContentType())
		s.Equal(s2.Metadata{"k": "v"}, got.Metadata())

		// Clearing the directory that holds it still takes it along.
		s.Require().NoError(root.DeleteRecursive(ctx, "photos/"))
		res, err := root.List(ctx, s2.ListOptions{Recursive: true})
		s.Require().NoError(err)
		s.Empty(res.Objects)
	})

	// A regular file named ".meta" is not the sidecar directory. Skipping it
	// as one would skip the rest of the directory holding it, and the walk
	// must delete it rather than refuse the name it just read.
	s.Run("a regular file named like the meta dir", func() {
		fsys := memfs.New()
		for _, name := range []string{".meta", "sub/kept.txt"} {
			_, err := fsys.WriteFile(name, []byte("x"), fs.ModePerm)
			s.Require().NoError(err)
		}
		strg := &storage{lk: newNameLocker(), fsys: fsys, typ: s2.TypeMemFS}

		// It is not an object either, so no listing offers a name that Get
		// would then refuse.
		for _, opts := range []s2.ListOptions{{Recursive: true}, {}} {
			res, err := strg.List(ctx, opts)
			s.Require().NoError(err)
			for _, obj := range res.Objects {
				s.NotEqual(".meta", obj.Name())
			}
		}

		s.Require().NoError(strg.DeleteRecursive(ctx, "sub/"))
		_, err := fs.Stat(fsys, "sub/kept.txt")
		s.Require().ErrorIs(err, fs.ErrNotExist)

		s.Require().NoError(strg.DeleteRecursive(ctx, ""))
		_, err = fs.Stat(fsys, ".meta")
		s.Require().ErrorIs(err, fs.ErrNotExist)
	})

	obj, err := strg.Get(ctx, "a.txt")
	s.Require().NoError(err)
	s.Equal("text/plain", obj.ContentType())
	s.Equal(s2.Metadata{"k": "v"}, obj.Metadata())
}

// Every view of a directory finds the same metadata file, whichever one wrote it (#291).
func (s *StorageTestSuite) TestMetaSharedAcrossViews() {
	ctx := context.Background()
	testCases := []struct {
		caseName   string
		writerPath string
		writerName string
		readerPath string
		readerName string
	}{
		{caseName: "sub writes, root reads", writerPath: "photos", writerName: "a.txt", readerPath: "", readerName: "photos/a.txt"},
		{caseName: "root writes, sub reads", writerPath: "", writerName: "photos/a.txt", readerPath: "photos", readerName: "a.txt"},
		{caseName: "nested sub writes, sub reads", writerPath: "b/photos", writerName: "a.txt", readerPath: "b", readerName: "photos/a.txt"},
		{caseName: "sub writes, nested sub reads", writerPath: "b", writerName: "photos/a.txt", readerPath: "b/photos", readerName: "a.txt"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			root := NewStorageFS(s2.Config{}, memfs.New())
			view := func(p string) s2.Storage {
				if p == "" {
					return root
				}
				sub, err := root.Sub(ctx, p)
				s.Require().NoError(err)
				return sub
			}
			s.Require().NoError(view(tc.writerPath).Put(ctx, s2.NewObjectBytes(tc.writerName, []byte("body"),
				s2.WithContentType("text/plain"), s2.WithMetadata(s2.Metadata{"k": "v"}))))
			want, err := view(tc.writerPath).Get(ctx, tc.writerName)
			s.Require().NoError(err)

			got, err := view(tc.readerPath).Get(ctx, tc.readerName)
			s.Require().NoError(err)
			s.Equal("text/plain", got.ContentType())
			s.Equal(s2.Metadata{"k": "v"}, got.Metadata())
			s.Equal(want.ETag(), got.ETag())
			s.NotContains(got.ETag(), "-", "the stored MD5, not the synthetic form")
		})
	}
}

// A metadata file at the pre-v0.20.0 location is read, and the next write moves it without leaving a directory behind.
func (s *StorageTestSuite) TestMetaLegacyLocation() {
	ctx := context.Background()
	const legacyJSON = `{"etag":"\"legacy\"","content_type":"text/plain","metadata":{"k":"v"}}`
	testCases := []struct {
		caseName string
		write    func(strg s2.Storage) error
		// name is where the object lives afterwards; empty when it is gone.
		name      string
		wantCT    string
		wantMeta  s2.Metadata
		wantETagF func(etag string) bool
		// keepsLegacy is set when nothing is written, so the legacy metadata file stays.
		keepsLegacy bool
	}{
		{
			caseName:    "read only",
			write:       func(s2.Storage) error { return nil },
			name:        "photos/a.txt",
			wantCT:      "text/plain",
			wantMeta:    s2.Metadata{"k": "v"},
			wantETagF:   func(etag string) bool { return etag == `"legacy"` },
			keepsLegacy: true,
		},
		{
			caseName: "put metadata",
			write: func(strg s2.Storage) error {
				return strg.PutMetadata(ctx, "photos/a.txt", s2.Metadata{"k": "w"})
			},
			name:      "photos/a.txt",
			wantCT:    "text/plain",
			wantMeta:  s2.Metadata{"k": "w"},
			wantETagF: func(etag string) bool { return etag == `"legacy"` },
		},
		{
			caseName: "put",
			write: func(strg s2.Storage) error {
				return strg.Put(ctx, s2.NewObjectBytes("photos/a.txt", []byte("new"), s2.WithContentType("text/csv")))
			},
			name:      "photos/a.txt",
			wantCT:    "text/csv",
			wantMeta:  s2.Metadata{},
			wantETagF: func(etag string) bool { return etag != `"legacy"` && !strings.Contains(etag, "-") },
		},
		{
			caseName: "move",
			write: func(strg s2.Storage) error {
				return s2.Move(ctx, strg, "photos/a.txt", "docs/b.txt")
			},
			name:      "docs/b.txt",
			wantCT:    "text/plain",
			wantMeta:  s2.Metadata{"k": "v"},
			wantETagF: func(etag string) bool { return etag == `"legacy"` },
		},
		{
			caseName: "delete",
			write: func(strg s2.Storage) error {
				return strg.Delete(ctx, "photos/a.txt")
			},
		},
		{
			caseName: "delete recursive",
			write: func(strg s2.Storage) error {
				return strg.DeleteRecursive(ctx, "photos/")
			},
		},
	}
	fsyses := []struct {
		caseName string
		newFS    func() fs.FS
	}{
		{caseName: "memfs", newFS: func() fs.FS { return memfs.New() }},
		{caseName: "osfs", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }},
	}
	for _, fc := range fsyses {
		for _, tc := range testCases {
			s.Run(fc.caseName+"/"+tc.caseName, func() {
				fsys := fc.newFS()
				for name, body := range map[string]string{"photos/a.txt": "body", ".meta/photos/a.txt": legacyJSON} {
					_, err := wfs.WriteFile(fsys, name, []byte(body), fs.ModePerm)
					s.Require().NoError(err)
				}
				strg := NewStorageFS(s2.Config{}, fsys)

				s.Require().NoError(tc.write(strg))

				if tc.name != "" {
					got, err := strg.Get(ctx, tc.name)
					s.Require().NoError(err)
					s.Equal(tc.wantCT, got.ContentType())
					s.Equal(tc.wantMeta, got.Metadata())
					s.True(tc.wantETagF(got.ETag()), "etag %s", got.ETag())
				}
				if tc.keepsLegacy {
					return
				}
				_, err := fs.Stat(fsys, ".meta/photos/a.txt")
				s.ErrorIs(err, fs.ErrNotExist, "the legacy metadata file is gone")
				entries, err := fs.ReadDir(fsys, ".meta/photos")
				s.NoError(err, ".meta/photos stays for Lock(\"photos\") to remove")
				s.Empty(entries)
			})
		}
	}
}

// Deleting a directory takes its metadata files along.
func (s *StorageTestSuite) TestMetaDeleteRecursiveLeavesNothing() {
	ctx := context.Background()
	fsys := memfs.New()
	strg := NewStorageFS(s2.Config{}, fsys)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("photos/a.txt", []byte("body"), s2.WithContentType("text/plain"))))

	s.Require().NoError(strg.DeleteRecursive(ctx, "photos/"))

	var left []string
	s.Require().NoError(fs.WalkDir(fsys, ".", func(name string, _ fs.DirEntry, err error) error {
		if name != "." {
			left = append(left, name)
		}
		return err
	}))
	s.Empty(left)
}

// The review scenarios for #291: a file named .meta beside objects, and legacy metadata files another view wrote, which need MigrateMeta.
func (s *StorageTestSuite) TestMetaLegacyEdgeCases() {
	ctx := context.Background()
	// wantBlocked accepts only ErrMetaBlocked, worded without the host path.
	wantBlocked := func(err error) error {
		if !errors.Is(err, ErrMetaBlocked) {
			return fmt.Errorf("want ErrMetaBlocked, got %v", err)
		}
		if strings.Contains(err.Error(), filepath.Clean(os.TempDir())) {
			return fmt.Errorf("host path in %q", err)
		}
		return nil
	}
	const legacyJSON = `{"etag":"\"legacy\"","content_type":"text/plain","metadata":{"k":"v"}}`
	testCases := []struct {
		caseName string
		seed     map[string]string
		act      func(root s2.Storage) error
		check    func(fsys fs.FS, root s2.Storage)
	}{
		{
			caseName: "a file named .meta does not hide its neighbours",
			seed:     map[string]string{"photos/.meta": "x", "photos/a.txt": "body", ".meta/photos/a.txt": legacyJSON},
			act:      func(s2.Storage) error { return nil },
			check: func(_ fs.FS, root s2.Storage) {
				got, err := root.Get(ctx, "photos/a.txt")
				s.Require().NoError(err)
				s.Equal("text/plain", got.ContentType())
			},
		},
		{
			caseName: "a file named .meta and no legacy metadata file",
			seed:     map[string]string{"photos/.meta": "x", "photos/a.txt": "body"},
			act:      func(s2.Storage) error { return nil },
			check: func(_ fs.FS, root s2.Storage) {
				got, err := root.Get(ctx, "photos/a.txt")
				s.Require().NoError(err)
				s.Empty(got.ContentType())
			},
		},
		{
			caseName: "a directory where the new metadata file goes",
			seed:     map[string]string{"X/.meta/sub/k": legacyJSON, "X/sub": "body", ".meta/X/sub": legacyJSON},
			act:      func(s2.Storage) error { return nil },
			check: func(_ fs.FS, root s2.Storage) {
				got, err := root.Get(ctx, "X/sub")
				s.Require().NoError(err)
				s.Equal("text/plain", got.ContentType(), "read from the legacy location")
			},
		},
		{
			caseName: "a file at the new location that is not a metadata file names itself in the error",
			seed:     map[string]string{"img/logo.png": "png", "img/.meta/logo.png": "\x89PNG", ".meta/img/logo.png": legacyJSON},
			act:      func(s2.Storage) error { return nil },
			check: func(_ fs.FS, root s2.Storage) {
				_, err := root.Get(ctx, "img/logo.png")
				s.Require().Error(err)
				s.Contains(err.Error(), `failed to decode metadata file "img/.meta/logo.png"`)
				s.NotContains(err.Error(), filepath.Clean(os.TempDir()))
			},
		},
		{
			caseName: "moving onto a name whose metadata location is a full directory",
			seed:     map[string]string{"src.txt": "body", ".meta/dst/x": "kept"},
			act:      func(root s2.Storage) error { return s2.Move(ctx, root, "src.txt", "dst") },
			check: func(fsys fs.FS, root s2.Storage) {
				_, err := root.Get(ctx, "dst")
				s.Require().NoError(err)
				_, err = fs.Stat(fsys, "src.txt")
				s.ErrorIs(err, fs.ErrNotExist)
			},
		},
		{
			caseName: "put metadata over an empty leftover directory",
			seed:     map[string]string{"name": "body", ".meta/name/": ""},
			act:      func(root s2.Storage) error { return root.PutMetadata(ctx, "name", s2.Metadata{"k": "v"}) },
			check: func(_ fs.FS, root s2.Storage) {
				got, err := root.Get(ctx, "name")
				s.Require().NoError(err)
				s.Equal(s2.Metadata{"k": "v"}, got.Metadata())
			},
		},
		{
			caseName: "a top-level put over empty directories a v0.19 delete left",
			seed:     map[string]string{".meta/sub/deeper/": ""},
			act: func(root s2.Storage) error {
				return root.Put(ctx, s2.NewObjectBytes("sub", []byte("file"), s2.WithContentType("text/csv")))
			},
			check: func(_ fs.FS, root s2.Storage) {
				got, err := root.Get(ctx, "sub")
				s.Require().NoError(err)
				s.Equal("text/csv", got.ContentType())
			},
		},
		{
			caseName: "moving into a folder holding a file named .meta",
			seed:     map[string]string{"a.txt": "body", "photos/.meta": "key"},
			act:      func(root s2.Storage) error { return s2.Move(ctx, root, "a.txt", "photos/b.txt") },
			check: func(_ fs.FS, root s2.Storage) {
				_, err := root.Get(ctx, "photos/b.txt")
				s.Require().NoError(err)
			},
		},
		{
			caseName: "a put into a folder holding a file named .meta changes nothing",
			seed:     map[string]string{"photos/a.txt": "old", "photos/.meta": "key", ".meta/photos/a.txt": legacyJSON},
			act: func(root s2.Storage) error {
				return wantBlocked(root.Put(ctx, s2.NewObjectBytes("photos/a.txt", []byte("new"))))
			},
			check: func(fsys fs.FS, root s2.Storage) {
				body, err := fs.ReadFile(fsys, "photos/a.txt")
				s.Require().NoError(err)
				s.Equal("old", string(body))
				got, err := root.Get(ctx, "photos/a.txt")
				s.Require().NoError(err)
				s.Equal("text/plain", got.ContentType())
			},
		},
		{
			caseName: "a copy into a folder holding a file named .meta changes nothing",
			seed:     map[string]string{"src.txt": "new", "photos/a.txt": "old", "photos/.meta": "key", ".meta/photos/a.txt": legacyJSON},
			act: func(root s2.Storage) error {
				return wantBlocked(root.Copy(ctx, "src.txt", "photos/a.txt"))
			},
			check: func(fsys fs.FS, root s2.Storage) {
				body, err := fs.ReadFile(fsys, "photos/a.txt")
				s.Require().NoError(err)
				s.Equal("old", string(body))
				got, err := root.Get(ctx, "photos/a.txt")
				s.Require().NoError(err)
				s.Equal("text/plain", got.ContentType())
			},
		},
		{
			caseName: "moving an object with a metadata file into a folder holding a file named .meta changes nothing",
			seed:     map[string]string{"a.txt": "body", ".meta/a.txt": legacyJSON, "photos/.meta": "key"},
			act:      func(root s2.Storage) error { return wantBlocked(s2.Move(ctx, root, "a.txt", "photos/b.txt")) },
			check: func(fsys fs.FS, root s2.Storage) {
				got, err := root.Get(ctx, "a.txt")
				s.Require().NoError(err)
				s.Equal("text/plain", got.ContentType())
				_, err = fs.Stat(fsys, "photos/b.txt")
				s.ErrorIs(err, fs.ErrNotExist)
			},
		},
		{
			caseName: "a top-level put named like a kept legacy directory changes nothing",
			seed:     map[string]string{".meta/photos/old.txt": legacyJSON},
			act: func(root s2.Storage) error {
				return wantBlocked(root.Put(ctx, s2.NewObjectBytes("photos", []byte("new"))))
			},
			check: func(fsys fs.FS, _ s2.Storage) {
				_, err := fs.Stat(fsys, "photos")
				s.ErrorIs(err, fs.ErrNotExist, "no body written")
			},
		},
		{
			caseName: "moving onto an object whose metadata file a sub wrote",
			seed:     map[string]string{"b/x.txt": "new", "b/photos/a.txt": "old", "b/.meta/photos/a.txt": legacyJSON},
			act: func(root s2.Storage) error {
				if _, err := MigrateMeta(ctx, root); err != nil {
					return err
				}
				return s2.Move(ctx, root, "b/x.txt", "b/photos/a.txt")
			},
			check: func(fsys fs.FS, root s2.Storage) {
				sub, err := root.Sub(ctx, "b")
				s.Require().NoError(err)
				got, err := sub.Get(ctx, "photos/a.txt")
				s.Require().NoError(err)
				s.Empty(got.ContentType(), "the moved body has no metadata file")
				s.NotEqual(`"legacy"`, got.ETag())
				_, err = fs.Stat(fsys, "b/.meta/photos")
				s.ErrorIs(err, fs.ErrNotExist)
			},
		},
		{
			caseName: "a folder deleted through the root, then an object of its name",
			seed:     map[string]string{"X/sub/k": "body", "X/.meta/sub/k": legacyJSON},
			act: func(root s2.Storage) error {
				if _, err := MigrateMeta(ctx, root); err != nil {
					return err
				}
				if err := root.DeleteRecursive(ctx, "X/sub/"); err != nil {
					return err
				}
				return root.Put(ctx, s2.NewObjectBytes("X/sub", []byte("file"), s2.WithContentType("text/csv")))
			},
			check: func(_ fs.FS, root s2.Storage) {
				got, err := root.Get(ctx, "X/sub")
				s.Require().NoError(err)
				s.Equal("text/csv", got.ContentType())
			},
		},
	}
	fsyses := []struct {
		caseName string
		newFS    func() fs.FS
	}{
		{caseName: "memfs", newFS: func() fs.FS { return memfs.New() }},
		{caseName: "osfs", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }},
	}
	for _, fc := range fsyses {
		for _, tc := range testCases {
			s.Run(fc.caseName+"/"+tc.caseName, func() {
				fsys := fc.newFS()
				// A name ending in "/" is a directory to create.
				for name, body := range tc.seed {
					if dir, ok := strings.CutSuffix(name, "/"); ok {
						s.Require().NoError(wfs.MkdirAll(fsys, dir, fs.ModePerm))
						continue
					}
					_, err := wfs.WriteFile(fsys, name, []byte(body), fs.ModePerm)
					s.Require().NoError(err)
				}
				root := NewStorageFS(s2.Config{}, fsys)

				s.Require().NoError(tc.act(root))
				tc.check(fsys, root)
			})
		}
	}
}

// A path the tree cannot hold reads as no key and is refused as a write, worded without the host path.
func (s *StorageTestSuite) TestTreeConstraints() {
	ctx := context.Background()
	exists := func(strg s2.Storage, name string) error {
		ok, err := strg.Exists(ctx, name)
		if err == nil && ok {
			return errors.New("exists")
		}
		return err
	}
	testCases := []struct {
		caseName string
		act      func(strg s2.Storage) error
		wantErr  error
	}{
		{caseName: "get below an object", act: func(strg s2.Storage) error { _, err := strg.Get(ctx, "a.txt/sub"); return err }, wantErr: s2.ErrNotExist},
		{caseName: "get deeper below an object", act: func(strg s2.Storage) error { _, err := strg.Get(ctx, "dir/x.txt/sub/y"); return err }, wantErr: s2.ErrNotExist},
		{caseName: "exists below an object", act: func(strg s2.Storage) error { return exists(strg, "a.txt/sub") }},
		{caseName: "put metadata below an object", act: func(strg s2.Storage) error { return strg.PutMetadata(ctx, "a.txt/sub", nil) }, wantErr: s2.ErrNotExist},
		{caseName: "delete below an object", act: func(strg s2.Storage) error { return strg.Delete(ctx, "a.txt/sub") }},
		{caseName: "copy from below an object", act: func(strg s2.Storage) error { return strg.Copy(ctx, "a.txt/sub", "b.txt") }, wantErr: s2.ErrNotExist},
		{caseName: "put below an object", act: func(strg s2.Storage) error { return strg.Put(ctx, s2.NewObjectBytes("a.txt/sub", []byte("x"))) }, wantErr: s2.ErrInvalidName},
		{caseName: "put deeper below an object", act: func(strg s2.Storage) error { return strg.Put(ctx, s2.NewObjectBytes("dir/x.txt/sub/y", []byte("x"))) }, wantErr: s2.ErrInvalidName},
		{caseName: "put over a directory", act: func(strg s2.Storage) error { return strg.Put(ctx, s2.NewObjectBytes("dir", []byte("x"))) }, wantErr: s2.ErrInvalidName},
		{caseName: "copy over a directory", act: func(strg s2.Storage) error { return strg.Copy(ctx, "a.txt", "dir") }, wantErr: s2.ErrInvalidName},
		{caseName: "copy below an object", act: func(strg s2.Storage) error { return strg.Copy(ctx, "dir/x.txt", "a.txt/sub") }, wantErr: s2.ErrInvalidName},
		{caseName: "move over a directory", act: func(strg s2.Storage) error { return s2.Move(ctx, strg, "a.txt", "dir") }, wantErr: s2.ErrInvalidName},
		{caseName: "move below an object", act: func(strg s2.Storage) error { return s2.Move(ctx, strg, "dir/x.txt", "a.txt/sub") }, wantErr: s2.ErrInvalidName},
	}
	fsyses := []struct {
		caseName string
		newFS    func() fs.FS
	}{
		{caseName: "memfs", newFS: func() fs.FS { return memfs.New() }},
		{caseName: "osfs", newFS: func() fs.FS { return osfs.DirFS(s.T().TempDir()) }},
	}
	for _, fc := range fsyses {
		for _, tc := range testCases {
			s.Run(fc.caseName+"/"+tc.caseName, func() {
				strg := NewStorageFS(s2.Config{}, fc.newFS())
				for _, name := range []string{"a.txt", "dir/x.txt"} {
					s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(name, []byte("x"))))
				}

				err := tc.act(strg)
				if tc.wantErr == nil {
					s.Require().NoError(err)
				} else {
					s.Require().ErrorIs(err, tc.wantErr)
					s.NotContains(err.Error(), filepath.Clean(os.TempDir()))
				}
				for _, name := range []string{"a.txt", "dir/x.txt"} {
					_, err := strg.Get(ctx, name)
					s.NoErrorf(err, "%s after the refused call", name)
				}
			})
		}
	}
}

// A delete the tree refuses for another reason, such as permissions, stays an error.
func (s *StorageTestSuite) TestDeleteKeepsPermissionErrors() {
	if os.Getuid() == 0 {
		s.T().Skip("root ignores directory permissions")
	}
	ctx := context.Background()
	root := s.T().TempDir()
	strg := NewStorageFS(s2.Config{}, osfs.DirFS(root))
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("dir/x.txt", []byte("x"))))
	dir := filepath.Join(root, "dir")
	s.Require().NoError(os.Chmod(dir, 0o555))
	s.T().Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	err := strg.Delete(ctx, "dir/x.txt")
	s.Require().ErrorIs(err, fs.ErrPermission)
	_, err = strg.Get(ctx, "dir/x.txt")
	s.NoError(err)
}

// writableFS is what the hooked filesystems below wrap.
type writableFS interface {
	wfs.WriteFileFS
	wfs.RenameFS
	wfs.RemoveFileFS
	RemoveAll(path string) error
}

// hookFile runs onClose once the temp file is written, outside any locked section.
type hookFile struct {
	wfs.WriterFile
	onClose func() error
}

func (f *hookFile) Sync() error {
	if sf, ok := f.WriterFile.(wfs.SyncWriterFile); ok {
		return sf.Sync()
	}
	return nil
}

func (f *hookFile) Close() error {
	if err := f.WriterFile.Close(); err != nil {
		return err
	}
	return f.onClose()
}

// fsHooks injects steps into a storage's filesystem; each fires once.
type fsHooks struct {
	mu       sync.Mutex
	closed   map[string]func() error // by the name a temp file replaces, once it is written
	creating map[string]func()       // by the name a temp file replaces, between the mkdir of its parent and its creation
	mkdir    map[string]func()       // by directory, between the mkdir of its parent and its own
	renamed  func(oldpath, newpath string) error
	readDir  map[string]func()       // by directory, before it is read. Only the unlocked walk and removeEmptyTree read directories.
	removing map[string]func() error // by name, before it is removed; an error stands in for the removal's
}

func newFSHooks() *fsHooks {
	return &fsHooks{closed: map[string]func() error{}, creating: map[string]func(){}, mkdir: map[string]func(){}, readDir: map[string]func(){}, removing: map[string]func() error{}}
}

// take pops the hook under key.
func take[H any](h *fsHooks, hooks map[string]H, key string) (H, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	hook, ok := hooks[key]
	delete(hooks, key)
	return hook, ok
}

// takeTemp pops the hook of the object name the temp file tmp stands for.
func takeTemp[H any](h *fsHooks, hooks map[string]H, tmp string) (H, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	dir, file := path.Split(tmp)
	for target, hook := range hooks {
		tdir, tbase := path.Split(target)
		if dir == tdir && strings.HasPrefix(file, tmpPrefix+tbase+".") {
			delete(hooks, target)
			return hook, true
		}
	}
	var none H
	return none, false
}

// newHookedStorage returns a storage on base whose temp files, directories, renames and removals go through hooks.
func newHookedStorage(base writableFS, hooks *fsHooks, spy *spyLocker) s2.Storage {
	fsys := wfs.DelegateFS(base)
	fsys.RemoveFileFunc = func(name string) error {
		if hook, ok := take(hooks, hooks.removing, name); ok {
			if err := hook(); err != nil {
				return err
			}
		}
		return base.RemoveFile(name)
	}
	fsys.RemoveAllFunc = base.RemoveAll
	fsys.MkdirAllFunc = func(dir string, mode fs.FileMode) error {
		hook, ok := take(hooks, hooks.mkdir, dir)
		if !ok {
			return base.MkdirAll(dir, mode)
		}
		// As os.MkdirAll: the parent first, then dir, which fails when the parent went away meanwhile.
		if err := base.MkdirAll(path.Dir(dir), mode); err != nil {
			return err
		}
		hook()
		if _, err := fs.Stat(base, path.Dir(dir)); err != nil {
			return err
		}
		return base.MkdirAll(dir, mode)
	}
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		if hook, ok := take(hooks, hooks.readDir, name); ok {
			hook()
		}
		return fs.ReadDir(base, name)
	}
	fsys.CreateFileFunc = func(name string, mode fs.FileMode) (wfs.WriterFile, error) {
		if hook, ok := takeTemp(hooks, hooks.creating, name); ok {
			// As wfs.CreateFile: the parent first, then the file, which fails when the parent went away meanwhile.
			if err := base.MkdirAll(path.Dir(name), mode); err != nil {
				return nil, err
			}
			hook()
			if _, err := fs.Stat(base, path.Dir(name)); err != nil {
				return nil, err
			}
		}
		f, err := base.CreateFile(name, mode)
		if err != nil {
			return nil, err
		}
		if hook, ok := takeTemp(hooks, hooks.closed, name); ok {
			return &hookFile{WriterFile: f, onClose: hook}, nil
		}
		return f, nil
	}
	fsys.RenameFunc = func(oldpath, newpath string) error {
		if hooks.renamed != nil {
			if err := hooks.renamed(oldpath, newpath); err != nil {
				return err
			}
		}
		return base.Rename(oldpath, newpath)
	}
	return NewStorageFS(s2.Config{}, fsys, WithNameLocker(spy))
}

// readConsistent reads name and fails unless its ETag and Content-Type belong to its body.
func readConsistent(t *testing.T, strg s2.Storage, name string) string {
	t.Helper()
	obj, err := strg.Get(context.Background(), name)
	require.NoError(t, err)
	rc, err := obj.Open()
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()

	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%q", fmt.Sprintf("%x", md5.Sum(body))), obj.ETag(), "ETag of %q", name)
	require.Equal(t, "text/"+string(body[:1]), obj.ContentType(), "Content-Type of %q", name)
	return string(body)
}

// putText writes body with the Content-Type readConsistent expects.
func putText(ctx context.Context, strg s2.Storage, name, body string, md s2.Metadata) error {
	return strg.Put(ctx, s2.NewObjectBytes(name, []byte(body), s2.WithContentType("text/"+body[:1]), s2.WithMetadata(md)))
}

// TestWritesCommitWholly checks a write racing another leaves a body and metadata file from one writer, and the tree it found (#329, #317).
func TestWritesCommitWholly(t *testing.T) {
	ctx := context.Background()
	put := func(name, body string) func(context.Context, s2.Storage) error {
		return func(ctx context.Context, strg s2.Storage) error { return putText(ctx, strg, name, body, nil) }
	}
	del := func(name string) func(context.Context, s2.Storage) error {
		return func(ctx context.Context, strg s2.Storage) error { return strg.Delete(ctx, name) }
	}
	testCases := []struct {
		caseName string
		seed     map[string]string
		raws     []string                                // files made around the storage, such as by an older s2
		write    func(context.Context, s2.Storage) error // A
		timeout  time.Duration                           // of A's ctx, if any
		// race is B, run at exactly one point of A: once A's metadata temp of raceOn is written, outside sections;
		// between the two mkdirs of mkdirOn, before A reads readDirOn or removes removingOn, inside A's first section; or, with pruneOn,
		// as A is about to take its exclusive prune lock, in a goroutine that parks on its own body temp's close
		// (parkOnClose) or creation (parkOnCreate) until A's ctx expires or, without a timeout, A has returned or
		// its prune is queued behind B.
		raceOn, mkdirOn, readDirOn, removingOn string
		pruneOn                                bool
		parkOnClose, parkOnCreate              string
		race                                   func(context.Context, s2.Storage) error
		wantErr, wantRaceErr                   error
		want                                   map[string]string // name → body; "" means absent, a directory included
	}{
		{
			caseName: "put x put",
			write:    put("x", "aaa"),
			raceOn:   "x",
			race:     put("x", "bbbbbb"),
			want:     map[string]string{"x": "aaa"},
		},
		{
			caseName: "put x put metadata",
			seed:     map[string]string{"x": "ooo"},
			write:    put("x", "aaa"),
			raceOn:   "x",
			race: func(ctx context.Context, strg s2.Storage) error {
				return strg.PutMetadata(ctx, "x", s2.Metadata{"k": "v"})
			},
			want: map[string]string{"x": "aaa"},
		},
		{
			caseName: "put x delete",
			seed:     map[string]string{"x": "ooo"},
			write:    put("x", "aaa"),
			raceOn:   "x",
			race:     del("x"),
			want:     map[string]string{"x": "aaa"},
		},
		{
			caseName: "put x move to it",
			seed:     map[string]string{"s": "sss"},
			write:    put("d", "aaa"),
			raceOn:   "d",
			race:     func(ctx context.Context, strg s2.Storage) error { return s2.Move(ctx, strg, "s", "d") },
			want:     map[string]string{"d": "aaa", "s": ""},
		},
		{
			caseName: "copy x put source",
			seed:     map[string]string{"s": "sss"},
			write:    func(ctx context.Context, strg s2.Storage) error { return strg.Copy(ctx, "s", "d") },
			raceOn:   "d",
			race:     put("s", "bbbbbb"),
			want:     map[string]string{"d": "sss", "s": "bbbbbb"},
		},
		{
			caseName: "put x delete of the last sibling",
			seed:     map[string]string{"a/b/y": "yyy"},
			write:    put("a/b/x", "aaa"),
			raceOn:   "a/b/x",
			race:     del("a/b/y"),
			want:     map[string]string{"a/b/x": "aaa", "a/b/y": ""},
		},
		{
			// B lands first, so A's commit finds a directory; the raw-error branch of begin and commit is stress-only.
			caseName: "put x put beneath it",
			write:    put("a", "aaa"),
			raceOn:   "a",
			race:     put("a/x", "bbbbbb"),
			wantErr:  s2.ErrInvalidName,
			want:     map[string]string{"a/x": "bbbbbb"},
		},
		{
			caseName:    "put beneath x put above it",
			write:       put("a/x", "aaa"),
			raceOn:      "a/x",
			race:        put("a", "bbbbbb"),
			wantRaceErr: s2.ErrInvalidName, // A's directory is there before B begins
			want:        map[string]string{"a/x": "aaa"},
		},
		{
			caseName: "put beneath x delete of the directory's name", // a prefix is not a key, even while empty between a writer's mkdirs
			write:    put("a/x", "aaa"),
			mkdirOn:  "a/.meta",
			race:     del("a"),
			want:     map[string]string{"a/x": "aaa"},
		},
		{
			caseName:    "put x delete of the last sibling, delete first", // the prune stops at the temps of a writer between its sections
			seed:        map[string]string{"a/b/y": "yyy"},
			write:       del("a/b/y"),
			pruneOn:     true,
			parkOnClose: "a/b/x",
			race:        put("a/b/x", "bbbbbb"),
			want:        map[string]string{"a/b/x": "bbbbbb", "a/b/y": ""},
		},
		{
			caseName:     "put x delete of the last sibling, prune during the put's first section", // #317: the prune waits for the writer's mkdir and create
			seed:         map[string]string{"a/b/y": "yyy"},
			write:        del("a/b/y"),
			pruneOn:      true,
			parkOnCreate: "a/b/x",
			race:         put("a/b/x", "bbbbbb"),
			want:         map[string]string{"a/b/x": "bbbbbb", "a/b/y": ""},
		},
		{
			caseName:     "delete whose ctx expires while its prune waits", // the deletion stands, so the prune must follow
			seed:         map[string]string{"a/b/y": "yyy"},
			write:        del("a/b/y"),
			timeout:      100 * time.Millisecond,
			pruneOn:      true,
			parkOnCreate: "c/z",
			race:         put("c/z", "bbbbbb"),
			want:         map[string]string{"a/b/y": "", "a/b": "", "c/z": "bbbbbb"},
		},
		{
			caseName:  "put x delete pruning the legacy directory it is clearing", // removeEmptyTree finds another remover's work done
			raws:      []string{".meta/photos/sub/x"},
			write:     put("photos", "aaa"),
			readDirOn: ".meta/photos",
			race:      del("photos/sub/x"),
			want:      map[string]string{"photos": "aaa"},
		},
		{
			caseName:   "delete pruning the legacy directory x put of its name", // the prune keeps .meta/photos, the put's metadata directory
			raws:       []string{".meta/photos/sub/x"},
			write:      del("photos/sub/x"),
			removingOn: ".meta/photos",
			race:       put("photos", "aaa"),
			want:       map[string]string{"photos": "aaa"},
		},
	}
	for _, backend := range []string{"memfs", "osfs"} {
		for _, tc := range testCases {
			t.Run(backend+"/"+tc.caseName, func(t *testing.T) {
				var base writableFS = memfs.New()
				if backend == "osfs" {
					base = osfs.DirFS(t.TempDir()).(*osfs.OSFS)
				}
				spy := newSpyLocker()
				hooks := newFSHooks()
				strg := newHookedStorage(base, hooks, spy)
				for name, body := range tc.seed {
					require.NoError(t, putText(ctx, strg, name, body, nil))
				}
				for _, name := range tc.raws {
					_, err := wfs.WriteFile(base, name, []byte("x"), fs.ModePerm)
					require.NoError(t, err)
				}
				spy.recorded()
				writeCtx := ctx
				if tc.timeout > 0 {
					var cancel context.CancelFunc
					writeCtx, cancel = context.WithTimeout(ctx, tc.timeout)
					defer cancel()
				}
				raceErr := make(chan error, 1)
				race := func() { raceErr <- tc.race(ctx, strg) }
				var done atomic.Bool
				release := make(chan struct{})
				var releaseOnce sync.Once
				released := func() { releaseOnce.Do(func() { close(release) }) }
				switch {
				case tc.raceOn != "":
					hooks.closed[metaPath(tc.raceOn)] = func() error {
						assert.False(t, spy.holding(), "the race would run inside a section")
						race()
						return nil
					}
				case tc.mkdirOn != "":
					hooks.mkdir[tc.mkdirOn] = race
				case tc.readDirOn != "":
					hooks.readDir[tc.readDirOn] = race
				case tc.removingOn != "":
					hooks.removing[tc.removingOn] = func() error { race(); return nil }
				case tc.pruneOn:
					parked := make(chan struct{})
					park := func() { parked <- struct{}{}; <-release }
					if tc.parkOnClose != "" {
						hooks.closed[tc.parkOnClose] = func() error { park(); return nil }
					}
					if tc.parkOnCreate != "" {
						hooks.creating[tc.parkOnCreate] = park
					}
					var fired atomic.Bool
					spy.onLock = func(name string, shared bool) {
						// A's first section took "." and its name; the next "." is the prune.
						spy.mu.Lock()
						calls := len(spy.calls)
						spy.mu.Unlock()
						if name != "." || calls < 2 || fired.Swap(true) {
							return
						}
						assert.False(t, shared, "the prune holds the root exclusively")
						go race()
						<-parked
						go func() {
							if tc.timeout > 0 {
								<-writeCtx.Done()
							} else {
								for !done.Load() && queued(spy.nameLocker, ".") == 0 {
									time.Sleep(time.Millisecond)
								}
							}
							released()
						}()
					}
				}

				require.ErrorIs(t, tc.write(writeCtx, strg), tc.wantErr)
				done.Store(true)
				released()
				// A that never removed removingOn runs B afterwards.
				if hook, ok := take(hooks, hooks.removing, tc.removingOn); ok {
					_ = hook()
				}
				require.ErrorIs(t, <-raceErr, tc.wantRaceErr)
				for name, want := range tc.want {
					if want == "" {
						ok, err := strg.Exists(ctx, name)
						require.NoError(t, err)
						require.False(t, ok, "%q should be gone", name)
						continue
					}
					require.Equal(t, want, readConsistent(t, strg, name))
				}
				require.NoError(t, fs.WalkDir(base, ".", func(name string, d fs.DirEntry, err error) error {
					require.NoError(t, err)
					require.False(t, strings.HasPrefix(d.Name(), tmpPrefix), "temp file %q left", name)
					return nil
				}))
			})
		}
	}
}

// TestWriteFailuresCleanUp checks a failed write leaves no temp file or directory it made, and the previous object intact.
func TestWriteFailuresCleanUp(t *testing.T) {
	ctx := context.Background()
	errInjected := errors.New("injected")
	failRename := func(target string) func(_, newpath string) error {
		return func(_, newpath string) error {
			if newpath == target {
				return errInjected
			}
			return nil
		}
	}
	testCases := []struct {
		caseName string
		seed     map[string]string
		name     string
		closed   func(strg s2.Storage) error // on the metadata temp of name
		renamed  func(oldpath, newpath string) error
		wantErr  error
		want     map[string]string // name → body; "" means absent
		wantDirs []string          // directories left at the root
	}{
		{
			caseName: "metadata write fails",
			name:     "a/b/x",
			closed:   func(s2.Storage) error { return errInjected },
			wantErr:  errInjected,
			want:     map[string]string{"a/b/x": ""},
		},
		{
			caseName: "body publish fails",
			name:     "a/b/x",
			renamed:  failRename("a/b/x"),
			wantErr:  errInjected,
			want:     map[string]string{"a/b/x": ""},
		},
		{
			caseName: "body publish fails over an object",
			seed:     map[string]string{"x": "ooo"},
			name:     "x",
			renamed:  failRename("x"),
			wantErr:  errInjected,
			want:     map[string]string{"x": "ooo"},
			wantDirs: []string{metaDir},
		},
		{
			caseName: "directory takes the name",
			name:     "x",
			closed:   func(strg s2.Storage) error { return putText(ctx, strg, "x/y", "yyy", nil) },
			wantErr:  s2.ErrInvalidName,
			want:     map[string]string{"x/y": "yyy"},
			wantDirs: []string{"x", metaDir}, // the root's .meta is never pruned
		},
	}
	for _, backend := range []string{"memfs", "osfs"} {
		for _, tc := range testCases {
			t.Run(backend+"/"+tc.caseName, func(t *testing.T) {
				var base writableFS = memfs.New()
				if backend == "osfs" {
					base = osfs.DirFS(t.TempDir()).(*osfs.OSFS)
				}
				hooks := newFSHooks()
				strg := newHookedStorage(base, hooks, newSpyLocker())
				for name, body := range tc.seed {
					require.NoError(t, putText(ctx, strg, name, body, nil))
				}
				if tc.closed != nil {
					hooks.closed[metaPath(tc.name)] = func() error { return tc.closed(strg) }
				}
				hooks.renamed = tc.renamed

				require.ErrorIs(t, putText(ctx, strg, tc.name, "aaa", nil), tc.wantErr)
				hooks.renamed = nil
				for name, want := range tc.want {
					if want == "" {
						ok, err := strg.Exists(ctx, name)
						require.NoError(t, err)
						require.False(t, ok, "%q should be absent", name)
						continue
					}
					require.Equal(t, want, readConsistent(t, strg, name))
				}
				var dirs []string
				require.NoError(t, fs.WalkDir(base, ".", func(name string, d fs.DirEntry, err error) error {
					require.NoError(t, err)
					require.False(t, strings.HasPrefix(d.Name(), tmpPrefix), "temp file %q left", name)
					if d.IsDir() && name != "." && !strings.Contains(name, "/") {
						dirs = append(dirs, name)
					}
					return nil
				}))
				require.ElementsMatch(t, tc.wantDirs, dirs)
			})
		}
	}
}

// TestMetadataPublishFailureDropsStaleMetadata checks the new body is left without the previous body's metadata file.
func TestMetadataPublishFailureDropsStaleMetadata(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		write    func(strg s2.Storage) error
		wantLen  uint64
	}{
		{
			caseName: "put",
			write:    func(strg s2.Storage) error { return putText(ctx, strg, "x", "aaa", nil) },
			wantLen:  3,
		},
		{
			caseName: "move",
			write: func(strg s2.Storage) error {
				if err := putText(ctx, strg, "y", "aaaaa", nil); err != nil {
					return err
				}
				return s2.Move(ctx, strg, "y", "x")
			},
			wantLen: 5,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			base := memfs.New()
			hooks := newFSHooks()
			strg := newHookedStorage(base, hooks, newSpyLocker())
			require.NoError(t, putText(ctx, strg, "x", "ooo", nil))
			hooks.renamed = func(_, newpath string) error {
				if newpath == metaPath("x") {
					return errors.New("injected")
				}
				return nil
			}

			require.Error(t, tc.write(strg))
			_, err := fs.Stat(base, metaPath("x"))
			require.ErrorIs(t, err, fs.ErrNotExist)
			obj, err := strg.Get(ctx, "x")
			require.NoError(t, err)
			require.Equal(t, tc.wantLen, obj.Length())
			require.Contains(t, obj.ETag(), "-", "synthetic ETag, not the old body's MD5")
			require.Empty(t, obj.ContentType())
		})
	}
}

// TestConcurrentWritesStress races two writers on one name and checks every round ends consistent (#329, #317).
func TestConcurrentWritesStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	rounds := 50
	if v := os.Getenv("S2_STRESS_ROUNDS"); v != "" {
		var err error
		rounds, err = strconv.Atoi(v)
		require.NoError(t, err)
	}
	ctx := context.Background()
	both := func(f, g func() error) {
		var wg sync.WaitGroup
		wg.Go(func() { _ = f() })
		wg.Go(func() { _ = g() })
		wg.Wait()
	}
	testCases := []struct {
		caseName string
		seed     map[string]string
		a, b     func(strg s2.Storage) error
		check    []string // names that must be consistent when present
	}{
		{
			caseName: "put x put",
			a:        func(strg s2.Storage) error { return putText(ctx, strg, "k", "aaa", nil) },
			b:        func(strg s2.Storage) error { return putText(ctx, strg, "k", "bbbbbb", nil) },
			check:    []string{"k"},
		},
		{
			caseName: "put metadata x put",
			seed:     map[string]string{"k": "aaa"},
			a:        func(strg s2.Storage) error { return strg.PutMetadata(ctx, "k", s2.Metadata{"m": "1"}) },
			b:        func(strg s2.Storage) error { return putText(ctx, strg, "k", "bbbbbb", nil) },
			check:    []string{"k"},
		},
		{
			caseName: "put x delete",
			seed:     map[string]string{"k": "aaa"},
			a:        func(strg s2.Storage) error { return putText(ctx, strg, "k", "bbbbbb", nil) },
			b:        func(strg s2.Storage) error { return strg.Delete(ctx, "k") },
			check:    []string{"k"},
		},
		{
			caseName: "move x put destination",
			seed:     map[string]string{"s": "aaa"},
			a:        func(strg s2.Storage) error { return s2.Move(ctx, strg, "s", "d") },
			b:        func(strg s2.Storage) error { return putText(ctx, strg, "d", "bbbbbb", nil) },
			check:    []string{"s", "d"},
		},
		{
			caseName: "move x delete source",
			seed:     map[string]string{"s": "aaa"},
			a:        func(strg s2.Storage) error { return s2.Move(ctx, strg, "s", "d") },
			b:        func(strg s2.Storage) error { return strg.Delete(ctx, "s") },
			check:    []string{"s", "d"},
		},
		{
			caseName: "copy x put source",
			seed:     map[string]string{"s": "aaa"},
			a:        func(strg s2.Storage) error { return strg.Copy(ctx, "s", "d") },
			b:        func(strg s2.Storage) error { return putText(ctx, strg, "s", "bbbbbb", nil) },
			check:    []string{"s", "d"},
		},
		{
			caseName: "put x delete of the last sibling",
			seed:     map[string]string{"a/b/c/y": "aaa"},
			a:        func(strg s2.Storage) error { return strg.Delete(ctx, "a/b/c/y") },
			b:        func(strg s2.Storage) error { return putText(ctx, strg, "a/b/c/x", "bbbbbb", nil) },
			check:    []string{"a/b/c/x"},
		},
		{
			caseName: "put x delete of its directory's name", // osfs makes the directory in two mkdirs, empty in between
			a: func(strg s2.Storage) error {
				for range 200 {
					_ = strg.Delete(ctx, "a")
				}
				return nil
			},
			b:     func(strg s2.Storage) error { return putText(ctx, strg, "a/x", "bbbbbb", nil) },
			check: []string{"a/x"},
		},
		{
			caseName: "put x put beneath it",
			a:        func(strg s2.Storage) error { return putText(ctx, strg, "a", "aaa", nil) },
			b:        func(strg s2.Storage) error { return putText(ctx, strg, "a/x", "bbbbbb", nil) },
			check:    []string{"a", "a/x"},
		},
	}
	for _, backend := range []string{"memfs", "osfs"} {
		for _, tc := range testCases {
			t.Run(backend+"/"+tc.caseName, func(t *testing.T) {
				for range rounds {
					strg := NewStorageMem(s2.Config{})
					if backend == "osfs" {
						strg = NewStorageDir(t.TempDir())
					}
					for name, body := range tc.seed {
						require.NoError(t, putText(ctx, strg, name, body, nil))
					}
					var errB error
					both(func() error { return tc.a(strg) }, func() error { errB = tc.b(strg); return errB })
					if strings.HasPrefix(tc.caseName, "put x delete of") {
						require.NoError(t, errB, "a put racing a prune")
					}
					for _, name := range tc.check {
						if _, err := strg.Get(ctx, name); err == nil { // not a directory the other writer made
							readConsistent(t, strg, name)
						}
					}
				}
			})
		}
	}
}

// TestDeleteRecursiveRaces checks DeleteRecursive against writes landing under its prefix while it walks (#317).
func TestDeleteRecursiveRaces(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		seed     map[string]string
		// Exactly one of these races: a write of name whose metadata file is closed, or a read of a directory by the walk.
		write   string
		readDir string
		prefix  string // of DeleteRecursive when readDir is set; "a/" if empty
		race    func(strg s2.Storage) error
		wantErr bool              // of the write
		want    map[string]string // name → body; "" means absent
	}{
		{
			caseName: "a put in flight",
			write:    "d/x",
			race:     func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "d/") },
			wantErr:  true,
			want:     map[string]string{"d/x": ""},
		},
		{
			caseName: "a prefix that selects only a temp name",
			write:    "a/x",
			race:     func(strg s2.Storage) error { return strg.DeleteRecursive(ctx, "a/.s") },
			want:     map[string]string{"a/x": "xxx"},
		},
		{
			caseName: "an object takes a directory's name",
			seed:     map[string]string{"a/b/c": "ccc"},
			readDir:  "a/b",
			race: func(strg s2.Storage) error {
				if err := strg.Delete(ctx, "a/b/c"); err != nil {
					return err
				}
				return putText(ctx, strg, "a/b", "bbb", nil)
			},
			want: map[string]string{"a/b": "", "a/b/c": ""},
		},
		{
			caseName: "an object takes the prefix's name",
			seed:     map[string]string{"a/b/c": "ccc"},
			readDir:  "a/b",
			race: func(strg s2.Storage) error {
				if err := strg.Delete(ctx, "a/b/c"); err != nil {
					return err
				}
				return putText(ctx, strg, "a", "aaa", nil)
			},
			want: map[string]string{"a": "aaa"},
		},
		{
			caseName: "an object takes a name the prefix selects",
			seed:     map[string]string{"a/b/c": "ccc"},
			readDir:  "a/b",
			prefix:   "a",
			race: func(strg s2.Storage) error {
				if err := strg.Delete(ctx, "a/b/c"); err != nil {
					return err
				}
				return putText(ctx, strg, "a", "aaa", nil)
			},
			want: map[string]string{"a": ""},
		},
	}
	for _, backend := range []string{"memfs", "osfs"} {
		for _, tc := range testCases {
			t.Run(backend+"/"+tc.caseName, func(t *testing.T) {
				var base writableFS = memfs.New()
				if backend == "osfs" {
					base = osfs.DirFS(t.TempDir()).(*osfs.OSFS)
				}
				spy := newSpyLocker()
				hooks := newFSHooks()
				strg := newHookedStorage(base, hooks, spy)
				for name, body := range tc.seed {
					require.NoError(t, putText(ctx, strg, name, body, nil))
				}
				race := func() {
					require.False(t, spy.holding(), "the race would run inside a section")
					require.NoError(t, tc.race(strg))
				}

				if tc.write != "" {
					hooks.closed[metaPath(tc.write)] = func() error { race(); return nil }
					err := putText(ctx, strg, tc.write, "xxx", nil)
					require.Equal(t, tc.wantErr, err != nil, "write error: %v", err)
				} else {
					hooks.readDir[tc.readDir] = race
					prefix := tc.prefix
					if prefix == "" {
						prefix = "a/"
					}
					require.NoError(t, strg.DeleteRecursive(ctx, prefix))
				}
				for name, want := range tc.want {
					if want == "" {
						ok, err := strg.Exists(ctx, name)
						require.NoError(t, err)
						require.False(t, ok, "%q should be absent", name)
						continue
					}
					require.Equal(t, want, readConsistent(t, strg, name))
				}
				require.NoError(t, fs.WalkDir(base, ".", func(name string, d fs.DirEntry, err error) error {
					require.NoError(t, err)
					require.False(t, strings.HasPrefix(d.Name(), tmpPrefix), "temp file %q left", name)
					return nil
				}))
			})
		}
	}
}

// TestDeleteRecursiveRemovesOrphanedTemps checks temp files a crash left behind do not keep a directory alive.
func TestDeleteRecursiveRemovesOrphanedTemps(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memfs", "osfs"} {
		t.Run(backend, func(t *testing.T) {
			var base writableFS = memfs.New()
			if backend == "osfs" {
				base = osfs.DirFS(t.TempDir()).(*osfs.OSFS)
			}
			strg := NewStorageFS(s2.Config{}, base)
			require.NoError(t, putText(ctx, strg, "d/x", "xxx", nil))
			for _, name := range []string{"d/" + tmpPrefix + "y.0", "d/.meta/" + tmpPrefix + "y.0"} {
				_, err := base.WriteFile(name, []byte("partial"), 0o644)
				require.NoError(t, err)
			}

			require.NoError(t, strg.DeleteRecursive(ctx, "d/"))
			_, err := fs.Stat(base, "d")
			require.ErrorIs(t, err, fs.ErrNotExist)
		})
	}
}

// TestDeleteRecursivePartialFailure checks a run that fails midway still removes the directories it emptied, keeps those holding objects and returns its error (#331).
func TestDeleteRecursivePartialFailure(t *testing.T) {
	errInjected := errors.New("injected")
	seed := []string{"a/gone/x", "a/keep/sub/z", "a/keep/y", "a/late/w"}
	testCases := []struct {
		caseName string
		removing string // the removal that fails or cancels
		cancel   bool   // cancel the context there instead of failing
		wantErr  error
		wantLeft []string
	}{
		{
			caseName: "a removal fails",
			removing: "a/keep/y",
			wantErr:  errInjected,
			wantLeft: []string{"a", "a/keep", "a/keep/y", "a/late", "a/late/w"},
		},
		{
			caseName: "the context is cancelled",
			removing: "a/keep/sub/z",
			cancel:   true,
			wantErr:  context.Canceled,
			wantLeft: []string{"a", "a/keep", "a/keep/y", "a/late", "a/late/w"},
		},
		{
			caseName: "the context is cancelled after the walk",
			removing: "a/late/w",
			cancel:   true,
			wantErr:  context.Canceled,
			wantLeft: []string{"a"}, // the prefix's own directory stays until a run succeeds
		},
	}
	for _, backend := range []string{"memfs", "osfs"} {
		for _, tc := range testCases {
			t.Run(backend+"/"+tc.caseName, func(t *testing.T) {
				var base writableFS = memfs.New()
				if backend == "osfs" {
					base = osfs.DirFS(t.TempDir()).(*osfs.OSFS)
				}
				hooks := newFSHooks()
				strg := newHookedStorage(base, hooks, newSpyLocker())
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				for _, name := range seed {
					require.NoError(t, putText(ctx, strg, name, "xxx", nil))
				}
				hooks.removing[tc.removing] = func() error {
					if tc.cancel {
						cancel()
						return nil
					}
					return errInjected
				}

				require.ErrorIs(t, strg.DeleteRecursive(ctx, "a/"), tc.wantErr)
				var left []string
				// Bodies and directories only: what a failed delete does to metadata files is not this test's concern.
				require.NoError(t, fs.WalkDir(base, ".", func(name string, d fs.DirEntry, err error) error {
					if d != nil && d.IsDir() && isMetaDir(d.Name()) {
						return fs.SkipDir
					}
					if name != "." {
						left = append(left, name)
					}
					return err
				}))
				require.Equal(t, tc.wantLeft, left)
			})
		}
	}
}

// countingFile counts how many temp files get closed.
type countingFile struct {
	wfs.WriterFile
	closed *atomic.Int32
}

func (f *countingFile) Close() error {
	f.closed.Add(1)
	return f.WriterFile.Close()
}

// TestFailedWriteClosesItsTemps checks every temp file a failed write opened is closed, not only removed.
func TestFailedWriteClosesItsTemps(t *testing.T) {
	ctx := context.Background()
	base := osfs.DirFS(t.TempDir()).(*osfs.OSFS)
	var created, closed atomic.Int32
	fsys := wfs.DelegateFS(base)
	fsys.MkdirAllFunc = base.MkdirAll
	fsys.RemoveFileFunc = base.RemoveFile
	fsys.RenameFunc = base.Rename
	fsys.CreateFileFunc = func(name string, mode fs.FileMode) (wfs.WriterFile, error) {
		f, err := base.CreateFile(name, mode)
		if err != nil {
			return nil, err
		}
		created.Add(1)
		return &countingFile{WriterFile: f, closed: &closed}, nil
	}
	strg := NewStorageFS(s2.Config{}, fsys)
	body := io.NopCloser(iotest.ErrReader(errors.New("client went away")))

	require.Error(t, strg.Put(ctx, s2.NewObjectReader("a/x", body, 10)))
	require.Equal(t, int32(2), created.Load())
	require.Equal(t, created.Load(), closed.Load())
}

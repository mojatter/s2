package fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/mojatter/wfs/memfs"
	"github.com/mojatter/wfs/osfs"
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
	files := map[string][]byte{
		"a.txt":     []byte("a"),
		"b.txt":     []byte("b"),
		"cc/c1.txt": []byte("c1"),
		"cc/c2.txt": []byte("c2"),
	}
	fsys := memfs.New()
	for name, b := range files {
		_, err := fsys.WriteFile(name, b, fs.ModePerm)
		s.Require().NoError(err)
	}
	return fsys
}

func (s *StorageTestSuite) TestS2TestList() {
	strg := &storage{fsys: s.testMemFS()}
	ctx := context.Background()

	err := s2test.TestStorageListRecursive(ctx, strg, "a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageListWithPrefixes(ctx, strg, "", []string{"cc"}, "a.txt", "b.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "cc", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)
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
			strg:     &storage{fsys: s.testMemFS()},
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
			strg:     &storage{fsys: s.testMemFS()},
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
					s.Equal(w.Metadata(), g.Metadata())
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
			strg:     &storage{fsys: s.testMemFS()},
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
			strg:     &storage{fsys: s.testMemFS()},
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
			strg:     &storage{fsys: s.testMemFS()},
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
					s.Equal(w.Metadata(), g.Metadata())
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
			strg:     &storage{fsys: s.testMemFS()},
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
			wantPrefixes: []string{"cc"},
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
			strg := &storage{fsys: s.testMemFS()}
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
			strg:     &storage{fsys: s.testMemFS()},
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
			strg := &storage{fsys: s.testMemFS()}
			ctx := context.Background()

			// Limit: 1 forces a new page per object, so a flat listing's
			// final page (which only has the "cc" CommonPrefix left, no
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

	strg := &storage{fsys: fsys}
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
	strg := &storage{fsys: &errReadDirFS{FS: base, errorPath: "cc"}}
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
			strg:     &storage{fsys: s.testMemFS()},
			ctx:      context.Background(),
			name:     "a.txt",
		},
		{
			caseName: "not found",
			strg:     &storage{fsys: s.testMemFS()},
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
			strg:     &storage{fsys: s.testMemFS()},
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
				strg := &storage{fsys: s.testMemFS()}
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
			strg:     &storage{fsys: s.testMemFS()},
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
			strg:     &storage{fsys: s.testMemFS()},
			ctx:      context.Background(),
			prefix:   "c",
			wantLeft: []string{"a.txt", "b.txt"},
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
			root := &storage{fsys: fsys, typ: tc.typ}
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
	strg := &storage{fsys: &errStatMemFS{memfs.New()}}

	s.ErrorIs(strg.DeleteRecursive(context.Background(), ""), fs.ErrPermission)
}

func (s *StorageTestSuite) TestSub() {
	strg := &storage{fsys: s.testMemFS(), typ: s2.TypeMemFS}

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
		root := &storage{fsys: memfs.New(), typ: s2.TypeMemFS}
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
	strg := &storage{fsys: s.testMemFS()}

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

// errMetaRenameMemFS is a writable memfs that fails to rename into .meta/.
type errMetaRenameMemFS struct {
	*memfs.MemFS
}

func (e *errMetaRenameMemFS) Rename(oldpath, newpath string) error {
	if strings.HasPrefix(newpath, ".meta/") {
		return &fs.PathError{Op: "rename", Path: newpath, Err: fs.ErrPermission}
	}
	return e.MemFS.Rename(oldpath, newpath)
}

// A failed sidecar write must not leave the previous body's sidecar behind.
func (s *StorageTestSuite) TestSidecarWriteFails() {
	testCases := []struct {
		caseName string
		write    func(ctx context.Context, strg *storage) error
	}{
		{
			caseName: "put over an object",
			write: func(ctx context.Context, strg *storage) error {
				return strg.Put(ctx, s2.NewObjectBytes("a.txt", []byte("new body")))
			},
		},
		{
			caseName: "copy over an object",
			write: func(ctx context.Context, strg *storage) error {
				return strg.Copy(ctx, "src.txt", "a.txt")
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			mem := memfs.New()
			seed := NewStorageFS(s2.Config{}, mem)
			s.Require().NoError(seed.Put(ctx, s2.NewObjectBytes("a.txt", []byte("old"), s2.WithContentType("text/plain"))))
			s.Require().NoError(seed.Put(ctx, s2.NewObjectBytes("src.txt", []byte("new body"), s2.WithContentType("text/csv"))))
			strg := &storage{fsys: &errMetaRenameMemFS{mem}}

			s.ErrorIs(tc.write(ctx, strg), fs.ErrPermission)

			got, err := strg.Get(ctx, "a.txt")
			s.Require().NoError(err)
			s.Empty(got.ContentType())
			s.Contains(got.ETag(), "-", "synthetic ETag, not the old body's MD5")
		})
	}
}

func (s *StorageTestSuite) TestPutMetadata() {
	strg := &storage{fsys: s.testMemFS()}
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
	strg := &storage{fsys: s.testMemFS()}
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
	strg := &storage{fsys: s.testMemFS()}
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

// A moved file without a sidecar must not keep the destination's old one.
func (s *StorageTestSuite) TestMoveDropsStaleSidecar() {
	fsys := memfs.New()
	strg := &storage{fsys: fsys}
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
	strg := &storage{fsys: memfs.New(), typ: s2.TypeMemFS}
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
	strg := &storage{fsys: memfs.New(), typ: s2.TypeMemFS}
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
			// Nested too: a Sub writes its sidecars beside the names it
			// scopes, and neither listing would ever show the name.
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
		root := &storage{fsys: memfs.New(), typ: s2.TypeMemFS}
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
		strg := &storage{fsys: fsys, typ: s2.TypeMemFS}

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

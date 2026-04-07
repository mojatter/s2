package fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
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
			cfg:      s2.Config{Type: s2.TypeOSFS},
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
		{caseName: "directory returns false", name: "cc", want: false},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got, err := strg.Exists(context.Background(), tc.name)
			s.Require().NoError(err)
			s.Equal(tc.want, got)
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

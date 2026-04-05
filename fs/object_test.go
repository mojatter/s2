package fs

import (
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs/memfs"
	"github.com/stretchr/testify/suite"
)

type ObjectTestSuite struct {
	suite.Suite
}

func TestObjectTestSuite(t *testing.T) {
	suite.Run(t, &ObjectTestSuite{})
}

func (s *ObjectTestSuite) testMemFS() fs.FS {
	fsys := memfs.New()
	_, err := fsys.WriteFile("test.txt", []byte("test"), fs.ModePerm)
	s.Require().NoError(err)
	_, err = fsys.WriteFile(".meta/test.txt", []byte(`{"contentType":"text/plain"}`), fs.ModePerm)
	s.Require().NoError(err)
	_, err = fsys.WriteFile("no-meta.txt", []byte("no-meta"), fs.ModePerm)
	s.Require().NoError(err)
	return fsys
}

func (s *ObjectTestSuite) TestOpen() {
	fsys := s.testMemFS()
	testCases := []struct {
		caseName string
		obj      s2.Object
		wantErr  string
	}{
		{
			caseName: "typical",
			obj: &object{
				fsys: fsys,
				name: "test.txt",
			},
		},
		{
			caseName: "not found",
			obj: &object{
				fsys: fsys,
				name: "not-found.txt",
			},
			wantErr: "Open not-found.txt: file does not exist",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			rc, err := tc.obj.Open()
			if tc.wantErr != "" {
				s.EqualError(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			rc.Close()
		})
	}
}

func (s *ObjectTestSuite) TestLastModified() {
	fsys := s.testMemFS()
	info, err := fs.Stat(fsys, "test.txt")
	s.Require().NoError(err)
	obj := newObjectFileInfo(fsys, "test.txt", info)
	// memfs may return zero time; just verify the method doesn't panic
	_ = obj.LastModified()
}

func (s *ObjectTestSuite) TestOpenRange() {
	fsys := s.testMemFS()
	// Write a larger file for range testing
	wfsys := fsys.(*memfs.MemFS)
	_, err := wfsys.WriteFile("range.txt", []byte("Hello, World!"), fs.ModePerm)
	s.Require().NoError(err)

	info, err := fs.Stat(fsys, "range.txt")
	s.Require().NoError(err)
	obj := newObjectFileInfo(fsys, "range.txt", info)

	s.Run("full range", func() {
		rc, err := obj.OpenRange(0, obj.Length())
		s.Require().NoError(err)
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		s.Equal("Hello, World!", string(b))
	})

	s.Run("partial range with seek", func() {
		rc, err := obj.OpenRange(7, 5)
		s.Require().NoError(err)
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		s.Equal("World", string(b))
	})

	s.Run("offset zero partial", func() {
		rc, err := obj.OpenRange(0, 5)
		s.Require().NoError(err)
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		s.Equal("Hello", string(b))
	})

	s.Run("not found", func() {
		badObj := &object{fsys: fsys, name: "nope.txt", length: 10}
		_, err := badObj.OpenRange(0, 5)
		s.Error(err)
	})
}

// TestNewObjectFileInfo_NegativeSize verifies that newObjectFileInfo panics when
// fs.FileInfo.Size() returns a negative value. This covers the MustUint64
// conversion replacing the bare uint64() cast.
func (s *ObjectTestSuite) TestNewObjectFileInfo_NegativeSize() {
	s.Panics(func() {
		newObjectFileInfo(nil, "bad.txt", &fakeFileInfo{size: -1})
	})
}

// fakeFileInfo is a minimal fs.FileInfo with a controllable Size().
type fakeFileInfo struct {
	fs.FileInfo
	size int64
}

func (f *fakeFileInfo) Size() int64      { return f.size }
func (f *fakeFileInfo) IsDir() bool      { return false }
func (f *fakeFileInfo) Name() string     { return "fake" }
func (f *fakeFileInfo) Mode() fs.FileMode { return 0 }
func (f *fakeFileInfo) Sys() any         { return nil }
func (f *fakeFileInfo) ModTime() time.Time { return time.Time{} }

func (s *ObjectTestSuite) TestMetadata() {
	testCases := []struct {
		caseName string
		obj      s2.Object
		want     s2.Metadata
	}{
		{
			caseName: "typical",
			obj: &object{
				metadata: s2.MetadataMap{"contentType": "text/plain"},
			},
			want: s2.MetadataMap{"contentType": "text/plain"},
		},
		{
			caseName: "no meta",
			obj:      &object{},
			want:     s2.MetadataMap{},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got := tc.obj.Metadata()
			s.Equal(tc.want, got)
		})
	}
}

package buckets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"
)

type objectsSuite struct {
	suite.Suite
	server *server.Server
}

func (s *objectsSuite) SetupTest() {
	cfg := server.DefaultConfig()
	cfg.Root = s.T().TempDir()
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
}

func (s *objectsSuite) createBucket(name string) {
	s.T().Helper()
	s.Require().NoError(s.server.Buckets.Create(context.Background(), name))
}

func (s *objectsSuite) putObject(bucket, key, content string) {
	s.T().Helper()
	ctx := context.Background()
	strg, err := s.server.Buckets.Get(ctx, bucket)
	s.Require().NoError(err)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(key, []byte(content))))
}

type ObjectsTestSuite struct{ objectsSuite }

func TestObjectsTestSuite(t *testing.T) {
	suite.Run(t, &ObjectsTestSuite{})
}

// --- GET /buckets/{name} ---

func (s *ObjectsTestSuite) TestHandleObjects() {
	s.Run("empty bucket", func() {
		s.createBucket("empty")

		req := httptest.NewRequest("GET", "/buckets/empty", nil)
		req.SetPathValue("name", "empty")
		req.Header.Set("HX-Request", "true")
		w := httptest.NewRecorder()
		handleObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Contains(w.Body.String(), "This folder is empty")
	})

	s.Run("with objects", func() {
		s.createBucket("files")
		s.putObject("files", "readme.txt", "hello")

		req := httptest.NewRequest("GET", "/buckets/files", nil)
		req.SetPathValue("name", "files")
		req.Header.Set("HX-Request", "true")
		w := httptest.NewRecorder()
		handleObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Contains(w.Body.String(), "readme.txt")
	})

	s.Run("with prefix", func() {
		s.createBucket("nested")
		s.server.Buckets.CreateFolder(context.Background(), "nested", "sub")
		s.putObject("nested", "sub/file.txt", "data")

		req := httptest.NewRequest("GET", "/buckets/nested?prefix=sub", nil)
		req.SetPathValue("name", "nested")
		req.Header.Set("HX-Request", "true")
		w := httptest.NewRecorder()
		handleObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		body := w.Body.String()
		s.Contains(body, "file.txt")
		s.Contains(body, "Parent Directory")
	})

	s.Run("nonexistent bucket", func() {
		req := httptest.NewRequest("GET", "/buckets/nope", nil)
		req.SetPathValue("name", "nope")
		w := httptest.NewRecorder()
		handleObjects(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})

	s.Run("full page without HX-Request redirects to index", func() {
		s.createBucket("full")

		req := httptest.NewRequest("GET", "/buckets/full", nil)
		req.SetPathValue("name", "full")
		w := httptest.NewRecorder()
		handleObjects(s.server, w, req)

		s.Equal(http.StatusFound, w.Code)
		s.Equal("/", w.Header().Get("Location"))
	})
}

// --- POST /buckets/{name}/folders ---

func (s *ObjectsTestSuite) TestHandleCreateFolder() {
	s.Run("success", func() {
		s.createBucket("fld")

		form := url.Values{"prefix": {""}, "folder_name": {"photos"}}
		req := httptest.NewRequest("POST", "/buckets/fld/folders", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("HX-Request", "true")
		req.SetPathValue("name", "fld")
		w := httptest.NewRecorder()
		handleCreateFolder(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Contains(w.Body.String(), "photos")
	})

	s.Run("empty name", func() {
		s.createBucket("fld2")

		form := url.Values{"prefix": {""}, "folder_name": {""}}
		req := httptest.NewRequest("POST", "/buckets/fld2/folders", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetPathValue("name", "fld2")
		w := httptest.NewRecorder()
		handleCreateFolder(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code)
	})
}

// --- DELETE /buckets/{name}/objects ---

func (s *ObjectsTestSuite) TestHandleDeleteObject() {
	s.Run("delete file", func() {
		s.createBucket("del")
		s.putObject("del", "a.txt", "data")

		req := httptest.NewRequest("DELETE", "/buckets/del/objects?key=a.txt&prefix=", nil)
		req.SetPathValue("name", "del")
		req.Header.Set("HX-Request", "true")
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.NotContains(w.Body.String(), "a.txt")
	})

	s.Run("delete folder recursively", func() {
		s.createBucket("delr")
		s.server.Buckets.CreateFolder(context.Background(), "delr", "dir")
		s.putObject("delr", "dir/b.txt", "data")

		req := httptest.NewRequest("DELETE", "/buckets/delr/objects?key=dir/&prefix=", nil)
		req.SetPathValue("name", "delr")
		req.Header.Set("HX-Request", "true")
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "delr")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), "dir/b.txt")
		s.Require().NoError(err)
		s.False(exists)
	})

	s.Run("missing key", func() {
		s.createBucket("delm")

		req := httptest.NewRequest("DELETE", "/buckets/delm/objects", nil)
		req.SetPathValue("name", "delm")
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code)
	})
}

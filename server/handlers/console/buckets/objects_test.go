package buckets

import (
	"bytes"
	"context"
	"mime/multipart"
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
	testCases := []struct {
		caseName        string
		setup           func()
		bucketName      string
		url             string
		htmx            bool
		wantCode        int
		wantContains    []string
		wantNotContains []string
		wantHeader      map[string]string
	}{
		{
			caseName:     "empty bucket",
			setup:        func() { s.createBucket("empty") },
			bucketName:   "empty",
			url:          "/buckets/empty",
			htmx:         true,
			wantCode:     http.StatusOK,
			wantContains: []string{"This folder is empty"},
		},
		{
			caseName:     "with objects",
			setup:        func() { s.createBucket("files"); s.putObject("files", "readme.txt", "hello") },
			bucketName:   "files",
			url:          "/buckets/files",
			htmx:         true,
			wantCode:     http.StatusOK,
			wantContains: []string{"readme.txt"},
		},
		{
			caseName: "with prefix",
			setup: func() {
				s.createBucket("nested")
				s.Require().NoError(s.server.Buckets.CreateFolder(context.Background(), "nested", "sub"))
				s.putObject("nested", "sub/file.txt", "data")
			},
			bucketName:   "nested",
			url:          "/buckets/nested?prefix=sub",
			htmx:         true,
			wantCode:     http.StatusOK,
			wantContains: []string{"file.txt", "Parent Directory"},
		},
		{
			caseName:   "nonexistent bucket",
			setup:      func() {},
			bucketName: "nope",
			url:        "/buckets/nope",
			htmx:       false,
			wantCode:   http.StatusNotFound,
		},
		{
			// search="logo" matches keys starting with "logo"
			caseName: "search at root finds files recursively",
			setup: func() {
				s.createBucket("srch")
				s.putObject("srch", "logo.png", "data")
				s.putObject("srch", "logo/small.png", "data")
				s.putObject("srch", "other.png", "data")
			},
			bucketName:      "srch",
			url:             "/buckets/srch?search=logo",
			htmx:            true,
			wantCode:        http.StatusOK,
			wantContains:    []string{"logo.png", "logo/small.png"},
			wantNotContains: []string{"other.png"},
		},
		{
			// prefix="a", search="s2" → listPrefix="a/s2"; b/s2* excluded
			caseName: "search with prefix scopes results to prefix",
			setup: func() {
				s.createBucket("srchp")
				s.putObject("srchp", "a/s2-foo.png", "data")
				s.putObject("srchp", "a/s2/bar.png", "data")
				s.putObject("srchp", "b/s2-baz.png", "data")
			},
			bucketName:      "srchp",
			url:             "/buckets/srchp?prefix=a&search=s2",
			htmx:            true,
			wantCode:        http.StatusOK,
			wantContains:    []string{"s2-foo.png", "s2/bar.png"},
			wantNotContains: []string{"s2-baz.png"},
		},
		{
			caseName:     "search with no matches shows empty state",
			setup:        func() { s.createBucket("srchem"); s.putObject("srchem", "readme.txt", "data") },
			bucketName:   "srchem",
			url:          "/buckets/srchem?search=nomatch",
			htmx:         true,
			wantCode:     http.StatusOK,
			wantContains: []string{"This folder is empty"},
		},
		{
			caseName:     "search renders chip with search term",
			setup:        func() { s.createBucket("srchc"); s.putObject("srchc", "doc.txt", "data") },
			bucketName:   "srchc",
			url:          "/buckets/srchc?search=doc",
			htmx:         true,
			wantCode:     http.StatusOK,
			wantContains: []string{"search-chip"},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			tc.setup()

			req := httptest.NewRequest("GET", tc.url, nil)
			req.SetPathValue("name", tc.bucketName)
			if tc.htmx {
				req.Header.Set("HX-Request", "true")
			}
			w := httptest.NewRecorder()
			handleObjects(s.server, w, req)

			s.Equal(tc.wantCode, w.Code)
			body := w.Body.String()
			for _, want := range tc.wantContains {
				s.Contains(body, want)
			}
			for _, notWant := range tc.wantNotContains {
				s.NotContains(body, notWant)
			}
			for k, v := range tc.wantHeader {
				s.Equal(v, w.Header().Get(k))
			}
		})
	}
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

// --- POST /buckets/{name}/upload ---

func (s *ObjectsTestSuite) TestHandleUploadFile() {
	testCases := []struct {
		caseName   string
		setup      func()
		bucketName string
		prefix     string
		filename   string
		content    []byte
		omitFile   bool
		wantCode   int
		wantKey    string // if non-empty, verify this key landed in the bucket
	}{
		{
			caseName:   "success at root",
			setup:      func() { s.createBucket("up") },
			bucketName: "up",
			prefix:     "",
			filename:   "hello.txt",
			content:    []byte("hello"),
			wantCode:   http.StatusOK,
			wantKey:    "hello.txt",
		},
		{
			caseName: "success with prefix",
			setup: func() {
				s.createBucket("upp")
				s.Require().NoError(s.server.Buckets.CreateFolder(context.Background(), "upp", "docs"))
			},
			bucketName: "upp",
			prefix:     "docs",
			filename:   "report.txt",
			content:    []byte("content"),
			wantCode:   http.StatusOK,
			wantKey:    "docs/report.txt",
		},
		{
			caseName:   "nonexistent bucket",
			setup:      func() {},
			bucketName: "nope",
			prefix:     "",
			filename:   "x.txt",
			content:    []byte("x"),
			wantCode:   http.StatusNotFound,
		},
		{
			caseName:   "missing file field",
			setup:      func() { s.createBucket("upn") },
			bucketName: "upn",
			prefix:     "",
			omitFile:   true,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			tc.setup()

			body := &bytes.Buffer{}
			mw := multipart.NewWriter(body)
			s.Require().NoError(mw.WriteField("prefix", tc.prefix))
			if !tc.omitFile {
				fw, err := mw.CreateFormFile("file", tc.filename)
				s.Require().NoError(err)
				_, err = fw.Write(tc.content)
				s.Require().NoError(err)
			}

			s.Require().NoError(mw.Close())

			req := httptest.NewRequest("POST", "/buckets/"+tc.bucketName+"/upload", body)
			req.Header.Set("Content-Type", mw.FormDataContentType())
			req.Header.Set("HX-Request", "true")
			req.SetPathValue("name", tc.bucketName)
			w := httptest.NewRecorder()
			handleUploadFile(s.server, w, req)

			s.Equal(tc.wantCode, w.Code)
			if tc.wantKey != "" {
				strg, err := s.server.Buckets.Get(context.Background(), tc.bucketName)
				s.Require().NoError(err)
				exists, err := strg.Exists(context.Background(), tc.wantKey)
				s.Require().NoError(err)
				s.True(exists, "object %q should exist after upload", tc.wantKey)
			}
		})
	}
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

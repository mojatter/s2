package objects

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"
)

type viewSuite struct {
	suite.Suite
	server *server.Server
}

func (s *viewSuite) SetupTest() {
	cfg := server.DefaultConfig()
	cfg.Root = s.T().TempDir()
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
}

func (s *viewSuite) createBucket(name string) {
	s.T().Helper()
	s.Require().NoError(s.server.Buckets.Create(context.Background(), name))
}

func (s *viewSuite) putObject(bucket, key string, content []byte, opts ...s2.ObjectOption) {
	s.T().Helper()
	ctx := context.Background()
	strg, err := s.server.Buckets.Get(ctx, bucket)
	s.Require().NoError(err)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(key, content, opts...)))
}

type ViewTestSuite struct{ viewSuite }

func TestViewTestSuite(t *testing.T) {
	suite.Run(t, &ViewTestSuite{})
}

// --- GET /buckets/{name}/view/{object...} ---

func (s *ViewTestSuite) TestHandleView() {
	s.Run("text file", func() {
		s.createBucket("view")
		s.putObject("view", "hello.txt", []byte("Hello, World!"))

		req := httptest.NewRequest("GET", "/buckets/view/view/hello.txt", nil)
		req.SetPathValue("name", "view")
		req.SetPathValue("object", "hello.txt")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Contains(w.Header().Get("Content-Type"), "text/plain")
		s.Contains(w.Header().Get("Content-Disposition"), "hello.txt")
		s.Equal("Hello, World!", w.Body.String())
	})

	s.Run("image file", func() {
		s.createBucket("view-img")
		s.putObject("view-img", "pic.png", []byte("fake-png"))

		req := httptest.NewRequest("GET", "/buckets/view-img/view/pic.png", nil)
		req.SetPathValue("name", "view-img")
		req.SetPathValue("object", "pic.png")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("image/png", w.Header().Get("Content-Type"))
	})

	s.Run("nested path", func() {
		s.createBucket("view-nest")
		s.server.Buckets.CreateFolder(context.Background(), "view-nest", "a/b")
		s.putObject("view-nest", "a/b/deep.txt", []byte("deep"))

		req := httptest.NewRequest("GET", "/buckets/view-nest/view/a/b/deep.txt", nil)
		req.SetPathValue("name", "view-nest")
		req.SetPathValue("object", "a/b/deep.txt")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("deep", w.Body.String())
	})

	s.Run("nonexistent bucket", func() {
		req := httptest.NewRequest("GET", "/buckets/nope/view/x.txt", nil)
		req.SetPathValue("name", "nope")
		req.SetPathValue("object", "x.txt")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})

	s.Run("nonexistent object", func() {
		s.createBucket("view-miss")

		req := httptest.NewRequest("GET", "/buckets/view-miss/view/missing.txt", nil)
		req.SetPathValue("name", "view-miss")
		req.SetPathValue("object", "missing.txt")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})
}

// --- contentTypeByExt ---

func (s *ViewTestSuite) TestContentTypeByExt() {
	// contentTypeByExt tries mime.TypeByExtension first, then falls
	// back to a built-in switch. The OS MIME database differs between
	// macOS and Linux, so we only assert on properties that hold
	// regardless of the platform.
	testCases := []struct {
		caseName      string
		ext           string
		wantNonEmpty  bool   // result must not be empty
		wantContains  string // result must contain this substring (if non-empty)
	}{
		{caseName: "Go source returns text", ext: ".go", wantNonEmpty: true, wantContains: "text/"},
		{caseName: "JSON", ext: ".json", wantNonEmpty: true, wantContains: "json"},
		{caseName: "CSS", ext: ".css", wantNonEmpty: true, wantContains: "css"},
		{caseName: "PNG image", ext: ".png", wantNonEmpty: true, wantContains: "image/png"},
		{caseName: "WebP image", ext: ".webp", wantNonEmpty: true, wantContains: "image/webp"},
		{caseName: "Wasm binary", ext: ".wasm", wantNonEmpty: true, wantContains: "wasm"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got := contentTypeByExt(tc.ext)
			if tc.wantNonEmpty {
				s.NotEmpty(got)
			}
			if tc.wantContains != "" {
				s.Contains(got, tc.wantContains)
			}
		})
	}
}

// --- GET /buckets/{name}/meta/{object...} ---

func (s *ViewTestSuite) TestHandleMeta() {
	s.Run("basic metadata", func() {
		s.createBucket("meta")
		s.putObject("meta", "doc.txt", []byte("content"))

		req := httptest.NewRequest("GET", "/buckets/meta/meta/doc.txt", nil)
		req.SetPathValue("name", "meta")
		req.SetPathValue("object", "doc.txt")
		w := httptest.NewRecorder()
		handleMeta(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("application/json", w.Header().Get("Content-Type"))

		var resp map[string]any
		s.Require().NoError(json.Unmarshal(w.Body.Bytes(), &resp))
		s.Equal("doc.txt", resp["name"])
		s.Contains(resp["contentType"], "text/plain")
		s.Equal(float64(7), resp["size"])
		s.NotEmpty(resp["lastModified"])
	})

	s.Run("with custom metadata", func() {
		s.createBucket("meta-custom")
		md := s2.Metadata{"author": "test", "version": "1"}
		s.putObject("meta-custom", "with-meta.txt", []byte("x"), s2.WithMetadata(md))

		req := httptest.NewRequest("GET", "/buckets/meta-custom/meta/with-meta.txt", nil)
		req.SetPathValue("name", "meta-custom")
		req.SetPathValue("object", "with-meta.txt")
		w := httptest.NewRecorder()
		handleMeta(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)

		var resp map[string]any
		s.Require().NoError(json.Unmarshal(w.Body.Bytes(), &resp))
		metadata, ok := resp["metadata"].(map[string]any)
		s.True(ok)
		s.Equal("test", metadata["author"])
		s.Equal("1", metadata["version"])
	})

	s.Run("no custom metadata omits field", func() {
		s.createBucket("meta-none")
		s.putObject("meta-none", "plain.txt", []byte("x"))

		req := httptest.NewRequest("GET", "/buckets/meta-none/meta/plain.txt", nil)
		req.SetPathValue("name", "meta-none")
		req.SetPathValue("object", "plain.txt")
		w := httptest.NewRecorder()
		handleMeta(s.server, w, req)

		var resp map[string]any
		s.Require().NoError(json.Unmarshal(w.Body.Bytes(), &resp))
		_, hasMetadata := resp["metadata"]
		s.False(hasMetadata)
	})

	s.Run("nonexistent bucket", func() {
		req := httptest.NewRequest("GET", "/buckets/nope/meta/x.txt", nil)
		req.SetPathValue("name", "nope")
		req.SetPathValue("object", "x.txt")
		w := httptest.NewRecorder()
		handleMeta(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})

	s.Run("nonexistent object", func() {
		s.createBucket("meta-miss")

		req := httptest.NewRequest("GET", "/buckets/meta-miss/meta/missing.txt", nil)
		req.SetPathValue("name", "meta-miss")
		req.SetPathValue("object", "missing.txt")
		w := httptest.NewRecorder()
		handleMeta(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})
}

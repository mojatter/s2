package objects

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"syscall"
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

	s.Run("stored Content-Type takes precedence over the extension guess", func() {
		s.createBucket("view-ct")
		// Neither the extension table nor a sniff of "{}" would ever
		// answer "application/json" on its own, so a match here can only
		// come from the stored Content-Type this test sets.
		s.putObject("view-ct", "report", []byte("{}"), s2.WithContentType("application/json"))

		req := httptest.NewRequest("GET", "/buckets/view-ct/view/report", nil)
		req.SetPathValue("name", "view-ct")
		req.SetPathValue("object", "report")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("application/json", w.Header().Get("Content-Type"))
	})

	s.Run("unrecognized extension resolves to the default", func() {
		// The console must hand the browser a type even when the
		// extension says nothing.
		s.createBucket("view-unk")
		s.putObject("view-unk", "notes.nopesuchtype", []byte("plain words"))

		req := httptest.NewRequest("GET", "/buckets/view-unk/view/notes.nopesuchtype", nil)
		req.SetPathValue("name", "view-unk")
		req.SetPathValue("object", "notes.nopesuchtype")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal(server.DefaultContentType, w.Header().Get("Content-Type"))
	})

	s.Run("served objects are sandboxed", func() {
		// The stored Content-Type is the uploader's, and the preview
		// modal links straight here (#199).
		s.createBucket("view-csp")
		s.putObject("view-csp", "page.html", []byte("<h1>hi</h1>"))

		req := httptest.NewRequest("GET", "/buckets/view-csp/view/page.html", nil)
		req.SetPathValue("name", "view-csp")
		req.SetPathValue("object", "page.html")
		w := httptest.NewRecorder()
		handleView(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("sandbox", w.Header().Get("Content-Security-Policy"))
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

	s.Run("stored Content-Type takes precedence over the extension guess", func() {
		s.createBucket("meta-ct")
		s.putObject("meta-ct", "report", []byte("{}"), s2.WithContentType("application/json"))

		req := httptest.NewRequest("GET", "/buckets/meta-ct/meta/report", nil)
		req.SetPathValue("name", "meta-ct")
		req.SetPathValue("object", "report")
		w := httptest.NewRecorder()
		handleMeta(s.server, w, req)

		var resp map[string]any
		s.Require().NoError(json.Unmarshal(w.Body.Bytes(), &resp))
		s.Equal("application/json", resp["contentType"])
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

	s.Run("ETag and Content-Type are not listed as metadata", func() {
		s.createBucket("meta-internal")
		s.putObject("meta-internal", "doc.txt", []byte("x"), s2.WithMetadata(s2.Metadata{"author": "test"}), s2.WithContentType("text/plain"))

		req := httptest.NewRequest("GET", "/buckets/meta-internal/meta/doc.txt", nil)
		req.SetPathValue("name", "meta-internal")
		req.SetPathValue("object", "doc.txt")
		w := httptest.NewRecorder()
		handleMeta(s.server, w, req)

		var resp map[string]any
		s.Require().NoError(json.Unmarshal(w.Body.Bytes(), &resp))
		metadata, ok := resp["metadata"].(map[string]any)
		s.Require().True(ok)
		s.Equal(map[string]any{"author": "test"}, metadata)
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

// --- GET /buckets/{name}/preview/{object...} ---

func (s *ViewTestSuite) TestHandlePreview() {
	testCases := []struct {
		caseName        string
		setup           func()
		bucketName      string
		objectName      string
		wantCode        int
		wantContains    []string
		wantNotContains []string
	}{
		{
			caseName: "text file embeds content",
			setup: func() {
				s.createBucket("prv")
				s.putObject("prv", "hello.txt", []byte("hello world"))
			},
			bucketName:   "prv",
			objectName:   "hello.txt",
			wantCode:     http.StatusOK,
			wantContains: []string{"hello world", "hello.txt"},
		},
		{
			caseName: "image renders via view URL, not inline bytes",
			setup: func() {
				s.createBucket("prvi")
				s.putObject("prvi", "pic.png", []byte("fake-png-bytes"))
			},
			bucketName:      "prvi",
			objectName:      "pic.png",
			wantCode:        http.StatusOK,
			wantContains:    []string{"pic.png", "<img"},
			wantNotContains: []string{"fake-png-bytes"},
		},
		{
			caseName: "pdf renders an iframe",
			setup: func() {
				s.createBucket("prvp")
				s.putObject("prvp", "doc.pdf", []byte("%PDF-fake"))
			},
			bucketName:   "prvp",
			objectName:   "doc.pdf",
			wantCode:     http.StatusOK,
			wantContains: []string{"doc.pdf", "<iframe"},
		},
		{
			caseName: "custom metadata appears in the table",
			setup: func() {
				s.createBucket("prvm")
				md := s2.Metadata{"author": "tester"}
				s.putObject("prvm", "meta.txt", []byte("x"), s2.WithMetadata(md))
			},
			bucketName:   "prvm",
			objectName:   "meta.txt",
			wantCode:     http.StatusOK,
			wantContains: []string{"author", "tester"},
		},
		{
			caseName: "view URL escapes the bucket and the key (#341)",
			setup: func() {
				s.createBucket("prv#h")
				s.putObject("prv#h", "a#b.png", []byte("fake-png-bytes"))
			},
			bucketName:   "prv#h",
			objectName:   "a#b.png",
			wantCode:     http.StatusOK,
			wantContains: []string{`src="/buckets/prv%23h/view/a%23b.png"`},
		},
		{
			caseName:        "nonexistent bucket",
			setup:           func() {},
			bucketName:      "nope",
			objectName:      "x.txt",
			wantCode:        http.StatusNotFound,
			wantContains:    []string{"bucket not found"},
			wantNotContains: []string{"nope"},
		},
		{
			caseName:        "nonexistent object",
			setup:           func() { s.createBucket("prvx") },
			bucketName:      "prvx",
			objectName:      "missing.txt",
			wantCode:        http.StatusNotFound,
			wantContains:    []string{"object not found"},
			wantNotContains: []string{"missing.txt"},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			tc.setup()

			req := httptest.NewRequest("GET", "/buckets/"+tc.bucketName+"/preview/"+tc.objectName, nil)
			req.SetPathValue("name", tc.bucketName)
			req.SetPathValue("object", tc.objectName)
			w := httptest.NewRecorder()
			handlePreview(s.server, w, req)

			s.Equal(tc.wantCode, w.Code)
			body := w.Body.String()
			for _, want := range tc.wantContains {
				s.Contains(body, want)
			}
			for _, notWant := range tc.wantNotContains {
				s.NotContains(body, notWant)
			}
		})
	}
}

const typeBroken = s2.Type("broken-view-test")

// brokenStorage fails Get of broken.txt as an unreachable backend would, and the Open of gone.txt and eio.txt.
// The Length of short.txt is 1, below its stored bytes, and that of huge.txt is one over DefaultMaxPreviewSize.
type brokenStorage struct {
	s2.Storage
}

func (b brokenStorage) Sub(ctx context.Context, prefix string) (s2.Storage, error) {
	sub, err := b.Storage.Sub(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return brokenStorage{sub}, nil
}

func (b brokenStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	if name == "broken.txt" {
		return nil, errors.New("backend unreachable")
	}
	obj, err := b.Storage.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	switch name {
	case "gone.txt":
		return brokenObject{Object: obj, err: fmt.Errorf("%w: gone.txt", s2.ErrNotExist)}, nil
	case "eio.txt":
		return brokenObject{Object: obj, err: syscall.EIO}, nil
	case "short.txt":
		return shortObject{obj}, nil
	case "huge.txt":
		return hugeObject{obj}, nil
	}
	return obj, nil
}

// shortObject reports a Length of 1 whatever it stores.
type shortObject struct {
	s2.Object
}

func (shortObject) Length() uint64 {
	return 1
}

// hugeObject reports a Length one over DefaultMaxPreviewSize whatever it stores.
type hugeObject struct {
	s2.Object
}

func (hugeObject) Length() uint64 {
	return server.DefaultMaxPreviewSize + 1
}

// brokenObject fails its Open with err.
type brokenObject struct {
	s2.Object
	err error
}

func (o brokenObject) Open() (io.ReadCloser, error) {
	return nil, o.err
}

// brokenServer returns a server whose buckets are brokenStorage, with bucket eb holding names.
func (s *ViewTestSuite) brokenServer(names ...string) *server.Server {
	s.T().Helper()
	root := s.T().TempDir()
	base, err := s2.NewStorage(context.Background(), s2.Config{Type: s2.TypeOSFS, Root: root})
	s.Require().NoError(err)
	s2.RegisterNewStorageFunc(typeBroken, func(context.Context, s2.Config) (s2.Storage, error) {
		return brokenStorage{base}, nil
	})
	s.T().Cleanup(func() { s2.UnregisterNewStorageFunc(typeBroken) })
	cfg := server.DefaultConfig()
	cfg.Root = root
	cfg.Type = typeBroken
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.Require().NoError(srv.Buckets.Create(context.Background(), "eb"))
	strg, err := srv.Buckets.Get(context.Background(), "eb")
	s.Require().NoError(err)
	for _, name := range names {
		s.Require().NoError(strg.Put(context.Background(), s2.NewObjectBytes(name, []byte("0123456789"))))
	}
	return srv
}

// TestErrorStatus checks the status follows the error, not the handler (#418).
func (s *ViewTestSuite) TestErrorStatus() {
	srv := s.brokenServer("gone.txt", "eio.txt")

	handlers := []struct {
		caseName string
		handler  func(*server.Server, http.ResponseWriter, *http.Request)
		opens    bool
	}{
		{caseName: "view", handler: handleView, opens: true},
		{caseName: "meta", handler: handleMeta},
		{caseName: "preview", handler: handlePreview, opens: true},
	}
	testCases := []struct {
		caseName   string
		bucket     string
		object     string
		opens      bool
		wantStatus int
	}{
		{caseName: "backend failure", bucket: "eb", object: "broken.txt", wantStatus: http.StatusInternalServerError},
		{caseName: "invalid name", bucket: "eb", object: "a/../b.txt", wantStatus: http.StatusBadRequest},
		{caseName: "missing object", bucket: "eb", object: "missing.txt", wantStatus: http.StatusNotFound},
		{caseName: "missing bucket", bucket: "nope", object: "x.txt", wantStatus: http.StatusNotFound},
		{caseName: "object gone before Open", bucket: "eb", object: "gone.txt", opens: true, wantStatus: http.StatusNotFound},
		{caseName: "Open fails", bucket: "eb", object: "eio.txt", opens: true, wantStatus: http.StatusInternalServerError},
	}
	for _, h := range handlers {
		for _, tc := range testCases {
			if tc.opens && !h.opens {
				continue
			}
			s.Run(h.caseName+"/"+tc.caseName, func() {
				req := httptest.NewRequest(http.MethodGet, "/buckets/x/"+h.caseName+"/x", nil)
				req.SetPathValue("name", tc.bucket)
				req.SetPathValue("object", tc.object)
				w := httptest.NewRecorder()
				h.handler(srv, w, req)
				s.Equal(tc.wantStatus, w.Code)
			})
		}
	}
}

// TestHandlePreviewSizeLimit checks a text preview never reads past MaxPreviewSize (#463).
func (s *ViewTestSuite) TestHandlePreviewSizeLimit() {
	srv := s.brokenServer("fits.txt", "large.txt", "short.txt", "eio.txt", "huge.txt")
	testCases := []struct {
		caseName     string
		object       string
		maxSize      int64
		wantContains string
		wantNoText   bool
	}{
		{caseName: "within the limit", object: "fits.txt", maxSize: 10, wantContains: "<pre>0123456789</pre>"},
		{caseName: "Length over the limit", object: "large.txt", maxSize: 9, wantContains: "File too large to preview.", wantNoText: true},
		{caseName: "stored bytes over the limit", object: "short.txt", maxSize: 9, wantContains: "File too large to preview.", wantNoText: true},
		{caseName: "Open skipped over the limit", object: "eio.txt", maxSize: 9, wantContains: "File too large to preview.", wantNoText: true},
		{caseName: "zero falls back to the default", object: "fits.txt", maxSize: 0, wantContains: "<pre>0123456789</pre>"},
		{caseName: "zero caps at the default", object: "huge.txt", maxSize: 0, wantContains: "File too large to preview.", wantNoText: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			srv.Config.MaxPreviewSize = tc.maxSize
			req := httptest.NewRequest(http.MethodGet, "/buckets/eb/preview/"+tc.object, nil)
			req.SetPathValue("name", "eb")
			req.SetPathValue("object", tc.object)
			w := httptest.NewRecorder()
			handlePreview(srv, w, req)
			s.Equal(http.StatusOK, w.Code)
			s.Contains(w.Body.String(), tc.wantContains)
			if tc.wantNoText {
				s.NotContains(w.Body.String(), "<pre>")
			}
		})
	}
}

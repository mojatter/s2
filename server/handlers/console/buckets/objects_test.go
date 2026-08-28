package buckets

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"

	_ "github.com/mojatter/s2/server/handlers/console"                 // registers GET /static/{filepath...}
	_ "github.com/mojatter/s2/server/handlers/console/buckets/objects" // registers GET /buckets/{name}/view/{object...}
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

	s.Run("explicit deny on the exact key is not bypassed by a wildcard allow", func() {
		s.createBucket("fld3")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{server.ActionPutObject}, Resource: []string{"arn:aws:s3:::fld3/*"}},
			{Effect: "Deny", Action: []string{server.ActionPutObject}, Resource: []string{"arn:aws:s3:::fld3/secret"}},
		}}}

		form := url.Values{"prefix": {""}, "folder_name": {"secret"}}
		req := httptest.NewRequest("POST", "/buckets/fld3/folders", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetPathValue("name", "fld3")
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleCreateFolder(s.server, w, req)

		s.Equal(http.StatusForbidden, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "fld3")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), "secret/.keep")
		s.Require().NoError(err)
		s.False(exists)
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

	s.Run("explicit deny on the exact filename is not bypassed by a wildcard allow", func() {
		s.createBucket("upd")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{server.ActionPutObject}, Resource: []string{"arn:aws:s3:::upd/*"}},
			{Effect: "Deny", Action: []string{server.ActionPutObject}, Resource: []string{"arn:aws:s3:::upd/secret.txt"}},
		}}}

		body := &bytes.Buffer{}
		mw := multipart.NewWriter(body)
		s.Require().NoError(mw.WriteField("prefix", ""))
		fw, err := mw.CreateFormFile("file", "secret.txt")
		s.Require().NoError(err)
		_, err = fw.Write([]byte("leaked"))
		s.Require().NoError(err)
		s.Require().NoError(mw.Close())

		req := httptest.NewRequest("POST", "/buckets/upd/upload", body)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.SetPathValue("name", "upd")
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleUploadFile(s.server, w, req)

		s.Equal(http.StatusForbidden, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "upd")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), "secret.txt")
		s.Require().NoError(err)
		s.False(exists)
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

	s.Run("recursive delete aborts entirely when one descendant is denied", func() {
		s.createBucket("delp")
		s.server.Buckets.CreateFolder(context.Background(), "delp", "dir")
		s.putObject("delp", "dir/keep.txt", "must survive")
		s.putObject("delp", "dir/other.txt", "data")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{server.ActionDeleteObject}, Resource: []string{"arn:aws:s3:::delp/dir/*"}},
			{Effect: "Deny", Action: []string{server.ActionDeleteObject}, Resource: []string{"arn:aws:s3:::delp/dir/keep.txt"}},
		}}}

		req := httptest.NewRequest("DELETE", "/buckets/delp/objects?key=dir/&prefix=", nil)
		req.SetPathValue("name", "delp")
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusForbidden, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "delp")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), "dir/keep.txt")
		s.Require().NoError(err)
		s.True(exists, "keep.txt must survive since it was explicitly denied")
		exists, err = strg.Exists(context.Background(), "dir/other.txt")
		s.Require().NoError(err)
		s.True(exists, "other.txt must also survive: the whole operation aborts on any denial")
	})

	s.Run("denial past the first list page (1000 objects) is still honored", func() {
		s.createBucket("delbig")
		s.server.Buckets.CreateFolder(context.Background(), "delbig", "dir")

		const total = 1200
		for i := range total {
			s.putObject("delbig", fmt.Sprintf("dir/obj-%04d.txt", i), "data")
		}
		// Lexicographically last, so it only appears once List's default
		// 1000-item page is exhausted and a second page is fetched.
		deniedKey := fmt.Sprintf("dir/obj-%04d.txt", total-1)

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{server.ActionDeleteObject}, Resource: []string{"arn:aws:s3:::delbig/dir/*"}},
			{Effect: "Deny", Action: []string{server.ActionDeleteObject}, Resource: []string{"arn:aws:s3:::delbig/" + deniedKey}},
		}}}

		req := httptest.NewRequest("DELETE", "/buckets/delbig/objects?key=dir/&prefix=", nil)
		req.SetPathValue("name", "delbig")
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusForbidden, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "delbig")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), deniedKey)
		s.Require().NoError(err)
		s.True(exists, "the denied object beyond the first page must survive")
		// Nothing should have been deleted either, since the check runs
		// to completion across all pages before any delete happens.
		exists, err = strg.Exists(context.Background(), "dir/obj-0000.txt")
		s.Require().NoError(err)
		s.True(exists, "even an allowed object from the first page must survive: the whole operation aborts on any denial")
	})
}

// tinyPNG is a 1x1 transparent PNG, used as gallery thumbnail fixture content.
var tinyPNG = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
	0x0b, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x64, 0x60, 0x00, 0x00,
	0x00, 0x06, 0x00, 0x03, 0x36, 0x05, 0x24, 0xdf, 0x00, 0x00, 0x00, 0x00,
	0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
}

// TestGalleryView_ThumbnailAndPersistenceAcrossReload drives a real headless
// Chrome against the Web Console to catch the class of bug where gallery
// thumbnail lazy-loading and view-mode persistence only run on htmx-driven
// navigation but silently no-op on a full page load/reload. Requires a local
// Chrome/Chromium; skipped with `go test -short`.
func (s *ObjectsTestSuite) TestGalleryView_ThumbnailAndPersistenceAcrossReload() {
	if testing.Short() {
		s.T().Skip("skipping browser test in short mode")
	}

	s.createBucket("gallery")
	ctx := context.Background()
	strg, err := s.server.Buckets.Get(ctx, "gallery")
	s.Require().NoError(err)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("photo.png", tinyPNG)))

	ts := httptest.NewServer(s.server.ConsoleHandler())
	defer ts.Close()

	// --no-sandbox avoids Chrome sandbox-init failures seen on some CI
	// runners; not needed locally but harmless there.
	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:], chromedp.NoSandbox)
	// chromedp's auto-detection tries "chromium"/"chromium-browser" before
	// "google-chrome". On GitHub's ubuntu-latest runner, chromium is a snap
	// shim that fails to launch headless (actions/runner-images#12096);
	// google-chrome works, so prefer it explicitly when present. Falls back
	// to chromedp's own auto-detect elsewhere (e.g. macOS's Chrome.app).
	if p, err := exec.LookPath("google-chrome"); err == nil {
		allocOpts = append(allocOpts, chromedp.ExecPath(p))
	}
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	defer cancelAlloc()

	browserCtx, cancel := chromedp.NewContext(allocCtx)
	defer cancel()
	// This deadline covers the whole test (both s.Run subtests share
	// browserCtx). 15s was enough locally but flaked in CI with "chrome
	// failed to start: context deadline exceeded" on a busy runner.
	browserCtx, cancelTimeout := context.WithTimeout(browserCtx, 30*time.Second)
	defer cancelTimeout()

	pageURL := ts.URL + "/buckets/gallery?prefix="

	s.Run("thumbnail loads after switching to gallery view", func() {
		s.Require().NoError(chromedp.Run(browserCtx,
			chromedp.Navigate(pageURL),
			chromedp.WaitVisible(`button[title="Gallery View"]`),
			chromedp.Click(`button[title="Gallery View"]`),
			chromedp.WaitVisible(`.gallery-thumb img`),
		))
	})

	s.Run("gallery view and thumbnail survive a full reload", func() {
		s.Require().NoError(chromedp.Run(browserCtx,
			chromedp.Reload(),
			chromedp.WaitVisible(`#gallery-view.gallery-grid`),
		))

		var galleryDisplay string
		s.Require().NoError(chromedp.Run(browserCtx,
			chromedp.EvaluateAsDevTools(
				`getComputedStyle(document.getElementById('gallery-view')).display`,
				&galleryDisplay,
			),
		))
		s.NotEqual("none", galleryDisplay)

		s.Require().NoError(chromedp.Run(browserCtx,
			chromedp.WaitVisible(`.gallery-thumb img`),
		))
	})
}

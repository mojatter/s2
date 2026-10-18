package buckets

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is required for S3-compatible ETag
	"errors"
	"fmt"
	"html"
	"iter"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/require"
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
		historyRestore  bool
		wantCode        int
		wantContains    []string
		wantNotContains []string
		wantHeader      map[string]string
	}{
		{
			caseName:       "a restore past the history cache gets the whole page",
			setup:          func() { s.createBucket("restore"); s.putObject("restore", "readme.txt", "hello") },
			bucketName:     "restore",
			url:            "/buckets/restore",
			htmx:           true,
			historyRestore: true,
			wantCode:       http.StatusOK,
			wantContains:   []string{`class="sidebar"`, `id="main-content"`, "readme.txt"},
		},
		{
			caseName:       "a restore without HX-Request gets the whole page",
			setup:          func() { s.createBucket("restore-plain"); s.putObject("restore-plain", "readme.txt", "hello") },
			bucketName:     "restore-plain",
			url:            "/buckets/restore-plain",
			historyRestore: true,
			wantCode:       http.StatusOK,
			wantContains:   []string{`class="sidebar"`, "readme.txt"},
		},
		{
			caseName:       "a restore to a missing bucket is still not found",
			setup:          func() {},
			bucketName:     "nope-restore",
			url:            "/buckets/nope-restore",
			htmx:           true,
			historyRestore: true,
			wantCode:       http.StatusNotFound,
			wantContains:   []string{"bucket not found"},
		},
		{
			caseName:     "a page load gets the whole page",
			setup:        func() { s.createBucket("page"); s.putObject("page", "readme.txt", "hello") },
			bucketName:   "page",
			url:          "/buckets/page",
			wantCode:     http.StatusOK,
			wantContains: []string{`class="sidebar"`, "readme.txt"},
		},
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
			caseName:        "with objects",
			setup:           func() { s.createBucket("files"); s.putObject("files", "readme.txt", "hello") },
			bucketName:      "files",
			url:             "/buckets/files",
			htmx:            true,
			wantCode:        http.StatusOK,
			wantContains:    []string{"readme.txt"},
			wantNotContains: []string{`class="sidebar"`},
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
			// A search term is free text, not a path: the storage refuses
			// "." as a name, but typing it must still find the dotfiles.
			caseName: "search takes a term the storage refuses as a name",
			setup: func() {
				s.createBucket("srchd")
				s.putObject("srchd", ".hidden.txt", "data")
				s.putObject("srchd", "..dots.txt", "data")
				s.putObject("srchd", "plain.txt", "data")
			},
			bucketName:      "srchd",
			url:             "/buckets/srchd?search=.",
			htmx:            true,
			wantCode:        http.StatusOK,
			wantContains:    []string{".hidden.txt", "..dots.txt"},
			wantNotContains: []string{"plain.txt"},
		},
		{
			caseName: "search takes a term that is two dots",
			setup: func() {
				s.createBucket("srchdd")
				s.putObject("srchdd", "..dots.txt", "data")
				s.putObject("srchdd", ".hidden.txt", "data")
			},
			bucketName:      "srchdd",
			url:             "/buckets/srchdd?search=..",
			htmx:            true,
			wantCode:        http.StatusOK,
			wantContains:    []string{"..dots.txt"},
			wantNotContains: []string{".hidden.txt"},
		},
		{
			caseName: "search inside a folder takes such a term too",
			setup: func() {
				s.createBucket("srchdf")
				s.putObject("srchdf", "docs/.notes.txt", "data")
				s.putObject("srchdf", "docs/plain.txt", "data")
			},
			bucketName:      "srchdf",
			url:             "/buckets/srchdf?prefix=docs&search=.",
			htmx:            true,
			wantCode:        http.StatusOK,
			wantContains:    []string{"docs/.notes.txt"},
			wantNotContains: []string{"docs/plain.txt"},
		},
		{
			caseName: "a term that matches nothing is an empty list, not an error",
			setup: func() {
				s.createBucket("srche")
				s.putObject("srche", "readme.txt", "data")
			},
			bucketName:   "srche",
			url:          "/buckets/srche?search=a%2F%2Fb",
			htmx:         true,
			wantCode:     http.StatusOK,
			wantContains: []string{"This folder is empty"},
		},
		{
			// A backend reserves names of its own, which the shared
			// validator cannot know about. Same rule: it is a term.
			caseName: "search takes a term the backend reserves",
			setup: func() {
				s.createBucket("srchm")
				s.putObject("srchm", ".metadata.json", "data")
				s.putObject("srchm", "plain.txt", "data")
			},
			bucketName:      "srchm",
			url:             "/buckets/srchm?search=.meta",
			htmx:            true,
			wantCode:        http.StatusOK,
			wantContains:    []string{".metadata.json"},
			wantNotContains: []string{"plain.txt"},
		},
		{
			// The folder is a path, and one the storage refuses is still a
			// bad request -- the link that produced it is what is wrong.
			caseName:   "a folder the storage refuses is still rejected",
			setup:      func() { s.createBucket("srchbf"); s.putObject("srchbf", "a.txt", "data") },
			bucketName: "srchbf",
			url:        "/buckets/srchbf?prefix=..%2Fother&search=a",
			htmx:       true,
			wantCode:   http.StatusBadRequest,
		},
		{
			// The folder link names an object, not a directory. It selects no
			// key, so the folder is empty rather than a broken link.
			caseName:     "a folder that names an object is empty",
			setup:        func() { s.createBucket("fold"); s.putObject("fold", "a.txt", "data") },
			bucketName:   "fold",
			url:          "/buckets/fold?prefix=a.txt",
			htmx:         true,
			wantCode:     http.StatusOK,
			wantContains: []string{"This folder is empty"},
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
			if tc.historyRestore {
				req.Header.Set("HX-History-Restore-Request", "true")
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
			s.Equal("HX-Request, HX-History-Restore-Request", w.Header().Get("Vary"))
			for k, v := range tc.wantHeader {
				s.Equal(v, w.Header().Get(k))
			}
		})
	}
}

// TestSearchPagesTheFolder drives the fallback behind a term the storage
// refuses as a prefix. Its own memfs server: the case needs more objects than
// one backend page, which is slow to lay down on a temp directory.
func TestSearchPagesTheFolder(t *testing.T) {
	ctx := context.Background()
	cfg := server.DefaultConfig()
	cfg.Type = s2.TypeMemFS
	srv, err := server.NewServer(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Buckets.Create(ctx, "paged"))
	strg, err := srv.Buckets.Get(ctx, "paged")
	require.NoError(t, err)

	// "!" sorts below ".", so the match is past the backend's first page.
	for i := range 1200 {
		require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(fmt.Sprintf("!%04d.txt", i), []byte("data"))))
	}
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(".hit.txt", []byte("data"))))

	req := httptest.NewRequest("GET", "/buckets/paged?search=.", nil)
	req.SetPathValue("name", "paged")
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	handleObjects(srv, w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), ".hit.txt")
}

// TestListTruncatedNotice checks the console says so when a listing stops at a backend page, rather than cutting it silently.
func TestListTruncatedNotice(t *testing.T) {
	testCases := []struct {
		caseName   string
		key        string
		count      int
		url        string
		extra      string
		wantNotice string
		wantShown  string
		wantGone   string
	}{
		{caseName: "small folder", key: "k%04d", count: 10, url: "/buckets/b"},
		{caseName: "folder past one page", key: "k%04d", count: 1001, url: "/buckets/b", wantNotice: "there may be more entries."},
		{caseName: "search past one page", key: "k%04d", count: 1001, url: "/buckets/b?search=k", wantNotice: "more objects may match."},
		// The fallback keeps one page of matches, as the pushed-down search does (#270).
		{caseName: "refused term past one page of matches", key: ".d%04d", count: maxSearchMatches + 1, url: "/buckets/b?search=.", wantNotice: "more objects may match.", wantShown: ".d0999", wantGone: ".d1000"},
		// The bucket's .keep is a match too, so this is exactly one page: no notice.
		{caseName: "refused term filling one page of matches", key: ".d%04d", count: maxSearchMatches - 1, url: "/buckets/b?search=."},
		// A refused term scans at most maxSearchFetches pages; "!" sorts below ".", so none of them holds the match.
		{caseName: "refused term past the scan", key: "!%05d", count: maxSearchFetches * 1000, extra: ".hit", url: "/buckets/b?search=.", wantNotice: "more objects may match.", wantGone: ".hit"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			ctx := context.Background()
			cfg := server.DefaultConfig()
			cfg.Type = s2.TypeMemFS
			srv, err := server.NewServer(ctx, cfg)
			require.NoError(t, err)
			require.NoError(t, srv.Buckets.Create(ctx, "b"))
			strg, err := srv.Buckets.Get(ctx, "b")
			require.NoError(t, err)
			for i := range tc.count {
				require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(fmt.Sprintf(tc.key, i), []byte("x"))))
			}
			if tc.extra != "" {
				require.NoError(t, strg.Put(ctx, s2.NewObjectBytes(tc.extra, []byte("x"))))
			}

			req := httptest.NewRequest("GET", tc.url, nil)
			req.SetPathValue("name", "b")
			req.Header.Set("HX-Request", "true")
			w := httptest.NewRecorder()
			handleObjects(srv, w, req)

			require.Equal(t, http.StatusOK, w.Code)
			if tc.wantNotice == "" {
				require.NotContains(t, w.Body.String(), "list-truncated")
				return
			}
			require.Contains(t, w.Body.String(), tc.wantNotice)
			if tc.wantShown != "" {
				require.Contains(t, w.Body.String(), tc.wantShown)
			}
			if tc.wantGone != "" {
				require.NotContains(t, w.Body.String(), tc.wantGone)
			}
		})
	}
}

// TestFolderLinksCarryOneSlash checks a folder link carries one "/", not the listed one plus the template's (#315).
func TestFolderLinksCarryOneSlash(t *testing.T) {
	ctx := context.Background()
	cfg := server.DefaultConfig()
	cfg.Type = s2.TypeMemFS
	srv, err := server.NewServer(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Buckets.Create(ctx, "b"))
	strg, err := srv.Buckets.Get(ctx, "b")
	require.NoError(t, err)
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("FOLDER/x.txt", []byte("x"))))

	req := httptest.NewRequest("GET", "/buckets/b", nil)
	req.SetPathValue("name", "b")
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	handleObjects(srv, w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "?prefix=FOLDER/")
	require.NotContains(t, w.Body.String(), "FOLDER//")
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

	s.Run("missing bucket", func() {
		form := url.Values{"prefix": {""}, "folder_name": {"docs"}}
		req := httptest.NewRequest("POST", "/buckets/fld-ghost/folders", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetPathValue("name", "fld-ghost")
		w := httptest.NewRecorder()
		handleCreateFolder(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
		exists, err := s.server.Buckets.Exists(context.Background(), "fld-ghost")
		s.Require().NoError(err)
		s.False(exists)
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

	// The marker is the object that lands, so a Deny covering where it lands
	// applies even though it does not match the folder's own name.
	s.Run("explicit deny on the prefix the marker lands in", func() {
		s.createBucket("fld4")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{server.ActionPutObject}, Resource: []string{"arn:aws:s3:::fld4/*"}},
			{Effect: "Deny", Action: []string{server.ActionPutObject}, Resource: []string{"arn:aws:s3:::fld4/private/*"}},
		}}}

		form := url.Values{"prefix": {""}, "folder_name": {"private"}}
		req := httptest.NewRequest("POST", "/buckets/fld4/folders", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetPathValue("name", "fld4")
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleCreateFolder(s.server, w, req)

		s.Equal(http.StatusForbidden, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "fld4")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), "private/.keep")
		s.Require().NoError(err)
		s.False(exists)
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

// renderObjects returns the objects fragment the console renders for prefix in bucket.
func (s *ObjectsTestSuite) renderObjects(bucket, prefix string) string {
	s.T().Helper()
	req := httptest.NewRequest("GET", "/buckets/"+url.PathEscape(bucket)+"?prefix="+url.QueryEscape(prefix), nil)
	req.SetPathValue("name", bucket)
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	handleObjects(s.server, w, req)
	s.Require().Equal(http.StatusOK, w.Code, w.Body.String())
	return w.Body.String()
}

// renderBuckets returns the bucket list the console renders for buckets.
func (s *ObjectsTestSuite) renderBuckets(buckets ...string) string {
	s.T().Helper()
	var buf bytes.Buffer
	s.Require().NoError(s.server.Template.ExecuteTemplate(&buf, "console/buckets/list.html", struct{ Buckets []string }{Buckets: buckets}))
	return buf.String()
}

// consoleURLAttr matches a console URL in an href, src or hx-* attribute.
var consoleURLAttr = regexp.MustCompile(`(?:href|data-src|hx-get|hx-post|hx-delete)="(/buckets/[^"]*)"`)

// renderedURLs parses every console URL the fragment carries; a stray "%" is a parse error, not a dropped value.
func (s *ObjectsTestSuite) renderedURLs(body string) []*url.URL {
	s.T().Helper()
	var urls []*url.URL
	for _, m := range consoleURLAttr.FindAllStringSubmatch(body, -1) {
		u, err := url.Parse(html.UnescapeString(m[1]))
		s.Require().NoError(err)
		_, err = url.ParseQuery(u.RawQuery)
		s.Require().NoError(err, m[1])
		urls = append(urls, u)
	}
	return urls
}

// TestConsoleURLsCarryNamesAsWritten checks every URL the console renders names a bucket, object or folder exactly (#341).
func (s *ObjectsTestSuite) TestConsoleURLsCarryNamesAsWritten() {
	testCases := []struct {
		caseName string
		name     string
	}{
		{caseName: "hash", name: "a#b"},
		{caseName: "plus", name: "a+b"},
		{caseName: "ampersand", name: "a&b"},
		{caseName: "percent", name: "a%b"},
		{caseName: "question mark", name: "a?b"},
		{caseName: "space", name: "a b"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			// A directory under the root browses as a bucket whatever its name.
			bucket := "esc " + tc.name
			s.Require().NoError(os.Mkdir(filepath.Join(s.server.Config.Root, bucket), 0o755))
			// A .png renders in the gallery too, so its data-src is checked.
			file := tc.name + ".png"
			folder := tc.name + "/"
			s.putObject(bucket, file, "x")
			s.putObject(bucket, folder+file, "x")

			// Every key or prefix a URL may carry: the root listing, the folder listing (breadcrumb, parent, delete) and the forms.
			known := map[string]bool{tc.name: true, folder: true, file: true, folder + file: true}
			seen := map[string]bool{}
			bucketList := s.renderedURLs(s.renderBuckets(bucket))
			s.NotEmpty(bucketList, "bucket list links %q", bucket)
			for _, u := range slices.Concat(bucketList, s.renderedURLs(s.renderObjects(bucket, "")), s.renderedURLs(s.renderObjects(bucket, folder))) {
				rest, ok := strings.CutPrefix(u.Path, "/buckets/"+bucket)
				s.True(ok, "%q names bucket %q", u, bucket)
				for _, kind := range []string{"/view/", "/preview/"} {
					if name, ok := strings.CutPrefix(rest, kind); ok {
						s.True(known[name], "%q names an object", u)
						seen[kind] = true
					}
				}
				for _, k := range []string{"key", "prefix"} {
					if v, ok := u.Query()[k]; ok {
						// Only a prefix may be empty: the root listing's own links.
						s.True(known[v[0]] || k == "prefix" && v[0] == "", "%q names a %s", u, k)
						seen[k+"="+v[0]] = true
					}
				}
			}
			for _, want := range []string{"/view/", "/preview/", "key=" + file, "key=" + folder, "prefix=" + folder} {
				s.True(seen[want], "a URL carries %s", want)
			}
		})
	}
}

// hxGet matches any start tag that issues an hx-get.
var hxGet = regexp.MustCompile(`<[a-z]+\s[^>]*\bhx-get="[^>]*>`)

// TestNavigationsSyncOnMainContent checks every GET into #main-content replaces the one in flight (#422).
func (s *ObjectsTestSuite) TestNavigationsSyncOnMainContent() {
	s.createBucket("nav")
	s.putObject("nav", "f/g/x.png", "x")
	s.putObject("nav", "f/x.png", "x")
	// render answers the objects fragment for query, as htmx asks for it.
	render := func(query string) string {
		req := httptest.NewRequest("GET", "/buckets/nav?"+query, nil)
		req.SetPathValue("name", "nav")
		req.Header.Set("HX-Request", "true")
		w := httptest.NewRecorder()
		handleObjects(s.server, w, req)
		s.Require().Equal(http.StatusOK, w.Code, w.Body.String())
		return w.Body.String()
	}
	testCases := []struct {
		caseName string
		body     func() string
		minTags  int
	}{
		{caseName: "bucket list", body: func() string { return s.renderBuckets("nav", "other") }, minTags: 2},
		{caseName: "root listing", body: func() string { return render("prefix=") }, minTags: 4},
		{caseName: "folder listing", body: func() string { return render("prefix=f/") }, minTags: 7},
		{caseName: "searched listing", body: func() string { return render("prefix=f/&search=x") }, minTags: 4},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			var tags []string
			for _, tag := range hxGet.FindAllString(tc.body(), -1) {
				if strings.Contains(tag, `hx-target="#main-content"`) {
					tags = append(tags, tag)
				}
			}
			// A floor, so a regex that stops matching cannot pass on nothing.
			s.GreaterOrEqual(len(tags), tc.minTags)
			for _, tag := range tags {
				s.Contains(tag, `hx-sync="#main-content:replace"`)
			}
		})
	}
}

// TestDeleteButtonDeletesItsOwnObject sends the rendered delete URL of "a#b" and checks "a" survives (#341).
func (s *ObjectsTestSuite) TestDeleteButtonDeletesItsOwnObject() {
	s.createBucket("del")
	s.putObject("del", "a", "keep")
	s.putObject("del", "a#b", "drop")

	var target string
	for _, m := range regexp.MustCompile(`hx-delete="([^"]*)" hx-confirm="Are you sure you want to delete file '([^']*)'`).FindAllStringSubmatch(s.renderObjects("del", ""), -1) {
		if html.UnescapeString(m[2]) == "a#b" {
			target = html.UnescapeString(m[1])
		}
	}
	s.Require().NotEmpty(target)
	// A browser drops the fragment before sending, which httptest.NewRequest would keep in the query.
	u, err := url.Parse(target)
	s.Require().NoError(err)
	u.Fragment = ""

	req := httptest.NewRequest("DELETE", u.String(), nil)
	req.SetPathValue("name", "del")
	w := httptest.NewRecorder()
	handleDeleteObject(s.server, w, req)
	s.Require().Equal(http.StatusOK, w.Code, w.Body.String())

	strg, err := s.server.Buckets.Get(context.Background(), "del")
	s.Require().NoError(err)
	exists, err := strg.Exists(context.Background(), "a")
	s.Require().NoError(err)
	s.True(exists, "a must survive deleting a#b")
	exists, err = strg.Exists(context.Background(), "a#b")
	s.Require().NoError(err)
	s.False(exists, "a#b must be deleted")
}

// TestCreateFolderRejectsAFoldingName checks a folder name that is not one path element is refused, not folded (#271).
func (s *ObjectsTestSuite) TestCreateFolderRejectsAFoldingName() {
	s.createBucket("fold")
	s.Require().NoError(s.server.Buckets.CreateFolder(context.Background(), "fold", "photos/2024"))
	strg, err := s.server.Buckets.Get(context.Background(), "fold")
	s.Require().NoError(err)
	keys := func() []string {
		res, err := strg.List(context.Background(), s2.ListOptions{Recursive: true})
		s.Require().NoError(err)
		var names []string
		for _, obj := range res.Objects {
			names = append(names, obj.Name())
		}
		return names
	}
	before := keys()

	testCases := []struct {
		caseName   string
		prefix     string
		folderName string
	}{
		{caseName: "parent then name", prefix: "photos/2024", folderName: "../evil"},
		{caseName: "parent", prefix: "photos/2024", folderName: ".."},
		{caseName: "current", prefix: "photos/2024", folderName: "."},
		{caseName: "nested", prefix: "photos/2024", folderName: "a/b"},
		{caseName: "trailing slash", prefix: "photos/2024", folderName: "evil/"},
		{caseName: "leading slash", prefix: "", folderName: "/escape"},
		// A hand-crafted prefix is not folded either: the joined key fails s2.ValidateName, and nothing is written.
		{caseName: "folding prefix", prefix: "photos/../x", folderName: "evil"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			form := url.Values{"prefix": {tc.prefix}, "folder_name": {tc.folderName}}
			req := httptest.NewRequest("POST", "/buckets/fold/folders", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.SetPathValue("name", "fold")
			w := httptest.NewRecorder()
			handleCreateFolder(s.server, w, req)

			s.Equal(http.StatusBadRequest, w.Code, w.Body.String())
			s.Equal(before, keys())
		})
	}
}

func (s *ObjectsTestSuite) TestConsoleBucketSegmentIsOnePathElement() {
	ctx := context.Background()
	s.createBucket("b1")
	strg, err := s.server.Buckets.Get(ctx, "b1")
	s.Require().NoError(err)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("private/secret.txt", []byte("TOPSECRET"),
		s2.WithContentType("text/plain"), s2.WithMetadata(s2.Metadata{"k": "v"}))))

	ts := httptest.NewServer(s.server.ConsoleHandler())
	defer ts.Close()

	// The console takes its bucket from the same {name} wildcard the S3 API
	// does, so an escaped separator scopes it to a prefix too. Both muxes are
	// covered by the check in server.Buckets, not by either handler.
	testCases := []struct {
		caseName string
		method   string
		target   string
	}{
		{caseName: "the sidecar directory", method: http.MethodGet, target: "/buckets/b1%2F.meta"},
		{caseName: "a sub-prefix", method: http.MethodGet, target: "/buckets/b1%2Fprivate"},
		{caseName: "delete through the sidecar directory", method: http.MethodDelete, target: "/buckets/b1%2F.meta/objects?key=private%2Fsecret.txt"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req, err := http.NewRequest(tc.method, ts.URL+tc.target, nil)
			s.Require().NoError(err)
			resp, err := ts.Client().Do(req)
			s.Require().NoError(err)

			defer func() { _ = resp.Body.Close() }()

			s.Equal(http.StatusNotFound, resp.StatusCode)
		})
	}

	obj, err := strg.Get(ctx, "private/secret.txt")
	s.Require().NoError(err)
	s.Equal("text/plain", obj.ContentType())
	s.Equal(s2.Metadata{"k": "v"}, obj.Metadata())
}

func (s *ObjectsTestSuite) TestConsoleAnswers400ForARefusedName() {
	s.createBucket("rej")
	allowAll := &server.User{Policy: &server.Policy{Statement: []server.Statement{
		{Effect: "Allow", Action: []string{"s3:*"}, Resource: []string{"arn:aws:s3:::rej/*"}},
	}}}

	// Without the mapping these read as 404 or 500, and htmx leaves the page as
	// it was -- the rejected name looks like nothing happened.
	testCases := []struct {
		caseName string
		user     *server.User
		call     func(w http.ResponseWriter, r *http.Request)
		req      func() *http.Request
	}{
		{
			caseName: "list an escaping prefix",
			call:     func(w http.ResponseWriter, r *http.Request) { handleObjects(s.server, w, r) },
			req: func() *http.Request {
				return httptest.NewRequest("GET", "/buckets/rej?prefix=..%2Fother", nil)
			},
		},
		{
			// The key passes isPathElement and s2.ValidateName; the fs storage refuses its reserved metadata directory.
			caseName: "create a folder named the metadata directory",
			call:     func(w http.ResponseWriter, r *http.Request) { handleCreateFolder(s.server, w, r) },
			req: func() *http.Request {
				form := url.Values{"prefix": {""}, "folder_name": {".meta"}}
				r := httptest.NewRequest("POST", "/buckets/rej/folders", strings.NewReader(form.Encode()))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				return r
			},
		},
		{
			caseName: "delete an escaping key",
			call:     func(w http.ResponseWriter, r *http.Request) { handleDeleteObject(s.server, w, r) },
			req: func() *http.Request {
				return httptest.NewRequest("DELETE", "/buckets/rej/objects?key=..%2Fother.txt", nil)
			},
		},
		{
			// A policy sends the recursive delete down its own paginated path.
			caseName: "delete an escaping folder as a policy holder",
			user:     allowAll,
			call:     func(w http.ResponseWriter, r *http.Request) { handleDeleteObject(s.server, w, r) },
			req: func() *http.Request {
				return httptest.NewRequest("DELETE", "/buckets/rej/objects?key=..%2Fother%2F", nil)
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req := tc.req()
			req.SetPathValue("name", "rej")
			if tc.user != nil {
				req = req.WithContext(server.WithUser(req.Context(), tc.user))
			}
			w := httptest.NewRecorder()
			tc.call(w, req)
			s.Equal(http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
}

const typeBrokenList = s2.Type("broken-list-test")

// brokenListStorage fails every List, Put and Delete as an unreachable backend would.
type brokenListStorage struct {
	s2.Storage
}

func (b brokenListStorage) Sub(ctx context.Context, prefix string) (s2.Storage, error) {
	sub, err := b.Storage.Sub(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return brokenListStorage{sub}, nil
}

func (brokenListStorage) List(context.Context, s2.ListOptions) (s2.ListResult, error) {
	return s2.ListResult{}, errors.New("backend unreachable")
}

func (brokenListStorage) Put(context.Context, s2.Object) error {
	return errors.New("backend unreachable")
}

func (brokenListStorage) Delete(context.Context, string) error {
	return errors.New("backend unreachable")
}

// TestConsoleAnswers500ForABackendFailure checks a backend failure is not answered as a missing bucket or object (#418).
func (s *ObjectsTestSuite) TestConsoleAnswers500ForABackendFailure() {
	root := s.T().TempDir()
	base, err := s2.NewStorage(context.Background(), s2.Config{Type: s2.TypeOSFS, Root: root})
	s.Require().NoError(err)
	s2.RegisterNewStorageFunc(typeBrokenList, func(context.Context, s2.Config) (s2.Storage, error) {
		return brokenListStorage{base}, nil
	})
	s.T().Cleanup(func() { s2.UnregisterNewStorageFunc(typeBrokenList) })
	// The bucket is made through the working storage, since the broken one cannot write.
	plain := server.DefaultConfig()
	plain.Root = root
	plainSrv, err := server.NewServer(context.Background(), plain)
	s.Require().NoError(err)
	s.Require().NoError(plainSrv.Buckets.Create(context.Background(), "bb"))
	cfg := server.DefaultConfig()
	cfg.Root = root
	cfg.Type = typeBrokenList
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)

	testCases := []struct {
		caseName string
		call     func(w http.ResponseWriter, r *http.Request)
		req      func() *http.Request
	}{
		{
			caseName: "list as a full page",
			call:     func(w http.ResponseWriter, r *http.Request) { handleObjects(srv, w, r) },
			req:      func() *http.Request { return httptest.NewRequest("GET", "/buckets/bb", nil) },
		},
		{
			caseName: "list as an htmx fragment",
			call:     func(w http.ResponseWriter, r *http.Request) { handleObjects(srv, w, r) },
			req: func() *http.Request {
				r := httptest.NewRequest("GET", "/buckets/bb", nil)
				r.Header.Set("HX-Request", "true")
				return r
			},
		},
		{
			caseName: "upload",
			call:     func(w http.ResponseWriter, r *http.Request) { handleUploadFile(srv, w, r) },
			req: func() *http.Request {
				body := &bytes.Buffer{}
				mw := multipart.NewWriter(body)
				fw, err := mw.CreateFormFile("file", "x.txt")
				s.Require().NoError(err)
				_, _ = fw.Write([]byte("x"))
				s.Require().NoError(mw.Close())
				r := httptest.NewRequest("POST", "/buckets/bb/upload", body)
				r.Header.Set("Content-Type", mw.FormDataContentType())
				return r
			},
		},
		{
			caseName: "delete",
			call:     func(w http.ResponseWriter, r *http.Request) { handleDeleteObject(srv, w, r) },
			req:      func() *http.Request { return httptest.NewRequest("DELETE", "/buckets/bb/objects?key=x.txt", nil) },
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req := tc.req()
			req.SetPathValue("name", "bb")
			w := httptest.NewRecorder()
			tc.call(w, req)
			s.Equal(http.StatusInternalServerError, w.Code)
			s.Equal("Internal Server Error\n", w.Body.String())
		})
	}
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
		partName   string // if non-empty, the file part's name instead of "file"
		maxUpload  int64  // if non-zero, the upload size limit for this case
		wantCode   int
		wantShort  bool   // if set, the body read must stop at the limit plus the framing allowance (#340)
		declared   int64  // if non-zero, the Content-Length the request declares; -1 is chunked
		wantUnread bool   // if set, the handler must not read the body at all
		wantNoTemp bool   // if set, no multipart-* temp file may remain once the handler returns
		noSpoolDir bool   // if set, the temp directory multipart spools to does not exist
		wantBody   string // if non-empty, the response body must contain it
		wantKey    string // if non-empty, verify this key landed in the bucket
		wantAbsent string // if non-empty, verify this key did not land in the bucket
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
			// A bucket name the storage refuses is a bad request, as it is
			// for every other console handler.
			caseName:   "a bucket name the storage refuses",
			setup:      func() {},
			bucketName: "\xff",
			prefix:     "",
			filename:   "x.txt",
			content:    []byte("x"),
			wantCode:   http.StatusBadRequest,
		},
		{
			// A name the console would fold onto the prefix's parent is refused (#271).
			caseName:   "parent as the file name",
			setup:      func() { s.createBucket("upd") },
			bucketName: "upd",
			prefix:     "notyet/sub",
			filename:   "..",
			content:    []byte("x"),
			wantCode:   http.StatusBadRequest,
			wantAbsent: "notyet",
		},
		{
			caseName:   "current as the file name",
			setup:      func() { s.createBucket("upc") },
			bucketName: "upc",
			prefix:     "notyet/sub",
			filename:   ".",
			content:    []byte("x"),
			wantCode:   http.StatusBadRequest,
			wantAbsent: "notyet/sub",
		},
		{
			// A hand-crafted prefix is not folded either: the joined key fails s2.ValidateName, and nothing is written.
			caseName:   "folding prefix",
			setup:      func() { s.createBucket("upf") },
			bucketName: "upf",
			prefix:     "photos/../x",
			filename:   "evil.txt",
			content:    []byte("x"),
			wantCode:   http.StatusBadRequest,
			wantAbsent: "x/evil.txt",
		},
		{
			caseName:   "missing file field",
			setup:      func() { s.createBucket("upn") },
			bucketName: "upn",
			prefix:     "",
			omitFile:   true,
			wantCode:   http.StatusBadRequest,
		},
		{
			// A file of exactly the limit lands.
			caseName:   "exactly the upload size limit",
			setup:      func() { s.createBucket("upe") },
			bucketName: "upe",
			prefix:     "docs",
			filename:   "exact.bin",
			content:    bytes.Repeat([]byte("x"), 1024),
			maxUpload:  1024,
			wantCode:   http.StatusOK,
			wantKey:    "docs/exact.bin",
		},
		{
			caseName:   "one byte over the upload size limit",
			setup:      func() { s.createBucket("upo") },
			bucketName: "upo",
			prefix:     "docs",
			filename:   "over.bin",
			content:    bytes.Repeat([]byte("x"), 1025),
			maxUpload:  1024,
			wantCode:   http.StatusRequestEntityTooLarge,
			wantBody:   "max 1024 bytes",
			wantAbsent: "docs/over.bin",
		},
		{
			// Past the framing allowance the body read itself stops, before the file ends (#340).
			caseName:   "far over the upload size limit",
			setup:      func() { s.createBucket("upf2") },
			bucketName: "upf2",
			prefix:     "docs",
			filename:   "huge.bin",
			content:    bytes.Repeat([]byte("x"), 2<<20),
			maxUpload:  1024,
			declared:   -1, // chunked, so the bounded read, not the length check, refuses it
			wantCode:   http.StatusRequestEntityTooLarge,
			wantShort:  true,
			wantBody:   "max 1024 bytes",
			wantAbsent: "docs/huge.bin",
		},
		{
			// A declared length past the cap is refused before the body is read.
			caseName:   "declared length over the upload size limit",
			setup:      func() { s.createBucket("upd2") },
			bucketName: "upd2",
			prefix:     "docs",
			filename:   "declared.bin",
			content:    bytes.Repeat([]byte("x"), 16),
			maxUpload:  1024,
			declared:   1024 + uploadFormOverhead + 1,
			wantCode:   http.StatusRequestEntityTooLarge,
			wantBody:   "max 1024 bytes",
			wantAbsent: "docs/declared.bin",
			wantUnread: true,
		},
		{
			// Past multipart's 32 MiB memory cap the file is spooled to disk; the handler removes it, since net/http never sees BasicAuth's request copy.
			caseName:   "spooled to disk leaves no temp file",
			setup:      func() { s.createBucket("upsp") },
			bucketName: "upsp",
			prefix:     "docs",
			filename:   "spool.bin",
			content:    bytes.Repeat([]byte("x"), 32<<20+1),
			maxUpload:  64 << 20,
			wantCode:   http.StatusOK,
			wantKey:    "docs/spool.bin",
			wantNoTemp: true,
		},
		{
			// The form parses, so its spool file exists, before the missing part is reported.
			caseName:   "spooled part under another name leaves no temp file",
			setup:      func() { s.createBucket("upsn") },
			bucketName: "upsn",
			prefix:     "docs",
			filename:   "spool.bin",
			partName:   "other",
			content:    bytes.Repeat([]byte("x"), 32<<20+1),
			maxUpload:  64 << 20,
			wantCode:   http.StatusBadRequest,
			wantBody:   http.ErrMissingFile.Error(),
			wantAbsent: "docs/spool.bin",
			wantNoTemp: true,
		},
		{
			// Failing to spool is the server's fault, as on a read-only root or the pre-#358 Docker image.
			caseName:   "no temp directory to spool to",
			setup:      func() { s.createBucket("upnt") },
			bucketName: "upnt",
			prefix:     "docs",
			filename:   "spool.bin",
			content:    bytes.Repeat([]byte("x"), 32<<20+1),
			maxUpload:  64 << 20,
			noSpoolDir: true,
			wantCode:   http.StatusInternalServerError,
			wantBody:   "Internal Server Error",
			wantAbsent: "docs/spool.bin",
		},
		{
			caseName:   "one byte under the upload size limit",
			setup:      func() { s.createBucket("upu1") },
			bucketName: "upu1",
			prefix:     "docs",
			filename:   "under.bin",
			content:    bytes.Repeat([]byte("x"), 1023),
			maxUpload:  1024,
			wantCode:   http.StatusOK,
			wantKey:    "docs/under.bin",
		},
		{
			caseName:   "within the upload size limit",
			setup:      func() { s.createBucket("ups") },
			bucketName: "ups",
			prefix:     "docs",
			filename:   "small.bin",
			content:    bytes.Repeat([]byte("x"), 16),
			maxUpload:  1024,
			wantCode:   http.StatusOK,
			wantKey:    "docs/small.bin",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			tc.setup()
			if tc.maxUpload != 0 {
				prev := s.server.Config.MaxUploadSize
				s.server.Config.MaxUploadSize = tc.maxUpload
				defer func() { s.server.Config.MaxUploadSize = prev }()
			}

			body := &bytes.Buffer{}
			mw := multipart.NewWriter(body)
			s.Require().NoError(mw.WriteField("prefix", tc.prefix))
			if !tc.omitFile {
				fw, err := mw.CreateFormFile(cmp.Or(tc.partName, "file"), tc.filename)
				s.Require().NoError(err)
				_, err = fw.Write(tc.content)
				s.Require().NoError(err)
			}

			s.Require().NoError(mw.Close())

			if tc.wantNoTemp || tc.noSpoolDir {
				dir := s.T().TempDir() // where multipart spools; the recorder runs none of net/http's cleanup
				if tc.noSpoolDir {
					dir = filepath.Join(dir, "missing")
				}
				s.T().Setenv("TMPDIR", dir)
			}
			sent := body.Len() // what the handler consumes is what is gone from the buffer afterwards
			req := httptest.NewRequest("POST", "/buckets/"+tc.bucketName+"/upload", body)
			if tc.declared != 0 {
				req.ContentLength = tc.declared
			}
			req.Header.Set("Content-Type", mw.FormDataContentType())
			req.Header.Set("HX-Request", "true")
			req.SetPathValue("name", tc.bucketName)
			w := httptest.NewRecorder()
			handleUploadFile(s.server, w, req)

			s.Equal(tc.wantCode, w.Code)
			read := int64(sent - body.Len())
			if tc.wantUnread {
				s.Zero(read, "a declared length past the cap must be refused unread")
			}
			if tc.wantShort {
				// MaxBytesReader reads at most one byte past its limit.
				s.LessOrEqual(read, tc.maxUpload+uploadFormOverhead+1, "the body read should stop at the limit plus the framing allowance")
			}
			if tc.wantBody != "" {
				s.Contains(w.Body.String(), tc.wantBody)
			}
			if tc.wantNoTemp {
				left, err := filepath.Glob(filepath.Join(os.TempDir(), "multipart-*"))
				s.Require().NoError(err)
				s.Empty(left, "the spooled form must be removed")
			}
			if tc.wantKey != "" {
				strg, err := s.server.Buckets.Get(context.Background(), tc.bucketName)
				s.Require().NoError(err)
				exists, err := strg.Exists(context.Background(), tc.wantKey)
				s.Require().NoError(err)
				s.True(exists, "object %q should exist after upload", tc.wantKey)
			}
			if tc.wantAbsent != "" {
				strg, err := s.server.Buckets.Get(context.Background(), tc.bucketName)
				s.Require().NoError(err)
				exists, err := strg.Exists(context.Background(), tc.wantAbsent)
				s.Require().NoError(err)
				s.False(exists, "object %q should not exist after upload", tc.wantAbsent)
			}
		})
	}

	s.Run("upload records the ETag and the browser's Content-Type", func() {
		s.createBucket("upm")
		content := []byte("<h1>hi</h1>")

		body := &bytes.Buffer{}
		mw := multipart.NewWriter(body)
		s.Require().NoError(mw.WriteField("prefix", ""))
		hdr := make(textproto.MIMEHeader)
		hdr.Set("Content-Disposition", `form-data; name="file"; filename="page.html"`)
		hdr.Set("Content-Type", "text/html")
		fw, err := mw.CreatePart(hdr)
		s.Require().NoError(err)
		_, err = fw.Write(content)
		s.Require().NoError(err)
		s.Require().NoError(mw.Close())

		req := httptest.NewRequest("POST", "/buckets/upm/upload", body)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.SetPathValue("name", "upm")
		w := httptest.NewRecorder()
		handleUploadFile(s.server, w, req)
		s.Equal(http.StatusOK, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "upm")
		s.Require().NoError(err)
		obj, err := strg.Get(context.Background(), "page.html")
		s.Require().NoError(err)

		s.Equal("text/html", obj.ContentType())
		s.Equal(fmt.Sprintf("%q", fmt.Sprintf("%x", md5.Sum(content))), obj.ETag())
	})

	s.Run("generic part Content-Type is replaced by the extension guess", func() {
		// Browsers send application/octet-stream for any extension the OS does not know; S3 and Azure would store their default.
		s.createBucket("upg")
		testCases := []struct {
			caseName string
			filename string
			partType string
			want     string
		}{
			// The exact type depends on the host mime database, so compare with the guess itself.
			{caseName: "known extension", filename: "app.log", partType: "application/octet-stream", want: server.ContentTypeByExt(".log")},
			{caseName: "no part type", filename: "app.log", want: server.ContentTypeByExt(".log")},
			{caseName: "no extension", filename: "README", partType: "application/octet-stream", want: ""},
		}
		for _, tc := range testCases {
			s.Run(tc.caseName, func() {
				body := &bytes.Buffer{}
				mw := multipart.NewWriter(body)
				s.Require().NoError(mw.WriteField("prefix", ""))
				hdr := make(textproto.MIMEHeader)
				hdr.Set("Content-Disposition", `form-data; name="file"; filename="`+tc.filename+`"`)
				if tc.partType != "" {
					hdr.Set("Content-Type", tc.partType)
				}
				fw, err := mw.CreatePart(hdr)
				s.Require().NoError(err)
				_, err = fw.Write([]byte("log line"))
				s.Require().NoError(err)
				s.Require().NoError(mw.Close())

				req := httptest.NewRequest("POST", "/buckets/upg/upload", body)
				req.Header.Set("Content-Type", mw.FormDataContentType())
				req.SetPathValue("name", "upg")
				w := httptest.NewRecorder()
				handleUploadFile(s.server, w, req)
				s.Equal(http.StatusOK, w.Code)

				strg, err := s.server.Buckets.Get(context.Background(), "upg")
				s.Require().NoError(err)
				obj, err := strg.Get(context.Background(), tc.filename)
				s.Require().NoError(err)

				s.Equal(tc.want, obj.ContentType())
			})
		}
	})

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

	s.Run("a bucket name the storage refuses", func() {
		req := httptest.NewRequest("DELETE", "/buckets/x/objects?key=a.txt&prefix=", nil)
		req.SetPathValue("name", "\xff")
		req.Header.Set("HX-Request", "true")
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code, w.Body.String())
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

	s.Run("recursive delete stops at the first denied descendant", func() {
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

		s.Equal(http.StatusOK, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "delp")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), "dir/keep.txt")
		s.Require().NoError(err)
		s.True(exists, "keep.txt must survive since it was explicitly denied")
		exists, err = strg.Exists(context.Background(), "dir/other.txt")
		s.Require().NoError(err)
		// "keep.txt" sorts before "other.txt", so processing stops at
		// keep.txt before other.txt is ever reached.
		s.True(exists, "other.txt must also survive: processing stops at the first denial instead of skipping past it")
	})

	s.Run("denial past the first list page (1000 objects) stops the sweep there", func() {
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

		s.Equal(http.StatusOK, w.Code)

		strg, err := s.server.Buckets.Get(context.Background(), "delbig")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), deniedKey)
		s.Require().NoError(err)
		s.True(exists, "the denied object beyond the first page must survive")
		// Everything before the denied key in listing order -- on both the
		// first page and the start of the second page -- is deleted before
		// the sweep reaches and stops at the denial.
		exists, err = strg.Exists(context.Background(), "dir/obj-0000.txt")
		s.Require().NoError(err)
		s.False(exists, "an allowed object from the first page must still be deleted")
		exists, err = strg.Exists(context.Background(), fmt.Sprintf("dir/obj-%04d.txt", total-2))
		s.Require().NoError(err)
		s.False(exists, "an allowed object immediately before the denied one, on the second page, must still be deleted")
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

// newBrowser serves the console and starts a headless Chrome on it, both closed when the test ends; the test fails on any exception the page left uncaught by then; skipped with `go test -short`.
func (s *ObjectsTestSuite) newBrowser() (context.Context, string) {
	s.T().Helper()
	if testing.Short() {
		s.T().Skip("skipping browser test in short mode")
	}
	ts := httptest.NewServer(s.server.ConsoleHandler())
	s.T().Cleanup(ts.Close)

	// --no-sandbox avoids Chrome sandbox-init failures seen on some CI
	// runners; not needed locally but harmless there.
	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:], chromedp.NoSandbox)
	// Resolve every host but the test server to NOTFOUND, so no network fetch can hold the load event Navigate waits for.
	allocOpts = append(allocOpts, chromedp.Flag("host-resolver-rules", "MAP * ~NOTFOUND, EXCLUDE 127.0.0.1"))
	// Chrome drops clicks dispatched before the first frame of a new document (acked, not delivered); this lets them through, as Puppeteer and Playwright do.
	allocOpts = append(allocOpts, chromedp.Flag("allow-pre-commit-input", true))
	// chromedp's auto-detection tries "chromium"/"chromium-browser" before
	// "google-chrome". On GitHub's ubuntu-latest runner, chromium is a snap
	// shim that fails to launch headless (actions/runner-images#12096);
	// google-chrome works, so prefer it explicitly when present. Falls back
	// to chromedp's own auto-detect elsewhere (e.g. macOS's Chrome.app).
	if p, err := exec.LookPath("google-chrome"); err == nil {
		allocOpts = append(allocOpts, chromedp.ExecPath(p))
	}
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	s.T().Cleanup(cancelAlloc)

	browserCtx, cancel := chromedp.NewContext(allocCtx)
	s.T().Cleanup(cancel)
	// Start Chrome before the step timeout, since a timeout on the first Do would bound the browser's lifetime too.
	s.Require().NoError(chromedp.Do(browserCtx), "start Chrome")
	// Subscribe on a context the step timeout does not bound, so every message of the test reaches the check below.
	consoleCtx, cancelConsole := context.WithCancel(browserCtx)
	s.T().Cleanup(cancelConsole)
	messages := chromedp.Console(consoleCtx)
	// 30s covers the test's steps.
	stepCtx, cancelTimeout := context.WithTimeout(browserCtx, 30*time.Second)
	s.T().Cleanup(cancelTimeout)
	t := s.T()
	// Registered last, so it runs before the browser closes: fail on any exception nothing caught, up to a sentinel logged now.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(browserCtx, 10*time.Second)
		defer cancel()

		stop := context.AfterFunc(ctx, cancelConsole)
		defer stop()

		// htmx removes htmx-settling in the task that fires afterSettle, so its handlers have run once the classes are gone.
		require.NoError(t, chromedp.Do(ctx, chromedp.WaitNotPresent(chromedp.CSS(".htmx-request, .htmx-swapping, .htmx-settling"))), "wait for htmx to settle (a request still in flight or an open alert/confirm dialog blocks it)")
		require.NoError(t, chromedp.Do(ctx, chromedp.Evaluate[chromedp.Void](`console.log("sentinel")`)), "log the sentinel (an open alert/confirm dialog blocks it)")
		var exceptions []string
		seen := false
		for m, err := range messages {
			// The deadline cancels the subscription, so its own error would only say "canceled".
			require.NoError(t, ctx.Err(), "wait for the sentinel")
			require.NoError(t, err, "read the console")
			if m.Type == chromedp.ConsoleLog && m.Text == "sentinel" {
				seen = true
				break
			}
			if m.IsException() {
				text := m.String()
				if m.URL != "" {
					// The protocol counts lines from 0.
					text += fmt.Sprintf(" (%s:%d)", m.URL, m.Line+1)
				}
				exceptions = append(exceptions, text)
			}
		}
		// The iterator can end on the closed subscription without yielding the deadline's error.
		if !seen {
			require.NoError(t, ctx.Err(), "wait for the sentinel")
		}
		require.True(t, seen, "the console closed before the sentinel")
		require.Empty(t, exceptions, "uncaught exceptions on the page")
	})
	return stepCtx, ts.URL
}

// TestBrowserShowsWhyAnUploadWasRefused uploads past the size limit in Chrome and checks the reason reaches an alert, once per pick (#340).
func (s *ObjectsTestSuite) TestBrowserShowsWhyAnUploadWasRefused() {
	browserCtx, consoleURL := s.newBrowser()
	s.createBucket("big")
	prev := s.server.Config.MaxUploadSize
	s.server.Config.MaxUploadSize = 1024
	defer func() { s.server.Config.MaxUploadSize = prev }()

	file := filepath.Join(s.T().TempDir(), "big.bin")
	s.Require().NoError(os.WriteFile(file, bytes.Repeat([]byte("x"), 4096), 0o600))

	fileInput := chromedp.CSS(`input[name="file"]`)
	s.Require().NoError(chromedp.Do(browserCtx,
		chromedp.Navigate(consoleURL+"/buckets/big"),
		chromedp.WaitReady(fileInput),
		// Record alerts instead of opening a dialog headless Chrome would leave open.
		chromedp.Evaluate[chromedp.Void](`window.alerts = []; window.alert = m => window.alerts.push(m)`),
		chromedp.SetUploadFiles(fileInput, []string{file}),
	))
	alerted, err := chromedp.Run(browserCtx, chromedp.Poll[string](`window.alerts[0]`))
	s.Require().NoError(err)
	s.Contains(alerted, "max 1024 bytes")

	// Picking the same file again fires change only if the form was reset after the refusal.
	s.Require().NoError(chromedp.Do(browserCtx, chromedp.SetUploadFiles(fileInput, []string{file})))
	alerted, err = chromedp.Run(browserCtx, chromedp.Poll[string](`window.alerts[1]`))
	s.Require().NoError(err)
	s.Contains(alerted, "max 1024 bytes")

	strg, err := s.server.Buckets.Get(context.Background(), "big")
	s.Require().NoError(err)
	exists, err := strg.Exists(context.Background(), "big.bin")
	s.Require().NoError(err)
	s.False(exists)
}

// TestBrowserActsOnTheClickedName clicks a folder and a delete button named with "#" in Chrome, which drops a URL's fragment (#341).
func (s *ObjectsTestSuite) TestBrowserActsOnTheClickedName() {
	browserCtx, consoleURL := s.newBrowser()
	s.createBucket("hash")
	s.putObject("hash", "a", "keep")
	s.putObject("hash", "a#b", "drop")
	s.putObject("hash", "f#x/inside.txt", "x")

	s.Run("folder link opens the folder", func() {
		s.Require().NoError(chromedp.Do(browserCtx,
			chromedp.Navigate(consoleURL+"/buckets/hash"),
			chromedp.Click(chromedp.CSS(`a[title="f#x/"]`)),
			chromedp.WaitVisible(chromedp.CSS(`span.obj-name[title="inside.txt"]`)),
		))
	})

	s.Run("delete button deletes its own object", func() {
		target := chromedp.CSS(`span.obj-name[title="a#b"]`)
		s.Require().NoError(chromedp.Do(browserCtx,
			chromedp.Navigate(consoleURL+"/buckets/hash"),
			chromedp.WaitVisible(target),
			// hx-confirm asks window.confirm, which headless Chrome would leave open.
			chromedp.Evaluate[chromedp.Void](`window.confirm = () => true`),
			chromedp.Evaluate[chromedp.Void](`document.querySelector('button[title="Delete File"][hx-confirm*="\'a#b\'"]').click()`),
			chromedp.WaitNotPresent(target),
		))
		strg, err := s.server.Buckets.Get(context.Background(), "hash")
		s.Require().NoError(err)
		exists, err := strg.Exists(context.Background(), "a")
		s.Require().NoError(err)
		s.True(exists, "a must survive deleting a#b")
		exists, err = strg.Exists(context.Background(), "a#b")
		s.Require().NoError(err)
		s.False(exists, "a#b must be deleted")
	})
}

// TestGalleryView_ThumbnailAndPersistenceAcrossReload checks in Chrome that gallery thumbnails load and the view mode persists on a full page reload, not only on htmx navigation.
func (s *ObjectsTestSuite) TestGalleryView_ThumbnailAndPersistenceAcrossReload() {
	browserCtx, consoleURL := s.newBrowser()
	s.createBucket("gallery")
	ctx := context.Background()
	strg, err := s.server.Buckets.Get(ctx, "gallery")
	s.Require().NoError(err)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("photo.png", tinyPNG)))

	pageURL := consoleURL + "/buckets/gallery?prefix="
	galleryButton := chromedp.CSS(`button[title="Gallery View"]`)
	thumb := chromedp.CSS(`.gallery-thumb img`)

	s.Run("thumbnail loads after switching to gallery view", func() {
		s.Require().NoError(chromedp.Do(browserCtx,
			chromedp.Navigate(pageURL),
			chromedp.WaitVisible(galleryButton),
			chromedp.Click(galleryButton),
			chromedp.WaitVisible(thumb),
		))
	})

	s.Run("gallery view and thumbnail survive a full reload", func() {
		s.Require().NoError(chromedp.Do(browserCtx,
			chromedp.Reload(),
			chromedp.WaitVisible(chromedp.CSS(`#gallery-view.gallery-grid`)),
		))

		galleryDisplay, err := chromedp.Run(browserCtx, chromedp.Evaluate[string](
			`getComputedStyle(document.getElementById('gallery-view')).display`,
		))
		s.Require().NoError(err)
		s.NotEqual("none", galleryDisplay)

		s.Require().NoError(chromedp.Do(browserCtx,
			chromedp.WaitVisible(thumb),
		))
	})
}

// TestBrowserOverviewRunsTheObjectsScript checks in Chrome that the objects script, loaded on every page, throws nothing on the overview, which has no object views.
func (s *ObjectsTestSuite) TestBrowserOverviewRunsTheObjectsScript() {
	browserCtx, consoleURL := s.newBrowser()
	// newBrowser fails the test on any uncaught exception.
	s.Require().NoError(chromedp.Do(browserCtx, chromedp.Navigate(consoleURL+"/")))
}

// TestBrowserScrollsTheSidebarToTheLastBucket checks in Chrome that the wheel brings a bucket below the window's height into view, in the desktop sidebar and in the drawer (#393).
func (s *ObjectsTestSuite) TestBrowserScrollsTheSidebarToTheLastBucket() {
	for i := range 25 {
		s.createBucket(fmt.Sprintf("b%02d", i))
	}
	// True once the last bucket is inside the window and nothing covers its center.
	const reachable = `(() => {
		const a = document.querySelector('.bucket-item[title="b24"]');
		const r = a.getBoundingClientRect();
		return r.bottom <= innerHeight && document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2)?.closest('.bucket-item') === a;
	})()`
	testCases := []struct {
		caseName string
		width    int64
		drawer   bool
	}{
		{caseName: "desktop sidebar", width: 1280},
		{caseName: "drawer at the breakpoint", width: 900, drawer: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			browserCtx, consoleURL := s.newBrowser()
			s.Require().NoError(chromedp.Do(browserCtx,
				chromedp.EmulateViewport(tc.width, 800),
				chromedp.Navigate(consoleURL+"/"),
			))
			if tc.drawer {
				s.Require().NoError(chromedp.Do(browserCtx, chromedp.Click(chromedp.CSS(".mobile-sidebar-toggle"))))
				// The drawer slides in over 0.2s; aim the wheel only once it has landed.
				_, err := chromedp.Run(browserCtx, chromedp.Poll[bool](`document.querySelector('.sidebar').getBoundingClientRect().left === 0`, chromedp.WithPollingTimeout(5*time.Second)))
				s.Require().NoError(err, "open the drawer")
			}
			s.Require().NoError(chromedp.Do(browserCtx, chromedp.WaitVisible(chromedp.CSS(`.bucket-item[title="b00"]`))))
			reached, err := chromedp.Run(browserCtx, chromedp.Evaluate[bool](reachable))
			s.Require().NoError(err)
			s.Require().False(reached, "25 buckets must overflow an 800px window")

			// Chrome requires deltaX, which cdproto omits when zero; 1px sideways cannot scroll a sidebar that hides overflow-x.
			wheel := func(p *input.DispatchMouseEventParams) { p.DeltaX, p.DeltaY = 1, 5000 }
			s.Require().NoError(chromedp.Do(browserCtx, chromedp.MouseEvent(chromedp.MouseWheel, 100, 400, wheel)))
			_, err = chromedp.Run(browserCtx, chromedp.Poll[bool](reachable, chromedp.WithPollingTimeout(5*time.Second)))
			s.Require().NoError(err, "the wheel must bring the last bucket into view")
		})
	}
}

// TestBrowserRevealsTheOpenBucket checks in Chrome that the open bucket is marked and scrolled into the sidebar's view, whether opened by URL or just created (#403).
func (s *ObjectsTestSuite) TestBrowserRevealsTheOpenBucket() {
	for i := range 25 {
		s.createBucket(fmt.Sprintf("b%02d", i))
	}
	s.createBucket("zz#1")
	// The active entry's name, once it is inside the window and nothing covers its center.
	const revealed = `(() => {
		const a = document.querySelector('.bucket-item.active');
		if (!a) return '';
		const r = a.getBoundingClientRect();
		const shown = r.top >= 0 && r.bottom <= innerHeight && document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2)?.closest('.bucket-item') === a;
		return shown ? a.title : '';
	})()`
	testCases := []struct {
		caseName  string
		width     int64
		collapsed bool
		path      string
		create    string
		back      bool
		wantName  string
	}{
		{caseName: "opened by URL", width: 1280, path: "/buckets/b24", wantName: "b24"},
		{caseName: "opened by URL with a name that needs escaping", width: 1280, path: "/buckets/zz%231", wantName: "zz#1"},
		{caseName: "created from the sidebar", width: 1280, path: "/", create: "zz-new", wantName: "zz-new"},
		{caseName: "opened with the sidebar collapsed, then expanded", width: 1280, collapsed: true, path: "/buckets/b24", wantName: "b24"},
		{caseName: "opened by URL, then the drawer opened", width: 600, collapsed: true, path: "/buckets/b24", wantName: "b24"},
		{caseName: "opened by URL, left, then back", width: 1280, path: "/buckets/b24", back: true, wantName: "b24"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			browserCtx, consoleURL := s.newBrowser()
			s.Require().NoError(chromedp.Do(browserCtx, chromedp.EmulateViewport(tc.width, 800)))
			if tc.collapsed {
				// Each browser starts with empty storage, so the collapsed state is stored before the page under test loads.
				s.Require().NoError(chromedp.Do(browserCtx,
					chromedp.Navigate(consoleURL+"/"),
					chromedp.Evaluate[chromedp.Void](`localStorage.setItem('s2-sidebar-collapsed', 'true')`),
				))
			}
			s.Require().NoError(chromedp.Do(browserCtx,
				chromedp.Navigate(consoleURL+tc.path),
				chromedp.WaitReady(chromedp.CSS(`.bucket-item[title="b00"]`)),
			))
			if tc.collapsed {
				toggle := ".sidebar-toggle"
				if tc.width <= 900 {
					toggle = ".mobile-sidebar-toggle"
				}
				s.Require().NoError(chromedp.Do(browserCtx, chromedp.Click(chromedp.CSS(toggle))))
				// The sidebar widens or slides in over 0.2s; look for the entry only once it has landed.
				_, err := chromedp.Run(browserCtx, chromedp.Poll[bool](`(() => { const r = document.querySelector('.sidebar').getBoundingClientRect(); return r.left === 0 && r.width >= 280; })()`, chromedp.WithPollingTimeout(5*time.Second)))
				s.Require().NoError(err, "open the sidebar")
			}
			if tc.back {
				// A history restore rebuilds the sidebar scrolled to the top, with the entry already marked.
				s.Require().NoError(chromedp.Do(browserCtx, chromedp.Click(chromedp.CSS(`.bucket-item[title="b00"]`))))
				_, err := chromedp.Run(browserCtx, chromedp.Poll[bool](`location.pathname === '/buckets/b00' && document.querySelector('.bucket-item.active')?.title === 'b00' || null`, chromedp.WithPollingTimeout(5*time.Second)))
				s.Require().NoError(err, "open b00")
				s.Require().NoError(chromedp.Do(browserCtx, chromedp.Evaluate[chromedp.Void](`history.back()`)))
			}
			if tc.create != "" {
				s.Require().NoError(chromedp.Do(browserCtx,
					chromedp.Click(chromedp.CSS(".section-header .btn")),
					chromedp.SendKeys(chromedp.CSS(".new-bucket-form input[name=name]"), tc.create),
					chromedp.Click(chromedp.CSS(".new-bucket-form button[type=submit]")),
				))
			}
			got, err := chromedp.Run(browserCtx, chromedp.Poll[string](`(() => { const n = `+revealed+`; return n || null; })()`, chromedp.WithPollingTimeout(5*time.Second)))
			s.Require().NoError(err, "the open bucket must be marked and in view")
			s.Equal(tc.wantName, got)
		})
	}
}

// TestBrowserMarksABucketOnceItOpens checks in Chrome that a sidebar click marks its bucket only when the bucket opens, so Back and a failed open keep the mark on the bucket shown (#403).
func (s *ObjectsTestSuite) TestBrowserMarksABucketOnceItOpens() {
	const active = `document.querySelector('.bucket-item.active')?.title ?? null`
	testCases := []struct {
		caseName string
		gone     bool
		create   string
		back     bool
		miss     bool
		lost     bool
		wantName string
	}{
		{caseName: "back restores the earlier mark", back: true, wantName: "a"},
		{caseName: "back past the history cache restores the page and its mark", back: true, miss: true, wantName: "a"},
		{caseName: "back past the history cache to a deleted bucket shows it is gone", back: true, miss: true, lost: true},
		{caseName: "a bucket that fails to open is not marked", gone: true, wantName: "a"},
		{caseName: "back after a create restores the earlier mark", create: "zz", back: true, wantName: "a"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.createBucket("a")
			s.createBucket("b")
			browserCtx, consoleURL := s.newBrowser()
			s.Require().NoError(chromedp.Do(browserCtx,
				chromedp.EmulateViewport(1280, 800),
				chromedp.Navigate(consoleURL+"/buckets/a"),
				chromedp.WaitVisible(chromedp.CSS(`.bucket-item[title="b"]`)),
			))
			if tc.gone {
				s.Require().NoError(s.server.Buckets.Delete(context.Background(), "b"))
			}
			opened := "b"
			if tc.create != "" {
				opened = tc.create
				s.Require().NoError(chromedp.Do(browserCtx,
					chromedp.Click(chromedp.CSS(".section-header .btn")),
					chromedp.SendKeys(chromedp.CSS(".new-bucket-form input[name=name]"), tc.create),
					chromedp.Click(chromedp.CSS(".new-bucket-form button[type=submit]")),
				))
			} else {
				s.Require().NoError(chromedp.Do(browserCtx, chromedp.Click(chromedp.CSS(`.bucket-item[title="b"]`))))
			}
			// htmx removes htmx-request once the response is handled, whatever its status.
			s.Require().NoError(chromedp.Do(browserCtx, chromedp.WaitNotPresent(chromedp.CSS(".htmx-request, .htmx-settling"))))
			if tc.back {
				_, err := chromedp.Run(browserCtx, chromedp.Poll[string](`location.pathname === '/buckets/`+opened+`' ? 'opened' : null`, chromedp.WithPollingTimeout(5*time.Second)))
				s.Require().NoError(err, "open "+opened)
				if tc.miss {
					// Without its snapshot htmx fetches the page with HX-Request and swaps the response into body.
					s.Require().NoError(chromedp.Do(browserCtx, chromedp.Evaluate[chromedp.Void](`sessionStorage.removeItem('htmx-history-cache')`)))
				}
				if tc.lost {
					s.Require().NoError(s.server.Buckets.Delete(context.Background(), "a"))
					s.Require().NoError(chromedp.Do(browserCtx, chromedp.Evaluate[chromedp.Void](`history.back()`)))
					// The page reloads, which ends any Poll running on it, so each check is its own evaluation.
					s.Eventually(func() bool {
						gone, err := chromedp.Run(browserCtx, chromedp.Evaluate[bool](`location.pathname === '/buckets/a' && document.body.innerText.includes('bucket not found')`))
						return err == nil && gone
					}, 5*time.Second, 100*time.Millisecond, "the restored URL must show that a is gone, not b's page")
					return
				}
				s.Require().NoError(chromedp.Do(browserCtx, chromedp.Evaluate[chromedp.Void](`history.back()`)))
				_, err = chromedp.Run(browserCtx, chromedp.Poll[string](`location.pathname === '/buckets/a' && document.querySelector('#main-content h1')?.textContent === 'a' ? 'a' : null`, chromedp.WithPollingTimeout(5*time.Second)))
				s.Require().NoError(err, "go back to a")
			}
			layouts, err := chromedp.Run(browserCtx, chromedp.Evaluate[int](`document.querySelectorAll('.layout').length`))
			s.Require().NoError(err)
			s.Equal(1, layouts, "one page layout, around the main view")
			got, err := chromedp.Run(browserCtx, chromedp.Evaluate[string](active))
			s.Require().NoError(err)
			s.Equal(tc.wantName, got)
		})
	}
}

// TestBrowserCancelsAHeldNavigation holds a bucket's GET in Chrome, checks the pending entry shows it, and moves on meanwhile, so the held response never lands over the later view (#422).
func (s *ObjectsTestSuite) TestBrowserCancelsAHeldNavigation() {
	testCases := []struct {
		caseName string
		move     func(ctx context.Context) error
		wantURL  string
		wantMark string
		after    func(ctx context.Context) error
	}{
		{
			caseName: "another bucket clicked",
			move: func(ctx context.Context) error {
				return chromedp.Do(ctx, chromedp.Click(chromedp.CSS(`.bucket-item[title="c"]`)))
			},
			wantURL:  "/buckets/c",
			wantMark: "c",
		},
		{
			caseName: "a folder of the open bucket clicked",
			move: func(ctx context.Context) error {
				return chromedp.Do(ctx, chromedp.Click(chromedp.CSS(`#main-content a[title="f/"]`)))
			},
			wantURL:  "/buckets/a?prefix=f/",
			wantMark: "a",
		},
		{
			// The create opens the new bucket by clicking its entry, which replaces the held request too.
			caseName: "a bucket created and opened",
			move: func(ctx context.Context) error {
				return chromedp.Do(ctx,
					chromedp.Click(chromedp.CSS(".section-header .btn")),
					chromedp.SendKeys(chromedp.CSS(".new-bucket-form input[name=name]"), "zz"),
					chromedp.Click(chromedp.CSS(".new-bucket-form button[type=submit]")),
				)
			},
			wantURL:  "/buckets/zz",
			wantMark: "zz",
		},
		{
			// The delete resets the main view out of band, so it aborts the held request itself.
			caseName: "another bucket deleted",
			move: func(ctx context.Context) error {
				return chromedp.Do(ctx,
					// hx-confirm asks window.confirm, which headless Chrome would leave open.
					chromedp.Evaluate[chromedp.Void](`window.confirm = () => true`),
					chromedp.Evaluate[chromedp.Void](`document.querySelector('button[title="Delete Bucket"][hx-delete="/buckets/c"]').click()`),
				)
			},
			wantURL: "/",
		},
		{
			// htmx restores the page without aborting a request in flight, whose response would still push its URL.
			caseName: "back pressed",
			move: func(ctx context.Context) error {
				return chromedp.Do(ctx, chromedp.Evaluate[chromedp.Void](`history.back()`))
			},
			wantURL: "/",
			// Forward restores a page again, which re-runs the scripts in body but not those in head.
			after: func(ctx context.Context) error {
				if err := chromedp.Do(ctx, chromedp.Evaluate[chromedp.Void](`history.forward()`)); err != nil {
					return err
				}
				_, err := chromedp.Run(ctx, chromedp.Poll[bool](`location.pathname === '/buckets/a' || null`, chromedp.WithPollingTimeout(5*time.Second)))
				return err
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.createBucket("a")
			s.createBucket("b")
			s.createBucket("c")
			s.putObject("a", "f/x.txt", "x")
			browserCtx, consoleURL := s.newBrowser()
			var target *chromedp.Target
			s.Require().NoError(chromedp.Do(browserCtx,
				chromedp.EmulateViewport(1280, 800),
				// Counts the popstate listeners added, which a history restore must not add again.
				chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
					_, err := cdp.Call(ctx, t, page.AddScriptToEvaluateOnNewDocument, page.AddScriptToEvaluateOnNewDocumentParams{Source: `window.popstateAdds = 0; const add = window.addEventListener; window.addEventListener = function(type, ...rest) { if (type === 'popstate') window.popstateAdds++; return add.call(this, type, ...rest); };`})
					return err
				}),
				// Opened by a click, so Back has a page to return to.
				chromedp.Navigate(consoleURL+"/"),
				chromedp.Click(chromedp.CSS(`.bucket-item[title="a"]`)),
				chromedp.WaitVisible(chromedp.CSS(`#main-content a[title="f/"]`)),
				chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
					target = t
					_, err := cdp.Call(ctx, t, fetch.Enable, fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: "*/buckets/b"}}})
					return err
				}),
				// Watches b's own xhr: the entry that sent it can be detached before the abort, so htmx's events would not reach document.
				chromedp.Evaluate[chromedp.Void](`window.bAborted = false; window.bDone = false; document.addEventListener('htmx:beforeRequest', e => {
					if (!e.detail.pathInfo.requestPath.endsWith('/buckets/b')) return;
					e.detail.xhr.addEventListener('abort', () => { window.bAborted = true; });
					e.detail.xhr.addEventListener('loadend', () => { window.bDone = true; });
				})`),
			))
			// Subscribed before the click, so the pause cannot be missed.
			next, stop := iter.Pull2(cdp.Events(browserCtx, target, fetch.RequestPaused))
			defer stop()

			s.Require().NoError(chromedp.Do(browserCtx, chromedp.Click(chromedp.CSS(`.bucket-item[title="b"]`))))
			paused, err, ok := next()
			s.Require().True(ok, "the GET of b must pause")
			s.Require().NoError(err)
			// Marked only once b opens; move the pointer off so :hover is not what shows, and outlast the entries' 0.2s transition.
			s.Require().NoError(chromedp.Do(browserCtx, chromedp.MouseEvent(chromedp.MouseMoved, 1000, 700), chromedp.Sleep(500*time.Millisecond)))
			pending, err := chromedp.Run(browserCtx, chromedp.Evaluate[bool](`(() => { const bg = t => getComputedStyle(document.querySelector('.bucket-item[title="' + t + '"]')).backgroundColor; return bg('b') !== bg('c'); })()`))
			s.Require().NoError(err)
			s.True(pending, "the clicked entry must show its request is pending")

			s.Require().NoError(tc.move(browserCtx))
			landed := `location.pathname + location.search === '` + tc.wantURL + `' && (document.querySelector('.bucket-item.active')?.title ?? '') === '` + tc.wantMark + `'`
			_, err = chromedp.Run(browserCtx, chromedp.Poll[bool](landed+` || null`, chromedp.WithPollingTimeout(5*time.Second)))
			s.Require().NoError(err, "move on while b is held")
			aborted, err := chromedp.Run(browserCtx, chromedp.Evaluate[bool](`window.bAborted`))
			s.Require().NoError(err)
			s.True(aborted, "the held request must be aborted")

			// An aborted request is gone from the fetch domain, so its error is not the point.
			_, _ = cdp.Call(browserCtx, target, fetch.ContinueRequest, fetch.ContinueRequestParams{RequestID: paused.RequestID})
			// loadend follows the swap and push of a response that got through, so checking after it catches one.
			_, err = chromedp.Run(browserCtx, chromedp.Poll[bool](`window.bDone || null`, chromedp.WithPollingTimeout(5*time.Second)))
			s.Require().NoError(err, "b's xhr must end")
			stayed, err := chromedp.Run(browserCtx, chromedp.Evaluate[bool](landed+` && !document.querySelector('.bucket-item.htmx-request')`))
			s.Require().NoError(err)
			s.True(stayed, "b must not land over the later view")

			if tc.after != nil {
				s.Require().NoError(tc.after(browserCtx))
			}
			adds, err := chromedp.Run(browserCtx, chromedp.Evaluate[int](`window.popstateAdds`))
			s.Require().NoError(err)
			s.Equal(1, adds, "the popstate listener must be added once")
		})
	}
}

// TestBrowserKeepsTheNameColumnReadable checks in Chrome that the object table hides metadata columns by its own width, so Name stays readable beside the sidebar (#394).
func (s *ObjectsTestSuite) TestBrowserKeepsTheNameColumnReadable() {
	s.createBucket("cols")
	s.putObject("cols", "a-long-object-name-that-runs-well-past-sixty-characters-in-the-list.txt", "x")
	// The visible headers, the cells shown in the object's row, and the room for text inside the Name header.
	const measure = `(() => {
		const shown = el => getComputedStyle(el).display !== 'none';
		const ths = [...document.querySelectorAll('#list-view th')].filter(shown);
		const tds = [...document.querySelectorAll('#list-view tbody tr:last-child td')].filter(shown);
		const st = getComputedStyle(ths[0]);
		const text = ths[0].getBoundingClientRect().width - parseFloat(st.paddingLeft) - parseFloat(st.paddingRight);
		return {headers: ths.map(th => th.textContent.trim()), cells: tds.length, text: text};
	})()`
	type layout struct {
		Headers []string `json:"headers"`
		Cells   int      `json:"cells"`
		Text    float64  `json:"text"`
	}
	testCases := []struct {
		caseName    string
		width       int64
		collapsed   bool
		wantHeaders []string
		minText     float64
	}{
		{caseName: "wide desktop", width: 1280, wantHeaders: []string{"Name", "Size", "Last Modified", "Actions"}, minText: 140},
		{caseName: "desktop just above the breakpoint", width: 901, wantHeaders: []string{"Name", "Size", "Actions"}, minText: 140},
		{caseName: "collapsed sidebar just above the breakpoint", width: 901, collapsed: true, wantHeaders: []string{"Name", "Size", "Last Modified", "Actions"}, minText: 140},
		// The table is 608px beside the open sidebar, under the 650px that hides Last Modified.
		{caseName: "laptop", width: 1024, wantHeaders: []string{"Name", "Size", "Actions"}, minText: 140},
		{caseName: "drawer tier", width: 640, wantHeaders: []string{"Name", "Size", "Actions"}, minText: 140},
		// The table is 448px, under the 470px that hides Size.
		{caseName: "small phone landscape", width: 480, wantHeaders: []string{"Name", "Actions"}, minText: 140},
		// Only Actions is left beside Name, so its text room follows the window.
		{caseName: "phone", width: 375, wantHeaders: []string{"Name", "Actions"}, minText: 120},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			browserCtx, consoleURL := s.newBrowser()
			s.Require().NoError(chromedp.Do(browserCtx,
				chromedp.EmulateViewport(tc.width, 800),
				chromedp.Navigate(consoleURL+"/"),
				chromedp.Evaluate[chromedp.Void](fmt.Sprintf(`localStorage.setItem('s2-sidebar-collapsed', '%t')`, tc.collapsed)),
				chromedp.Navigate(consoleURL+"/buckets/cols"),
				chromedp.WaitVisible(chromedp.CSS(`#list-view th`)),
			))
			got, err := chromedp.Run(browserCtx, chromedp.Evaluate[layout](measure))
			s.Require().NoError(err)
			s.Equal(tc.wantHeaders, got.Headers)
			s.Equal(len(tc.wantHeaders), got.Cells, "the row must hide the same columns as the header")
			s.GreaterOrEqual(got.Text, tc.minText, "the Name column must keep room for a name")
		})
	}
}

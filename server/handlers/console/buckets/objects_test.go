package buckets

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is required for S3-compatible ETag
	"fmt"
	"html"
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

			if tc.wantNoTemp {
				s.T().Setenv("TMPDIR", s.T().TempDir()) // where multipart spools; the recorder runs none of net/http's cleanup
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

// newBrowser serves the console and starts a headless Chrome on it, both closed when the test ends; skipped with `go test -short`.
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
	// 30s covers the whole test; 15s flaked in CI with "chrome failed to start: context deadline exceeded".
	browserCtx, cancelTimeout := context.WithTimeout(browserCtx, 30*time.Second)
	s.T().Cleanup(cancelTimeout)
	return browserCtx, ts.URL
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

	var alerted string
	s.Require().NoError(chromedp.Run(browserCtx,
		chromedp.Navigate(consoleURL+"/buckets/big"),
		chromedp.WaitReady(`input[name="file"]`, chromedp.ByQuery),
		// Record alerts instead of opening a dialog headless Chrome would leave open.
		chromedp.Evaluate(`window.alerts = []; window.alert = m => window.alerts.push(m)`, nil),
		chromedp.SetUploadFiles(`input[name="file"]`, []string{file}, chromedp.ByQuery),
		chromedp.Poll(`window.alerts[0]`, &alerted),
	))
	s.Contains(alerted, "max 1024 bytes")

	// Picking the same file again fires change only if the form was reset after the refusal.
	s.Require().NoError(chromedp.Run(browserCtx,
		chromedp.SetUploadFiles(`input[name="file"]`, []string{file}, chromedp.ByQuery),
		chromedp.Poll(`window.alerts[1]`, &alerted),
	))
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
		s.Require().NoError(chromedp.Run(browserCtx,
			chromedp.Navigate(consoleURL+"/buckets/hash"),
			chromedp.Click(`a[title="f#x/"]`, chromedp.ByQuery),
			chromedp.WaitVisible(`span.obj-name[title="inside.txt"]`, chromedp.ByQuery),
		))
	})

	s.Run("delete button deletes its own object", func() {
		s.Require().NoError(chromedp.Run(browserCtx,
			chromedp.Navigate(consoleURL+"/buckets/hash"),
			chromedp.WaitVisible(`span.obj-name[title="a#b"]`, chromedp.ByQuery),
			// hx-confirm asks window.confirm, which headless Chrome would leave open.
			chromedp.Evaluate(`window.confirm = () => true`, nil),
			chromedp.Evaluate(`document.querySelector('button[title="Delete File"][hx-confirm*="\'a#b\'"]').click()`, nil),
			chromedp.WaitNotPresent(`span.obj-name[title="a#b"]`, chromedp.ByQuery),
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

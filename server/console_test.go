package server

import (
	"context"
	"io/fs"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderConsoleIndex(t *testing.T) {
	restrictedUser := &User{Policy: &Policy{Statement: []Statement{
		allowStatement("s3:ListBucket", "arn:aws:s3:::alpha"),
	}}}

	testCases := []struct {
		caseName        string
		buckets         []string
		user            *User
		wantContains    []string
		wantNotContains []string
	}{
		{
			caseName:     "no buckets renders index page",
			wantContains: []string{`id="main-content"`},
		},
		{
			caseName:     "bucket names appear in rendered page",
			buckets:      []string{"alpha", "bravo"},
			wantContains: []string{`id="main-content"`, "alpha", "bravo"},
		},
		{
			caseName:        "policy filters bucket names in rendered page",
			buckets:         []string{"alpha", "bravo"},
			user:            restrictedUser,
			wantContains:    []string{`id="main-content"`, "alpha"},
			wantNotContains: []string{"bravo"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Root = t.TempDir()
			srv, err := NewServer(context.Background(), cfg)
			require.NoError(t, err)

			for _, name := range tc.buckets {
				require.NoError(t, srv.Buckets.Create(context.Background(), name))
			}

			ctx := WithUser(context.Background(), tc.user)
			w := httptest.NewRecorder()
			require.NoError(t, srv.RenderConsoleIndex(ctx, w, nil))

			body := w.Body.String()
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want)
			}
			for _, notWant := range tc.wantNotContains {
				assert.NotContains(t, body, notWant)
			}
		})
	}
}

// otherHostURL matches an absolute or protocol-relative URL in HTML, CSS or JS.
var otherHostURL = regexp.MustCompile(`(?i)["'(=\x60][ \t]*(https?:)?//`)

// The console references no other host, so it works offline; any absolute URL fails.
func TestConsoleLoadsNothingFromOtherHosts(t *testing.T) {
	// The Google Fonts link the console used to load must match, or the scan below proves nothing.
	require.Regexp(t, otherHostURL, `<link href="https://fonts.googleapis.com/css2?family=Inter" rel="stylesheet">`)

	testCases := []struct {
		caseName string
		fsys     fs.FS
	}{
		{caseName: "templates", fsys: templatesFS},
		{caseName: "static", fsys: StaticFS},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			scanned := 0
			err := fs.WalkDir(tc.fsys, ".", func(name string, d fs.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return err
				}
				b, err := fs.ReadFile(tc.fsys, name)
				if err != nil {
					return err
				}
				scanned++
				assert.NotRegexp(t, otherHostURL, string(b), name)
				return nil
			})
			require.NoError(t, err)
			assert.NotZero(t, scanned, "walk visited no files; the scan proves nothing")
		})
	}
}

package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/mojatter/s2"
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

func TestConsoleErrorStatus(t *testing.T) {
	testCases := []struct {
		caseName string
		err      error
		want     int
	}{
		{caseName: "invalid name", err: fmt.Errorf("%w: a/../b", s2.ErrInvalidName), want: http.StatusBadRequest},
		{caseName: "reserved bucket name", err: ErrReservedBucketName, want: http.StatusBadRequest},
		{caseName: "missing bucket", err: &ErrBucketNotFound{Name: "b"}, want: http.StatusNotFound},
		{caseName: "missing object", err: fmt.Errorf("%w: k", s2.ErrNotExist), want: http.StatusNotFound},
		{caseName: "modified", err: s2.ErrModified, want: http.StatusInternalServerError},
		{caseName: "backend failure", err: errors.New("permission denied"), want: http.StatusInternalServerError},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, ConsoleErrorStatus(tc.err))
		})
	}
}

func TestWriteConsoleError(t *testing.T) {
	testCases := []struct {
		caseName string
		status   int
		err      error
		wantBody string
		wantLog  []string
	}{
		{
			caseName: "server error hides its detail",
			status:   http.StatusInternalServerError,
			err:      errors.New("open /tmp/multipart-1: no such file or directory"),
			wantBody: "Internal Server Error",
			wantLog:  []string{"level=ERROR", `msg="Console error"`, "method=POST", "path=/buckets/b/upload", "status=500", `error="open /tmp/multipart-1: no such file or directory"`},
		},
		{
			caseName: "client gone logs at debug",
			status:   http.StatusInternalServerError,
			err:      context.Canceled,
			wantBody: "Internal Server Error",
			wantLog:  []string{"level=DEBUG", "status=500"},
		},
		{
			caseName: "invalid name keeps its text",
			status:   http.StatusBadRequest,
			err:      fmt.Errorf("%w: a/.meta", s2.ErrInvalidName),
			wantBody: "s2: invalid name: a/.meta",
			wantLog:  []string{"level=DEBUG", "status=400"},
		},
		{
			caseName: "reserved bucket name keeps its text",
			status:   http.StatusBadRequest,
			err:      fmt.Errorf("%w: %q is internal state", ErrReservedBucketName, ".buckets"),
			wantBody: `bucket name is reserved: ".buckets" is internal state`,
			wantLog:  []string{"level=DEBUG", "status=400"},
		},
		{
			caseName: "missing bucket",
			status:   http.StatusNotFound,
			err:      &ErrBucketNotFound{Name: "b"},
			wantBody: "bucket not found",
			wantLog:  []string{"level=DEBUG", "status=404"},
		},
		{
			caseName: "missing object",
			status:   http.StatusNotFound,
			err:      fmt.Errorf("%w: /root/b/k", s2.ErrNotExist),
			wantBody: "object not found",
			wantLog:  []string{"level=DEBUG", "status=404", "/root/b/k"},
		},
		{
			caseName: "modified since get",
			status:   http.StatusInternalServerError,
			err:      fmt.Errorf("%w: k", s2.ErrModified),
			wantBody: "object modified while it was being read; try again",
			wantLog:  []string{"level=ERROR", "status=500"},
		},
		{
			caseName: "backend error answered not found",
			status:   http.StatusNotFound,
			err:      errors.New("stat /root/b: permission denied"),
			wantBody: "Not Found",
			wantLog:  []string{"level=DEBUG", "permission denied"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			var buf strings.Builder
			prev, prevW, prevFlags := slog.Default(), log.Writer(), log.Flags()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer func() {
				slog.SetDefault(prev)
				log.SetOutput(prevW)
				log.SetFlags(prevFlags)
			}()

			w := httptest.NewRecorder()
			WriteConsoleError(w, httptest.NewRequest(http.MethodPost, "/buckets/b/upload", nil), tc.status, tc.err)
			assert.Equal(t, tc.status, w.Code)
			assert.Equal(t, tc.wantBody+"\n", w.Body.String())
			for _, want := range tc.wantLog {
				assert.Contains(t, buf.String(), want)
			}
		})
	}
}

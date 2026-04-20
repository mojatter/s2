package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestServer(t *testing.T) *server.Server {
	t.Helper()
	cfg := server.DefaultConfig()
	cfg.Root = t.TempDir()
	srv, err := server.NewServer(context.Background(), cfg)
	require.NoError(t, err)
	return srv
}

func TestServeConsoleIndex(t *testing.T) {
	testCases := []struct {
		caseName     string
		htmx         bool
		wantCode     int
		wantContains string
		handlerCalled bool
	}{
		{
			caseName:      "HTMX request passes through to next handler",
			htmx:          true,
			wantCode:      http.StatusOK,
			handlerCalled: true,
		},
		{
			caseName:     "non-HTMX request returns index page",
			htmx:         false,
			wantCode:     http.StatusOK,
			wantContains: `id="main-content"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			srv := newTestServer(t)
			called := false
			next := func(_ *server.Server, w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			}

			req := httptest.NewRequest("GET", "/buckets/test", nil)
			if tc.htmx {
				req.Header.Set("HX-Request", "true")
			}
			w := httptest.NewRecorder()
			ServeConsoleIndex(next)(srv, w, req)

			assert.Equal(t, tc.wantCode, w.Code)
			assert.Equal(t, tc.handlerCalled, called)
			if tc.wantContains != "" {
				assert.Contains(t, w.Body.String(), tc.wantContains)
			}
		})
	}
}

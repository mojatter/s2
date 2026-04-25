package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderConsoleIndex(t *testing.T) {
	testCases := []struct {
		caseName     string
		buckets      []string
		wantContains []string
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

			w := httptest.NewRecorder()
			require.NoError(t, srv.RenderConsoleIndex(context.Background(), w, nil))

			body := w.Body.String()
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

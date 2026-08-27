package server

import (
	"context"
	"net/http/httptest"
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

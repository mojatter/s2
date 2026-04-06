package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mojatter/s2/server"
)

func TestHandleHealthz(t *testing.T) {
	cfg := server.DefaultConfig()
	cfg.Root = t.TempDir()
	cfg.User = "testuser"
	cfg.Password = "testpass"

	srv, err := server.NewServer(context.Background(), cfg)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handleHealthz(srv, w, r)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "ok", string(body))
}

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/assert"
)

func noopHandler(_ *server.Server, w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestBasicAuth_NoAuth(t *testing.T) {
	srv := &server.Server{Config: &server.Config{}}
	handler := BasicAuth(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestBasicAuth_MissingCredentials(t *testing.T) {
	srv := &server.Server{Config: &server.Config{User: "admin", Password: "secret"}}
	handler := BasicAuth(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, `Basic realm="s2"`, w.Header().Get("WWW-Authenticate"))
}

func TestBasicAuth_WrongCredentials(t *testing.T) {
	srv := &server.Server{Config: &server.Config{User: "admin", Password: "secret"}}
	handler := BasicAuth(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.SetBasicAuth("admin", "wrong")
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestBasicAuth_CorrectCredentials(t *testing.T) {
	srv := &server.Server{Config: &server.Config{User: "admin", Password: "secret"}}
	handler := BasicAuth(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.SetBasicAuth("admin", "secret")
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusOK, w.Code)
}

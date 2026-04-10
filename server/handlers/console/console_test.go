package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"
)

type consoleSuite struct {
	suite.Suite
	server *server.Server
}

func (s *consoleSuite) SetupTest() {
	cfg := server.DefaultConfig()
	cfg.Root = s.T().TempDir()
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
}

func (s *consoleSuite) createBucket(name string) {
	s.T().Helper()
	s.Require().NoError(s.server.Buckets.Create(context.Background(), name))
}

type IndexTestSuite struct{ consoleSuite }

func TestIndexTestSuite(t *testing.T) {
	suite.Run(t, &IndexTestSuite{})
}

// --- GET / ---

func (s *IndexTestSuite) TestHandleIndex() {
	s.Run("empty", func() {
		req := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()
		handleIndex(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Contains(w.Body.String(), "Storage Overview")
	})

	s.Run("with buckets", func() {
		s.createBucket("alpha")
		s.createBucket("beta")

		req := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()
		handleIndex(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		body := w.Body.String()
		s.Contains(body, "alpha")
		s.Contains(body, "beta")
	})
}

// --- POST /buckets ---

func (s *IndexTestSuite) TestHandleCreateBucket() {
	s.Run("success", func() {
		form := url.Values{"name": {"new-bucket"}}
		req := httptest.NewRequest("POST", "/buckets", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		handleCreateBucket(s.server, w, req)

		s.Equal(http.StatusFound, w.Code)
		exists, err := s.server.Buckets.Exists("new-bucket")
		s.Require().NoError(err)
		s.True(exists)
	})

	s.Run("empty name", func() {
		form := url.Values{"name": {""}}
		req := httptest.NewRequest("POST", "/buckets", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		handleCreateBucket(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code)
	})
}

// --- DELETE /buckets/{name} ---

func (s *IndexTestSuite) TestHandleDeleteBucket() {
	s.Run("success", func() {
		s.createBucket("to-delete")

		req := httptest.NewRequest("DELETE", "/buckets/to-delete", nil)
		req.SetPathValue("name", "to-delete")
		w := httptest.NewRecorder()
		handleDeleteBucket(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("/", w.Header().Get("HX-Redirect"))

		exists, err := s.server.Buckets.Exists("to-delete")
		s.Require().NoError(err)
		s.False(exists)
	})

	s.Run("empty name", func() {
		req := httptest.NewRequest("DELETE", "/buckets/", nil)
		req.SetPathValue("name", "")
		w := httptest.NewRecorder()
		handleDeleteBucket(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code)
	})
}

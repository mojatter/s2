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

	s.Run("filtered by policy", func() {
		s.createBucket("visible")
		s.createBucket("denied-bucket")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{"arn:aws:s3:::visible"}},
		}}}

		req := httptest.NewRequest("GET", "/", nil)
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleIndex(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		body := w.Body.String()
		s.Contains(body, "visible")
		s.NotContains(body, "denied-bucket")
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

		s.Equal(http.StatusOK, w.Code)
		s.Contains(w.Body.String(), "new-bucket")

		exists, err := s.server.Buckets.Exists(req.Context(), "new-bucket")
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

	s.Run("rendered bucket list is filtered by policy", func() {
		s.createBucket("denied-bucket")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{"s3:CreateBucket"}, Resource: []string{"arn:aws:s3:::*"}},
			{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{"arn:aws:s3:::just-created"}},
		}}}

		form := url.Values{"name": {"just-created"}}
		req := httptest.NewRequest("POST", "/buckets", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleCreateBucket(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		body := w.Body.String()
		s.Contains(body, "just-created")
		s.NotContains(body, "denied-bucket")
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
		s.Equal("/", w.Header().Get("HX-Push-Url"))
		s.Contains(w.Body.String(), "bucket-list")

		exists, err := s.server.Buckets.Exists(req.Context(), "to-delete")
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

	s.Run("remaining bucket list is filtered by policy", func() {
		s.createBucket("to-delete-2")
		s.createBucket("visible")
		s.createBucket("denied-bucket")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{"s3:DeleteBucket"}, Resource: []string{"arn:aws:s3:::*"}},
			{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{"arn:aws:s3:::visible"}},
		}}}

		req := httptest.NewRequest("DELETE", "/buckets/to-delete-2", nil)
		req.SetPathValue("name", "to-delete-2")
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		handleDeleteBucket(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		body := w.Body.String()
		s.Contains(body, "visible")
		s.NotContains(body, "denied-bucket")
	})
}

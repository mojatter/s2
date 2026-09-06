package s3api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"
)

// s3apiSuite is the base test suite providing common setup and helpers.
// It embeds suite.Suite but defines no Test* methods, so it can be
// embedded by BucketsTestSuite and ObjectsTestSuite without duplication.
type s3apiSuite struct {
	suite.Suite
	server *server.Server
}

func (s *s3apiSuite) SetupTest() {
	cfg := server.DefaultConfig()
	cfg.Root = s.T().TempDir()
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
}

func (s *s3apiSuite) putObject(bucket, key, content string) {
	s.T().Helper()
	ctx := context.Background()
	if ok, _ := s.server.Buckets.Exists(ctx, bucket); !ok {
		s.Require().NoError(s.server.Buckets.Create(ctx, bucket))
	}
	strg, err := s.server.Buckets.Get(ctx, bucket)
	s.Require().NoError(err)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(key, []byte(content))))
}

func (s *s3apiSuite) createBucket(name string) {
	s.T().Helper()
	s.Require().NoError(s.server.Buckets.Create(context.Background(), name))
}

// roundTrip drives target through a real server and returns the response,
// body drained and closed. Unlike httptest.ResponseRecorder, this shows
// what the client receives -- net/http's own Content-Type sniffing
// included (#188).
func (s *s3apiSuite) roundTrip(srv *server.Server, method, target string) *http.Response {
	s.T().Helper()
	ts := httptest.NewServer(srv.S3Handler())
	defer ts.Close()

	req, err := http.NewRequest(method, ts.URL+target, nil)
	s.Require().NoError(err)
	resp, err := ts.Client().Do(req)
	s.Require().NoError(err)
	defer func() { _ = resp.Body.Close() }()

	_, err = io.Copy(io.Discard, resp.Body)
	s.Require().NoError(err)
	return resp
}

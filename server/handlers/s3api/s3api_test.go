package s3api

import (
	"context"

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

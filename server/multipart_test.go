package server

import (
	"context"
	"io"
	"testing"

	"github.com/mojatter/s2"
	"github.com/stretchr/testify/suite"
)

type MultipartStoreTestSuite struct {
	suite.Suite
	server *Server
}

func TestMultipartStoreTestSuite(t *testing.T) {
	suite.Run(t, &MultipartStoreTestSuite{})
}

func (s *MultipartStoreTestSuite) SetupTest() {
	cfg := DefaultConfig()
	cfg.Type = s2.TypeOSFS
	cfg.Root = s.T().TempDir()
	cfg.ConsoleListen = ""
	srv, err := NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
}

func (s *MultipartStoreTestSuite) TestStorageIsRootedAtMultipartDir() {
	ctx := context.Background()

	obj := s2.NewObjectBytes("deadbeef/00001", []byte("part"))
	s.Require().NoError(s.server.Multipart.Storage().Put(ctx, obj))

	got, err := s.server.Buckets.strg.Get(ctx, multipartDir+"/deadbeef/00001")
	s.Require().NoError(err)

	r, err := got.Open()
	s.Require().NoError(err)

	defer r.Close() //nolint:errcheck // read-only

	data, err := io.ReadAll(r)
	s.Require().NoError(err)
	s.Equal("part", string(data))
}

func (s *MultipartStoreTestSuite) TestIsHiddenBucketEntry() {
	testCases := []struct {
		caseName string
		name     string
		want     bool
	}{
		{caseName: "multipart dir", name: multipartDir, want: true},
		{caseName: "any dot prefix", name: ".git", want: true},
		{caseName: "ordinary bucket", name: "alpha", want: false},
		{caseName: "dot inside the name", name: "my.bucket", want: false},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.Equal(tc.want, isHiddenBucketEntry(tc.name))
		})
	}
}

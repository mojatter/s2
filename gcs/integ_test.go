//go:build integtest

package gcs

import (
	"context"
	"os"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/stretchr/testify/suite"
)

// Run: S2_TEST_GCS_BUCKET=my-bucket go test -tags integtest -run TestGCSIntegration ./gcs/...
//
// Authentication uses Application Default Credentials (gcloud auth application-default login).
// Optional environment variables:
//   - S2_TEST_GCS_CREDENTIALS_FILE: path to a service account JSON key file

type GCSIntegrationSuite struct {
	suite.Suite
	strg s2.Storage
}

func TestGCSIntegration(t *testing.T) {
	suite.Run(t, &GCSIntegrationSuite{})
}

func (s *GCSIntegrationSuite) SetupSuite() {
	bucket := os.Getenv("S2_TEST_GCS_BUCKET")
	if bucket == "" {
		s.T().Fatal("S2_TEST_GCS_BUCKET is required")
	}

	cfg := s2.Config{
		Type: s2.TypeGCS,
		Root: bucket + "/s2-integ-test",
	}
	if cf := os.Getenv("S2_TEST_GCS_CREDENTIALS_FILE"); cf != "" {
		cfg.GCS = &s2.GCSConfig{CredentialsFile: cf}
	}

	strg, err := NewStorage(context.Background(), cfg)
	s.Require().NoError(err)
	s.strg = strg
}

func (s *GCSIntegrationSuite) TearDownSuite() {
	if s.strg != nil {
		_ = s.strg.DeleteRecursive(context.Background(), "")
	}
}

func (s *GCSIntegrationSuite) TestGetPut() {
	s.Require().NoError(s2test.TestStorageGetPut(context.Background(), s.strg))
}

func (s *GCSIntegrationSuite) TestGetNotExist() {
	s.Require().NoError(s2test.TestStorageGetNotExist(context.Background(), s.strg))
}

func (s *GCSIntegrationSuite) TestExists() {
	s.Require().NoError(s2test.TestStorageExists(context.Background(), s.strg))
}

func (s *GCSIntegrationSuite) TestCopyMove() {
	s.Require().NoError(s2test.TestStorageCopyMove(context.Background(), s.strg))
}

func (s *GCSIntegrationSuite) TestDelete() {
	s.Require().NoError(s2test.TestStorageDelete(context.Background(), s.strg))
}

func (s *GCSIntegrationSuite) TestPutMetadata() {
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), s.strg))
}

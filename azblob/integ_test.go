//go:build integtest

package azblob

import (
	"context"
	"os"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/stretchr/testify/suite"
)

// Run: S2_TEST_AZBLOB_ACCOUNT_NAME=xxx S2_TEST_AZBLOB_ACCOUNT_KEY=yyy S2_TEST_AZBLOB_CONTAINER=zzz \
//   go test -tags integtest -run TestAzblobIntegration ./azblob/...
//
// Environment variables:
//   - S2_TEST_AZBLOB_CONTAINER: container name (required)
//   - S2_TEST_AZBLOB_ACCOUNT_NAME: storage account name
//   - S2_TEST_AZBLOB_ACCOUNT_KEY: shared key
//   - S2_TEST_AZBLOB_CONNECTION_STRING: full connection string (alternative to name+key)

type AzblobIntegrationSuite struct {
	suite.Suite
	strg s2.Storage
}

func TestAzblobIntegration(t *testing.T) {
	suite.Run(t, &AzblobIntegrationSuite{})
}

func (s *AzblobIntegrationSuite) SetupSuite() {
	ctr := os.Getenv("S2_TEST_AZBLOB_CONTAINER")
	if ctr == "" {
		s.T().Fatal("S2_TEST_AZBLOB_CONTAINER is required")
	}

	cfg := s2.Config{
		Type: s2.TypeAzblob,
		Root: ctr + "/s2-integ-test",
		Azblob: &s2.AzblobConfig{
			AccountName:      os.Getenv("S2_TEST_AZBLOB_ACCOUNT_NAME"),
			AccountKey:       os.Getenv("S2_TEST_AZBLOB_ACCOUNT_KEY"),
			ConnectionString: os.Getenv("S2_TEST_AZBLOB_CONNECTION_STRING"),
		},
	}

	strg, err := NewStorage(context.Background(), cfg)
	s.Require().NoError(err)
	s.strg = strg
}

func (s *AzblobIntegrationSuite) TearDownSuite() {
	if s.strg != nil {
		_ = s.strg.DeleteRecursive(context.Background(), "")
	}
}

func (s *AzblobIntegrationSuite) TestGetPut() {
	s.Require().NoError(s2test.TestStorageGetPut(context.Background(), s.strg))
}

func (s *AzblobIntegrationSuite) TestGetNotExist() {
	s.Require().NoError(s2test.TestStorageGetNotExist(context.Background(), s.strg))
}

func (s *AzblobIntegrationSuite) TestExists() {
	s.Require().NoError(s2test.TestStorageExists(context.Background(), s.strg))
}

func (s *AzblobIntegrationSuite) TestCopyMove() {
	s.Require().NoError(s2test.TestStorageCopyMove(context.Background(), s.strg))
}

func (s *AzblobIntegrationSuite) TestDelete() {
	s.Require().NoError(s2test.TestStorageDelete(context.Background(), s.strg))
}

func (s *AzblobIntegrationSuite) TestPutMetadata() {
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), s.strg))
}

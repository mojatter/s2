//go:build integtest

package s3

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/stretchr/testify/suite"
)

// Run: S2_TEST_S3_BUCKET=my-bucket go test -tags integtest -run TestS3Integration ./s3/...
//
// Optional environment variables:
//   - S2_TEST_S3_ENDPOINT: custom S3-compatible endpoint (e.g. http://localhost:9000)
//   - S2_TEST_S3_REGION: AWS region
//   - S2_TEST_S3_ACCESS_KEY_ID: access key
//   - S2_TEST_S3_SECRET_ACCESS_KEY: secret key

type S3IntegrationSuite struct {
	suite.Suite
	strg s2.Storage
}

func TestS3Integration(t *testing.T) {
	suite.Run(t, &S3IntegrationSuite{})
}

func (s *S3IntegrationSuite) SetupSuite() {
	bucket := os.Getenv("S2_TEST_S3_BUCKET")
	if bucket == "" {
		s.T().Fatal("S2_TEST_S3_BUCKET is required")
	}

	cfg := s2.Config{
		Type: s2.TypeS3,
		Root: bucket + "/s2-integ-test",
	}
	if ep := os.Getenv("S2_TEST_S3_ENDPOINT"); ep != "" {
		if cfg.S3 == nil {
			cfg.S3 = &s2.S3Config{}
		}
		cfg.S3.EndpointURL = ep
	}
	if r := os.Getenv("S2_TEST_S3_REGION"); r != "" {
		if cfg.S3 == nil {
			cfg.S3 = &s2.S3Config{}
		}
		cfg.S3.Region = r
	}
	if ak := os.Getenv("S2_TEST_S3_ACCESS_KEY_ID"); ak != "" {
		if cfg.S3 == nil {
			cfg.S3 = &s2.S3Config{}
		}
		cfg.S3.AccessKeyID = ak
		cfg.S3.SecretAccessKey = os.Getenv("S2_TEST_S3_SECRET_ACCESS_KEY")
	}

	strg, err := NewStorage(context.Background(), cfg)
	s.Require().NoError(err)
	s.strg = strg
}

func (s *S3IntegrationSuite) TearDownSuite() {
	if s.strg != nil {
		_ = s.strg.DeleteRecursive(context.Background(), "")
	}
}

func (s *S3IntegrationSuite) TestGetPut() {
	s.Require().NoError(s2test.TestStorageGetPut(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestList() {
	s.Require().NoError(s2test.TestStorageListPaging(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestCount() {
	s.Require().NoError(s2test.TestStorageCount(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestOpenSinceGet() {
	s.Require().NoError(s2test.TestStorageOpenSinceGet(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestUploadConditions() {
	ctx := context.Background()
	s.Require().NoError(s2test.TestStorageUploadConditions(ctx, s.strg))

	// The suite accepts ErrUnsupported; the provider must hold the conditions itself.
	const name = "integ-upload-conditions.txt"
	defer func() { _ = s.strg.Delete(ctx, name) }()

	res, err := s2.Upload(ctx, s.strg, s2.NewObjectBytes(name, []byte("first")), s2.UploadOptions{IfNoneMatch: "*"})
	s.Require().NoError(err)
	_, err = s2.Upload(ctx, s.strg, s2.NewObjectBytes(name, []byte("second")), s2.UploadOptions{IfNoneMatch: "*"})
	s.Require().ErrorIs(err, s2.ErrPreconditionFailed)
	_, err = s2.Upload(ctx, s.strg, s2.NewObjectBytes(name, []byte("third")), s2.UploadOptions{IfMatch: res.ETag})
	s.Require().NoError(err)
	_, err = s2.Upload(ctx, s.strg, s2.NewObjectBytes(name, []byte("fourth")), s2.UploadOptions{IfMatch: res.ETag})
	s.Require().ErrorIs(err, s2.ErrPreconditionFailed)
	s.Require().NoError(s.strg.Delete(ctx, name))
	_, err = s2.Upload(ctx, s.strg, s2.NewObjectBytes(name, []byte("fifth")), s2.UploadOptions{IfMatch: res.ETag})
	s.Require().ErrorIs(err, s2.ErrNotExist)
}

// A body above the multipart threshold goes up in parts, with the conditions held at CompleteMultipartUpload.
func (s *S3IntegrationSuite) TestUploadLargeBody() {
	ctx := context.Background()
	const name = "integ-upload-large.bin"
	defer func() { _ = s.strg.Delete(ctx, name) }()

	body := bytes.Repeat([]byte("0123456789abcdef"), (17<<20)/16)
	upload := func(opts s2.UploadOptions) (s2.UploadResult, error) {
		// NopCloser hides Seek, as the s3api handler's bodies do.
		return s2.Upload(ctx, s.strg, s2.NewObjectReader(name, io.NopCloser(bytes.NewReader(body)), uint64(len(body))), opts)
	}
	res, err := upload(s2.UploadOptions{IfNoneMatch: "*"})
	s.Require().NoError(err)
	_, err = upload(s2.UploadOptions{IfNoneMatch: "*"})
	s.Require().ErrorIs(err, s2.ErrPreconditionFailed)
	res, err = upload(s2.UploadOptions{IfMatch: res.ETag})
	s.Require().NoError(err)

	got, err := s.strg.Get(ctx, name)
	s.Require().NoError(err)
	s.Equal(res.ETag, got.ETag())
	rc, err := got.Open()
	s.Require().NoError(err)
	defer func() { _ = rc.Close() }()

	stored, err := io.ReadAll(rc)
	s.Require().NoError(err)
	s.True(bytes.Equal(body, stored))
}

func (s *S3IntegrationSuite) TestGetNotExist() {
	s.Require().NoError(s2test.TestStorageGetNotExist(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestExists() {
	s.Require().NoError(s2test.TestStorageExists(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestCopyMove() {
	s.Require().NoError(s2test.TestStorageCopyMove(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestDelete() {
	s.Require().NoError(s2test.TestStorageDelete(context.Background(), s.strg))
}

func (s *S3IntegrationSuite) TestPutMetadata() {
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), s.strg))
}

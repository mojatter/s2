package s3api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/suite"

	_ "github.com/mojatter/s2/fs"
	"github.com/mojatter/s2/server"
)

const (
	testAccessKey = "testkey"
	testSecretKey = "testsecret"
)

type IntegrationSuite struct {
	suite.Suite
	srv    *server.Server
	ts     *httptest.Server
	client *s3.Client
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(IntegrationSuite))
}

func (s *IntegrationSuite) SetupTest() {
	cfg := server.DefaultConfig()
	cfg.Root = s.T().TempDir()
	cfg.User = testAccessKey
	cfg.Password = testSecretKey

	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.srv = srv

	s.ts = httptest.NewServer(srv.S3Handler())

	s.client = s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s.ts.URL)
		o.UsePathStyle = true
	})
}

func (s *IntegrationSuite) TearDownTest() {
	if s.ts != nil {
		s.ts.Close()
	}
}

// --- Bucket operations ---

func (s *IntegrationSuite) TestCreateAndListBuckets() {
	ctx := context.Background()

	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("test-bucket"),
	})
	s.Require().NoError(err)

	out, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	s.Require().NoError(err)
	s.Require().Len(out.Buckets, 1)
	s.Equal("test-bucket", *out.Buckets[0].Name)
}

func (s *IntegrationSuite) TestHeadBucket() {
	ctx := context.Background()

	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("head-bucket"),
	})
	s.Require().NoError(err)

	_, err = s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("head-bucket"),
	})
	s.NoError(err)

	_, err = s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("no-such-bucket"),
	})
	s.Error(err)
}

func (s *IntegrationSuite) TestDeleteBucket() {
	ctx := context.Background()

	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("del-bucket"),
	})
	s.Require().NoError(err)

	_, err = s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String("del-bucket"),
	})
	s.Require().NoError(err)

	out, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	s.Require().NoError(err)
	s.Empty(out.Buckets)
}

// --- Object operations ---

func (s *IntegrationSuite) TestPutAndGetObject() {
	ctx := context.Background()
	bucket := "obj-bucket"
	s.createTestBucket(bucket)

	body := "hello, s2!"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("greeting.txt"),
		Body:   strings.NewReader(body),
	})
	s.Require().NoError(err)

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("greeting.txt"),
	})
	s.Require().NoError(err)
	defer out.Body.Close()

	var buf bytes.Buffer
	_, err = buf.ReadFrom(out.Body)
	s.Require().NoError(err)
	s.Equal(body, buf.String())
	s.NotEmpty(*out.ETag)
}

func (s *IntegrationSuite) TestHeadObject() {
	ctx := context.Background()
	bucket := "head-obj-bucket"
	s.createTestBucket(bucket)

	body := "head me"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader(body),
	})
	s.Require().NoError(err)

	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("file.txt"),
	})
	s.Require().NoError(err)
	s.Equal(int64(len(body)), *out.ContentLength)
	s.NotNil(out.LastModified)
}

func (s *IntegrationSuite) TestDeleteObject() {
	ctx := context.Background()
	bucket := "del-obj-bucket"
	s.createTestBucket(bucket)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("to-delete.txt"),
		Body:   strings.NewReader("bye"),
	})
	s.Require().NoError(err)

	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("to-delete.txt"),
	})
	s.Require().NoError(err)

	_, err = s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("to-delete.txt"),
	})
	s.Error(err)
}

func (s *IntegrationSuite) TestDeleteObjects() {
	ctx := context.Background()
	bucket := "batch-del-bucket"
	s.createTestBucket(bucket)

	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("x"),
		})
		s.Require().NoError(err)
	}

	_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("a.txt")},
				{Key: aws.String("b.txt")},
			},
			Quiet: aws.Bool(true),
		},
	})
	s.Require().NoError(err)

	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	s.Require().NoError(err)
	s.Require().Len(out.Contents, 1)
	s.Equal("c.txt", *out.Contents[0].Key)
}

// --- List objects ---

func (s *IntegrationSuite) TestListObjects() {
	ctx := context.Background()
	bucket := "list-bucket"
	s.createTestBucket(bucket)

	keys := []string{"photos/a.jpg", "photos/b.jpg", "docs/readme.md", "root.txt"}
	for _, key := range keys {
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("x"),
		})
		s.Require().NoError(err)
	}

	// With delimiter
	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("/"),
	})
	s.Require().NoError(err)
	s.Len(out.Contents, 1) // root.txt
	s.Len(out.CommonPrefixes, 2) // docs/, photos/

	// With prefix
	out, err = s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("photos/"),
	})
	s.Require().NoError(err)
	s.Len(out.Contents, 2)

	// Pagination
	out, err = s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(2),
	})
	s.Require().NoError(err)
	s.Len(out.Contents, 2)
	s.True(*out.IsTruncated)
	s.NotNil(out.NextContinuationToken)

	out2, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		ContinuationToken: out.NextContinuationToken,
	})
	s.Require().NoError(err)
	s.Len(out2.Contents, 2)
}

// --- Copy object ---

func (s *IntegrationSuite) TestCopyObject() {
	ctx := context.Background()
	src := "copy-src"
	dst := "copy-dst"
	s.createTestBucket(src)
	s.createTestBucket(dst)

	body := "copy me"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(src),
		Key:    aws.String("original.txt"),
		Body:   strings.NewReader(body),
	})
	s.Require().NoError(err)

	// Same bucket copy
	_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(src),
		Key:        aws.String("copied.txt"),
		CopySource: aws.String(src + "/original.txt"),
	})
	s.Require().NoError(err)

	// Cross bucket copy
	_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(dst),
		Key:        aws.String("cross.txt"),
		CopySource: aws.String(src + "/original.txt"),
	})
	s.Require().NoError(err)

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(dst),
		Key:    aws.String("cross.txt"),
	})
	s.Require().NoError(err)
	defer out.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(out.Body)
	s.Equal(body, buf.String())
}

// --- Range requests ---

func (s *IntegrationSuite) TestGetObjectRange() {
	ctx := context.Background()
	bucket := "range-bucket"
	s.createTestBucket(bucket)

	body := "0123456789"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("data.txt"),
		Body:   strings.NewReader(body),
	})
	s.Require().NoError(err)

	testCases := []struct {
		name     string
		rangeStr string
		want     string
	}{
		{"mid range", "bytes=2-5", "2345"},
		{"from start", "bytes=0-3", "0123"},
		{"suffix", "bytes=-3", "789"},
		{"open end", "bytes=7-", "789"},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("data.txt"),
				Range:  aws.String(tc.rangeStr),
			})
			s.Require().NoError(err)
			defer out.Body.Close()
			var buf bytes.Buffer
			buf.ReadFrom(out.Body)
			s.Equal(tc.want, buf.String())
		})
	}
}

// --- Multipart upload ---

func (s *IntegrationSuite) TestMultipartUpload() {
	ctx := context.Background()
	bucket := "mp-bucket"
	s.createTestBucket(bucket)

	// Initiate
	createOut, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("large.bin"),
	})
	s.Require().NoError(err)
	uploadID := createOut.UploadId
	s.NotEmpty(*uploadID)

	// Upload parts
	part1 := strings.Repeat("A", 1024)
	part2 := strings.Repeat("B", 1024)

	up1, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("large.bin"),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader(part1),
	})
	s.Require().NoError(err)

	up2, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("large.bin"),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		Body:       strings.NewReader(part2),
	})
	s.Require().NoError(err)

	// Complete
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("large.bin"),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: up1.ETag},
				{PartNumber: aws.Int32(2), ETag: up2.ETag},
			},
		},
	})
	s.Require().NoError(err)

	// Verify assembled object
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("large.bin"),
	})
	s.Require().NoError(err)
	defer out.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(out.Body)
	s.Equal(part1+part2, buf.String())
}

func (s *IntegrationSuite) TestMultipartUploadAbort() {
	ctx := context.Background()
	bucket := "mp-abort-bucket"
	s.createTestBucket(bucket)

	createOut, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("aborted.bin"),
	})
	s.Require().NoError(err)

	_, err = s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("aborted.bin"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("part data"),
	})
	s.Require().NoError(err)

	_, err = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("aborted.bin"),
		UploadId: createOut.UploadId,
	})
	s.Require().NoError(err)

	// Object should not exist
	_, err = s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("aborted.bin"),
	})
	s.Error(err)
}

// --- Metadata ---

func (s *IntegrationSuite) TestUserMetadata() {
	ctx := context.Background()
	bucket := "meta-bucket"
	s.createTestBucket(bucket)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("meta.txt"),
		Body:   strings.NewReader("metadata test"),
		Metadata: map[string]string{
			"author":  "s2test",
			"version": "42",
		},
	})
	s.Require().NoError(err)

	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("meta.txt"),
	})
	s.Require().NoError(err)
	s.Equal("s2test", out.Metadata["author"])
	s.Equal("42", out.Metadata["version"])
}

// --- Error cases ---

func (s *IntegrationSuite) TestGetObject_NoSuchBucket() {
	ctx := context.Background()
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("nonexistent"),
		Key:    aws.String("file.txt"),
	})
	s.Error(err)
}

func (s *IntegrationSuite) TestGetObject_NoSuchKey() {
	ctx := context.Background()
	bucket := "err-bucket"
	s.createTestBucket(bucket)

	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("missing.txt"),
	})
	s.Error(err)
}

// --- Presigned URL ---

func (s *IntegrationSuite) TestPresignedGetObject() {
	ctx := context.Background()
	bucket := "presign-get-bucket"
	s.createTestBucket(bucket)

	body := "presigned download"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader(body),
	})
	s.Require().NoError(err)

	presigner := s3.NewPresignClient(s.client)
	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("file.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	s.Require().NoError(err)

	resp, err := http.Get(req.URL)
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Equal(http.StatusOK, resp.StatusCode)
	got, err := io.ReadAll(resp.Body)
	s.Require().NoError(err)
	s.Equal(body, string(got))
}

func (s *IntegrationSuite) TestPresignedPutObject() {
	ctx := context.Background()
	bucket := "presign-put-bucket"
	s.createTestBucket(bucket)

	presigner := s3.NewPresignClient(s.client)
	req, err := presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("uploaded.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	s.Require().NoError(err)

	body := "uploaded via presigned URL"
	httpReq, err := http.NewRequest(http.MethodPut, req.URL, strings.NewReader(body))
	s.Require().NoError(err)
	resp, err := http.DefaultClient.Do(httpReq)
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Equal(http.StatusOK, resp.StatusCode)

	// Verify via the SDK client
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("uploaded.txt"),
	})
	s.Require().NoError(err)
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	s.Require().NoError(err)
	s.Equal(body, string(got))
}

func (s *IntegrationSuite) TestPresignedGetObject_TamperedSignatureRejected() {
	ctx := context.Background()
	bucket := "presign-tamper-bucket"
	s.createTestBucket(bucket)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader("secret"),
	})
	s.Require().NoError(err)

	presigner := s3.NewPresignClient(s.client)
	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("file.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	s.Require().NoError(err)

	// Tamper: replace the signature with an obvious bad value.
	tampered := strings.Replace(req.URL, "X-Amz-Signature=", "X-Amz-Signature=deadbeef&_=", 1)
	resp, err := http.Get(tampered)
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Equal(http.StatusForbidden, resp.StatusCode)
}

// --- Helpers ---

func (s *IntegrationSuite) createTestBucket(name string) {
	s.T().Helper()
	_, err := s.client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	s.Require().NoError(err)
}

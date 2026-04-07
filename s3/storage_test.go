package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/stretchr/testify/suite"
)

type mockObject struct {
	bucket       string
	key          string
	body         []byte
	lastModified time.Time
	metadata     map[string]string
}

type mockS3Client struct {
	mu      sync.RWMutex
	objects map[string]*mockObject
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{
		objects: make(map[string]*mockObject),
	}
}

func (m *mockS3Client) put(bucket, key string, body []byte, metadata map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objects[path.Join(bucket, key)] = &mockObject{
		bucket:       bucket,
		key:          key,
		body:         body,
		lastModified: time.Now(),
		metadata:     metadata,
	}
}

func (m *mockS3Client) delete(bucket, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, path.Join(bucket, key))
}

func (m *mockS3Client) get(bucket, key string) (*mockObject, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	obj, ok := m.objects[path.Join(bucket, key)]
	return obj, ok
}

// clientAPI and presignClientAPI implementation

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bucket := aws.ToString(params.Bucket)
	prefix := aws.ToString(params.Prefix)
	after := aws.ToString(params.StartAfter)
	delimiter := aws.ToString(params.Delimiter)
	limit := int(aws.ToInt32(params.MaxKeys))
	if limit == 0 {
		limit = 1000
	}

	var contents []s3types.Object
	var keys []string
	for k := range m.objects {
		keys = append(keys, k)
	}

	for _, objPath := range keys {
		obj := m.objects[objPath]
		if obj.bucket != bucket {
			continue
		}
		if !strings.HasPrefix(obj.key, prefix) {
			continue
		}
		remainder := obj.key[len(prefix):]
		if delimiter != "" {
			if strings.Contains(remainder, delimiter) {
				continue // skip objects deep in subdirectories
			}
		}
		if after != "" && obj.key <= after {
			continue
		}
		contents = append(contents, s3types.Object{
			Key:          aws.String(obj.key),
			Size:         aws.Int64(int64(len(obj.body))),
			LastModified: aws.Time(obj.lastModified),
		})
	}

	// Simple sort by key
	for i := 0; i < len(contents)-1; i++ {
		for j := i + 1; j < len(contents); j++ {
			if *contents[i].Key > *contents[j].Key {
				contents[i], contents[j] = contents[j], contents[i]
			}
		}
	}

	if len(contents) > limit {
		contents = contents[:limit]
	}

	return &s3.ListObjectsV2Output{
		Contents: contents,
	}, nil
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	obj, ok := m.get(aws.ToString(params.Bucket), aws.ToString(params.Key))
	if !ok {
		return nil, fmt.Errorf("NotFound: %w", &s3types.NoSuchKey{})
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(obj.body))),
		LastModified:  aws.Time(obj.lastModified),
		Metadata:      obj.metadata,
	}, nil
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.put(aws.ToString(params.Bucket), aws.ToString(params.Key), b, params.Metadata)
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.delete(aws.ToString(params.Bucket), aws.ToString(params.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	for _, id := range params.Delete.Objects {
		m.delete(aws.ToString(params.Bucket), aws.ToString(id.Key))
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	obj, ok := m.get(aws.ToString(params.Bucket), aws.ToString(params.Key))
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	body := obj.body
	if params.Range != nil {
		rangeStr := aws.ToString(params.Range)
		var start, end int64
		fmt.Sscanf(rangeStr, "bytes=%d-%d", &start, &end)
		if int(end) >= len(body) {
			end = int64(len(body)) - 1
		}
		body = body[start : end+1]
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: aws.Int64(int64(len(body))),
		LastModified:  aws.Time(obj.lastModified),
		Metadata:      obj.metadata,
	}, nil
}

func (m *mockS3Client) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	_, ok := m.get(aws.ToString(params.Bucket), aws.ToString(params.Key))
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	url := fmt.Sprintf("https://s3mock/%s/%s", aws.ToString(params.Bucket), aws.ToString(params.Key))
	return &v4.PresignedHTTPRequest{URL: url}, nil
}

func (m *mockS3Client) PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	url := fmt.Sprintf("https://s3mock/%s/%s?PUT", aws.ToString(params.Bucket), aws.ToString(params.Key))
	return &v4.PresignedHTTPRequest{URL: url}, nil
}

func (m *mockS3Client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	copySource := aws.ToString(params.CopySource)
	// format: bucket/key
	parts := strings.SplitN(copySource, "/", 2)
	srcBucket := parts[0]
	srcKey := parts[1]

	srcObj, ok := m.get(srcBucket, srcKey)
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}

	metadata := srcObj.metadata
	if params.MetadataDirective == s3types.MetadataDirectiveReplace {
		metadata = params.Metadata
	}

	m.put(aws.ToString(params.Bucket), aws.ToString(params.Key), srcObj.body, metadata)
	return &s3.CopyObjectOutput{}, nil
}

type StorageTestSuite struct {
	suite.Suite
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, &StorageTestSuite{})
}

func (s *StorageTestSuite) testMockClient() (*mockS3Client, s2.Storage) {
	m := newMockS3Client()
	files := map[string][]byte{
		"a.txt":     []byte("a"),
		"b.txt":     []byte("b"),
		"cc/c1.txt": []byte("c1"),
		"cc/c2.txt": []byte("c2"),
	}
	for key, b := range files {
		m.put("mybucket", key, b, nil)
	}
	return m, &storage{
		client:        m,
		presignClient: m,
		bucket:        "mybucket",
		prefix:        "",
	}
}

func (s *StorageTestSuite) TestS2TestList() {
	_, strg := s.testMockClient()
	ctx := context.Background()

	err := s2test.TestStorageListRecursive(ctx, strg, "a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "", "a.txt", "b.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "cc", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)
}

func (s *StorageTestSuite) TestS2TestGetPut() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageGetPut(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestGetNotExist() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageGetNotExist(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestExists() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageExists(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestCopyMove() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageCopyMove(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestDelete() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageDelete(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestPutMetadata() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), strg))
}

func (s *StorageTestSuite) TestNewStorageError() {
	_, err := NewStorage(context.Background(), s2.Config{})
	s.Require().ErrorIs(err, ErrRequiredConfigRoot)
}

func (s *StorageTestSuite) TestNewStorage() {
	testCases := []struct {
		caseName   string
		cfg        s2.Config
		wantBucket string
		wantPrefix string
	}{
		{
			caseName:   "bucket only",
			cfg:        s2.Config{Type: s2.TypeS3, Root: "my-bucket"},
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			caseName:   "bucket with prefix",
			cfg:        s2.Config{Type: s2.TypeS3, Root: "my-bucket/some/prefix"},
			wantBucket: "my-bucket",
			wantPrefix: "some/prefix",
		},
		{
			caseName:   "root with slashes trimmed",
			cfg:        s2.Config{Type: s2.TypeS3, Root: "/my-bucket/pfx/"},
			wantBucket: "my-bucket",
			wantPrefix: "pfx",
		},
		{
			caseName: "with S3Config region only",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3:   &s2.S3Config{Region: "us-west-2"},
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			caseName: "with S3Config credentials",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3: &s2.S3Config{
					AccessKeyID:     "AKID",
					SecretAccessKey: "SECRET",
				},
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			caseName: "with S3Config endpoint",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3: &s2.S3Config{
					EndpointURL: "http://localhost:9000/s3api",
				},
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			caseName: "with full S3Config",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket/data",
				S3: &s2.S3Config{
					EndpointURL:     "http://localhost:9000/s3api",
					Region:          "ap-northeast-1",
					AccessKeyID:     "minioadmin",
					SecretAccessKey: "minioadmin",
				},
			},
			wantBucket: "my-bucket",
			wantPrefix: "data",
		},
		{
			caseName: "nil S3Config",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3:   nil,
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			strg, err := NewStorage(context.Background(), tc.cfg)
			s.Require().NoError(err)

			st := strg.(*storage)
			s.Equal(tc.wantBucket, st.bucket)
			s.Equal(tc.wantPrefix, st.prefix)
			s.NotNil(st.client)
		})
	}
}

func (s *StorageTestSuite) TestType() {
	_, strg := s.testMockClient()
	s.Equal(s2.TypeS3, strg.Type())
}

func (s *StorageTestSuite) TestGet() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		name     string
		wantErr  string
	}{
		{
			caseName: "found",
			name:     "a.txt",
		},
		{
			caseName: "not found",
			name:     "not-found.txt",
			wantErr:  "not exist: not-found.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()

			got, err := strg.Get(tc.ctx, tc.name)
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.name, got.Name())

			rc, err := got.Open()
			s.Require().NoError(err)
			defer rc.Close()

			b, _ := io.ReadAll(rc)
			s.Equal("a", string(b))
		})
	}
	s.Run("open-error", func() {
		m, _ := s.testMockClient()
		obj := &object{
			client: m,
			bucket: "mybucket",
			name:   "not-found.txt",
		}
		_, err := obj.Open()
		s.Error(err)
		s.ErrorIs(err, s2.ErrNotExist)
	})
}

func (s *StorageTestSuite) TestPut() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		obj      s2.Object
	}{
		{
			caseName: "new-file",
			obj: func() s2.Object {
				o := s2.NewObjectBytes("new.txt", []byte("new content"))
				o.Metadata().Set("key", "val")
				return o
			}(),
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()

			err := strg.Put(tc.ctx, tc.obj)
			s.Require().NoError(err)

			got, err := strg.Get(tc.ctx, tc.obj.Name())
			s.Require().NoError(err)

			rc, err := got.Open()
			s.Require().NoError(err)
			defer rc.Close()

			body, _ := io.ReadAll(rc)
			s.Equal("new content", string(body))
			v, _ := got.Metadata().Get("key")
			s.Equal("val", v)
		})
	}
}

func (s *StorageTestSuite) TestDelete() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		name     string
	}{
		{
			caseName: "typical",
			name:     "a.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()

			err := strg.Delete(tc.ctx, tc.name)
			s.Require().NoError(err)

			_, err = strg.Get(tc.ctx, tc.name)
			s.Error(err)
		})
	}
}

func (s *StorageTestSuite) TestDeleteRecursive() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		prefix   string
		wantErr  string
		wantLeft []string
	}{
		{
			caseName: "typical",
			prefix:   "cc",
			wantLeft: []string{"a.txt", "b.txt"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()

			err := strg.DeleteRecursive(tc.ctx, tc.prefix)
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)

			res, err := strg.List(tc.ctx, s2.ListOptions{Limit: 10, Recursive: true})
			s.Require().NoError(err)
			objs := res.Objects
			s.Equal(len(tc.wantLeft), len(objs))
			for i, w := range tc.wantLeft {
				s.Equal(w, objs[i].Name())
			}
		})
	}
	s.Run("empty-page", func() {
		m := newMockS3Client()
		strg := &storage{
			client: m,
			bucket: "mybucket",
		}
		err := strg.DeleteRecursive(context.Background(), "prefix")
		s.Require().NoError(err)
	})
}

func (s *StorageTestSuite) TestSub() {
	_, strg := s.testMockClient()
	ctx := context.Background()

	sub, err := strg.Sub(ctx, "cc")
	s.Require().NoError(err)
	s.Equal(s2.TypeS3, sub.Type())

	res, err := sub.List(ctx, s2.ListOptions{Limit: 10, Recursive: true})
	s.Require().NoError(err)
	s.Len(res.Objects, 2)
}

func (s *StorageTestSuite) TestExists() {
	testCases := []struct {
		caseName string
		name     string
		want     bool
	}{
		{caseName: "found", name: "a.txt", want: true},
		{caseName: "not found", name: "not-found.txt", want: false},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()
			got, err := strg.Exists(context.Background(), tc.name)
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
}

func (s *StorageTestSuite) TestPutMetadata() {
	_, strg := s.testMockClient()
	ctx := context.Background()

	err := strg.PutMetadata(ctx, "a.txt", s2.Metadata{"new-key": "new-val"})
	s.Require().NoError(err)

	obj, err := strg.Get(ctx, "a.txt")
	s.Require().NoError(err)
	v, ok := obj.Metadata().Get("new-key")
	s.True(ok)
	s.Equal("new-val", v)
}

func (s *StorageTestSuite) TestCopy() {
	s.Run("typical", func() {
		_, strg := s.testMockClient()
		ctx := context.Background()

		err := strg.Copy(ctx, "a.txt", "a-copy.txt")
		s.Require().NoError(err)

		got, err := strg.Get(ctx, "a-copy.txt")
		s.Require().NoError(err)
		rc, err := got.Open()
		s.Require().NoError(err)
		defer rc.Close()
		body, _ := io.ReadAll(rc)
		s.Equal("a", string(body))
	})

	s.Run("not found", func() {
		_, strg := s.testMockClient()
		err := strg.Copy(context.Background(), "not-found.txt", "dst.txt")
		s.Error(err)
	})
}

func (s *StorageTestSuite) TestMove() {
	_, strg := s.testMockClient()
	ctx := context.Background()

	err := s2.Move(ctx, strg, "a.txt", "moved.txt")
	s.Require().NoError(err)

	_, err = strg.Get(ctx, "a.txt")
	s.Error(err)

	got, err := strg.Get(ctx, "moved.txt")
	s.Require().NoError(err)
	rc, err := got.Open()
	s.Require().NoError(err)
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	s.Equal("a", string(body))
}

func (s *StorageTestSuite) TestSignedURL() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		name     string
		ttl      time.Duration
		want     string
		wantErr  string
	}{
		{
			caseName: "typical",
			name:     "a.txt",
			ttl:      time.Hour,
			want:     "https://s3mock/mybucket/a.txt",
		},
		{
			caseName: "not found",
			name:     "not-found.txt",
			ttl:      time.Hour,
			wantErr:  "not exist: not-found.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()

			got, err := strg.SignedURL(tc.ctx, s2.SignedURLOptions{Name: tc.name, TTL: tc.ttl})
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
	s.Run("unknown-client-type", func() {
		strg := &storage{
			client: &mockS3Client{}, // Not a *s3.Client
		}
		_, err := strg.SignedURL(context.Background(), s2.SignedURLOptions{Name: "a.txt", TTL: time.Hour})
		s.Error(err)
		s.ErrorContains(err, "unknown client type")
	})
}

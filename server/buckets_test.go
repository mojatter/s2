package server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/stretchr/testify/suite"
)

type BucketsTestSuite struct {
	suite.Suite
	buckets *Buckets
}

func TestBucketsTestSuite(t *testing.T) {
	suite.Run(t, &BucketsTestSuite{})
}

func (s *BucketsTestSuite) SetupTest() {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Type = s2.TypeOSFS
	cfg.Root = s.T().TempDir()
	bs, err := newBuckets(ctx, cfg)
	s.Require().NoError(err)
	s.buckets = bs
}

func (s *BucketsTestSuite) TestNewBuckets() {
	ctx := context.Background()

	s.Run("memfs", func() {
		cfg := DefaultConfig()
		cfg.Type = s2.TypeMemFS
		bs, err := newBuckets(ctx, cfg)
		s.Require().NoError(err)
		s.NotNil(bs)
	})

	s.Run("osfs", func() {
		cfg := DefaultConfig()
		cfg.Type = s2.TypeOSFS
		cfg.Root = s.T().TempDir()
		bs, err := newBuckets(ctx, cfg)
		s.Require().NoError(err)
		s.NotNil(bs)
	})

	s.Run("unknown type", func() {
		cfg := DefaultConfig()
		cfg.Type = s2.Type("invalid")
		_, err := newBuckets(ctx, cfg)
		s.Error(err)
	})
}

func (s *BucketsTestSuite) TestHealthPathReservedBucket() {
	testCases := []struct {
		caseName   string
		healthPath string
		want       string
	}{
		{caseName: "default reserves healthz", healthPath: "/healthz", want: "healthz"},
		{caseName: "dash-prefixed reserves nothing", healthPath: "/-/healthz", want: ""},
		{caseName: "disabled", healthPath: "", want: ""},
		{caseName: "nested under valid name", healthPath: "/ping/now", want: "ping"},
		{caseName: "leading dot is invalid bucket", healthPath: "/.internal/healthz", want: ""},
		{caseName: "uppercase is invalid bucket", healthPath: "/Health/ok", want: ""},
		{caseName: "short is invalid bucket", healthPath: "/hi", want: ""},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.Equal(tc.want, healthPathReservedBucket(tc.healthPath))
		})
	}
}

func (s *BucketsTestSuite) TestCreateRejectsReservedName() {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Type = s2.TypeOSFS
	cfg.Root = s.T().TempDir()
	// Default HealthPath is "/healthz" which reserves the bucket name "healthz".
	bs, err := newBuckets(ctx, cfg)
	s.Require().NoError(err)

	err = bs.Create(ctx, "healthz")
	s.Require().Error(err)
	s.True(errors.Is(err, ErrReservedBucketName), "expected ErrReservedBucketName, got %v", err)

	// Neighboring names are still allowed.
	s.NoError(bs.Create(ctx, "healthz-sibling"))
}

func (s *BucketsTestSuite) TestCreateAndNames() {
	ctx := context.Background()

	s.Require().NoError(s.buckets.Create(ctx, "alpha"))
	s.Require().NoError(s.buckets.Create(ctx, "beta"))

	names, err := s.buckets.Names(ctx)
	s.Require().NoError(err)
	s.Len(names, 2)
	s.Contains(names, "alpha")
	s.Contains(names, "beta")
}

func (s *BucketsTestSuite) TestExists() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.Create(ctx, "exists-test"))

	ok, err := s.buckets.Exists(ctx, "exists-test")
	s.Require().NoError(err)
	s.True(ok)

	ok, err = s.buckets.Exists(ctx, "no-such-bucket")
	s.Require().NoError(err)
	s.False(ok)
}

func (s *BucketsTestSuite) TestGetAndPut() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.Create(ctx, "mybucket"))

	strg, err := s.buckets.Get(ctx, "mybucket")
	s.Require().NoError(err)

	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("hello.txt", []byte("hello"))))

	obj, err := strg.Get(ctx, "hello.txt")
	s.Require().NoError(err)
	s.Equal("hello.txt", obj.Name())

	rc, err := obj.Open()
	s.Require().NoError(err)
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	s.Equal("hello", string(b))
}

func (s *BucketsTestSuite) TestDelete() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.Create(ctx, "to-delete"))

	ok, _ := s.buckets.Exists(ctx, "to-delete")
	s.True(ok)

	s.Require().NoError(s.buckets.Delete(ctx, "to-delete"))

	ok, _ = s.buckets.Exists(ctx, "to-delete")
	s.False(ok)
}

func (s *BucketsTestSuite) TestCreateFolder() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.Create(ctx, "fb"))

	s.Require().NoError(s.buckets.CreateFolder(ctx, "fb", "sub/dir"))

	strg, err := s.buckets.Get(ctx, "fb")
	s.Require().NoError(err)

	// sub/dir/.keep should exist under the bucket
	res, err := strg.List(ctx, s2.ListOptions{Prefix: "sub/", Recursive: true})
	s.Require().NoError(err)
	s.NotEmpty(res.Objects)

	// "sub" should appear as a prefix in directory listing
	res, err = strg.List(ctx, s2.ListOptions{})
	s.Require().NoError(err)
	s.Contains(res.CommonPrefixes, "sub")
}

func (s *BucketsTestSuite) TestCreatedAt() {
	ctx := context.Background()
	before := time.Now()
	s.Require().NoError(s.buckets.Create(ctx, "ts-bucket"))
	after := time.Now()

	got := s.buckets.CreatedAt(ctx, "ts-bucket")
	s.False(got.Before(before.Add(-time.Second)), "CreatedAt should not be before bucket creation")
	s.False(got.After(after.Add(time.Second)), "CreatedAt should not be after bucket creation")
}

func (s *BucketsTestSuite) TestCreatedAtMissing() {
	ctx := context.Background()
	before := time.Now()
	got := s.buckets.CreatedAt(ctx, "nonexistent")
	s.False(got.Before(before.Add(-time.Second)), "fallback should return approximately now")
}

func (s *BucketsTestSuite) TestGetNotFound() {
	ctx := context.Background()

	_, err := s.buckets.Get(ctx, "no-such-bucket")
	s.Require().Error(err)

	var bucketErr *ErrBucketNotFound
	s.True(errors.As(err, &bucketErr))
	s.Equal("no-such-bucket", bucketErr.Name)
	s.Contains(err.Error(), "bucket not found: no-such-bucket")
}

func (s *BucketsTestSuite) TestGetAfterDelete() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.Create(ctx, "temp"))

	_, err := s.buckets.Get(ctx, "temp")
	s.Require().NoError(err)

	s.Require().NoError(s.buckets.Delete(ctx, "temp"))

	_, err = s.buckets.Get(ctx, "temp")
	s.Require().Error(err)
	var bucketErr *ErrBucketNotFound
	s.True(errors.As(err, &bucketErr))
}

func (s *BucketsTestSuite) TestFilterKeep() {
	objs := []s2.Object{
		s2.NewObjectBytes(".keep", []byte{}),
		s2.NewObjectBytes("file.txt", []byte("x")),
		s2.NewObjectBytes("sub/.keep", []byte{}),
		s2.NewObjectBytes("sub/data.csv", []byte("y")),
	}

	filtered := FilterKeep(objs)
	s.Len(filtered, 2)
	s.Equal("file.txt", filtered[0].Name())
	s.Equal("sub/data.csv", filtered[1].Name())
}

func (s *BucketsTestSuite) TestFilterKeepEmpty() {
	filtered := FilterKeep(nil)
	s.Empty(filtered)
}

func (s *BucketsTestSuite) TestKeepFileNotVisibleInList() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.Create(ctx, "kb"))

	strg, err := s.buckets.Get(ctx, "kb")
	s.Require().NoError(err)

	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("real.txt", []byte("data"))))

	res, err := strg.List(ctx, s2.ListOptions{})
	s.Require().NoError(err)

	objs := FilterKeep(res.Objects)
	s.Len(objs, 1)
	s.Equal("real.txt", objs[0].Name())
}

package server

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
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
	for _, name := range []string{"to-delete", "to-delete-archive", "to-delet"} {
		s.Require().NoError(s.buckets.Create(ctx, name))
	}

	s.Require().NoError(s.buckets.Delete(ctx, "to-delete"))

	testCases := []struct {
		caseName string
		bucket   string
		want     bool
	}{
		{caseName: "the bucket itself is gone", bucket: "to-delete", want: false},
		{caseName: "a bucket sharing the prefix survives", bucket: "to-delete-archive", want: true},
		{caseName: "a shorter name survives", bucket: "to-delet", want: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ok, err := s.buckets.Exists(ctx, tc.bucket)
			s.Require().NoError(err)
			s.Equal(tc.want, ok)
		})
	}
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

// A folder must not create the bucket it is written into (#224).
func (s *BucketsTestSuite) TestCreateFolderMissingBucket() {
	ctx := context.Background()

	err := s.buckets.CreateFolder(ctx, "ghost", "docs")
	var notFound *ErrBucketNotFound
	s.ErrorAs(err, &notFound)

	exists, err := s.buckets.Exists(ctx, "ghost")
	s.Require().NoError(err)
	s.False(exists)
	names, err := s.buckets.Names(ctx)
	s.Require().NoError(err)
	s.NotContains(names, "ghost")
}

func (s *BucketsTestSuite) TestCreatedAt() {
	hourAgo := time.Now().Add(-time.Hour).Truncate(time.Second)
	testCases := []struct {
		caseName string
		prepare  func(ctx context.Context, root string, bs *Buckets)
		want     func(got time.Time)
	}{
		{
			caseName: "a new bucket is dated now",
			prepare:  func(context.Context, string, *Buckets) {},
			want: func(got time.Time) {
				s.WithinDuration(time.Now(), got, time.Minute)
			},
		},
		{
			caseName: "re-creating keeps the date",
			prepare: func(ctx context.Context, root string, bs *Buckets) {
				s.Require().NoError(os.Chtimes(filepath.Join(root, "photos", keepFile), hourAgo, hourAgo))
				s.Require().NoError(bs.Create(ctx, "photos"))
			},
			want: func(got time.Time) { s.True(got.Equal(hourAgo), got) },
		},
		{
			caseName: "deleting and creating again moves it",
			prepare: func(ctx context.Context, root string, bs *Buckets) {
				s.Require().NoError(os.Chtimes(filepath.Join(root, "photos", keepFile), hourAgo, hourAgo))
				s.Require().NoError(bs.Delete(ctx, "photos"))
				s.Require().NoError(bs.Create(ctx, "photos"))
			},
			want: func(got time.Time) { s.True(got.After(hourAgo), got) },
		},
		{
			caseName: "a bucket s2 did not create is undated",
			prepare: func(ctx context.Context, _ string, bs *Buckets) {
				sub, err := bs.strg.Sub(ctx, "photos")
				s.Require().NoError(err)
				s.Require().NoError(sub.Delete(ctx, keepFile))
				s.Require().NoError(sub.Put(ctx, s2.NewObjectBytes("a.txt", []byte("x"))))
			},
			want: func(got time.Time) { s.True(got.IsZero(), got) },
		},
		{
			caseName: "creating over a directory s2 did not make leaves it undated",
			prepare: func(ctx context.Context, _ string, bs *Buckets) {
				sub, err := bs.strg.Sub(ctx, "photos")
				s.Require().NoError(err)
				s.Require().NoError(sub.Delete(ctx, keepFile))
				s.Require().NoError(sub.Put(ctx, s2.NewObjectBytes("a.txt", []byte("x"))))
				s.Require().NoError(bs.Create(ctx, "photos"))
			},
			want: func(got time.Time) { s.True(got.IsZero(), got) },
		},
		{
			caseName: "a missing bucket is undated",
			prepare: func(ctx context.Context, _ string, bs *Buckets) {
				s.Require().NoError(bs.Delete(ctx, "photos"))
			},
			want: func(got time.Time) { s.True(got.IsZero(), got) },
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			cfg := DefaultConfig()
			cfg.Root = s.T().TempDir()
			bs, err := newBuckets(ctx, cfg)
			s.Require().NoError(err)
			s.Require().NoError(bs.Create(ctx, "photos"))
			tc.prepare(ctx, cfg.Root, bs)

			got, err := bs.CreatedAt(ctx, "photos")

			s.Require().NoError(err)
			tc.want(got)
		})
	}
}

func (s *BucketsTestSuite) TestGeneration() {
	hourAgo := time.Now().Add(-time.Hour).Truncate(time.Second)
	testCases := []struct {
		caseName string
		prepare  func(ctx context.Context, root string, bs *Buckets)
		want     func(got int64)
	}{
		{
			caseName: "a new bucket starts one now",
			prepare:  func(context.Context, string, *Buckets) {},
			want:     func(got int64) { s.WithinDuration(time.Now(), time.Unix(0, got), time.Minute) },
		},
		{
			caseName: "re-creating keeps it",
			prepare: func(ctx context.Context, root string, bs *Buckets) {
				s.Require().NoError(os.Chtimes(filepath.Join(root, bucketStateDir, "photos"), hourAgo, hourAgo))
				s.Require().NoError(bs.Create(ctx, "photos"))
			},
			want: func(got int64) { s.Equal(hourAgo.UnixNano(), got) },
		},
		{
			caseName: "writing .keep keeps it",
			prepare: func(ctx context.Context, root string, bs *Buckets) {
				s.Require().NoError(os.Chtimes(filepath.Join(root, bucketStateDir, "photos"), hourAgo, hourAgo))
				sub, err := bs.strg.Sub(ctx, "photos")
				s.Require().NoError(err)
				s.Require().NoError(sub.Put(ctx, s2.NewObjectBytes(keepFile, []byte("x"))))
			},
			want: func(got int64) { s.Equal(hourAgo.UnixNano(), got) },
		},
		{
			caseName: "deleting and creating again moves it",
			prepare: func(ctx context.Context, root string, bs *Buckets) {
				s.Require().NoError(os.Chtimes(filepath.Join(root, bucketStateDir, "photos"), hourAgo, hourAgo))
				s.Require().NoError(bs.Delete(ctx, "photos"))
				s.Require().NoError(bs.Create(ctx, "photos"))
			},
			want: func(got int64) { s.Greater(got, hourAgo.UnixNano()) },
		},
		{
			caseName: "creating replaces one the bucket's directory outlived",
			prepare: func(ctx context.Context, root string, bs *Buckets) {
				s.Require().NoError(os.Chtimes(filepath.Join(root, bucketStateDir, "photos"), hourAgo, hourAgo))
				s.Require().NoError(os.RemoveAll(filepath.Join(root, "photos")))
				s.Require().NoError(bs.Create(ctx, "photos"))
			},
			want: func(got int64) { s.Greater(got, hourAgo.UnixNano()) },
		},
		{
			caseName: "a bucket s2 did not create gets one on first use",
			prepare: func(ctx context.Context, root string, bs *Buckets) {
				s.Require().NoError(bs.Delete(ctx, "photos"))
				s.Require().NoError(os.MkdirAll(filepath.Join(root, "photos"), 0o750))
			},
			want: func(got int64) { s.WithinDuration(time.Now(), time.Unix(0, got), time.Minute) },
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			cfg := DefaultConfig()
			cfg.Root = s.T().TempDir()
			bs, err := newBuckets(ctx, cfg)
			s.Require().NoError(err)
			s.Require().NoError(bs.Create(ctx, "photos"))
			tc.prepare(ctx, cfg.Root, bs)

			got, err := bs.Generation(ctx, "photos")
			s.Require().NoError(err)
			tc.want(got)
			again, err := bs.Generation(ctx, "photos")
			s.Require().NoError(err)
			s.Equal(got, again, "a generation must be stable across reads")
		})
	}
}

func (s *BucketsTestSuite) TestBucketNameIsOnePathElement() {
	ctx := context.Background()
	// One real bucket, so the per-bucket state exists to be listed below.
	s.Require().NoError(s.buckets.Create(ctx, "photos"))

	// A name that spans directories is not a bucket, on every entry point:
	// the S3 API's {bucket} wildcard hands one over whenever a request
	// spells the separator as "%2F".
	testCases := []struct {
		caseName string
		call     func(name string) error
	}{
		{"create", func(name string) error { return s.buckets.Create(ctx, name) }},
		{"delete", func(name string) error { return s.buckets.Delete(ctx, name) }},
		{"created at", func(name string) error { _, err := s.buckets.CreatedAt(ctx, name); return err }},
		{"generation", func(name string) error { _, err := s.buckets.Generation(ctx, name); return err }},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.Error(tc.call("outer/inner"))
		})
	}
	exists, err := s.buckets.Exists(ctx, "outer/inner")
	s.Require().NoError(err)
	s.False(exists)

	// Nothing was recorded for it either. A listing, not Exists: a
	// generation is stored as a file, so statting a name under one fails for
	// a reason that has nothing to do with the guard.
	meta, err := s.buckets.state(ctx)
	s.Require().NoError(err)
	res, err := meta.List(ctx, s2.ListOptions{Recursive: true})
	s.Require().NoError(err)
	for _, obj := range res.Objects {
		s.NotEqual("outer/inner", obj.Name())
	}
}

func (s *BucketsTestSuite) TestDeleteRemovesGeneration() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.Create(ctx, "photos"))
	s.Require().NoError(s.buckets.Delete(ctx, "photos"))

	meta, err := s.buckets.state(ctx)
	s.Require().NoError(err)
	exists, err := meta.Exists(ctx, "photos")
	s.Require().NoError(err)
	s.False(exists)
	names, err := s.buckets.Names(ctx)
	s.Require().NoError(err)
	s.Empty(names)
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

func (s *BucketsTestSuite) TestNamesSkipsHiddenEntries() {
	ctx := context.Background()

	s.Require().NoError(s.buckets.Create(ctx, "alpha"))
	s.Require().NoError(s.buckets.strg.Put(ctx, s2.NewObjectBytes(multipartDir+"/deadbeef/00001", []byte("x"))))

	names, err := s.buckets.Names(ctx)
	s.Require().NoError(err)
	s.Equal([]string{"alpha"}, names)
}

func (s *BucketsTestSuite) TestHiddenEntryIsNotABucket() {
	ctx := context.Background()
	s.Require().NoError(s.buckets.strg.Put(ctx, s2.NewObjectBytes(multipartDir+"/deadbeef/00001", []byte("x"))))

	err := s.buckets.Create(ctx, multipartDir)
	s.ErrorIs(err, ErrReservedBucketName)

	exists, err := s.buckets.Exists(ctx, multipartDir)
	s.Require().NoError(err)
	s.False(exists)

	_, err = s.buckets.Get(ctx, multipartDir)
	var notFound *ErrBucketNotFound
	s.ErrorAs(err, &notFound)

	err = s.buckets.Delete(ctx, multipartDir)
	s.ErrorAs(err, &notFound)

	err = s.buckets.CreateFolder(ctx, multipartDir, "deadbeef")
	s.ErrorAs(err, &notFound)
	keep, err := s.buckets.strg.Exists(ctx, multipartDir+"/deadbeef/"+keepFile)
	s.Require().NoError(err)
	s.False(keep)

	// The state itself is untouched.
	ok, err := s.buckets.strg.Exists(ctx, multipartDir+"/deadbeef/00001")
	s.Require().NoError(err)
	s.True(ok)
}

// wrappedStorage is what a library user registers with RegisterNewStorageFunc:
// a decorator holding the s2.Storage interface, not a backend's concrete type.
type wrappedStorage struct {
	s2.Storage
}

func (w wrappedStorage) Sub(ctx context.Context, prefix string) (s2.Storage, error) {
	sub, err := w.Storage.Sub(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return wrappedStorage{sub}, nil
}

// Buckets speaks s2.Storage and nothing else, so a decorator around any
// backend serves it.
func (s *BucketsTestSuite) TestBucketsOverAWrappedStorage() {
	testCases := []struct {
		caseName string
		typ      s2.Type
	}{
		{caseName: "osfs", typ: s2.TypeOSFS},
		{caseName: "memfs", typ: s2.TypeMemFS},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			base, err := s2.NewStorage(ctx, s2.Config{Type: tc.typ, Root: s.T().TempDir()})
			s.Require().NoError(err)
			bs := &Buckets{strg: wrappedStorage{base}}

			s.Require().NoError(bs.Create(ctx, "photos"))
			gen, err := bs.Generation(ctx, "photos")
			s.Require().NoError(err)
			s.NotZero(gen)

			names, err := bs.Names(ctx)
			s.Require().NoError(err)
			s.Equal([]string{"photos"}, names)

			s.Require().NoError(bs.Delete(ctx, "photos"))
			names, err = bs.Names(ctx)
			s.Require().NoError(err)
			s.Empty(names)
		})
	}
}

// Per-bucket state is s2-server's own and lives beside the buckets, not in a
// directory a backend keeps for itself.
func (s *BucketsTestSuite) TestStateLivesBesideTheBuckets() {
	ctx := context.Background()
	name := bucketStateDir + "/photos"
	s.Require().NoError(s.buckets.Create(ctx, "photos"))

	exists, err := s.buckets.strg.Exists(ctx, name)
	s.Require().NoError(err)
	s.True(exists)

	s.Require().NoError(s.buckets.Delete(ctx, "photos"))
	exists, err = s.buckets.strg.Exists(ctx, name)
	s.Require().NoError(err)
	s.False(exists)
}

// getHook is shared by every hookedStorage a Sub produces.
type getHook struct {
	misses   atomic.Int64
	parkOn   int64  // the miss of name to park on: 1 is Generation's own Get,
	onMiss   func() // 2 the re-check inside recordGeneration, under the lock
	putNamed atomic.Int64
	name     string
}

// hookedStorage parks the first caller that finds name missing, so a second
// caller can finish while it waits.
type hookedStorage struct {
	s2.Storage
	hook *getHook
}

func (h hookedStorage) Sub(ctx context.Context, prefix string) (s2.Storage, error) {
	sub, err := h.Storage.Sub(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return hookedStorage{sub, h.hook}, nil
}

func (h hookedStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	obj, err := h.Storage.Get(ctx, name)
	// Counted, not sync.Once: Do holds a mutex for the whole call, which would
	// park the second caller here too.
	if name == h.hook.name && isNotExist(err) && h.hook.misses.Add(1) == h.hook.parkOn {
		h.hook.onMiss()
	}
	return obj, err
}

func (h hookedStorage) Put(ctx context.Context, obj s2.Object) error {
	if obj.Name() == h.hook.name {
		h.hook.putNamed.Add(1)
	}
	return h.Storage.Put(ctx, obj)
}

// A bucket an upgrade left without a marker is recorded once, however many
// callers find it missing at the same time: the value handed to the first
// must be the one that stays. Parked on its own Get, the second caller runs
// to completion and the re-check must see it; parked on that re-check, which
// runs under the lock, the second caller must not get in at all.
func (s *BucketsTestSuite) TestGenerationIsRecordedOnce() {
	testCases := []struct {
		caseName   string
		parkOn     int64
		wantSecond bool // whether the second caller finishes while the first is parked
	}{
		{caseName: "the re-check sees a marker written meanwhile", parkOn: 1, wantSecond: true},
		{caseName: "the lock keeps a second writer out", parkOn: 2, wantSecond: false},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			base, err := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})
			s.Require().NoError(err)

			parked, release := make(chan struct{}), make(chan struct{})
			hook := &getHook{name: "photos", parkOn: tc.parkOn, onMiss: func() {
				close(parked)
				<-release
			}}
			bs := &Buckets{strg: hookedStorage{base, hook}}
			// The upgraded shape: the bucket exists, its marker does not.
			sub, err := bs.strg.Sub(ctx, "photos")
			s.Require().NoError(err)
			s.Require().NoError(sub.Put(ctx, s2.NewObjectBytes(keepFile, []byte{})))

			var first, second int64
			firstDone := make(chan error, 1)
			go func() {
				gen, err := bs.Generation(ctx, "photos")
				first = gen
				firstDone <- err
			}()
			<-parked

			secondDone := make(chan error, 1)
			go func() {
				gen, err := bs.Generation(ctx, "photos")
				second = gen
				secondDone <- err
			}()
			if tc.wantSecond {
				select {
				case err := <-secondDone:
					s.Require().NoError(err)
				case <-time.After(10 * time.Second):
					s.FailNow("the second caller never finished")
				}
			} else {
				select {
				case <-secondDone:
					s.FailNow("the second caller wrote while the first held the lock")
				case <-time.After(200 * time.Millisecond):
				}
			}

			close(release)
			s.Require().NoError(<-firstDone)
			if !tc.wantSecond {
				s.Require().NoError(<-secondDone)
			}

			third, err := bs.Generation(ctx, "photos")
			s.Require().NoError(err)
			s.Equal(int64(1), hook.putNamed.Load(), "the marker must be written once")
			s.Equal(first, second, "both callers must take the recorded generation")
			s.Equal(first, third)
		})
	}
}

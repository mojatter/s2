package server

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

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

func (s *MultipartStoreTestSuite) TestMetadata() {
	ctx := context.Background()
	ms := s.server.Multipart
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 1, s2.Metadata{"author": "uz"}))

	testCases := []struct {
		caseName string
		id       string
		bucket   string
		key      string
		gen      int64
		wantErr  error
	}{
		{caseName: "match", id: "id1", bucket: "photos", key: "a.jpg", gen: 1},
		{caseName: "unknown id", id: "id2", bucket: "photos", key: "a.jpg", gen: 1, wantErr: ErrNoSuchUpload},
		{caseName: "another key", id: "id1", bucket: "photos", key: "b.jpg", gen: 1, wantErr: ErrNoSuchUpload},
		{caseName: "another bucket", id: "id1", bucket: "videos", key: "a.jpg", gen: 1, wantErr: ErrNoSuchUpload},
		{caseName: "a recreated bucket", id: "id1", bucket: "photos", key: "a.jpg", gen: 2, wantErr: ErrNoSuchUpload},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			md, err := ms.Metadata(ctx, tc.id, tc.bucket, tc.key, tc.gen)
			if tc.wantErr != nil {
				s.ErrorIs(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Equal("uz", md["author"])
		})
	}
}

// Expiry is judged on access, not by the sweep.
func (s *MultipartStoreTestSuite) TestMetadataExpiry() {
	testCases := []struct {
		caseName string
		maxAge   int64
		wantErr  error
	}{
		{caseName: "past maxAge reads as unknown", maxAge: 3600, wantErr: ErrNoSuchUpload},
		{caseName: "within maxAge", maxAge: 3 * 3600},
		{caseName: "a disabled sweeper keeps it forever", maxAge: -1},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			root := s.T().TempDir()
			ms := s.newStoreAt(s2.TypeOSFS, root, tc.maxAge)
			s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
			s.backdate(root, "id1", uploadMetaName, 2*time.Hour)

			_, err := ms.Metadata(ctx, "id1", "photos", "a.jpg", 0)

			if tc.wantErr != nil {
				s.ErrorIs(err, tc.wantErr)
				return
			}
			s.NoError(err)
		})
	}
}

// undatedStorage reports no modification time, as some backends do.
type undatedStorage struct{ s2.Storage }

func (u undatedStorage) Sub(ctx context.Context, name string) (s2.Storage, error) {
	sub, err := u.Storage.Sub(ctx, name)
	return undatedStorage{sub}, err
}

func (u undatedStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	obj, err := u.Storage.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return undatedObject{obj}, nil
}

type undatedObject struct{ s2.Object }

func (undatedObject) LastModified() time.Time { return time.Time{} }

// Without a modification time nothing can age out.
func (s *MultipartStoreTestSuite) TestUndatedUploadNeverExpires() {
	ctx := context.Background()
	base := s.newStore(s2.TypeMemFS)
	s.Require().NoError(base.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
	ms := &MultipartStore{strg: undatedStorage{base.Storage()}, maxAge: time.Nanosecond}

	s.Require().NoError(ms.Sweep(ctx))

	_, err := ms.Metadata(ctx, "id1", "photos", "a.jpg", 0)
	s.NoError(err)
	exists, err := ms.Storage().Exists(ctx, "id1")
	s.Require().NoError(err)
	s.True(exists)
}

// failingStorage fails every Get.
type failingStorage struct {
	s2.Storage
	err error
}

func (f failingStorage) Sub(context.Context, string) (s2.Storage, error) { return f, nil }

func (f failingStorage) Get(context.Context, string) (s2.Object, error) { return nil, f.err }

// A backend failure must not read as an unknown upload.
func (s *MultipartStoreTestSuite) TestMetadataSurfacesReadFailure() {
	want := errors.New("backend unavailable")
	ms := &MultipartStore{strg: failingStorage{err: want}}

	_, err := ms.Metadata(context.Background(), "id1", "photos", "a.jpg", 0)
	s.ErrorIs(err, want)
	s.NotErrorIs(err, ErrNoSuchUpload)
}

// existsFailingStorage stores objects but cannot answer Exists.
type existsFailingStorage struct {
	s2.Storage
	err error
}

func (e existsFailingStorage) Sub(ctx context.Context, name string) (s2.Storage, error) {
	sub, err := e.Storage.Sub(ctx, name)
	return existsFailingStorage{sub, e.err}, err
}

func (e existsFailingStorage) Exists(context.Context, string) (bool, error) { return false, e.err }

// A part stored after its upload was aborted must not outlive it.
func (s *MultipartStoreTestSuite) TestPutPartAfterAbort() {
	ctx := context.Background()
	ms := s.newStore(s2.TypeOSFS)
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
	s.Require().NoError(ms.Remove(ctx, "id1"))

	err := ms.PutPart(ctx, "id1", 1, []byte("late"), `"x"`)

	s.ErrorIs(err, ErrNoSuchUpload)
	exists, err := ms.Storage().Exists(ctx, "id1")
	s.Require().NoError(err)
	s.False(exists)
}

// A probe that fails must not fail a part that is already stored.
func (s *MultipartStoreTestSuite) TestPutPartIgnoresProbeFailure() {
	ctx := context.Background()
	ms := s.newStore(s2.TypeOSFS)
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
	probeBroken := &MultipartStore{strg: existsFailingStorage{ms.Storage(), errors.New("probe failed")}}

	s.Require().NoError(probeBroken.PutPart(ctx, "id1", 1, []byte("hello"), `"x"`))

	obj, err := ms.Part(ctx, "id1", 1)
	s.Require().NoError(err)
	s.Equal(uint64(5), obj.Length())
}

// vanishingStorage loses meta between Get and Open, as a racing Abort does.
type vanishingStorage struct{ s2.Storage }

func (v vanishingStorage) Sub(context.Context, string) (s2.Storage, error) { return v, nil }

func (v vanishingStorage) Get(context.Context, string) (s2.Object, error) {
	return vanishingObject{s2.NewObjectBytes(uploadMetaName, nil)}, nil
}

type vanishingObject struct{ s2.Object }

func (vanishingObject) Open() (io.ReadCloser, error) {
	return nil, &fs.PathError{Op: "open", Path: uploadMetaName, Err: fs.ErrNotExist}
}

func (s *MultipartStoreTestSuite) TestMetadataOfVanishingRecord() {
	ms := &MultipartStore{strg: vanishingStorage{}}

	_, err := ms.Metadata(context.Background(), "id1", "photos", "a.jpg", 0)
	s.ErrorIs(err, ErrNoSuchUpload)
}

func (s *MultipartStoreTestSuite) TestAbort() {
	testCases := []struct {
		caseName string
		setup    func(ctx context.Context, root string, ms *MultipartStore)
		bucket   string
		key      string
		wantErr  error
		wantGone bool
	}{
		{
			caseName: "bound upload",
			setup: func(ctx context.Context, _ string, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
			},
			bucket: "photos", key: "a.jpg", wantGone: true,
		},
		{
			caseName: "bound elsewhere is left alone",
			setup: func(ctx context.Context, _ string, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
			},
			bucket: "photos", key: "b.jpg", wantErr: ErrNoSuchUpload,
		},
		{
			// Abort ignores age.
			caseName: "expired upload",
			setup: func(ctx context.Context, root string, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
				s.backdate(root, "id1", uploadMetaName, 2*time.Hour)
			},
			bucket: "photos", key: "a.jpg", wantGone: true,
		},
		{
			caseName: "leftover parts without a record",
			setup: func(ctx context.Context, _ string, ms *MultipartStore) {
				u, err := ms.upload(ctx, "id1")
				s.Require().NoError(err)
				s.Require().NoError(u.Put(ctx, s2.NewObjectBytes(uploadPartName(1), []byte("x"))))
			},
			bucket: "photos", key: "a.jpg", wantGone: true,
		},
		{
			// Reading it may fail for a moment; removing another upload's parts is forever.
			caseName: "unreadable record",
			setup: func(ctx context.Context, _ string, ms *MultipartStore) {
				u, err := ms.upload(ctx, "id1")
				s.Require().NoError(err)
				s.Require().NoError(u.Put(ctx, s2.NewObjectBytes(uploadMetaName, []byte("{"))))
			},
			bucket: "photos", key: "a.jpg", wantErr: io.ErrUnexpectedEOF,
		},
		{
			caseName: "nothing at all",
			setup:    func(context.Context, string, *MultipartStore) {},
			bucket:   "photos", key: "a.jpg", wantErr: ErrNoSuchUpload, wantGone: true,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			root := s.T().TempDir()
			ms := s.newStoreAt(s2.TypeOSFS, root, 3600)
			tc.setup(ctx, root, ms)

			err := ms.Abort(ctx, "id1", tc.bucket, tc.key, 0)

			if tc.wantErr != nil {
				s.ErrorIs(err, tc.wantErr)
			} else {
				s.NoError(err)
			}
			exists, err := ms.Storage().Exists(ctx, "id1")
			s.Require().NoError(err)
			s.Equal(tc.wantGone, !exists)
		})
	}
}

func (s *MultipartStoreTestSuite) TestRemoveMissingIsNoop() {
	for _, typ := range []s2.Type{s2.TypeOSFS, s2.TypeMemFS} {
		s.Run(string(typ), func() {
			s.NoError(s.newStore(typ).Remove(context.Background(), "id1"))
		})
	}
}

// listFailingStorage cannot enumerate uploads.
type listFailingStorage struct {
	s2.Storage
	err error
}

func (l listFailingStorage) List(context.Context, s2.ListOptions) (s2.ListResult, error) {
	return s2.ListResult{}, l.err
}

func (s *MultipartStoreTestSuite) TestSweepSurfacesListFailure() {
	want := errors.New("backend unavailable")
	ms := &MultipartStore{strg: listFailingStorage{err: want}, maxAge: time.Hour}

	s.ErrorIs(ms.Sweep(context.Background()), want)
}

// pagingStorage lists one upload per page, as s3 does past MaxKeys.
type pagingStorage struct{ s2.Storage }

func (p pagingStorage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
	res, err := p.Storage.List(ctx, s2.ListOptions{})
	if err != nil {
		return s2.ListResult{}, err
	}
	for i, id := range res.CommonPrefixes {
		if id <= opts.After {
			continue
		}
		page := s2.ListResult{CommonPrefixes: []string{id}}
		if i < len(res.CommonPrefixes)-1 {
			page.NextAfter = id
		}
		return page, nil
	}
	return s2.ListResult{}, nil
}

func (s *MultipartStoreTestSuite) TestSweepPages() {
	ctx := context.Background()
	root := s.T().TempDir()
	base := s.newStoreAt(s2.TypeOSFS, root, 3600)
	for _, id := range []string{"id1", "id2", "id3"} {
		s.Require().NoError(base.Create(ctx, id, "photos", id+".jpg", 0, nil))
	}
	s.backdate(root, "id1", uploadMetaName, 2*time.Hour)
	s.backdate(root, "id3", uploadMetaName, 2*time.Hour)
	ms := &MultipartStore{strg: pagingStorage{base.Storage()}, maxAge: time.Hour}

	s.Require().NoError(ms.Sweep(ctx))

	for id, wantGone := range map[string]bool{"id1": true, "id2": false, "id3": true} {
		exists, err := base.Storage().Exists(ctx, id)
		s.Require().NoError(err)
		s.Equalf(wantGone, !exists, "%s", id)
	}
}

func (s *MultipartStoreTestSuite) TestSweep() {
	const maxAge = int64(3600)

	testCases := []struct {
		caseName string
		setup    func(ctx context.Context, root string, ms *MultipartStore)
		wantGone bool
	}{
		{
			caseName: "a fresh upload is kept",
			setup: func(ctx context.Context, _ string, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
			},
		},
		{
			caseName: "an upload past maxAge is removed",
			setup: func(ctx context.Context, root string, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
				s.backdate(root, "id1", uploadMetaName, 2*time.Hour)
			},
			wantGone: true,
		},
		{
			caseName: "a recent part does not refresh a stale record",
			setup: func(ctx context.Context, root string, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
				s.backdate(root, "id1", uploadMetaName, 2*time.Hour)
				s.Require().NoError(ms.PutPart(ctx, "id1", 1, []byte("x"), `"etag"`))
			},
			wantGone: true,
		},
		{
			// Only the sweep can reclaim it.
			caseName: "a record that cannot be read is dated by its contents",
			setup: func(ctx context.Context, root string, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, s2.Metadata{"author": "uz"}))
				sidecar := filepath.Join(root, multipartDir, "id1", ".meta", uploadMetaName)
				s.Require().NoError(os.WriteFile(sidecar, []byte("{"), 0o600))
				s.backdate(root, "id1", uploadMetaName, 2*time.Hour)
			},
			wantGone: true,
		},
		{
			caseName: "without a record a stale part decides",
			setup: func(ctx context.Context, root string, ms *MultipartStore) {
				s.putPart(ctx, ms, "id1", 1)
				s.backdate(root, "id1", uploadPartName(1), 2*time.Hour)
			},
			wantGone: true,
		},
		{
			caseName: "without a record a fresh part is kept",
			setup: func(ctx context.Context, _ string, ms *MultipartStore) {
				s.putPart(ctx, ms, "id1", 1)
			},
		},
		{
			// It may be a Create in flight.
			caseName: "an undatable upload is left alone",
			setup: func(_ context.Context, root string, _ *MultipartStore) {
				s.Require().NoError(os.MkdirAll(filepath.Join(root, multipartDir, "id1"), 0o750))
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			root := s.T().TempDir()
			ms := s.newStoreAt(s2.TypeOSFS, root, maxAge)
			tc.setup(ctx, root, ms)
			s.Require().NoError(ms.Create(ctx, "id2", "photos", "b.jpg", 0, nil))

			s.Require().NoError(ms.Sweep(ctx))

			exists, err := ms.Storage().Exists(ctx, "id1")
			s.Require().NoError(err)
			s.Equal(tc.wantGone, !exists)
			exists, err = ms.Storage().Exists(ctx, "id2")
			s.Require().NoError(err)
			s.True(exists, "a fresh sibling must survive")
		})
	}
}

// putPart writes a part without a record.
func (s *MultipartStoreTestSuite) putPart(ctx context.Context, ms *MultipartStore, id string, n int) {
	s.T().Helper()

	u, err := ms.upload(ctx, id)
	s.Require().NoError(err)
	s.Require().NoError(u.Put(ctx, s2.NewObjectBytes(uploadPartName(n), []byte("x"))))
}

// backdate sets the mtime of an upload's object.
func (s *MultipartStoreTestSuite) backdate(root, id, name string, age time.Duration) {
	s.T().Helper()

	at := time.Now().Add(-age)
	s.Require().NoError(os.Chtimes(filepath.Join(root, multipartDir, id, name), at, at))
}

func (s *MultipartStoreTestSuite) newStore(typ s2.Type) *MultipartStore {
	s.T().Helper()

	return s.newStoreAt(typ, s.T().TempDir(), 0)
}

// newStoreAt builds a store whose uploads expire after maxAge seconds.
func (s *MultipartStoreTestSuite) newStoreAt(typ s2.Type, root string, maxAge int64) *MultipartStore {
	s.T().Helper()

	cfg := DefaultConfig()
	cfg.Type = typ
	cfg.Root = root
	cfg.ConsoleListen = ""
	cfg.MultipartMaxAge = maxAge
	srv, err := NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	return srv.Multipart
}

func (s *MultipartStoreTestSuite) TestRemove() {
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
			ms := s.newStore(tc.typ)

			s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
			s.Require().NoError(ms.PutPart(ctx, "id1", 1, []byte("x"), `"etag"`))
			s.Require().NoError(ms.Create(ctx, "id2", "photos", "b.jpg", 0, nil))

			s.Require().NoError(ms.Remove(ctx, "id1"))

			// fs keeps metadata in .meta sidecars; they must go with the upload.
			for _, name := range []string{"id1", ".meta/id1"} {
				exists, err := ms.Storage().Exists(ctx, name)
				s.Require().NoError(err)
				s.Falsef(exists, "%s should have been removed", name)
			}
			_, err := ms.Metadata(ctx, "id2", "photos", "b.jpg", 0)
			s.NoError(err, "a sibling upload must survive")
		})
	}
}

func (s *MultipartStoreTestSuite) TestUploads() {
	ctx := context.Background()
	root := s.T().TempDir()
	ms := s.newStoreAt(s2.TypeOSFS, root, 3600)
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 7, nil))
	s.Require().NoError(ms.Create(ctx, "id2", "videos", "b.mp4", 0, nil))
	s.Require().NoError(ms.Create(ctx, "id3", "photos", "old.jpg", 7, nil))
	s.backdate(root, "id3", uploadMetaName, 2*time.Hour)
	s.putPart(ctx, ms, "id4", 1)
	u, err := ms.upload(ctx, "id5")
	s.Require().NoError(err)
	s.Require().NoError(u.Put(ctx, s2.NewObjectBytes(uploadMetaName, []byte("{"))))

	got, err := ms.Uploads(ctx)

	s.Require().NoError(err)
	byID := map[string]Upload{}
	for _, up := range got {
		byID[up.ID] = up
	}
	s.Len(byID, 2, "only live, readable uploads are listed")
	s.Equal("photos", byID["id1"].Bucket)
	s.Equal("a.jpg", byID["id1"].Key)
	s.Equal(int64(7), byID["id1"].Generation)
	s.WithinDuration(time.Now(), byID["id1"].Initiated, time.Minute)
	s.Equal("videos", byID["id2"].Bucket)
}

func (s *MultipartStoreTestSuite) TestParts() {
	ctx := context.Background()
	ms := s.newStore(s2.TypeOSFS)
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", 0, nil))
	for _, n := range []int{10000, 1, 3} {
		s.Require().NoError(ms.PutPart(ctx, "id1", n, []byte(strconv.Itoa(n)), `"etag-`+strconv.Itoa(n)+`"`))
	}
	u, err := ms.upload(ctx, "id1")
	s.Require().NoError(err)
	s.Require().NoError(u.Put(ctx, s2.NewObjectBytes("notapart", []byte("x"))))
	s.Require().NoError(u.Put(ctx, s2.NewObjectBytes("1", []byte("x"))))

	got, err := ms.Parts(ctx, "id1")

	s.Require().NoError(err)
	s.Require().Len(got, 3)
	for i, n := range []int{1, 3, 10000} {
		s.Equal(n, got[i].Number)
		s.Equal(`"etag-`+strconv.Itoa(n)+`"`, got[i].ETag)
		s.Equal(uint64(len(strconv.Itoa(n))), got[i].Size)
		s.False(got[i].LastModified.IsZero())
	}
}

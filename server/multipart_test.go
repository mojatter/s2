package server

import (
	"context"
	"errors"
	"io"
	"io/fs"
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

func (s *MultipartStoreTestSuite) TestMetadata() {
	ctx := context.Background()
	ms := s.server.Multipart
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", s2.Metadata{"author": "uz"}))

	testCases := []struct {
		caseName string
		id       string
		bucket   string
		key      string
		wantErr  error
	}{
		{caseName: "match", id: "id1", bucket: "photos", key: "a.jpg"},
		{caseName: "unknown id", id: "id2", bucket: "photos", key: "a.jpg", wantErr: ErrNoSuchUpload},
		{caseName: "another key", id: "id1", bucket: "photos", key: "b.jpg", wantErr: ErrNoSuchUpload},
		{caseName: "another bucket", id: "id1", bucket: "videos", key: "a.jpg", wantErr: ErrNoSuchUpload},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			md, err := ms.Metadata(ctx, tc.id, tc.bucket, tc.key)
			if tc.wantErr != nil {
				s.ErrorIs(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Equal("uz", md["author"])
		})
	}
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

	_, err := ms.Metadata(context.Background(), "id1", "photos", "a.jpg")
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
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", nil))
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
	s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", nil))
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

	_, err := ms.Metadata(context.Background(), "id1", "photos", "a.jpg")
	s.ErrorIs(err, ErrNoSuchUpload)
}

func (s *MultipartStoreTestSuite) TestAbort() {
	testCases := []struct {
		caseName string
		setup    func(ctx context.Context, ms *MultipartStore)
		bucket   string
		key      string
		wantErr  error
		wantGone bool
	}{
		{
			caseName: "bound upload",
			setup: func(ctx context.Context, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", nil))
			},
			bucket: "photos", key: "a.jpg", wantGone: true,
		},
		{
			caseName: "bound elsewhere is left alone",
			setup: func(ctx context.Context, ms *MultipartStore) {
				s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", nil))
			},
			bucket: "photos", key: "b.jpg", wantErr: ErrNoSuchUpload,
		},
		{
			caseName: "leftover parts without a record",
			setup: func(ctx context.Context, ms *MultipartStore) {
				u, err := ms.upload(ctx, "id1")
				s.Require().NoError(err)
				s.Require().NoError(u.Put(ctx, s2.NewObjectBytes(uploadPartName(1), []byte("x"))))
			},
			bucket: "photos", key: "a.jpg", wantGone: true,
		},
		{
			// Reading it may fail for a moment; removing another upload's parts is forever.
			caseName: "unreadable record",
			setup: func(ctx context.Context, ms *MultipartStore) {
				u, err := ms.upload(ctx, "id1")
				s.Require().NoError(err)
				s.Require().NoError(u.Put(ctx, s2.NewObjectBytes(uploadMetaName, []byte("{"))))
			},
			bucket: "photos", key: "a.jpg", wantErr: io.ErrUnexpectedEOF,
		},
		{
			caseName: "nothing at all",
			setup:    func(context.Context, *MultipartStore) {},
			bucket:   "photos", key: "a.jpg", wantErr: ErrNoSuchUpload, wantGone: true,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			ms := s.newStore(s2.TypeOSFS)
			tc.setup(ctx, ms)

			err := ms.Abort(ctx, "id1", tc.bucket, tc.key)

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

func (s *MultipartStoreTestSuite) newStore(typ s2.Type) *MultipartStore {
	s.T().Helper()

	cfg := DefaultConfig()
	cfg.Type = typ
	cfg.Root = s.T().TempDir()
	cfg.ConsoleListen = ""
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

			s.Require().NoError(ms.Create(ctx, "id1", "photos", "a.jpg", nil))
			s.Require().NoError(ms.PutPart(ctx, "id1", 1, []byte("x"), `"etag"`))
			s.Require().NoError(ms.Create(ctx, "id2", "photos", "b.jpg", nil))

			s.Require().NoError(ms.Remove(ctx, "id1"))

			// fs keeps metadata in .meta sidecars; they must go with the upload.
			for _, name := range []string{"id1", ".meta/id1"} {
				exists, err := ms.Storage().Exists(ctx, name)
				s.Require().NoError(err)
				s.Falsef(exists, "%s should have been removed", name)
			}
			_, err := ms.Metadata(ctx, "id2", "photos", "b.jpg")
			s.NoError(err, "a sibling upload must survive")
		})
	}
}

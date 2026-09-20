package s2

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type nilStorage struct {
	Storage
}

var _ Storage = (*nilStorage)(nil)

func TestNewStorage(t *testing.T) {
	testType := Type("test")
	RegisterNewStorageFunc(testType, func(ctx context.Context, cfg Config) (Storage, error) {
		return &nilStorage{}, nil
	})
	defer UnregisterNewStorageFunc(testType)

	testCases := []struct {
		caseName string
		ctx      context.Context
		cfg      Config
		wantErr  string
	}{
		{
			caseName: "typical",
			cfg: Config{
				Type: testType,
			},
		},
		{
			caseName: "unknown type",
			cfg: Config{
				Type: "UNKNOWN",
			},
			wantErr: "s2: unknown storage type: UNKNOWN",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			_, err := NewStorage(tc.ctx, tc.cfg)
			if tc.wantErr != "" {
				assert.EqualError(t, err, tc.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

// uploaderStorage satisfies the optional Uploader capability.
type uploaderStorage struct {
	Storage
	putCalled bool
}

func (s *uploaderStorage) Put(ctx context.Context, obj Object) error {
	s.putCalled = true
	return nil
}

func (s *uploaderStorage) Upload(ctx context.Context, obj Object, opts UploadOptions) (UploadResult, error) {
	return UploadResult{ETag: `"from-upload"`}, nil
}

// putGetStorage has no Upload, so Upload must fall back to Put then Get.
type putGetStorage struct {
	Storage
	etag      string
	putErr    error
	getErr    error
	putCalled bool
	getCalled bool
}

func (s *putGetStorage) Put(ctx context.Context, obj Object) error {
	s.putCalled = true
	return s.putErr
}

func (s *putGetStorage) Get(ctx context.Context, name string) (Object, error) {
	s.getCalled = true
	if s.getErr != nil {
		return nil, s.getErr
	}
	return etagObject{etag: s.etag}, nil
}

type etagObject struct {
	Object
	etag string
}

func (o etagObject) ETag() string { return o.etag }

func TestUpload(t *testing.T) {
	testCases := []struct {
		caseName string
		strg     Storage
		wantETag string
		wantErr  string
		check    func(t *testing.T, strg Storage)
	}{
		{
			caseName: "uses the capability and skips the read-back",
			strg:     &uploaderStorage{},
			wantETag: `"from-upload"`,
			check: func(t *testing.T, strg Storage) {
				assert.False(t, strg.(*uploaderStorage).putCalled, "Put must not be called when Upload exists")
			},
		},
		{
			caseName: "reads back when the capability reports no ETag",
			strg:     &emptyUploaderStorage{etag: `"from-get"`},
			wantETag: `"from-get"`,
			check: func(t *testing.T, strg Storage) {
				assert.True(t, strg.(*emptyUploaderStorage).getCalled)
			},
		},
		{
			caseName: "falls back to Put and Get",
			strg:     &putGetStorage{etag: `"from-get"`},
			wantETag: `"from-get"`,
			check: func(t *testing.T, strg Storage) {
				s := strg.(*putGetStorage)
				assert.True(t, s.putCalled)
				assert.True(t, s.getCalled)
			},
		},
		{
			caseName: "reports a Put failure without reading back",
			strg:     &putGetStorage{putErr: errors.New("put failed")},
			wantErr:  "put failed",
			check: func(t *testing.T, strg Storage) {
				assert.False(t, strg.(*putGetStorage).getCalled)
			},
		},
		{
			caseName: "reports a Get failure as ErrUnknownETag",
			strg:     &putGetStorage{getErr: errors.New("get failed")},
			wantErr:  "get failed",
			check: func(t *testing.T, strg Storage) {
				_, err := Upload(t.Context(), strg, NewObjectBytes("a.txt", []byte("a")), UploadOptions{})
				assert.ErrorIs(t, err, ErrUnknownETag, "the object was stored; only the ETag is missing")
			},
		},
		{
			caseName: "keeps the read-back error's identity under ErrUnknownETag",
			strg:     &putGetStorage{getErr: ErrNotExist},
			wantErr:  "not exist",
			check: func(t *testing.T, strg Storage) {
				_, err := Upload(t.Context(), strg, NewObjectBytes("a.txt", []byte("a")), UploadOptions{})
				assert.ErrorIs(t, err, ErrUnknownETag)
				assert.ErrorIs(t, err, ErrNotExist)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			res, err := Upload(t.Context(), tc.strg, NewObjectBytes("a.txt", []byte("a")), UploadOptions{})
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				assert.Empty(t, res.ETag)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantETag, res.ETag)
			}
			if tc.check != nil {
				tc.check(t, tc.strg)
			}
		})
	}
}

// emptyUploaderStorage has the capability but cannot report an ETag, so Upload
// must still answer the one a Get reports.
type emptyUploaderStorage struct {
	Storage
	etag      string
	getCalled bool
}

func (s *emptyUploaderStorage) Upload(ctx context.Context, obj Object, opts UploadOptions) (UploadResult, error) {
	return UploadResult{}, nil
}

func (s *emptyUploaderStorage) Get(ctx context.Context, name string) (Object, error) {
	s.getCalled = true
	return etagObject{etag: s.etag}, nil
}

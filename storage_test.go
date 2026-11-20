package s2

import (
	"context"
	"errors"
	"math"
	"strings"
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
		opts     UploadOptions
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
		{
			caseName: "refuses a condition the fallback cannot hold",
			strg:     &putGetStorage{etag: `"from-get"`},
			opts:     UploadOptions{IfNoneMatch: "*"},
			wantErr:  errors.ErrUnsupported.Error(),
			check: func(t *testing.T, strg Storage) {
				assert.False(t, strg.(*putGetStorage).putCalled)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			res, err := Upload(t.Context(), tc.strg, NewObjectBytes("a.txt", []byte("a")), tc.opts)
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

func TestUploadOptionsCheck(t *testing.T) {
	testCases := []struct {
		caseName string
		opts     UploadOptions
		etag     string
		exists   bool
		want     error
	}{
		{caseName: "no condition over an object", etag: `"a"`, exists: true},
		{caseName: "no condition over no object"},
		{caseName: "IfNoneMatch over no object", opts: UploadOptions{IfNoneMatch: "*"}},
		{caseName: "IfNoneMatch over an object", opts: UploadOptions{IfNoneMatch: "*"}, etag: `"a"`, exists: true, want: ErrPreconditionFailed},
		{caseName: "IfNoneMatch naming an ETag", opts: UploadOptions{IfNoneMatch: `"a"`}, want: errors.ErrUnsupported},
		{caseName: "IfMatch the ETag", opts: UploadOptions{IfMatch: `"a"`}, etag: `"a"`, exists: true},
		{caseName: "IfMatch another ETag", opts: UploadOptions{IfMatch: `"b"`}, etag: `"a"`, exists: true, want: ErrPreconditionFailed},
		{caseName: "IfMatch over no object", opts: UploadOptions{IfMatch: `"a"`}, want: ErrNotExist},
		{caseName: "IfMatch any over an object", opts: UploadOptions{IfMatch: "*"}, etag: `"a"`, exists: true, want: errors.ErrUnsupported},
		{caseName: "IfMatch any over no object", opts: UploadOptions{IfMatch: "*"}, want: errors.ErrUnsupported},
		{caseName: "both conditions over an object", opts: UploadOptions{IfMatch: `"a"`, IfNoneMatch: "*"}, etag: `"a"`, exists: true, want: errors.ErrUnsupported},
		{caseName: "both conditions over no object", opts: UploadOptions{IfMatch: `"a"`, IfNoneMatch: "*"}, want: errors.ErrUnsupported},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			err := tc.opts.Check("a.txt", tc.etag, tc.exists)
			if tc.want == nil {
				assert.NoError(t, err)
				return
			}
			assert.ErrorIs(t, err, tc.want)
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

// pagedStorage lists names in pages of pageSize, recording the Limit of each call.
type pagedStorage struct {
	Storage
	names    []string
	pageSize int
	stuck    bool
	listErr  error
	limits   []int
}

func (s *pagedStorage) List(ctx context.Context, opts ListOptions) (ListResult, error) {
	s.limits = append(s.limits, opts.Limit)
	if s.listErr != nil {
		return ListResult{}, s.listErr
	}
	if s.stuck {
		return ListResult{Objects: []Object{NewObjectBytes("a", nil)}, NextAfter: "a"}, nil
	}
	limit := s.pageSize
	if opts.Limit > 0 && opts.Limit < limit {
		limit = opts.Limit
	}
	var res ListResult
	for _, name := range s.names {
		if !strings.HasPrefix(name, opts.Prefix) || name <= opts.After {
			continue
		}
		if len(res.Objects) == limit {
			res.NextAfter = res.Objects[limit-1].Name()
			break
		}
		res.Objects = append(res.Objects, NewObjectBytes(name, nil))
	}
	return res, nil
}

// counterStorage answers Count itself; its nil Storage panics if Count lists.
type counterStorage struct {
	Storage
	res CountResult
}

func (s *counterStorage) Count(ctx context.Context, opts CountOptions) (CountResult, error) {
	return s.res, nil
}

func TestCount(t *testing.T) {
	names := []string{"a/1", "a/2", "a/3", "a/4", "a/5", "b/1"}
	testCases := []struct {
		caseName   string
		strg       Storage
		opts       CountOptions
		want       CountResult
		wantLimits []int
		wantErr    string
	}{
		{
			caseName:   "pages through every object under the prefix",
			strg:       &pagedStorage{names: names, pageSize: 2},
			opts:       CountOptions{Prefix: "a/"},
			want:       CountResult{Objects: 5},
			wantLimits: []int{0, 0, 0},
		},
		{
			caseName:   "counts everything with an empty prefix",
			strg:       &pagedStorage{names: names, pageSize: 1000},
			want:       CountResult{Objects: 6},
			wantLimits: []int{0},
		},
		{
			caseName:   "stops once more than UpTo objects are seen",
			strg:       &pagedStorage{names: names, pageSize: 1000},
			opts:       CountOptions{Prefix: "a/", UpTo: 2},
			want:       CountResult{Objects: 2, Truncated: true},
			wantLimits: []int{3},
		},
		{
			caseName:   "asks only for what is left past the earlier pages",
			strg:       &pagedStorage{names: names, pageSize: 2},
			opts:       CountOptions{Prefix: "a/", UpTo: 3},
			want:       CountResult{Objects: 3, Truncated: true},
			wantLimits: []int{4, 2},
		},
		{
			caseName:   "is exact when the objects number UpTo",
			strg:       &pagedStorage{names: names, pageSize: 1000},
			opts:       CountOptions{Prefix: "a/", UpTo: 5},
			want:       CountResult{Objects: 5},
			wantLimits: []int{6},
		},
		{
			caseName:   "leaves the page size to the backend for a large UpTo",
			strg:       &pagedStorage{names: names, pageSize: 1000},
			opts:       CountOptions{Prefix: "a/", UpTo: 5000},
			want:       CountResult{Objects: 5},
			wantLimits: []int{0},
		},
		{
			caseName:   "leaves the page size alone for the largest UpTo",
			strg:       &pagedStorage{names: names, pageSize: 1000},
			opts:       CountOptions{Prefix: "a/", UpTo: math.MaxInt64},
			want:       CountResult{Objects: 5},
			wantLimits: []int{0},
		},
		{
			caseName: "refuses a NextAfter that does not advance",
			strg:     &pagedStorage{stuck: true},
			wantErr:  "does not advance",
		},
		{
			caseName: "reports a List failure without a partial count",
			strg:     &pagedStorage{listErr: errors.New("list failed")},
			wantErr:  "list failed",
		},
		{
			caseName: "refuses a negative UpTo",
			strg:     &pagedStorage{},
			opts:     CountOptions{UpTo: -1},
			wantErr:  "negative",
		},
		{
			caseName: "uses the capability instead of listing",
			strg:     &counterStorage{res: CountResult{Objects: 7}},
			want:     CountResult{Objects: 7},
		},
		{
			caseName: "caps a capability that ignores UpTo",
			strg:     &counterStorage{res: CountResult{Objects: 7}},
			opts:     CountOptions{UpTo: 3},
			want:     CountResult{Objects: 3, Truncated: true},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			got, err := Count(t.Context(), tc.strg, tc.opts)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				assert.Zero(t, got)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
			if p, ok := tc.strg.(*pagedStorage); ok {
				assert.Equal(t, tc.wantLimits, p.limits)
			}
		})
	}
}

// moveStorage records the calls s2.Move's fallback makes; Get finds only "k".
type moveStorage struct {
	Storage
	calls []string
}

func (s *moveStorage) Get(_ context.Context, name string) (Object, error) {
	s.calls = append(s.calls, "Get "+name)
	if name != "k" {
		return nil, ErrNotExist
	}
	return NewObjectBytes(name, nil), nil
}

func (s *moveStorage) Copy(_ context.Context, src, dst string) error {
	s.calls = append(s.calls, "Copy "+src+" "+dst)
	return nil
}

func (s *moveStorage) Delete(_ context.Context, name string) error {
	s.calls = append(s.calls, "Delete "+name)
	return nil
}

func TestMove(t *testing.T) {
	testCases := []struct {
		caseName  string
		src, dst  string
		wantErr   error
		wantCalls []string
	}{
		{caseName: "copy then delete", src: "k", dst: "k2", wantCalls: []string{"Copy k k2", "Delete k"}},
		{caseName: "onto itself", src: "k", dst: "k", wantCalls: []string{"Get k"}},
		{caseName: "missing onto itself", src: "x", dst: "x", wantErr: ErrNotExist, wantCalls: []string{"Get x"}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			s := &moveStorage{}
			err := Move(context.Background(), s, tc.src, tc.dst)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.Equal(t, tc.wantCalls, s.calls)
		})
	}
}

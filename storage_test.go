package s2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
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

// mapStorage keeps objects in memory; like fs, Put refuses a marker with a body.
type mapStorage struct {
	Storage
	objs          map[string]Object
	pageSize      int
	unreadable    string
	failGet       string
	failListAfter string
	stuck         bool
	invalid       string // refused by name, as fs refuses its reserved names
	getErrStored  error  // Get answers it once the object is stored
	copies        []string
	gets          []string
	puts          []string
}

func newMapStorage(objs ...Object) *mapStorage {
	s := &mapStorage{objs: map[string]Object{}, pageSize: 1000}
	for _, o := range objs {
		s.objs[o.Name()] = o
	}
	return s
}

func (s *mapStorage) List(_ context.Context, opts ListOptions) (ListResult, error) {
	if s.failListAfter != "" && opts.After == s.failListAfter {
		return ListResult{}, errors.New("list failed")
	}
	names := make([]string, 0, len(s.objs))
	for name := range s.objs {
		names = append(names, name)
	}
	sort.Strings(names)
	after := opts.After
	if after == "" {
		after = opts.StartAfter
	}
	var res ListResult
	for _, name := range names {
		rest, ok := strings.CutPrefix(name, opts.Prefix)
		if !ok || name <= after || (!opts.Recursive && strings.Contains(rest, "/")) {
			continue
		}
		if len(res.Objects) == s.pageSize {
			res.NextAfter = res.Objects[s.pageSize-1].Name()
			break
		}
		res.Objects = append(res.Objects, NewObjectBytes(name, nil))
	}
	if s.stuck {
		res.NextAfter = opts.After
	}
	return res, nil
}

func (s *mapStorage) Get(_ context.Context, name string) (Object, error) {
	s.gets = append(s.gets, name)
	if _, ok := s.objs[name]; ok && s.getErrStored != nil {
		return nil, s.getErrStored
	}
	switch name {
	case s.invalid:
		return nil, fmt.Errorf("%w: %s", ErrInvalidName, name)
	case s.unreadable:
		return nil, fmt.Errorf("%w: %s", errors.ErrUnsupported, name)
	case s.failGet:
		return nil, errors.New("get failed")
	}
	o, ok := s.objs[name]
	if !ok {
		return nil, ErrNotExist
	}
	return o, nil
}

// Exists also reports a prefix, as s3 does.
func (s *mapStorage) Exists(_ context.Context, name string) (bool, error) {
	for key := range s.objs {
		if key == name || strings.HasPrefix(key, name+"/") {
			return true, nil
		}
	}
	return false, nil
}

func (s *mapStorage) Put(_ context.Context, obj Object) error {
	s.puts = append(s.puts, obj.Name())
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	body, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	if strings.HasSuffix(obj.Name(), "/") && len(body) > 0 {
		return fmt.Errorf("%w: %s has a body", ErrInvalidName, obj.Name())
	}
	s.objs[obj.Name()] = NewObjectBytes(obj.Name(), body, WithContentType(obj.ContentType()), WithMetadata(obj.Metadata()))
	return nil
}

func (s *mapStorage) Copy(ctx context.Context, src, dst string) error {
	s.copies = append(s.copies, src+" "+dst)
	o, err := s.Get(ctx, src)
	if err != nil {
		return err
	}
	return s.Put(ctx, renamedObject{Object: o, name: dst})
}

// uploaderMapStorage adds an Uploader that honours IfNoneMatch.
type uploaderMapStorage struct {
	*mapStorage
	unsupported bool // answers a conditional Upload with errors.ErrUnsupported
	noETag      bool // reports no ETag, so Upload reads it back
}

func (s *uploaderMapStorage) Upload(ctx context.Context, obj Object, opts UploadOptions) (UploadResult, error) {
	if s.unsupported && opts.Conditional() {
		return UploadResult{}, fmt.Errorf("conditional: %w", errors.ErrUnsupported)
	}
	if opts.IfNoneMatch == "*" && s.objs[obj.Name()] != nil {
		return UploadResult{}, ErrPreconditionFailed
	}
	if s.noETag {
		return UploadResult{}, s.Put(ctx, obj)
	}
	return UploadResult{ETag: `"e"`}, s.Put(ctx, obj)
}

// valueStorage is an incomparable storage value, as == on it panics.
type valueStorage struct {
	*mapStorage
	extra any
}

func TestCopyStorage(t *testing.T) {
	photo := func(name string) Object {
		return NewObjectBytes(name, []byte("body "+name), WithContentType("image/png"), WithMetadata(Metadata{"k": "v"}))
	}
	testCases := []struct {
		caseName    string
		src         *mapStorage
		dst         Storage // nil copies within src
		opts        CopyStorageOptions
		want        CopyStorageResult
		wantDst     []string
		wantCopies  []string
		wantGets    []string
		wantPuts    []string
		asValue     bool
		wantErr     string
		wantResume  *ListOptions
		wantKeepsMD string
	}{
		{
			caseName:    "copies every page under DstDir, keeping markers and metadata",
			src:         &mapStorage{objs: newMapStorage(photo("a/1"), NewObjectBytes("a/b/", nil), photo("a/b/2"), photo("c")).objs, pageSize: 1},
			dst:         newMapStorage(),
			opts:        CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			want:        CopyStorageResult{Copied: 3},
			wantDst:     []string{"bk/a/1", "bk/a/b/", "bk/a/b/2"},
			wantKeepsMD: "bk/a/1",
		},
		{
			caseName: "copies only the objects directly under a non-recursive prefix",
			src:      newMapStorage(photo("a/1"), NewObjectBytes("a/b/", nil), photo("a/b/2")),
			dst:      newMapStorage(),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Prefix: "a/"}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a/1"},
		},
		{
			caseName: "skips an existing object without an Uploader or reading it",
			src:      newMapStorage(photo("a"), photo("b")),
			dst:      newMapStorage(NewObjectBytes("a", []byte("old"))),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:  []string{"a=old", "b"},
			wantGets: []string{"b"},
		},
		{
			caseName: "skips an existing object through the Uploader without reading it",
			src:      newMapStorage(photo("a"), photo("b")),
			dst:      &uploaderMapStorage{mapStorage: newMapStorage(NewObjectBytes("a", []byte("old")))},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:  []string{"a=old", "b"},
			wantGets: []string{"b"},
		},
		{
			caseName: "falls back to Put when the Uploader answers ErrUnsupported",
			src:      newMapStorage(photo("a"), photo("b")),
			dst:      &uploaderMapStorage{mapStorage: newMapStorage(NewObjectBytes("a", []byte("old"))), unsupported: true},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:  []string{"a=old", "b"},
		},
		{
			caseName: "counts an object stored with an unknown ETag as copied",
			src:      newMapStorage(photo("a")),
			dst:      &uploaderMapStorage{mapStorage: &mapStorage{objs: map[string]Object{}, pageSize: 1000, getErrStored: errors.New("get failed")}, noETag: true},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
		},
		{
			caseName: "keeps an object stored with an unknown ETag from an unsupported read-back",
			src:      newMapStorage(photo("a")),
			dst:      &uploaderMapStorage{mapStorage: &mapStorage{objs: map[string]Object{}, pageSize: 1000, getErrStored: fmt.Errorf("read-back: %w", errors.ErrUnsupported)}, noETag: true},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
			wantPuts: []string{"a"},
		},
		{
			caseName: "copies a name dst holds only as a prefix",
			src:      newMapStorage(photo("x")),
			dst:      newMapStorage(photo("x/y")),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"x", "x/y"},
		},
		{
			caseName: "counts a name dst refuses before reading it",
			src:      newMapStorage(photo("a"), photo("b")),
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, invalid: "a"},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:  []string{"b"},
			wantGets: []string{"b"},
		},
		{
			caseName: "counts a name src lists but refuses",
			src:      &mapStorage{objs: newMapStorage(photo("a"), photo("b")).objs, pageSize: 1000, invalid: "a"},
			dst:      newMapStorage(),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:  []string{"b"},
		},
		{
			caseName: "replaces an existing object with Overwrite",
			src:      newMapStorage(photo("a")),
			dst:      newMapStorage(NewObjectBytes("a", []byte("old"))),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}, Overwrite: true},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
		},
		{
			caseName: "counts an object src cannot read",
			src:      &mapStorage{objs: newMapStorage(photo("a"), photo("b")).objs, pageSize: 1000, unreadable: "a"},
			dst:      newMapStorage(),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1, Unsupported: 1},
			wantDst:  []string{"b"},
		},
		{
			caseName: "counts a name dst refuses",
			src:      newMapStorage(NewObjectBytes("a/", []byte("x")), photo("b")),
			dst:      newMapStorage(),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:  []string{"b"},
		},
		{
			caseName:   "keeps SrcOptions when the first object fails",
			src:        &mapStorage{objs: newMapStorage(photo("a"), photo("b"), photo("c")).objs, pageSize: 1000, failGet: "a"},
			dst:        newMapStorage(),
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true, After: "0", Limit: 5}},
			want:       CopyStorageResult{Copied: 0},
			wantErr:    "get failed",
			wantResume: &ListOptions{Recursive: true, After: "0", Limit: 5},
		},
		{
			caseName:   "resumes past the objects done before a failure",
			src:        &mapStorage{objs: newMapStorage(photo("a"), photo("b"), photo("c")).objs, pageSize: 1000, failGet: "c"},
			dst:        newMapStorage(),
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true, After: "0", Limit: 5}},
			want:       CopyStorageResult{Copied: 2},
			wantDst:    []string{"a", "b"},
			wantErr:    "get failed",
			wantResume: &ListOptions{Recursive: true, StartAfter: "b", Limit: 5},
		},
		{
			caseName:   "resumes after the last object when a later List fails",
			src:        &mapStorage{objs: newMapStorage(photo("a"), photo("b"), photo("c")).objs, pageSize: 1, failListAfter: "a"},
			dst:        newMapStorage(),
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:       CopyStorageResult{Copied: 1},
			wantDst:    []string{"a"},
			wantErr:    "list failed",
			wantResume: &ListOptions{Recursive: true, StartAfter: "a"},
		},
		{
			caseName:   "refuses a NextAfter that does not advance",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, stuck: true},
			dst:        newMapStorage(),
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true, After: "0"}},
			want:       CopyStorageResult{Copied: 1},
			wantDst:    []string{"a"},
			wantErr:    "does not advance",
			wantResume: &ListOptions{Recursive: true, StartAfter: "a"},
		},
		{
			caseName: "treats an incomparable storage value as another storage",
			src:      newMapStorage(photo("a/1")),
			asValue:  true,
			opts:     CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a/1", "bk/a/1"},
		},
		{
			caseName:   "copies within one storage a name held only as a prefix",
			src:        newMapStorage(photo("x"), photo("bk/x/y")),
			opts:       CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "x", Recursive: true}},
			want:       CopyStorageResult{Copied: 1},
			wantDst:    []string{"bk/x", "bk/x/y", "x"},
			wantCopies: []string{"x bk/x"},
		},
		{
			caseName:   "copies within one storage with Copy",
			src:        newMapStorage(photo("a/1"), NewObjectBytes("a/b/", nil), photo("bk/a/1")),
			opts:       CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			want:       CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:    []string{"a/1", "a/b/", "bk/a/1", "bk/a/b/"},
			wantCopies: []string{"a/b/ bk/a/b/"},
		},
		{
			caseName:   "refuses a DstDir inside the selection on one storage",
			src:        newMapStorage(photo("a/1")),
			opts:       CopyStorageOptions{DstDir: "a/bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			wantErr:    "overlaps",
			wantDst:    []string{"a/1"},
			wantResume: &ListOptions{Prefix: "a/", Recursive: true},
		},
		{
			caseName:   "refuses a copy onto itself",
			src:        newMapStorage(photo("a/1")),
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			wantErr:    "overlaps",
			wantDst:    []string{"a/1"},
			wantResume: &ListOptions{Prefix: "a/", Recursive: true},
		},
		{
			caseName:   "refuses an invalid DstDir",
			src:        newMapStorage(photo("a")),
			dst:        newMapStorage(),
			opts:       CopyStorageOptions{DstDir: "../x"},
			wantErr:    "invalid name",
			wantResume: &ListOptions{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			var src, dst Storage = tc.src, tc.src
			if tc.asValue {
				src = valueStorage{mapStorage: tc.src, extra: []int{1}}
				dst = valueStorage{mapStorage: tc.src, extra: []int{1}}
			}
			if tc.dst != nil {
				dst = tc.dst
			}
			got, err := CopyStorage(t.Context(), src, dst, tc.opts)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantResume, got.ResumeOptions)
			got.ResumeOptions = nil
			assert.Equal(t, tc.want, got)

			var m *mapStorage
			switch d := dst.(type) {
			case *mapStorage:
				m = d
			case *uploaderMapStorage:
				m = d.mapStorage
			case valueStorage:
				m = d.mapStorage
			}
			var names []string
			for name, o := range m.objs {
				if readAll(t, o) == "old" {
					name += "=old"
				}
				names = append(names, name)
			}
			sort.Strings(names)
			if tc.wantDst == nil {
				assert.Empty(t, names)
			} else {
				assert.Equal(t, tc.wantDst, names)
			}
			assert.Equal(t, tc.wantCopies, tc.src.copies)
			if tc.wantGets != nil {
				assert.Equal(t, tc.wantGets, tc.src.gets)
			}
			if tc.wantPuts != nil {
				assert.Equal(t, tc.wantPuts, m.puts)
			}
			if tc.wantKeepsMD != "" {
				o := m.objs[tc.wantKeepsMD]
				assert.Equal(t, "image/png", o.ContentType())
				assert.Equal(t, Metadata{"k": "v"}, o.Metadata())
			}
		})
	}
}

func readAll(t *testing.T, o Object) string {
	t.Helper()
	rc, err := o.Open()
	if !assert.NoError(t, err) {
		return ""
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)
	assert.NoError(t, err)
	return string(b)
}

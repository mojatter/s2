package s2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	copyErr       error  // Copy answers it while Get still reads the source
	uri           string
	copies        []string
	gets          []string
	puts          []string
}

func (s *mapStorage) URI() string { return s.uri }

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
	limit := s.pageSize
	if opts.Limit > 0 && opts.Limit < limit {
		limit = opts.Limit
	}
	var res ListResult
	for _, name := range names {
		rest, ok := strings.CutPrefix(name, opts.Prefix)
		if !ok || name <= after || (!opts.Recursive && strings.Contains(rest, "/")) {
			continue
		}
		if len(res.Objects) == limit {
			res.NextAfter = res.Objects[limit-1].Name()
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
	if s.copyErr != nil {
		return s.copyErr
	}
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
		wantErrIs   error
		wantResume  *ListOptions
		wantKeepsMD string
		wantReports []string // "status name>dstName", then ": err" when Err is set
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
			caseName:    "skips an existing object without an Uploader or reading it",
			src:         newMapStorage(photo("a"), photo("b")),
			dst:         newMapStorage(NewObjectBytes("a", []byte("old"))),
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:     []string{"a=old", "b"},
			wantGets:    []string{"b"},
			wantReports: []string{"skipped a>a", "copied b>b"},
		},
		{
			caseName:    "skips an existing object through the Uploader without reading it",
			src:         newMapStorage(photo("a"), photo("b")),
			dst:         &uploaderMapStorage{mapStorage: newMapStorage(NewObjectBytes("a", []byte("old")))},
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:     []string{"a=old", "b"},
			wantGets:    []string{"b"},
			wantReports: []string{"skipped a>a", "copied b>b"},
		},
		{
			caseName:    "falls back to Put when the Uploader answers ErrUnsupported",
			src:         newMapStorage(photo("a"), photo("b")),
			dst:         &uploaderMapStorage{mapStorage: newMapStorage(NewObjectBytes("a", []byte("old"))), unsupported: true},
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:     []string{"a=old", "b"},
			wantReports: []string{"skipped a>a", "copied b>b"},
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
			caseName:    "counts a name dst refuses before reading it",
			src:         newMapStorage(photo("a"), photo("b")),
			dst:         &mapStorage{objs: map[string]Object{}, pageSize: 1000, invalid: "a"},
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:     []string{"b"},
			wantGets:    []string{"b"},
			wantReports: []string{"rejected a>a: s2: invalid name: a", "copied b>b"},
		},
		{
			caseName:    "counts a name src lists but refuses",
			src:         &mapStorage{objs: newMapStorage(photo("a"), photo("b")).objs, pageSize: 1000, invalid: "a"},
			dst:         newMapStorage(),
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:     []string{"b"},
			wantReports: []string{"rejected a>a: s2: invalid name: a", "copied b>b"},
		},
		{
			caseName:    "replaces an existing object with Overwrite",
			src:         newMapStorage(photo("a")),
			dst:         newMapStorage(NewObjectBytes("a", []byte("old"))),
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}, Overwrite: true},
			want:        CopyStorageResult{Copied: 1},
			wantDst:     []string{"a"},
			wantReports: []string{"copied a>a"},
		},
		{
			caseName:    "counts an object src cannot read",
			src:         &mapStorage{objs: newMapStorage(photo("a"), photo("b")).objs, pageSize: 1000, unreadable: "a"},
			dst:         newMapStorage(),
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Unsupported: 1},
			wantDst:     []string{"b"},
			wantReports: []string{"unsupported a>a: unsupported operation: a", "copied b>b"},
		},
		{
			caseName:    "counts a name dst refuses",
			src:         newMapStorage(NewObjectBytes("a/", []byte("x")), photo("b")),
			dst:         newMapStorage(),
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:     []string{"b"},
			wantReports: []string{"rejected a/>a/: s2: invalid name: a/ has a body", "copied b>b"},
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
			caseName:    "keeps an invalid name out of ResumeOptions",
			src:         &mapStorage{objs: newMapStorage(photo("a"), photo("a//b"), photo("c")).objs, pageSize: 1000, failGet: "c"},
			dst:         newMapStorage(),
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:     []string{"a"},
			wantGets:    []string{"a", "c"},
			wantErr:     "get failed",
			wantResume:  &ListOptions{Recursive: true, StartAfter: "a"},
			wantReports: []string{"copied a>a", "rejected a//b>: s2: invalid name: a//b"},
		},
		{
			caseName:    "rejects an invalid name before joining it with DstDir",
			src:         newMapStorage(photo("a//x"), photo("a/x")),
			dst:         newMapStorage(),
			opts:        CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:     []string{"bk/a/x"},
			wantGets:    []string{"a/x"},
			wantReports: []string{"rejected a//x>: s2: invalid name: a//x", "copied a/x>bk/a/x"},
		},
		{
			caseName:   "replaces an existing object within one storage with Overwrite",
			src:        newMapStorage(photo("a/1"), NewObjectBytes("bk/a/1", []byte("old"))),
			opts:       CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}, Overwrite: true},
			want:       CopyStorageResult{Copied: 1},
			wantDst:    []string{"a/1", "bk/a/1"},
			wantCopies: []string{"a/1 bk/a/1"},
		},
		{
			caseName: "uses SrcOptions.Limit as the page size and copies every page",
			src:      newMapStorage(photo("a"), photo("b"), photo("c")),
			dst:      newMapStorage(),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true, Limit: 1}},
			want:     CopyStorageResult{Copied: 3},
			wantDst:  []string{"a", "b", "c"},
		},
		{
			caseName:   "resumes within one storage after a failing Copy",
			src:        &mapStorage{objs: newMapStorage(photo("a/1"), photo("a/2")).objs, pageSize: 1000, failGet: "a/2"},
			opts:       CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			want:       CopyStorageResult{Copied: 1},
			wantDst:    []string{"a/1", "a/2", "bk/a/1"},
			wantCopies: []string{"a/1 bk/a/1", "a/2 bk/a/2"},
			wantErr:    "get failed",
			wantResume: &ListOptions{Prefix: "a/", Recursive: true, StartAfter: "a/1"},
		},
		{
			caseName:    "counts a name Copy refuses within one storage",
			src:         newMapStorage(photo("a/1"), NewObjectBytes("a/b/", []byte("x"))),
			opts:        CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Rejected: 1},
			wantDst:     []string{"a/1", "a/b/", "bk/a/1"},
			wantCopies:  []string{"a/1 bk/a/1", "a/b/ bk/a/b/"},
			wantReports: []string{"copied a/1>bk/a/1", "rejected a/b/>bk/a/b/: s2: invalid name: bk/a/b/ has a body"},
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
			caseName:    "copies within one storage with Copy",
			src:         newMapStorage(photo("a/1"), NewObjectBytes("a/b/", nil), photo("bk/a/1")),
			opts:        CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Skipped: 1},
			wantDst:     []string{"a/1", "a/b/", "bk/a/1", "bk/a/b/"},
			wantCopies:  []string{"a/b/ bk/a/b/"},
			wantReports: []string{"skipped a/1>bk/a/1", "copied a/b/>bk/a/b/"},
		},
		{
			caseName:    "counts an object Copy cannot read within one storage",
			src:         &mapStorage{objs: newMapStorage(photo("a/1"), photo("a/2")).objs, pageSize: 1000, unreadable: "a/1"},
			opts:        CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			want:        CopyStorageResult{Copied: 1, Unsupported: 1},
			wantDst:     []string{"a/1", "a/2", "bk/a/2"},
			wantCopies:  []string{"a/1 bk/a/1", "a/2 bk/a/2"},
			wantReports: []string{"unsupported a/1>bk/a/1: unsupported operation: a/1", "copied a/2>bk/a/2"},
		},
		{
			caseName:   "stops on an ErrUnsupported from Copy when Get reads the source",
			src:        &mapStorage{objs: newMapStorage(photo("a/1")).objs, pageSize: 1000, copyErr: fmt.Errorf("copy: %w", errors.ErrUnsupported)},
			opts:       CopyStorageOptions{DstDir: "bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			wantDst:    []string{"a/1"},
			wantCopies: []string{"a/1 bk/a/1"},
			wantErr:    "copy: unsupported operation",
			wantErrIs:  errors.ErrUnsupported,
			wantResume: &ListOptions{Prefix: "a/", Recursive: true},
		},
		{
			caseName:   "refuses a DstDir inside the selection on one storage",
			src:        newMapStorage(photo("a/1")),
			opts:       CopyStorageOptions{DstDir: "a/bk", SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			wantErr:    "overlaps",
			wantErrIs:  ErrOverlap,
			wantDst:    []string{"a/1"},
			wantResume: &ListOptions{Prefix: "a/", Recursive: true},
		},
		{
			caseName:   "refuses a copy onto itself",
			src:        newMapStorage(photo("a/1")),
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Prefix: "a/", Recursive: true}},
			wantErr:    "overlaps",
			wantErrIs:  ErrOverlap,
			wantDst:    []string{"a/1"},
			wantResume: &ListOptions{Prefix: "a/", Recursive: true},
		},
		{
			caseName:   "refuses a dst whose URI lies inside the selection",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/backup/"},
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			wantErr:    `DstDir "" on mem://1/backup/ overlaps Prefix "" on mem://1/`,
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{Recursive: true},
		},
		{
			caseName: "copies into a dst whose URI holds the selection",
			src:      &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "s3://b/x/"},
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "s3://b"},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
		},
		{
			caseName:    "counts an object whose copy would land inside the selection",
			src:         &mapStorage{objs: newMapStorage(photo("a"), photo("x/a"), photo("z")).objs, pageSize: 1, uri: "s3://b/x/"},
			dst:         &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "s3://b"},
			opts:        CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}, Overwrite: true},
			want:        CopyStorageResult{Copied: 2, Rejected: 1},
			wantDst:     []string{"a", "z"},
			wantReports: []string{"copied a>a", `rejected x/a>x/a: s2: copy inside the selection: "x/a" on s3://b/ lands in Prefix "" on s3://b/x/`, "copied z>z"},
		},
		{
			caseName:   "resumes past an object whose copy would land inside the selection",
			src:        &mapStorage{objs: newMapStorage(photo("x/a"), photo("y")).objs, pageSize: 1000, uri: "s3://b/x/", failGet: "y"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "s3://b"},
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:       CopyStorageResult{Rejected: 1},
			wantErr:    "get failed",
			wantResume: &ListOptions{Recursive: true, StartAfter: "x/a"},
		},
		{
			caseName: "copies beside the selection under a DstDir sharing its first letters",
			src:      &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/backup2/"},
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/"},
			opts:     CopyStorageOptions{DstDir: "backup", SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"backup/a"},
		},
		{
			caseName:   "refuses a DstDir that is the selection",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/backup/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/"},
			opts:       CopyStorageOptions{DstDir: "backup", SrcOptions: ListOptions{Recursive: true}},
			wantErr:    "overlaps",
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{Recursive: true},
		},
		{
			caseName: "copies between hosts differing in case",
			src:      &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "s3://MyBucket/"},
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "s3://mybucket/"},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
		},
		{
			caseName: "copies into a view inside a non-recursive selection",
			src:      &mapStorage{objs: newMapStorage(photo("a"), photo("b")).objs, pageSize: 1, uri: "mem://1/"},
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/backup/"},
			want:     CopyStorageResult{Copied: 2},
			wantDst:  []string{"a", "b"},
		},
		{
			caseName:   "refuses a non-recursive copy onto the selection itself",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/"},
			wantErr:    "overlaps",
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{},
		},
		{
			caseName:   "refuses a non-recursive copy onto a selection named without its slash",
			src:        &mapStorage{objs: newMapStorage(photo("d/a")).objs, pageSize: 1000, uri: "mem://1/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/"},
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Prefix: "d"}},
			wantErr:    "overlaps",
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{Prefix: "d"},
		},
		{
			caseName:   "refuses a non-recursive copy into a DstDir that is the selection",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/x/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/"},
			opts:       CopyStorageOptions{DstDir: "x"},
			wantErr:    `DstDir "x" on mem://1/ overlaps Prefix "" on mem://1/x/`,
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{},
		},
		{
			caseName: "copies between opaque URIs",
			src:      &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "custom:a/"},
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "custom:b/"},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
		},
		{
			caseName:   "compares URIs cleaned",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "s3://b//x/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "s3://b/x/bk/"},
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			wantErr:    `DstDir "" on s3://b/x/bk/ overlaps Prefix "" on s3://b/x/`,
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{Recursive: true},
		},
		{
			caseName:   "leaves credentials and query out of the error",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "https://u:pw@h/b/?sv=x"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "https://h/b/bk/"},
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			wantErr:    `DstDir "" on https://h/b/bk/ overlaps Prefix "" on https://h/b/`,
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{Recursive: true},
		},
		{
			caseName:   "compares URIs unescaped",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "file:///d/%61/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "file:///d/a/bk/"},
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			wantErr:    "overlaps",
			wantErrIs:  ErrOverlap,
			wantResume: &ListOptions{Recursive: true},
		},
		{
			caseName: "copies to a view of src outside the selection",
			src:      &mapStorage{objs: newMapStorage(photo("data/a"), photo("x")).objs, pageSize: 1000, uri: "mem://1/"},
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/backup/"},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Prefix: "data/", Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"data/a"},
		},
		{
			caseName: "copies to a URI on another host",
			src:      &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/"},
			dst:      &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://2/backup/"},
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
		},
		{
			caseName: "copies when dst has no URI",
			src:      &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/"},
			dst:      newMapStorage(),
			opts:     CopyStorageOptions{SrcOptions: ListOptions{Recursive: true}},
			want:     CopyStorageResult{Copied: 1},
			wantDst:  []string{"a"},
		},
		{
			caseName:   "refuses an invalid DstDir",
			src:        newMapStorage(photo("a")),
			dst:        newMapStorage(),
			opts:       CopyStorageOptions{DstDir: "../x"},
			wantErr:    "invalid name",
			wantErrIs:  ErrInvalidName,
			wantResume: &ListOptions{},
		},
		{
			caseName:   "refuses an invalid Prefix on one storage",
			src:        newMapStorage(photo("a")),
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Prefix: "a//", Recursive: true}},
			wantErr:    "invalid name",
			wantErrIs:  ErrInvalidName,
			wantDst:    []string{"a"},
			wantResume: &ListOptions{Prefix: "a//", Recursive: true},
		},
		{
			caseName:   "refuses an invalid Prefix between views of one URI",
			src:        &mapStorage{objs: newMapStorage(photo("a")).objs, pageSize: 1000, uri: "mem://1/"},
			dst:        &mapStorage{objs: map[string]Object{}, pageSize: 1000, uri: "mem://1/"},
			opts:       CopyStorageOptions{SrcOptions: ListOptions{Prefix: "./a", Recursive: true}},
			wantErr:    "invalid name",
			wantErrIs:  ErrInvalidName,
			wantResume: &ListOptions{Prefix: "./a", Recursive: true},
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
			var reports []string
			counted := CopyStorageResult{}
			tc.opts.ObjectDone = func(r CopyStorageObjectResult) {
				line := fmt.Sprintf("%s %s>%s", r.Status, r.Name, r.DstName)
				if r.Err != nil {
					line += ": " + r.Err.Error()
				}
				reports = append(reports, line)
				switch r.Status {
				case CopyStorageCopied:
					counted.Copied++
				case CopyStorageSkipped:
					counted.Skipped++
				case CopyStorageUnsupported:
					counted.Unsupported++
				case CopyStorageRejected:
					counted.Rejected++
					if !errors.Is(r.Err, ErrInvalidName) {
						assert.ErrorIs(t, r.Err, ErrOverlap, "a rejection wraps ErrInvalidName or ErrOverlap")
					}
				}
			}
			got, err := CopyStorage(t.Context(), src, dst, tc.opts)
			if tc.wantErrIs != nil {
				assert.ErrorIs(t, err, tc.wantErrIs)
			}
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantResume, got.ResumeOptions)
			got.ResumeOptions = nil
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.want, counted, "reports must match the counts")
			if tc.wantReports != nil {
				assert.Equal(t, tc.wantReports, reports)
			}

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

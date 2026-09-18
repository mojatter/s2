package gcs

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501 -- mirrors GCS's MD5 hash
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/stretchr/testify/suite"
)

// --- mock implementations ---

type mockObject struct {
	bucket         string
	key            string
	body           []byte
	updated        time.Time
	metadata       map[string]string
	contentType    string
	generation     int64
	metageneration int64
}

type mockGCSClient struct {
	mu      sync.RWMutex
	objects map[string]*mockObject // keyed by "bucket/key"
	patches int                    // patch calls, so a test can pin how many requests PutMetadata takes
	onAttrs func()                 // runs after reading attrs, to simulate a racing writer
}

func newMockGCSClient() *mockGCSClient {
	return &mockGCSClient{objects: make(map[string]*mockObject)}
}

func (m *mockGCSClient) put(bucket, key string, body []byte, metadata map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// A copy, so a later update cannot reach back into the caller's map.
	var stored map[string]string
	if metadata != nil {
		stored = make(map[string]string, len(metadata))
		for k, v := range metadata {
			stored[k] = v
		}
	}
	// A new generation resets the metageneration, as the service does.
	generation := int64(1)
	if old, ok := m.objects[path.Join(bucket, key)]; ok {
		generation = old.generation + 1
	}
	m.objects[path.Join(bucket, key)] = &mockObject{
		bucket:         bucket,
		key:            key,
		body:           body,
		updated:        time.Now(),
		metadata:       stored,
		generation:     generation,
		metageneration: 1,
	}
}

func (m *mockGCSClient) get(bucket, key string) (*mockObject, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	obj, ok := m.objects[path.Join(bucket, key)]
	return obj, ok
}

func (m *mockGCSClient) del(bucket, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, path.Join(bucket, key))
}

func mockMD5(body []byte) []byte {
	sum := md5.Sum(body) // #nosec G401 -- mirrors GCS's MD5 hash
	return sum[:]
}

// gcsClient implementation

func (m *mockGCSClient) bucket(name string) gcsBucket {
	return &mockBucket{client: m, name: name}
}

type mockBucket struct {
	client *mockGCSClient
	name   string
}

func (b *mockBucket) object(name string) gcsObject {
	return &mockGCSObject{client: b.client, bucket: b.name, key: name}
}

func (b *mockBucket) objects(ctx context.Context, q *storage.Query) gcsObjectIterator {
	b.client.mu.RLock()
	defer b.client.mu.RUnlock()

	prefix := q.Prefix
	delimiter := q.Delimiter
	startOffset := q.StartOffset

	prefixSet := make(map[string]bool)
	var entries []struct {
		attrs *storage.ObjectAttrs
	}

	// Collect matching keys sorted.
	var keys []string
	for k := range b.client.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, objPath := range keys {
		obj := b.client.objects[objPath]
		if obj.bucket != b.name {
			continue
		}
		if !strings.HasPrefix(obj.key, prefix) {
			continue
		}
		if startOffset != "" && obj.key < startOffset {
			continue
		}

		remainder := obj.key[len(prefix):]

		if delimiter != "" {
			if idx := strings.Index(remainder, delimiter); idx >= 0 {
				cp := prefix + remainder[:idx+len(delimiter)]
				if !prefixSet[cp] {
					prefixSet[cp] = true
					entries = append(entries, struct {
						attrs *storage.ObjectAttrs
					}{
						attrs: &storage.ObjectAttrs{Prefix: cp},
					})
				}
				continue
			}
		}

		entries = append(entries, struct {
			attrs *storage.ObjectAttrs
		}{
			attrs: &storage.ObjectAttrs{
				Name:        obj.key,
				Size:        int64(len(obj.body)),
				Updated:     obj.updated,
				Metadata:    obj.metadata,
				MD5:         mockMD5(obj.body),
				ContentType: obj.contentType,
			},
		})
	}

	return &mockIterator{entries: entries}
}

func (b *mockBucket) signedURL(name string, opts *storage.SignedURLOptions) (string, error) {
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s?signed", b.name, name), nil
}

type mockGCSObject struct {
	client *mockGCSClient
	bucket string
	key    string
}

func (o *mockGCSObject) attrs(_ context.Context) (*storage.ObjectAttrs, error) {
	obj, ok := o.client.get(o.bucket, o.key)
	if !ok {
		return nil, storage.ErrObjectNotExist
	}
	attrs := &storage.ObjectAttrs{
		Name:           obj.key,
		Size:           int64(len(obj.body)),
		Updated:        obj.updated,
		Metadata:       obj.metadata,
		MD5:            mockMD5(obj.body),
		ContentType:    obj.contentType,
		Generation:     obj.generation,
		Metageneration: obj.metageneration,
	}
	if o.client.onAttrs != nil {
		o.client.onAttrs()
	}
	return attrs, nil
}

func (o *mockGCSObject) newReader(_ context.Context) (io.ReadCloser, error) {
	obj, ok := o.client.get(o.bucket, o.key)
	if !ok {
		return nil, storage.ErrObjectNotExist
	}
	return io.NopCloser(bytes.NewReader(obj.body)), nil
}

func (o *mockGCSObject) newRangeReader(_ context.Context, offset, length int64) (io.ReadCloser, error) {
	obj, ok := o.client.get(o.bucket, o.key)
	if !ok {
		return nil, storage.ErrObjectNotExist
	}
	body := obj.body
	end := offset + length
	if int(end) > len(body) {
		end = int64(len(body))
	}
	return io.NopCloser(bytes.NewReader(body[offset:end])), nil
}

func (o *mockGCSObject) newWriter(_ context.Context, metadata map[string]string, contentType string) io.WriteCloser {
	return &mockWriter{client: o.client, bucket: o.bucket, key: o.key, metadata: metadata, contentType: contentType}
}

func (o *mockGCSObject) patch(_ context.Context, p objectPatch) error {
	obj, ok := o.client.get(o.bucket, o.key)
	if !ok {
		// A missing object answers 404 before the preconditions are weighed, and the JSON API client has its own error type.
		return &googleapi.Error{Code: http.StatusNotFound, Message: "No such object"}
	}
	if (p.metageneration != 0 && p.metageneration != obj.metageneration) || (p.generation != 0 && p.generation != obj.generation) {
		return errPreconditionFailed
	}
	o.client.mu.Lock()
	o.client.patches++
	o.client.mu.Unlock()
	// Objects.Patch merges what it is given and deletes only the keys sent as null.
	if len(p.metadata) > 0 && obj.metadata == nil {
		obj.metadata = make(map[string]string, len(p.metadata))
	}
	for k, v := range p.metadata {
		obj.metadata[k] = v
	}
	for _, k := range p.deleteKeys {
		delete(obj.metadata, k)
	}
	if p.clearContentType {
		obj.contentType = ""
	} else if p.contentType != "" {
		obj.contentType = p.contentType
	}
	obj.metageneration++
	return nil
}

// errPreconditionFailed is the 412 a precondition mismatch returns, in the JSON API client's own type.
var errPreconditionFailed = &googleapi.Error{Code: http.StatusPreconditionFailed, Message: "Precondition Failed"}

func (o *mockGCSObject) copyTo(_ context.Context, dst gcsObject) error {
	src, ok := o.client.get(o.bucket, o.key)
	if !ok {
		return storage.ErrObjectNotExist
	}
	dstObj, ok := dst.(*mockGCSObject)
	if !ok {
		return errors.New("mock: destination is not a mockGCSObject")
	}
	body := make([]byte, len(src.body))
	copy(body, src.body)
	var meta map[string]string
	if src.metadata != nil {
		meta = make(map[string]string, len(src.metadata))
		for k, v := range src.metadata {
			meta[k] = v
		}
	}
	o.client.put(dstObj.bucket, dstObj.key, body, meta)
	if obj, ok := o.client.get(dstObj.bucket, dstObj.key); ok {
		obj.contentType = src.contentType
	}
	return nil
}

func (o *mockGCSObject) delete(_ context.Context) error {
	_, ok := o.client.get(o.bucket, o.key)
	if !ok {
		return storage.ErrObjectNotExist
	}
	o.client.del(o.bucket, o.key)
	return nil
}

type mockWriter struct {
	client      *mockGCSClient
	bucket      string
	key         string
	buf         bytes.Buffer
	metadata    map[string]string
	contentType string
}

func (w *mockWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *mockWriter) Close() error {
	w.client.put(w.bucket, w.key, w.buf.Bytes(), w.metadata)
	if obj, ok := w.client.get(w.bucket, w.key); ok {
		obj.contentType = w.contentType
	}
	return nil
}

// mockPageTokenPrefix marks the mock's page tokens. Real ones are opaque, so
// the mock rejects anything it did not hand out itself.
const mockPageTokenPrefix = "mock-page:"

type mockIterator struct {
	entries []struct {
		attrs *storage.ObjectAttrs
	}
	idx     int // next entry to return
	pageEnd int // exclusive end of the page currently buffered
	maxSize int
	err     error
}

func (i *mockIterator) next() (*storage.ObjectAttrs, error) {
	if i.err != nil {
		return nil, i.err
	}
	if i.idx >= len(i.entries) {
		return nil, iterator.Done
	}
	if i.idx >= i.pageEnd {
		// The buffered page is drained; fetch the next one.
		size := i.maxSize
		if size <= 0 {
			size = len(i.entries)
		}
		i.pageEnd = min(i.idx+size, len(i.entries))
	}
	a := i.entries[i.idx].attrs
	i.idx++
	return a, nil
}

func (i *mockIterator) setPageToken(token string) {
	if token == "" {
		return
	}
	n, err := strconv.Atoi(strings.TrimPrefix(token, mockPageTokenPrefix))
	if !strings.HasPrefix(token, mockPageTokenPrefix) || err != nil {
		i.err = fmt.Errorf("invalid page token %q", token)
		return
	}
	i.idx = n
	i.pageEnd = n
}

func (i *mockIterator) setMaxSize(n int) {
	i.maxSize = n
}

func (i *mockIterator) remaining() int {
	return i.pageEnd - i.idx
}

func (i *mockIterator) nextPageToken() string {
	if i.pageEnd >= len(i.entries) {
		return ""
	}
	return mockPageTokenPrefix + strconv.Itoa(i.pageEnd)
}

// --- test suite ---

type StorageTestSuite struct {
	suite.Suite
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, &StorageTestSuite{})
}

// Keys a pre-v0.18 s2-server wrote are lifted; a client's own s2-* keys stay user metadata.
func (s *StorageTestSuite) TestLegacyMetadata() {
	testCases := []struct {
		caseName        string
		metadata        map[string]string
		nativeType      string
		wantContentType string
		wantMetadata    s2.Metadata
	}{
		{
			caseName:        "pre-v0.18 write",
			metadata:        map[string]string{"s2-etag": `"x"`, "s2-content-type": "text/html", "author": "uz"},
			nativeType:      "text/plain",
			wantContentType: "text/html",
			wantMetadata:    s2.Metadata{"author": "uz"},
		},
		{
			// The SDK sniffed the body because v0.17 sent no type.
			caseName:        "pre-v0.18 write without a client Content-Type",
			metadata:        map[string]string{"s2-etag": `"x"`, "s2-content-type": "binary/octet-stream"},
			nativeType:      "text/html",
			wantContentType: "",
			wantMetadata:    s2.Metadata{},
		},
		{
			caseName:        "client metadata without s2-etag",
			metadata:        map[string]string{"s2-content-type": "text/html"},
			nativeType:      "text/plain",
			wantContentType: "text/plain",
			wantMetadata:    s2.Metadata{"s2-content-type": "text/html"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockStorage()
			ctx := context.Background()
			m.put("mybucket", "page.html", []byte("<p>"), tc.metadata)
			raw, _ := m.get("mybucket", "page.html")
			raw.contentType = tc.nativeType

			got, err := strg.Get(ctx, "page.html")
			s.Require().NoError(err)
			s.Equal(tc.wantContentType, got.ContentType())
			s.Equal(tc.wantMetadata, got.Metadata())

			res, err := strg.List(ctx, s2.ListOptions{})
			s.Require().NoError(err)
			for _, obj := range res.Objects {
				if obj.Name() == "page.html" {
					s.Equal(tc.wantContentType, obj.ContentType())
					s.Equal(tc.wantMetadata, obj.Metadata())
				}
			}

			s.Require().NoError(strg.PutMetadata(ctx, "page.html", got.Metadata()))

			got, err = strg.Get(ctx, "page.html")
			s.Require().NoError(err)
			s.Equal(tc.wantContentType, got.ContentType())
			s.Equal(tc.wantMetadata, got.Metadata())
			s.Equal(tc.wantContentType, raw.contentType, "the stored type after migration")
		})
	}
}

func (s *StorageTestSuite) testMockStorage() (*mockGCSClient, s2.Storage) {
	m := newMockGCSClient()
	files := map[string][]byte{
		"a.txt":     []byte("a"),
		"b.txt":     []byte("b"),
		"cc/c1.txt": []byte("c1"),
		"cc/c2.txt": []byte("c2"),
	}
	for key, b := range files {
		m.put("mybucket", key, b, nil)
	}
	return m, &gcsStorage{
		client: m,
		bucket: "mybucket",
	}
}

func (s *StorageTestSuite) TestNewStorageError() {
	s.Run("empty root", func() {
		_, err := NewStorage(context.Background(), s2.Config{})
		s.Require().ErrorIs(err, ErrRequiredConfigRoot)
	})

	s.Run("invalid credentials file", func() {
		_, err := NewStorage(context.Background(), s2.Config{
			Type: s2.TypeGCS,
			Root: "my-bucket",
			GCS:  &s2.GCSConfig{CredentialsFile: "/tmp/nonexistent.json"},
		})
		s.Require().Error(err)
		s.Contains(err.Error(), "gcs: failed to create client")
	})
}

func (s *StorageTestSuite) TestS2TestList() {
	_, strg := s.testMockStorage()
	ctx := context.Background()

	err := s2test.TestStorageListRecursive(ctx, strg, "a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "", "a.txt", "b.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "cc", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)
}

// TestListCursors separates the two resume paths: After is the SDK's page
// token, StartAfter a caller-chosen key mapped onto StartOffset.
func (s *StorageTestSuite) TestListCursors() {
	testCases := []struct {
		caseName   string
		prefix     string
		startAfter string
		limit      int
		want       []string
		wantNext   bool
	}{
		{
			caseName:   "start after skips the key itself",
			startAfter: "a.txt",
			want:       []string{"b.txt", "cc/c1.txt", "cc/c2.txt"},
		},
		{
			caseName:   "resolves the key against the storage prefix",
			prefix:     "cc",
			startAfter: "c1.txt",
			want:       []string{"c2.txt"},
		},
		{
			caseName: "limit yields a page token",
			limit:    2,
			want:     []string{"a.txt", "b.txt"},
			wantNext: true,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockStorage()
			strg.(*gcsStorage).prefix = tc.prefix
			ctx := context.Background()

			res, err := strg.List(ctx, s2.ListOptions{
				StartAfter: tc.startAfter,
				Limit:      tc.limit,
				Recursive:  true,
			})
			s.Require().NoError(err)
			s.Equal(tc.want, objectNames(res.Objects))
			if !tc.wantNext {
				s.Empty(res.NextAfter)
				return
			}

			// The token must round-trip as After.
			s.Require().NotEmpty(res.NextAfter)
			res2, err := strg.List(ctx, s2.ListOptions{After: res.NextAfter, Recursive: true})
			s.Require().NoError(err)
			s.Equal([]string{"cc/c1.txt", "cc/c2.txt"}, objectNames(res2.Objects))
		})
	}
}

// TestListAfterNotAKey passes a key name where a token belongs; the real SDK
// would fail the request too.
func (s *StorageTestSuite) TestListAfterNotAKey() {
	_, strg := s.testMockStorage()

	_, err := strg.List(context.Background(), s2.ListOptions{After: "a.txt", Recursive: true})
	s.Require().Error(err)
}

func objectNames(objs []s2.Object) []string {
	names := make([]string, 0, len(objs))
	for _, obj := range objs {
		names = append(names, obj.Name())
	}
	return names
}

// TestListPrefixesAreRelative pins that CommonPrefixes drop the storage
// prefix, as object names do.
func (s *StorageTestSuite) TestListPrefixesAreRelative() {
	m, strg := s.testMockStorage()
	m.put("mybucket", "cc/dd/x.txt", []byte("x"), nil)
	strg.(*gcsStorage).prefix = "cc"

	res, err := strg.List(context.Background(), s2.ListOptions{})
	s.Require().NoError(err)
	s.Equal([]string{"dd/"}, res.CommonPrefixes)
}

func (s *StorageTestSuite) TestS2TestGetPut() {
	_, strg := s.testMockStorage()
	s.Require().NoError(s2test.TestStorageGetPut(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestGetNotExist() {
	_, strg := s.testMockStorage()
	s.Require().NoError(s2test.TestStorageGetNotExist(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestExists() {
	_, strg := s.testMockStorage()
	s.Require().NoError(s2test.TestStorageExists(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestCopyMove() {
	_, strg := s.testMockStorage()
	s.Require().NoError(s2test.TestStorageCopyMove(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestDelete() {
	testCases := []struct {
		caseName string
		prefix   string
	}{
		{caseName: "no prefix"},
		{caseName: "with prefix", prefix: "pfx"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockStorage()
			strg.(*gcsStorage).prefix = tc.prefix
			s.Require().NoError(s2test.TestStorageDelete(context.Background(), strg))
		})
	}
}

func (s *StorageTestSuite) TestS2TestPutMetadata() {
	_, strg := s.testMockStorage()
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), strg))
}

func (s *StorageTestSuite) TestType() {
	_, strg := s.testMockStorage()
	s.Equal(s2.TypeGCS, strg.Type())
}

func (s *StorageTestSuite) TestGet() {
	testCases := []struct {
		caseName string
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
			_, strg := s.testMockStorage()
			ctx := context.Background()

			got, err := strg.Get(ctx, tc.name)
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
		m := newMockGCSClient()
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
			_, strg := s.testMockStorage()
			ctx := context.Background()

			err := strg.Put(ctx, tc.obj)
			s.Require().NoError(err)

			got, err := strg.Get(ctx, tc.obj.Name())
			s.Require().NoError(err)

			rc, err := got.Open()
			s.Require().NoError(err)
			defer rc.Close()

			body, _ := io.ReadAll(rc)
			s.Equal("new content", string(body))
		})
	}
}

func (s *StorageTestSuite) TestDelete() {
	_, strg := s.testMockStorage()
	ctx := context.Background()

	err := strg.Delete(ctx, "a.txt")
	s.Require().NoError(err)

	_, err = strg.Get(ctx, "a.txt")
	s.Error(err)
}

func (s *StorageTestSuite) TestDeleteRecursive() {
	testCases := []struct {
		caseName string
		prefix   string
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
			_, strg := s.testMockStorage()
			ctx := context.Background()

			err := strg.DeleteRecursive(ctx, tc.prefix)
			s.Require().NoError(err)

			res, err := strg.List(ctx, s2.ListOptions{Limit: 10, Recursive: true})
			s.Require().NoError(err)
			s.Equal(len(tc.wantLeft), len(res.Objects))
			for i, w := range tc.wantLeft {
				s.Equal(w, res.Objects[i].Name())
			}
		})
	}
}

func (s *StorageTestSuite) TestSub() {
	_, strg := s.testMockStorage()
	ctx := context.Background()

	sub, err := strg.Sub(ctx, "cc")
	s.Require().NoError(err)
	s.Equal(s2.TypeGCS, sub.Type())

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
		{caseName: "leaf object", name: "a.txt", want: true},
		{caseName: "leaf object missing", name: "not-found.txt", want: false},
		{caseName: "non-empty prefix", name: "cc", want: true},
		{caseName: "missing prefix", name: "no-such", want: false},
		{caseName: "storage root", name: "", want: true},
		{caseName: "storage root slash", name: "/", want: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockStorage()
			got, err := strg.Exists(context.Background(), tc.name)
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
}

func (s *StorageTestSuite) TestPutMetadata() {
	_, strg := s.testMockStorage()
	ctx := context.Background()

	err := strg.PutMetadata(ctx, "a.txt", s2.Metadata{"new-key": "new-val"})
	s.Require().NoError(err)

	obj, err := strg.Get(ctx, "a.txt")
	s.Require().NoError(err)
	v, ok := obj.Metadata().Get("new-key")
	s.True(ok)
	s.Equal("new-val", v)

	err = strg.PutMetadata(ctx, "missing.txt", s2.Metadata{"k": "v"})
	s.ErrorIs(err, s2.ErrNotExist)
}

// One patch replaces the metadata, whatever the replacement drops.
func (s *StorageTestSuite) TestPutMetadataPatchesOnce() {
	testCases := []struct {
		caseName   string
		metadata   map[string]string
		next       s2.Metadata
		wantStored map[string]string
	}{
		{
			caseName:   "every key is replaced",
			metadata:   map[string]string{"author": "uz"},
			next:       s2.Metadata{"author": "s2", "version": "1"},
			wantStored: map[string]string{"author": "s2", "version": "1"},
		},
		{
			caseName:   "a key is dropped",
			metadata:   map[string]string{"author": "uz", "stale": "x"},
			next:       s2.Metadata{"author": "s2"},
			wantStored: map[string]string{"author": "s2"},
		},
		{
			caseName:   "all keys are dropped",
			metadata:   map[string]string{"author": "uz"},
			next:       s2.Metadata{},
			wantStored: map[string]string{},
		},
		{
			caseName:   "no metadata to drop",
			next:       s2.Metadata{"author": "s2"},
			wantStored: map[string]string{"author": "s2"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockStorage()
			ctx := context.Background()
			m.put("mybucket", "page.html", []byte("<p>"), tc.metadata)
			m.patches = 0

			s.Require().NoError(strg.PutMetadata(ctx, "page.html", tc.next))

			s.Equal(1, m.patches, "PutMetadata must take one request")
			raw, _ := m.get("mybucket", "page.html")
			s.Equal(tc.wantStored, raw.metadata)
		})
	}
}

// A 412 is the caller's to resolve: it must not read as a missing object.
func (s *StorageTestSuite) TestPutMetadataKeepsThePreconditionError() {
	m, strg := s.testMockStorage()
	ctx := context.Background()
	m.put("mybucket", "page.html", []byte("<p>"), map[string]string{"author": "uz", "stale": "x"})
	m.onAttrs = func() {
		m.onAttrs = nil
		raw, _ := m.get("mybucket", "page.html")
		raw.metageneration++
	}

	err := strg.PutMetadata(ctx, "page.html", s2.Metadata{"author": "s2"})

	s.ErrorIs(err, errPreconditionFailed)
	s.NotErrorIs(err, s2.ErrNotExist)
	raw, _ := m.get("mybucket", "page.html")
	s.Equal(map[string]string{"author": "uz", "stale": "x"}, raw.metadata, "a rejected patch changes nothing")
}

// A legacy object's lifted Content-Type moves to its own in the same patch.
func (s *StorageTestSuite) TestPutMetadataCarriesTheLiftedContentType() {
	testCases := []struct {
		caseName        string
		metadata        map[string]string
		nativeType      string
		wantContentType string
	}{
		{
			caseName:        "a stored type moves to the object's own",
			metadata:        map[string]string{"s2-etag": `"x"`, "s2-content-type": "text/html", "author": "uz"},
			nativeType:      "text/plain",
			wantContentType: "text/html",
		},
		{
			caseName:        "a sniffed type is cleared",
			metadata:        map[string]string{"s2-etag": `"x"`, "author": "uz"},
			nativeType:      "text/html",
			wantContentType: "",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockStorage()
			ctx := context.Background()
			m.put("mybucket", "page.html", []byte("<p>"), tc.metadata)
			raw, _ := m.get("mybucket", "page.html")
			raw.contentType = tc.nativeType

			s.Require().NoError(strg.PutMetadata(ctx, "page.html", s2.Metadata{"author": "s2"}))

			s.Equal(tc.wantContentType, raw.contentType)
			s.Equal(map[string]string{"author": "s2"}, raw.metadata)
		})
	}
}

// The request body is what the fix rests on: a per-key null for a delete, and a forced empty map when every key goes.
func (s *StorageTestSuite) TestPatchObjectJSON() {
	testCases := []struct {
		caseName string
		patch    objectPatch
		want     string
	}{
		{
			caseName: "replace and drop",
			patch:    objectPatch{metadata: map[string]string{"author": "s2"}, deleteKeys: []string{"stale"}},
			want:     `{"metadata":{"author":"s2","stale":null}}`,
		},
		{
			caseName: "drop every key",
			patch:    objectPatch{deleteKeys: []string{"author", "stale"}},
			want:     `{"metadata":{"author":null,"stale":null}}`,
		},
		{
			caseName: "nothing to drop",
			patch:    objectPatch{metadata: map[string]string{"author": "s2"}},
			want:     `{"metadata":{"author":"s2"}}`,
		},
		{
			caseName: "lift a legacy Content-Type",
			patch:    objectPatch{metadata: map[string]string{"author": "s2"}, deleteKeys: []string{"s2-etag"}, contentType: "text/html"},
			want:     `{"contentType":"text/html","metadata":{"author":"s2","s2-etag":null}}`,
		},
		{
			caseName: "clear a sniffed Content-Type",
			patch:    objectPatch{metadata: map[string]string{"author": "s2"}, deleteKeys: []string{"s2-etag"}, clearContentType: true},
			want:     `{"contentType":null,"metadata":{"author":"s2","s2-etag":null}}`,
		},
		{
			caseName: "a key holding a dot",
			patch:    objectPatch{metadata: map[string]string{"a": "1"}, deleteKeys: []string{"my.key"}},
			want:     `{"metadata":{"a":"1","my.key":null}}`,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			body, err := patchObject(tc.patch).MarshalJSON()
			s.Require().NoError(err)
			s.JSONEq(tc.want, string(body))
		})
	}
}

// An object deleted between the read and the patch reads as missing, whichever client reports it.
func (s *StorageTestSuite) TestPutMetadataOfDeletedObject() {
	m, strg := s.testMockStorage()
	ctx := context.Background()
	m.put("mybucket", "page.html", []byte("<p>"), map[string]string{"author": "uz"})
	m.onAttrs = func() {
		m.onAttrs = nil
		m.del("mybucket", "page.html")
	}

	err := strg.PutMetadata(ctx, "page.html", s2.Metadata{"author": "s2"})

	s.ErrorIs(err, s2.ErrNotExist)
}

// The JSON API client needs the emulator endpoint spelled out; the SDK client finds it itself.
func (s *StorageTestSuite) TestEmulatorEndpoint() {
	testCases := []struct {
		caseName string
		host     string
		want     string
		wantErr  bool
	}{
		{caseName: "host and port", host: "localhost:9000", want: "http://localhost:9000/storage/v1/"},
		{caseName: "with a scheme", host: "https://localhost:9000", want: "https://localhost:9000/storage/v1/"},
		{caseName: "unparsable", host: "http://[::1", wantErr: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got, err := emulatorEndpoint(tc.host)
			if tc.wantErr {
				s.Require().Error(err, "an unparsable host must not fall back to production")
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
}

// A write landing between the read and the patch fails the call instead of being overwritten.
func (s *StorageTestSuite) TestPutMetadataDetectsConcurrentWrite() {
	testCases := []struct {
		caseName string
		race     func(m *mockGCSClient)
	}{
		{
			caseName: "metadata changed before the patch",
			race: func(m *mockGCSClient) {
				m.onAttrs = func() {
					m.onAttrs = nil
					raw, _ := m.get("mybucket", "page.html")
					raw.metageneration++
				}
			},
		},
		{
			// A new generation resets the metageneration, so only the generation catches this.
			caseName: "object overwritten before the patch",
			race: func(m *mockGCSClient) {
				m.onAttrs = func() {
					m.onAttrs = nil
					m.put("mybucket", "page.html", []byte("<h1>"), map[string]string{"owner": "bob"})
				}
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockStorage()
			ctx := context.Background()
			m.put("mybucket", "page.html", []byte("<p>"), map[string]string{"author": "uz", "stale": "x"})
			tc.race(m)

			err := strg.PutMetadata(ctx, "page.html", s2.Metadata{"author": "s2"})

			s.ErrorIs(err, errPreconditionFailed)
		})
	}
}

func (s *StorageTestSuite) TestCopy() {
	s.Run("typical", func() {
		_, strg := s.testMockStorage()
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
		_, strg := s.testMockStorage()
		err := strg.Copy(context.Background(), "not-found.txt", "dst.txt")
		s.True(errors.Is(err, s2.ErrNotExist), "got %v, want s2.ErrNotExist", err)
	})
}

func (s *StorageTestSuite) TestMove() {
	_, strg := s.testMockStorage()
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
	_, strg := s.testMockStorage()
	ctx := context.Background()

	url, err := strg.SignedURL(ctx, s2.SignedURLOptions{
		Name: "a.txt",
		TTL:  time.Hour,
	})
	s.Require().NoError(err)
	s.Contains(url, "mybucket")
	s.Contains(url, "a.txt")
}

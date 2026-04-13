package azblob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/stretchr/testify/suite"
)

// --- mock ---

type mockBlob struct {
	container string
	key       string
	body      []byte
	modified  time.Time
	metadata  map[string]*string
}

type mockAzblobClient struct {
	mu      sync.RWMutex
	blobs   map[string]*mockBlob // keyed by "container/key"
	svcURL  string
}

func newMockAzblobClient() *mockAzblobClient {
	return &mockAzblobClient{
		blobs:  make(map[string]*mockBlob),
		svcURL: "https://mockaccount.blob.core.windows.net/",
	}
}

func (m *mockAzblobClient) blobKey(container, key string) string {
	return container + "/" + key
}

func (m *mockAzblobClient) put(container, key string, body []byte, metadata map[string]*string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.blobs[m.blobKey(container, key)] = &mockBlob{
		container: container,
		key:       key,
		body:      body,
		modified:  time.Now(),
		metadata:  metadata,
	}
}

func (m *mockAzblobClient) get(container, key string) (*mockBlob, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	b, ok := m.blobs[m.blobKey(container, key)]
	return b, ok
}

func (m *mockAzblobClient) del(container, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.blobs, m.blobKey(container, key))
}

func (m *mockAzblobClient) serviceURL() string {
	return m.svcURL
}

func (m *mockAzblobClient) getProperties(_ context.Context, container, blobName string) (blobProps, error) {
	b, ok := m.get(container, blobName)
	if !ok {
		return blobProps{}, newBlobNotFoundError()
	}
	return blobProps{
		contentLength: int64(len(b.body)),
		lastModified:  b.modified,
		metadata:      b.metadata,
	}, nil
}

func (m *mockAzblobClient) downloadStream(_ context.Context, container, blobName string, offset, count int64) (io.ReadCloser, error) {
	b, ok := m.get(container, blobName)
	if !ok {
		return nil, newBlobNotFoundError()
	}
	body := b.body
	if offset > 0 || count > 0 {
		end := offset + count
		if int(end) > len(body) {
			end = int64(len(body))
		}
		body = body[offset:end]
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (m *mockAzblobClient) upload(_ context.Context, container, blobName string, body io.Reader, metadata map[string]*string) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	m.put(container, blobName, data, metadata)
	return nil
}

func (m *mockAzblobClient) deleteBlob(_ context.Context, container, blobName string) error {
	_, ok := m.get(container, blobName)
	if !ok {
		return newBlobNotFoundError()
	}
	m.del(container, blobName)
	return nil
}

func (m *mockAzblobClient) setMetadata(_ context.Context, container, blobName string, metadata map[string]*string) error {
	b, ok := m.get(container, blobName)
	if !ok {
		return newBlobNotFoundError()
	}
	b.metadata = metadata
	return nil
}

func (m *mockAzblobClient) copyBlob(_ context.Context, container, src, dst string) error {
	b, ok := m.get(container, src)
	if !ok {
		return newBlobNotFoundError()
	}
	body := make([]byte, len(b.body))
	copy(body, b.body)
	var meta map[string]*string
	if b.metadata != nil {
		meta = make(map[string]*string, len(b.metadata))
		for k, v := range b.metadata {
			v := *v
			meta[k] = &v
		}
	}
	m.put(container, dst, body, meta)
	return nil
}

func (m *mockAzblobClient) listBlobs(_ context.Context, ctr, prefix string, maxResults int32, marker string) (listBlobsResult, error) {
	return m.doList(ctr, prefix, "", maxResults, marker), nil
}

func (m *mockAzblobClient) listBlobsHierarchy(_ context.Context, ctr, prefix, delimiter string, maxResults int32, marker string) (listBlobsResult, error) {
	return m.doList(ctr, prefix, delimiter, maxResults, marker), nil
}

func (m *mockAzblobClient) doList(ctr, prefix, delimiter string, maxResults int32, marker string) listBlobsResult {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.blobs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	prefixSet := make(map[string]bool)
	var result listBlobsResult

	for _, objPath := range keys {
		b := m.blobs[objPath]
		if b.container != ctr {
			continue
		}
		if !strings.HasPrefix(b.key, prefix) {
			continue
		}
		if marker != "" && b.key <= marker {
			continue
		}

		remainder := b.key[len(prefix):]

		if delimiter != "" {
			if idx := strings.Index(remainder, delimiter); idx >= 0 {
				cp := prefix + remainder[:idx+len(delimiter)]
				if !prefixSet[cp] {
					prefixSet[cp] = true
					result.prefixes = append(result.prefixes, cp)
				}
				continue
			}
		}

		if int32(len(result.items)) >= maxResults {
			result.nextMarker = b.key
			break
		}

		result.items = append(result.items, blobItem{
			name:          b.key,
			contentLength: int64(len(b.body)),
			lastModified:  b.modified,
			metadata:      b.metadata,
		})
	}
	return result
}

func (m *mockAzblobClient) signedURL(container, blobName string, _ string, _ time.Time) (string, error) {
	return fmt.Sprintf("https://mockaccount.blob.core.windows.net/%s/%s?signed", container, blobName), nil
}

// newBlobNotFoundError returns an error that bloberror.HasCode recognizes as BlobNotFound.
// bloberror.HasCode checks for *azcore.ResponseError via errors.As.
func newBlobNotFoundError() error {
	return &azcore.ResponseError{ErrorCode: "BlobNotFound"}
}

// --- test suite ---

type StorageTestSuite struct {
	suite.Suite
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, &StorageTestSuite{})
}

func (s *StorageTestSuite) testMockStorage() (*mockAzblobClient, s2.Storage) {
	m := newMockAzblobClient()
	files := map[string][]byte{
		"a.txt":     []byte("a"),
		"b.txt":     []byte("b"),
		"cc/c1.txt": []byte("c1"),
		"cc/c2.txt": []byte("c2"),
	}
	for key, b := range files {
		m.put("mycontainer", key, b, nil)
	}
	return m, &azblobStorage{
		client:    m,
		container: "mycontainer",
	}
}

func (s *StorageTestSuite) TestNewStorageError() {
	s.Run("empty root", func() {
		_, err := NewStorage(context.Background(), s2.Config{})
		s.Require().ErrorIs(err, ErrRequiredConfigRoot)
	})

	s.Run("no account name or connection string", func() {
		_, err := NewStorage(context.Background(), s2.Config{
			Type:  s2.TypeAzblob,
			Root:  "my-container",
			Azblob: &s2.AzblobConfig{},
		})
		s.Require().ErrorIs(err, ErrRequiredAccountName)
	})
}

func (s *StorageTestSuite) TestNewStorage() {
	testCases := []struct {
		caseName      string
		cfg           s2.Config
		wantContainer string
		wantPrefix    string
	}{
		{
			caseName: "container only",
			cfg: s2.Config{
				Type:  s2.TypeAzblob,
				Root:  "my-container",
				Azblob: &s2.AzblobConfig{AccountName: "test", AccountKey: "dGVzdA=="},
			},
			wantContainer: "my-container",
			wantPrefix:    "",
		},
		{
			caseName: "container with prefix",
			cfg: s2.Config{
				Type:  s2.TypeAzblob,
				Root:  "my-container/some/prefix",
				Azblob: &s2.AzblobConfig{AccountName: "test", AccountKey: "dGVzdA=="},
			},
			wantContainer: "my-container",
			wantPrefix:    "some/prefix",
		},
		{
			caseName: "root with slashes trimmed",
			cfg: s2.Config{
				Type:  s2.TypeAzblob,
				Root:  "/my-container/pfx/",
				Azblob: &s2.AzblobConfig{AccountName: "test", AccountKey: "dGVzdA=="},
			},
			wantContainer: "my-container",
			wantPrefix:    "pfx",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			strg, err := NewStorage(context.Background(), tc.cfg)
			s.Require().NoError(err)

			st := strg.(*azblobStorage)
			s.Equal(tc.wantContainer, st.container)
			s.Equal(tc.wantPrefix, st.prefix)
			s.NotNil(st.client)
		})
	}
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
	_, strg := s.testMockStorage()
	s.Require().NoError(s2test.TestStorageDelete(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestPutMetadata() {
	_, strg := s.testMockStorage()
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), strg))
}

func (s *StorageTestSuite) TestType() {
	_, strg := s.testMockStorage()
	s.Equal(s2.TypeAzblob, strg.Type())
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
		m := newMockAzblobClient()
		obj := &object{
			client:    m,
			container: "mycontainer",
			name:      "not-found.txt",
		}
		_, err := obj.Open()
		s.Error(err)
		s.ErrorIs(err, s2.ErrNotExist)
	})
}

func (s *StorageTestSuite) TestPut() {
	_, strg := s.testMockStorage()
	ctx := context.Background()

	obj := func() s2.Object {
		o := s2.NewObjectBytes("new.txt", []byte("new content"))
		o.Metadata().Set("key", "val")
		return o
	}()

	err := strg.Put(ctx, obj)
	s.Require().NoError(err)

	got, err := strg.Get(ctx, obj.Name())
	s.Require().NoError(err)

	rc, err := got.Open()
	s.Require().NoError(err)
	defer rc.Close()

	body, _ := io.ReadAll(rc)
	s.Equal("new content", string(body))
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
	_, strg := s.testMockStorage()
	ctx := context.Background()

	err := strg.DeleteRecursive(ctx, "cc")
	s.Require().NoError(err)

	res, err := strg.List(ctx, s2.ListOptions{Limit: 10, Recursive: true})
	s.Require().NoError(err)
	s.Equal(2, len(res.Objects))
	s.Equal("a.txt", res.Objects[0].Name())
	s.Equal("b.txt", res.Objects[1].Name())
}

func (s *StorageTestSuite) TestSub() {
	_, strg := s.testMockStorage()
	ctx := context.Background()

	sub, err := strg.Sub(ctx, "cc")
	s.Require().NoError(err)
	s.Equal(s2.TypeAzblob, sub.Type())

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
		s.Error(err)
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
	s.Contains(url, "mycontainer")
	s.Contains(url, "a.txt")
}

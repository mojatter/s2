package s3

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501 -- mirrors S3's ETag
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/mojatter/s2"
	"github.com/mojatter/s2/s2test"
	"github.com/stretchr/testify/suite"
)

type mockObject struct {
	bucket       string
	key          string
	body         []byte
	lastModified time.Time
	metadata     map[string]string
	contentType  string
	etag         string
	tags         map[string]string
	// headers holds the HEAD fields beside Content-Type a test sets on the object.
	headers *s3.HeadObjectOutput
}

// mockDefaultContentType is what S3 stores when a write sends no Content-Type.
const mockDefaultContentType = "binary/octet-stream"

type mockS3Client struct {
	mu            sync.RWMutex
	objects       map[string]*mockObject
	lastListInput *s3.ListObjectsV2Input
	// ignoreIfMatch serves GetObject as an S3-compatible server that does not support If-Match.
	ignoreIfMatch bool
	// notImplemented answers every conditional PutObject and CompleteMultipartUpload with NotImplemented.
	notImplemented bool
	// preconditionFailed answers every PutObject and CompleteMultipartUpload with PreconditionFailed, as a proxy might.
	preconditionFailed bool
	// completeConflict answers every CompleteMultipartUpload with ConditionalRequestConflict, as S3 does for a racing write.
	completeConflict bool
	uploads          map[string]*mockUpload
	nextUploadID     int
	// maxPartLen is the largest UploadPart body seen.
	maxPartLen int
	// checksumAlgorithm is what the last CreateMultipartUpload asked for.
	checksumAlgorithm s3types.ChecksumAlgorithm
	// stuckToken hands a continued listing back its own token, as a broken endpoint might.
	stuckToken bool
	// refuseDelete names a raw key DeleteObjects keeps and reports in Errors.
	refuseDelete string
	// copyObjects counts CopyObject calls.
	copyObjects int
	// partCopies counts UploadPartCopy calls.
	partCopies int
	// getTaggings counts GetObjectTagging calls.
	getTaggings int
	// lastCreate is the last CreateMultipartUpload input.
	lastCreate *s3.CreateMultipartUploadInput
	// invalidRequest answers every CopyObject with InvalidRequest, whatever the source size.
	invalidRequest bool
	// largeCopy accepts CopyObject above maxCopyObjectSize, as s2-server does.
	largeCopy bool
	// headErr, if set, answers every HeadObject.
	headErr error
	// hideTagCount leaves TagCount out of HeadObject, as S3 does for a caller without s3:GetObjectTagging.
	hideTagCount bool
	// denyTagging answers GetObjectTagging with AccessDenied.
	denyTagging bool
	// afterCopyObject, if set, runs after each CopyObject the mock refuses.
	afterCopyObject func()
	// afterPartCopy, if set, runs after each UploadPartCopy with the count so far.
	afterPartCopy func(n int)
}

type mockUpload struct {
	parts       map[int32][]byte
	metadata    map[string]string
	contentType string
	tags        map[string]string
}

// mockKey keys the store verbatim. S3 keys are opaque strings, and path.Join
// would collapse "photos" and "photos/" into one entry -- the very fold these
// tests exist to catch.
func mockKey(bucket, key string) string {
	return bucket + "\x00" + key
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{
		objects: make(map[string]*mockObject),
		uploads: make(map[string]*mockUpload),
	}
}

func (m *mockS3Client) put(bucket, key string, body []byte, metadata map[string]string) {
	m.putWithContentType(bucket, key, body, metadata, "")
}

func (m *mockS3Client) putWithContentType(bucket, key string, body []byte, metadata map[string]string, contentType string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store(bucket, key, body, metadata, contentType)
}

// store writes the object; callers hold m.mu.
func (m *mockS3Client) store(bucket, key string, body []byte, metadata map[string]string, contentType string) {
	if contentType == "" {
		contentType = mockDefaultContentType
	}
	m.objects[mockKey(bucket, key)] = &mockObject{
		bucket:       bucket,
		key:          key,
		body:         body,
		lastModified: time.Now(),
		metadata:     metadata,
		contentType:  contentType,
		etag:         mockETag(body),
	}
}

func (m *mockS3Client) delete(bucket, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, mockKey(bucket, key))
}

func (m *mockS3Client) get(bucket, key string) (*mockObject, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	obj, ok := m.objects[mockKey(bucket, key)]
	return obj, ok
}

// mockETag is the quoted MD5 S3 assigns to a single-part upload.
func mockETag(body []byte) string {
	sum := md5.Sum(body) // #nosec G401 -- mirrors S3's ETag
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// clientAPI and presignClientAPI implementation

// mockContinuationPrefix marks the mock's continuation tokens. Real ones are
// opaque, so the mock rejects anything it did not hand out itself.
const mockContinuationPrefix = "mock-token:"

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.Lock()
	m.lastListInput = params
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()

	bucket := aws.ToString(params.Bucket)
	prefix := aws.ToString(params.Prefix)
	after := aws.ToString(params.StartAfter)
	if tok := aws.ToString(params.ContinuationToken); tok != "" {
		key, ok := strings.CutPrefix(tok, mockContinuationPrefix)
		if !ok {
			return nil, fmt.Errorf("invalid continuation token %q", tok)
		}
		// A continuation token overrides start-after, as in the real service.
		after = key
	}
	delimiter := aws.ToString(params.Delimiter)
	limit := int(aws.ToInt32(params.MaxKeys))
	if limit == 0 {
		limit = 1000
	}

	var objs []*mockObject
	for _, obj := range m.objects {
		if obj.bucket == bucket && strings.HasPrefix(obj.key, prefix) {
			objs = append(objs, obj)
		}
	}
	slices.SortFunc(objs, func(a, b *mockObject) int { return strings.Compare(a.key, b.key) })

	// Common prefixes count toward MaxKeys, and one not past the cursor is dropped, as in the real service.
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}
	n, last := 0, ""
	for _, obj := range objs {
		// A key with the delimiter past the prefix rolls up, even one ending at it, as in the real service.
		entry, rolled := obj.key, false
		if delimiter != "" {
			if idx := strings.Index(obj.key[len(prefix):], delimiter); idx >= 0 {
				entry, rolled = obj.key[:len(prefix)+idx+len(delimiter)], true
			}
		}
		if entry == last || entry <= after {
			continue
		}
		if n == limit {
			out.IsTruncated = aws.Bool(true)
			out.NextContinuationToken = aws.String(mockContinuationPrefix + last)
			if m.stuckToken && params.ContinuationToken != nil {
				out.NextContinuationToken = params.ContinuationToken
			}
			break
		}
		if rolled {
			out.CommonPrefixes = append(out.CommonPrefixes, s3types.CommonPrefix{Prefix: aws.String(entry)})
		} else {
			out.Contents = append(out.Contents, s3types.Object{
				Key:          aws.String(obj.key),
				Size:         aws.Int64(int64(len(obj.body))),
				LastModified: aws.Time(obj.lastModified),
				ETag:         aws.String(obj.etag),
			})
		}
		n++
		last = entry
	}
	return out, nil
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.RLock()
	headErr, hideTagCount := m.headErr, m.hideTagCount
	m.mu.RUnlock()
	if headErr != nil {
		return nil, headErr
	}
	obj, ok := m.get(aws.ToString(params.Bucket), aws.ToString(params.Key))
	if !ok {
		return nil, fmt.Errorf("NotFound: %w", &s3types.NoSuchKey{})
	}
	out := &s3.HeadObjectOutput{}
	if obj.headers != nil {
		*out = *obj.headers
	}
	out.ContentLength = aws.Int64(int64(len(obj.body)))
	out.LastModified = aws.Time(obj.lastModified)
	out.Metadata = obj.metadata
	out.ETag = aws.String(obj.etag)
	out.ContentType = aws.String(obj.contentType)
	if len(obj.tags) > 0 && !hideTagCount {
		out.TagCount = aws.Int32(int32(len(obj.tags))) // #nosec G115 -- a handful of test tags
	}
	return out, nil
}

func (m *mockS3Client) GetObjectTagging(ctx context.Context, params *s3.GetObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error) {
	obj, ok := m.get(aws.ToString(params.Bucket), aws.ToString(params.Key))
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.getTaggings++
	if m.denyTagging {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied"}
	}
	out := &s3.GetObjectTaggingOutput{}
	for k, v := range obj.tags {
		out.TagSet = append(out.TagSet, s3types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return out, nil
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	// The mock judges the conditions and writes under one lock, as the service holds them until the write lands.
	m.mu.Lock()
	defer m.mu.Unlock()

	bucket, key := aws.ToString(params.Bucket), aws.ToString(params.Key)
	if err := m.refuse(bucket, key, params.IfMatch, params.IfNoneMatch); err != nil {
		return nil, err
	}
	m.store(bucket, key, b, params.Metadata, aws.ToString(params.ContentType))
	// The real PutObject answers the stored object's ETag, as Head and Get do.
	return &s3.PutObjectOutput{ETag: aws.String(mockETag(b))}, nil
}

// refuse judges a write's conditions; callers hold m.mu.
func (m *mockS3Client) refuse(bucket, key string, ifMatch, ifNoneMatch *string) error {
	cur, exists := m.objects[mockKey(bucket, key)]
	switch {
	case m.notImplemented && (ifMatch != nil || ifNoneMatch != nil):
		return &smithy.GenericAPIError{Code: "NotImplemented"}
	case m.preconditionFailed:
		return &smithy.GenericAPIError{Code: "PreconditionFailed"}
	case ifMatch != nil && !exists:
		return &s3types.NoSuchKey{}
	case ifMatch != nil && aws.ToString(ifMatch) != cur.etag:
		return &smithy.GenericAPIError{Code: "PreconditionFailed"}
	case ifNoneMatch != nil && exists:
		return &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	return nil
}

func (m *mockS3Client) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextUploadID++
	m.checksumAlgorithm = params.ChecksumAlgorithm
	id := fmt.Sprintf("upload-%d", m.nextUploadID)
	m.lastCreate = params
	up := &mockUpload{parts: make(map[int32][]byte), metadata: params.Metadata, contentType: aws.ToString(params.ContentType)}
	if params.Tagging != nil {
		q, err := url.ParseQuery(aws.ToString(params.Tagging))
		if err != nil {
			return nil, &smithy.GenericAPIError{Code: "InvalidArgument"}
		}
		up.tags = make(map[string]string)
		for k := range q {
			up.tags[k] = q.Get(k)
		}
	}
	m.uploads[id] = up
	return &s3.CreateMultipartUploadOutput{Bucket: params.Bucket, Key: params.Key, UploadId: aws.String(id)}, nil
}

func (m *mockS3Client) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	b, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	up, ok := m.uploads[aws.ToString(params.UploadId)]
	if !ok {
		return nil, &s3types.NoSuchUpload{}
	}
	up.parts[aws.ToInt32(params.PartNumber)] = b
	m.maxPartLen = max(m.maxPartLen, len(b))
	return &s3.UploadPartOutput{ETag: aws.String(mockETag(b))}, nil
}

// CompleteMultipartUpload assigns S3's md5-of-md5s-N ETag.
func (m *mockS3Client) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	up, ok := m.uploads[aws.ToString(params.UploadId)]
	if !ok {
		return nil, &s3types.NoSuchUpload{}
	}
	if m.completeConflict {
		return nil, &smithy.GenericAPIError{Code: "ConditionalRequestConflict"}
	}
	bucket, key := aws.ToString(params.Bucket), aws.ToString(params.Key)
	if err := m.refuse(bucket, key, params.IfMatch, params.IfNoneMatch); err != nil {
		return nil, err
	}
	var body, sums []byte
	for _, p := range params.MultipartUpload.Parts {
		b := up.parts[aws.ToInt32(p.PartNumber)]
		sum := md5.Sum(b) // #nosec G401 -- mirrors S3's ETag
		body, sums = append(body, b...), append(sums, sum[:]...)
	}
	m.store(bucket, key, body, up.metadata, up.contentType)
	sum := md5.Sum(sums) // #nosec G401 -- mirrors S3's ETag
	etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(sum[:]), len(params.MultipartUpload.Parts))
	m.objects[mockKey(bucket, key)].etag = etag
	m.objects[mockKey(bucket, key)].tags = up.tags
	delete(m.uploads, aws.ToString(params.UploadId))
	return &s3.CompleteMultipartUploadOutput{Bucket: params.Bucket, Key: params.Key, ETag: aws.String(etag)}, nil
}

func (m *mockS3Client) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	// A canceled context fails the call, as it does on the real client.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.uploads, aws.ToString(params.UploadId))
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.delete(aws.ToString(params.Bucket), aws.ToString(params.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	out := &s3.DeleteObjectsOutput{}
	for _, id := range params.Delete.Objects {
		if aws.ToString(id.Key) == m.refuseDelete {
			out.Errors = append(out.Errors, s3types.Error{Key: id.Key, Code: aws.String("AccessDenied"), Message: aws.String("Access Denied")})
			continue
		}
		m.delete(aws.ToString(params.Bucket), aws.ToString(id.Key))
	}
	return out, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	obj, ok := m.get(aws.ToString(params.Bucket), aws.ToString(params.Key))
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	if params.IfMatch != nil && !m.ignoreIfMatch && aws.ToString(params.IfMatch) != obj.etag {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	body := obj.body
	if params.Range != nil {
		rangeStr := aws.ToString(params.Range)
		var start, end int64
		fmt.Sscanf(rangeStr, "bytes=%d-%d", &start, &end)
		if int(end) >= len(body) {
			end = int64(len(body)) - 1
		}
		body = body[start : end+1]
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: aws.Int64(int64(len(body))),
		ETag:          aws.String(obj.etag),
		LastModified:  aws.Time(obj.lastModified),
		Metadata:      obj.metadata,
	}, nil
}

func (m *mockS3Client) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	_, ok := m.get(aws.ToString(params.Bucket), aws.ToString(params.Key))
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	url := fmt.Sprintf("https://s3mock/%s/%s", aws.ToString(params.Bucket), aws.ToString(params.Key))
	return &v4.PresignedHTTPRequest{URL: url}, nil
}

func (m *mockS3Client) PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	url := fmt.Sprintf("https://s3mock/%s/%s?PUT", aws.ToString(params.Bucket), aws.ToString(params.Key))
	return &v4.PresignedHTTPRequest{URL: url}, nil
}

func (m *mockS3Client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	// S3 decodes the copy source as a query value, "+" included.
	copySource, err := url.QueryUnescape(aws.ToString(params.CopySource))
	if err != nil {
		return nil, &smithy.GenericAPIError{Code: "InvalidArgument"}
	}
	// format: bucket/key
	parts := strings.SplitN(copySource, "/", 2)
	srcBucket := parts[0]
	srcKey := parts[1]

	srcObj, ok := m.get(srcBucket, srcKey)
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	m.mu.Lock()
	m.copyObjects++
	m.mu.Unlock()
	if m.invalidRequest || (!m.largeCopy && int64(len(srcObj.body)) > maxCopyObjectSize) {
		if m.afterCopyObject != nil {
			m.afterCopyObject()
		}
		return nil, &smithy.GenericAPIError{Code: "InvalidRequest", Message: "The specified copy source is larger than the maximum allowable size for a copy source"}
	}

	// COPY keeps the source's metadata and Content-Type; REPLACE takes both from the request.
	metadata, contentType := srcObj.metadata, srcObj.contentType
	if params.MetadataDirective == s3types.MetadataDirectiveReplace {
		metadata, contentType = params.Metadata, aws.ToString(params.ContentType)
	}

	m.putWithContentType(aws.ToString(params.Bucket), aws.ToString(params.Key), srcObj.body, metadata, contentType)
	return &s3.CopyObjectOutput{}, nil
}

func (m *mockS3Client) UploadPartCopy(ctx context.Context, params *s3.UploadPartCopyInput, optFns ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error) {
	copySource, err := url.QueryUnescape(aws.ToString(params.CopySource))
	if err != nil {
		return nil, &smithy.GenericAPIError{Code: "InvalidArgument"}
	}
	srcBucket, srcKey, _ := strings.Cut(copySource, "/")
	srcObj, ok := m.get(srcBucket, srcKey)
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	if params.CopySourceIfMatch != nil && aws.ToString(params.CopySourceIfMatch) != srcObj.etag {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	var first, last int
	if _, err := fmt.Sscanf(aws.ToString(params.CopySourceRange), "bytes=%d-%d", &first, &last); err != nil || last >= len(srcObj.body) {
		return nil, &smithy.GenericAPIError{Code: "InvalidRange"}
	}
	b := srcObj.body[first : last+1]
	m.mu.Lock()
	up, ok := m.uploads[aws.ToString(params.UploadId)]
	if ok {
		up.parts[aws.ToInt32(params.PartNumber)] = b
		m.partCopies++
	}
	after, n := m.afterPartCopy, m.partCopies
	m.mu.Unlock()
	if !ok {
		return nil, &s3types.NoSuchUpload{}
	}
	if after != nil {
		after(n)
	}
	return &s3.UploadPartCopyOutput{CopyPartResult: &s3types.CopyPartResult{ETag: aws.String(mockETag(b))}}, nil
}

type StorageTestSuite struct {
	suite.Suite
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, &StorageTestSuite{})
}

func (s *StorageTestSuite) testMockClient() (*mockS3Client, s2.Storage) {
	m := newMockS3Client()
	files := map[string][]byte{
		"a.txt":     []byte("a"),
		"b.txt":     []byte("b"),
		"cc/c1.txt": []byte("c1"),
		"cc/c2.txt": []byte("c2"),
	}
	for key, b := range files {
		m.put("mybucket", key, b, nil)
	}
	return m, &storage{
		client:        m,
		presignClient: m,
		bucket:        "mybucket",
		prefix:        "",
		uri:           bucketURI("", "mybucket"),
	}
}

// Keys a pre-v0.18 s2-server wrote are lifted; a client's own s2-* keys stay user metadata.
func (s *StorageTestSuite) TestLegacyMetadata() {
	testCases := []struct {
		caseName        string
		metadata        map[string]string
		wantContentType string
		wantMetadata    s2.Metadata
	}{
		{
			caseName:        "pre-v0.18 write",
			metadata:        map[string]string{"s2-etag": `"x"`, "s2-content-type": "text/html", "author": "uz"},
			wantContentType: "text/html",
			wantMetadata:    s2.Metadata{"author": "uz"},
		},
		{
			caseName:        "pre-v0.18 write without a client Content-Type",
			metadata:        map[string]string{"s2-etag": `"x"`, "s2-content-type": "binary/octet-stream"},
			wantContentType: mockDefaultContentType,
			wantMetadata:    s2.Metadata{},
		},
		{
			caseName:        "client metadata without s2-etag",
			metadata:        map[string]string{"s2-content-type": "text/html"},
			wantContentType: mockDefaultContentType,
			wantMetadata:    s2.Metadata{"s2-content-type": "text/html"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockClient()
			ctx := context.Background()
			m.put("mybucket", "page.html", []byte("<p>"), tc.metadata)

			got, err := strg.Get(ctx, "page.html")
			s.Require().NoError(err)
			s.Equal(tc.wantContentType, got.ContentType())
			s.Equal(tc.wantMetadata, got.Metadata())

			s.Require().NoError(strg.PutMetadata(ctx, "page.html", got.Metadata()))

			got, err = strg.Get(ctx, "page.html")
			s.Require().NoError(err)
			s.Equal(tc.wantContentType, got.ContentType())
			s.Equal(tc.wantMetadata, got.Metadata())
		})
	}
}

func (s *StorageTestSuite) TestS2TestList() {
	_, strg := s.testMockClient()
	ctx := context.Background()

	err := s2test.TestStorageListRecursive(ctx, strg, "a.txt", "b.txt", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageListRecursivePrefix(ctx, strg)
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "", "a.txt", "b.txt")
	s.Require().NoError(err)

	err = s2test.TestStorageList(ctx, strg, "cc", "cc/c1.txt", "cc/c2.txt")
	s.Require().NoError(err)

	s.Require().NoError(s2test.TestStorageListDefaultPage(ctx, strg))

	s.Require().NoError(s2test.TestStorageCount(ctx, strg))
}

// TestListCursorMapping pins which ListObjectsV2 parameter each cursor field
// reaches: AWS reads StartAfter as a key name, so a token must never land
// there.
func (s *StorageTestSuite) TestListCursorMapping() {
	testCases := []struct {
		caseName       string
		prefix         string
		opts           s2.ListOptions
		wantToken      string
		wantStartAfter string
	}{
		{
			caseName:  "after becomes a continuation token",
			opts:      s2.ListOptions{After: mockContinuationPrefix + "a.txt"},
			wantToken: mockContinuationPrefix + "a.txt",
		},
		{
			caseName:       "start after becomes start-after",
			opts:           s2.ListOptions{StartAfter: "a.txt"},
			wantStartAfter: "a.txt",
		},
		{
			caseName:  "after wins over start after",
			opts:      s2.ListOptions{After: mockContinuationPrefix + "a.txt", StartAfter: "b.txt"},
			wantToken: mockContinuationPrefix + "a.txt",
		},
		{
			caseName:       "start after keeps a trailing delimiter",
			opts:           s2.ListOptions{StartAfter: "dir/"},
			wantStartAfter: "dir/",
		},
		{
			caseName:       "start after is resolved against the storage prefix",
			prefix:         "cc",
			opts:           s2.ListOptions{StartAfter: "c1.txt"},
			wantStartAfter: "cc/c1.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockClient()
			strg.(*storage).prefix = tc.prefix

			_, err := strg.List(context.Background(), tc.opts)
			s.Require().NoError(err)
			s.Equal(tc.wantToken, aws.ToString(m.lastListInput.ContinuationToken))
			s.Equal(tc.wantStartAfter, aws.ToString(m.lastListInput.StartAfter))
		})
	}
}

// TestListSubPrefixBoundary pins that a Sub storage lists only its own
// subtree: "data" must not pull in "data2/" keys.
func (s *StorageTestSuite) TestListSubPrefixBoundary() {
	m, strg := s.testMockClient()
	m.put("mybucket", "data", []byte("root key"), nil)
	m.put("mybucket", "data/a.txt", []byte("a"), nil)
	m.put("mybucket", "data2/secret.txt", []byte("s"), nil)
	strg.(*storage).prefix = "data"

	res, err := strg.List(context.Background(), s2.ListOptions{Recursive: true})
	s.Require().NoError(err)
	s.Equal([]string{"a.txt"}, objectNames(res.Objects))
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
	m, strg := s.testMockClient()
	m.put("mybucket", "cc/dd/x.txt", []byte("x"), nil)
	strg.(*storage).prefix = "cc"

	res, err := strg.List(context.Background(), s2.ListOptions{})
	s.Require().NoError(err)
	s.Equal([]string{"dd/"}, res.CommonPrefixes)
}

// A key ending in "/" is a directory marker, listed as any key; only the storage prefix's own is hidden, as its name would be "".
func (s *StorageTestSuite) TestListDirMarkers() {
	testCases := []struct {
		caseName     string
		prefix       string
		keys         map[string]string
		opts         s2.ListOptions
		stuck        bool
		want         []string
		wantPrefixes []string
		wantErr      string
	}{
		{caseName: "zero-byte marker", keys: map[string]string{"photos/": "", "photos/a.jpg": "a"}, opts: s2.ListOptions{Recursive: true}, want: []string{"photos/", "photos/a.jpg"}},
		{caseName: "marker with a body", keys: map[string]string{"photos/": "DATA"}, opts: s2.ListOptions{Recursive: true}, want: []string{"photos/"}},
		{caseName: "delimited listing of the marked prefix", keys: map[string]string{"photos/": "", "photos/a.jpg": "a"}, opts: s2.ListOptions{Prefix: "photos/"}, want: []string{"photos/", "photos/a.jpg"}},
		{caseName: "storage prefix's own marker", prefix: "b1", keys: map[string]string{"b1/": "", "b1/a.txt": "a"}, opts: s2.ListOptions{Recursive: true}, want: []string{"a.txt"}},
		{caseName: "page of the own marker only", prefix: "b1", keys: map[string]string{"b1/": "", "b1/a.txt": "a"}, opts: s2.ListOptions{Recursive: true, Limit: 1}, want: []string{"a.txt"}},
		{caseName: "markers across pages", keys: map[string]string{"a/": "", "b/": "", "c.txt": "c"}, opts: s2.ListOptions{Recursive: true, Limit: 1}, want: []string{"a/", "b/", "c.txt"}},
		{caseName: "marker-only prefix lists as a common prefix", keys: map[string]string{"photos/": ""}, want: []string{}, wantPrefixes: []string{"photos/"}},
		{caseName: "marker beside the object of its name", keys: map[string]string{"photos": "n", "photos/": ""}, opts: s2.ListOptions{Recursive: true}, want: []string{"photos", "photos/"}},
		{caseName: "token that does not advance", prefix: "b1", keys: map[string]string{"b1/": "", "b1/c.txt": "c"}, opts: s2.ListOptions{Recursive: true, Limit: 1, After: mockContinuationPrefix}, stuck: true, wantErr: "does not advance"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m := newMockS3Client()
			m.stuckToken = tc.stuck
			for k, v := range tc.keys {
				m.put("mybucket", k, []byte(v), nil)
			}
			strg := &storage{client: m, presignClient: m, bucket: "mybucket", prefix: tc.prefix}
			ctx := context.Background()

			got, prefixes := []string{}, []string{}
			opts := tc.opts
			for {
				res, err := strg.List(ctx, opts)
				if tc.wantErr != "" && err != nil {
					s.ErrorContains(err, tc.wantErr)
					return
				}
				s.Require().NoError(err)
				got = append(got, objectNames(res.Objects)...)
				prefixes = append(prefixes, res.CommonPrefixes...)
				if res.NextAfter == "" {
					break
				}
				s.NotEmpty(res.Objects, "a page with NextAfter is empty")
				opts.After = res.NextAfter
			}
			s.Empty(tc.wantErr, "List did not fail")
			s.Equal(tc.want, got)
			if tc.wantPrefixes != nil {
				s.Equal(tc.wantPrefixes, prefixes)
			}
		})
	}
	s.Run("DeleteRecursive removes a marker", func() {
		m := newMockS3Client()
		m.put("mybucket", "photos/", nil, nil)
		m.put("mybucket", "photos/a.jpg", []byte("a"), nil)
		strg := &storage{client: m, presignClient: m, bucket: "mybucket"}

		s.Require().NoError(strg.DeleteRecursive(context.Background(), "photos"))
		s.Empty(m.objects)
	})
}

func (s *StorageTestSuite) TestS2TestGetPut() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageGetPut(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestOpenSinceGet() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageOpenSinceGet(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestUploadConditions() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageUploadConditions(context.Background(), strg))
}

// A PreconditionFailed reads as s2's only for a call that carried a condition.
func (s *StorageTestSuite) TestUploadMapsPreconditionFailed() {
	testCases := []struct {
		caseName string
		opts     s2.UploadOptions
		mapped   bool
	}{
		{caseName: "conditional", opts: s2.UploadOptions{IfNoneMatch: "*"}, mapped: true},
		{caseName: "unconditional is passed through"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockClient()
			m.preconditionFailed = true

			_, err := s2.Upload(context.Background(), strg, s2.NewObjectBytes("k.txt", []byte("v")), tc.opts)

			s.Require().Error(err)
			s.Equal(tc.mapped, errors.Is(err, s2.ErrPreconditionFailed))
		})
	}
}

// S3's NotImplemented to a condition it does not take reads as ErrUnsupported, which s2-server answers 501.
func (s *StorageTestSuite) TestUploadMapsNotImplemented() {
	testCases := []struct {
		caseName string
		opts     s2.UploadOptions
		want     error
	}{
		{caseName: "conditional", opts: s2.UploadOptions{IfNoneMatch: "*"}, want: errors.ErrUnsupported},
		{caseName: "unconditional is not refused"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockClient()
			m.notImplemented = true

			_, err := s2.Upload(context.Background(), strg, s2.NewObjectBytes("k.txt", []byte("v")), tc.opts)

			if tc.want == nil {
				s.NoError(err)
				return
			}
			s.ErrorIs(err, tc.want)
		})
	}
}

// A body too large for one PutObject goes up in parts, never read whole, and a part or condition failure aborts the upload.
func (s *StorageTestSuite) TestUploadStreamsLargeBody() {
	const partLen = 8 << 20
	body := bytes.Repeat([]byte("0123456789abcdef"), (2*partLen+1)/16+1)
	errRead := errors.New("read failed")
	whole := func(context.CancelFunc) io.Reader { return bytes.NewReader(body) }
	testCases := []struct {
		caseName string
		existing bool
		opts     s2.UploadOptions
		body     func(cancel context.CancelFunc) io.Reader
		seekable bool
		checksum aws.RequestChecksumCalculation
		want     error
	}{
		{caseName: "unconditional", body: whole},
		{caseName: "seekable", body: whole, seekable: true},
		{caseName: "when_required sends no checksum", body: whole, checksum: aws.RequestChecksumCalculationWhenRequired},
		{caseName: "If-Match holds", existing: true, opts: s2.UploadOptions{IfMatch: mockETag([]byte("old"))}, body: whole},
		{caseName: "If-None-Match fails at Complete", existing: true, opts: s2.UploadOptions{IfNoneMatch: "*"}, body: whole, want: s2.ErrPreconditionFailed},
		{caseName: "If-Match on a missing key", opts: s2.UploadOptions{IfMatch: mockETag([]byte("old"))}, body: whole, want: s2.ErrNotExist},
		{caseName: "body fails midway", body: func(context.CancelFunc) io.Reader {
			return io.MultiReader(bytes.NewReader(body[:2*partLen+1]), iotest.ErrReader(errRead))
		}, want: errRead},
		{caseName: "caller's context canceled midway", body: func(cancel context.CancelFunc) io.Reader {
			return io.MultiReader(bytes.NewReader(body[:2*partLen+1]), cancelReader(cancel))
		}, want: context.Canceled},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			m, strg := s.testMockClient()
			strg.(*storage).checksum = tc.checksum
			if tc.existing {
				m.put("mybucket", "big.bin", []byte("old"), nil)
			}
			// NopCloser hides Seek, as the s3api handler's bodies do.
			r := tc.body(cancel)
			rc := io.NopCloser(r)
			if tc.seekable {
				rc = seekCloser{r.(io.ReadSeeker)}
			}
			obj := s2.NewObjectReader("big.bin", rc, uint64(len(body)), s2.WithContentType("application/x-big"), s2.WithMetadata(s2.Metadata{"k": "v"}))

			res, err := s2.Upload(ctx, strg, obj, tc.opts)

			s.Positive(m.nextUploadID, "the upload crossed the multipart threshold")
			s.Empty(m.uploads, "every multipart upload is completed or aborted")
			s.LessOrEqual(m.maxPartLen, partLen)
			wantAlgorithm := s3types.ChecksumAlgorithmCrc32
			if tc.checksum == aws.RequestChecksumCalculationWhenRequired {
				wantAlgorithm = ""
			}
			s.Equal(wantAlgorithm, m.checksumAlgorithm)
			if tc.want != nil {
				s.ErrorIs(err, tc.want)
				if got, ok := m.get("mybucket", "big.bin"); ok {
					s.Equal("old", string(got.body))
				}
				return
			}
			s.Require().NoError(err)
			s.True(strings.HasSuffix(res.ETag, `-3"`), "ETag %s", res.ETag)
			got, err := strg.Get(ctx, "big.bin")
			s.Require().NoError(err)
			s.Equal(res.ETag, got.ETag())
			s.Equal("application/x-big", got.ContentType())
			s.Equal(s2.Metadata{"k": "v"}, got.Metadata())
			stored, _ := m.get("mybucket", "big.bin")
			s.True(bytes.Equal(body, stored.body))
		})
	}
}

// A body one byte short of the threshold is one PutObject; one at the threshold is a multipart upload.
func (s *StorageTestSuite) TestUploadThreshold() {
	const threshold = 16 << 20
	testCases := []struct {
		caseName  string
		length    int
		multipart bool
	}{
		{caseName: "below", length: threshold - 1},
		{caseName: "at", length: threshold, multipart: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockClient()
			body := bytes.Repeat([]byte("x"), tc.length)

			_, err := s2.Upload(context.Background(), strg, s2.NewObjectReader("t.bin", io.NopCloser(bytes.NewReader(body)), uint64(len(body))), s2.UploadOptions{})

			s.Require().NoError(err)
			s.Equal(tc.multipart, m.nextUploadID > 0)
			stored, _ := m.get("mybucket", "t.bin")
			s.Len(stored.body, tc.length)
		})
	}
}

// seekCloser keeps Seek visible, as a file's body does.
type seekCloser struct{ io.ReadSeeker }

func (seekCloser) Close() error { return nil }

// cancelReader cancels the upload's context when read, as a client hanging up does.
type cancelReader context.CancelFunc

func (c cancelReader) Read([]byte) (int, error) {
	c()
	return 0, context.Canceled
}

func (s *StorageTestSuite) TestS2TestGetNotExist() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageGetNotExist(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestExists() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStorageExists(context.Background(), strg))
}

func (s *StorageTestSuite) TestS2TestCopyMove() {
	_, strg := s.testMockClient()
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
			_, strg := s.testMockClient()
			strg.(*storage).prefix = tc.prefix
			s.Require().NoError(s2test.TestStorageDelete(context.Background(), strg))
		})
	}
}

func (s *StorageTestSuite) TestS2TestNameEscape() {
	testCases := []struct {
		caseName string
		prefix   string
	}{
		{caseName: "no prefix"},
		{caseName: "with prefix", prefix: "pfx"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()
			strg.(*storage).prefix = tc.prefix
			s.Require().NoError(s2test.TestStorageNameEscape(context.Background(), strg))
		})
	}
}

func (s *StorageTestSuite) TestS2TestDirMarker() {
	testCases := []struct {
		caseName string
		prefix   string
	}{
		{caseName: "no prefix"},
		{caseName: "with prefix", prefix: "pfx"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()
			strg.(*storage).prefix = tc.prefix
			s.Require().NoError(s2test.TestStorageDirMarker(context.Background(), strg))
		})
	}
}

func (s *StorageTestSuite) TestS2TestDirMarkerBesideObject() {
	testCases := []struct {
		caseName string
		prefix   string
	}{
		{caseName: "no prefix"},
		{caseName: "with prefix", prefix: "pfx"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()
			strg.(*storage).prefix = tc.prefix
			s.Require().NoError(s2test.TestStorageDirMarkerBesideObject(context.Background(), strg))
		})
	}
}

func (s *StorageTestSuite) TestS2TestSubConsistency() {
	testCases := []struct {
		caseName string
		prefix   string
	}{
		{caseName: "no prefix"},
		{caseName: "with prefix", prefix: "pfx"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()
			strg.(*storage).prefix = tc.prefix
			s.Require().NoError(s2test.TestStorageSubConsistency(context.Background(), strg))
		})
	}
}

func (s *StorageTestSuite) TestS2TestPutMetadata() {
	_, strg := s.testMockClient()
	s.Require().NoError(s2test.TestStoragePutMetadata(context.Background(), strg))
}

func (s *StorageTestSuite) TestCopySourceEscape() {
	testCases := []struct {
		caseName string
		key      string
		decoy    string
	}{
		{caseName: "space", key: "sp ace.txt"},
		{caseName: "hash", key: "hash#.txt"},
		{caseName: "ampersand and equals", key: "amp&eq=.txt"},
		{caseName: "question mark", key: "q?x.txt"},
		{caseName: "bare percent", key: "pct%zz.txt"},
		{caseName: "percent escape", key: "pct%41.txt", decoy: "pctA.txt"},
		{caseName: "plus", key: "plus+.txt", decoy: "plus .txt"},
		{caseName: "non-ASCII", key: "dir/日本語.txt"},
		{caseName: "directory marker", key: "d+ir/"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			_, strg := s.testMockClient()
			s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(tc.key, []byte(tc.key))))
			if tc.decoy != "" {
				s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(tc.decoy, []byte("decoy"))))
			}

			s.Require().NoError(strg.Copy(ctx, tc.key, "copy/"+tc.key))
			s.Require().NoError(strg.PutMetadata(ctx, tc.key, s2.Metadata{"k": "v"}))

			for _, name := range []string{"copy/" + tc.key, tc.key} {
				obj, err := strg.Get(ctx, name)
				s.Require().NoError(err)
				s.Equal(uint64(len(tc.key)), obj.Length(), name)
			}
			obj, err := strg.Get(ctx, tc.key)
			s.Require().NoError(err)
			v, _ := obj.Metadata().Get("k")
			s.Equal("v", v)
			if tc.decoy != "" {
				obj, err := strg.Get(ctx, tc.decoy)
				s.Require().NoError(err)
				_, ok := obj.Metadata().Get("k")
				s.False(ok, "PutMetadata reached the decoy")
			}
		})
	}
}

func (s *StorageTestSuite) TestNewStorageError() {
	_, err := NewStorage(context.Background(), s2.Config{})
	s.Require().ErrorIs(err, ErrRequiredConfigRoot)
}

func (s *StorageTestSuite) TestNewStorage() {
	testCases := []struct {
		caseName   string
		cfg        s2.Config
		wantBucket string
		env        string // AWS_ENDPOINT_URL
		load       LoadOptionsFunc
		wantPrefix string
		wantURI    string
	}{
		{
			caseName:   "bucket only",
			cfg:        s2.Config{Type: s2.TypeS3, Root: "my-bucket"},
			wantBucket: "my-bucket",
			wantPrefix: "",
			wantURI:    "s3://my-bucket/",
		},
		{
			caseName:   "bucket with prefix",
			cfg:        s2.Config{Type: s2.TypeS3, Root: "my-bucket/some/prefix"},
			wantBucket: "my-bucket",
			wantPrefix: "some/prefix",
			wantURI:    "s3://my-bucket/some/prefix/",
		},
		{
			caseName:   "root with slashes trimmed",
			cfg:        s2.Config{Type: s2.TypeS3, Root: "/my-bucket/pfx/"},
			wantBucket: "my-bucket",
			wantPrefix: "pfx",
			wantURI:    "s3://my-bucket/pfx/",
		},
		{
			caseName: "with S3Config region only",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3:   &s2.S3Config{Region: "us-west-2"},
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
			wantURI:    "s3://my-bucket/",
		},
		{
			caseName: "with S3Config credentials",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3: &s2.S3Config{
					AccessKeyID:     "AKID",
					SecretAccessKey: "SECRET",
				},
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
			wantURI:    "s3://my-bucket/",
		},
		{
			caseName: "with S3Config endpoint",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3: &s2.S3Config{
					EndpointURL: "http://localhost:9000",
				},
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
			wantURI:    "http://localhost:9000/my-bucket/",
		},
		{
			caseName: "with full S3Config",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket/data",
				S3: &s2.S3Config{
					EndpointURL:     "http://localhost:9000",
					Region:          "ap-northeast-1",
					AccessKeyID:     "minioadmin",
					SecretAccessKey: "minioadmin",
				},
			},
			wantBucket: "my-bucket",
			wantPrefix: "data",
			wantURI:    "http://localhost:9000/my-bucket/data/",
		},
		{
			caseName: "nil S3Config",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3:   nil,
			},
			wantBucket: "my-bucket",
			wantPrefix: "",
			wantURI:    "s3://my-bucket/",
		},
		{
			caseName: "endpoint credentials, query and fragment dropped",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3:   &s2.S3Config{EndpointURL: "https://user:pw@example.com/base/?x=1#f"},
			},
			wantBucket: "my-bucket",
			wantURI:    "https://example.com/base/my-bucket/",
		},
		{
			caseName:   "endpoint from the environment",
			cfg:        s2.Config{Type: s2.TypeS3, Root: "my-bucket", S3: &s2.S3Config{Region: "us-east-1"}},
			env:        "http://env.example:9000",
			wantBucket: "my-bucket",
			wantURI:    "http://env.example:9000/my-bucket/",
		},
		{
			caseName: "endpoint from a deprecated resolver",
			cfg:      s2.Config{Type: s2.TypeS3, Root: "my-bucket", S3: &s2.S3Config{Region: "us-east-1"}},
			load: func(s2.Config) ([]func(*awsconfig.LoadOptions) error, error) {
				resolver := aws.EndpointResolverWithOptionsFunc(func(_, region string, _ ...any) (aws.Endpoint, error) { //nolint:staticcheck // the deprecated resolver is the case under test
					return aws.Endpoint{URL: "http://minio.example:9000"}, nil //nolint:staticcheck // the deprecated resolver is the case under test
				})
				return []func(*awsconfig.LoadOptions) error{awsconfig.WithEndpointResolverWithOptions(resolver)}, nil //nolint:staticcheck // the deprecated resolver is the case under test
			},
			wantBucket: "my-bucket",
		},
		{
			caseName: "endpoint without a scheme",
			cfg: s2.Config{
				Type: s2.TypeS3,
				Root: "my-bucket",
				S3:   &s2.S3Config{EndpointURL: "localhost:9000"},
			},
			wantBucket: "my-bucket",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			// Keeps the developer's own endpoint settings out of the expected URIs.
			s.T().Setenv("AWS_ENDPOINT_URL", tc.env)
			s.T().Setenv("AWS_ENDPOINT_URL_S3", "")
			s.T().Setenv("AWS_CONFIG_FILE", filepath.Join(s.T().TempDir(), "config"))
			s.T().Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(s.T().TempDir(), "credentials"))
			if tc.load != nil {
				RegisterLoadOptionsFunc(tc.load)
				defer UnregisterLoadOptionsFunc()
			}
			strg, err := NewStorage(context.Background(), tc.cfg)
			s.Require().NoError(err)

			st := strg.(*storage)
			s.Equal(tc.wantBucket, st.bucket)
			s.Equal(tc.wantPrefix, st.prefix)
			s.NotNil(st.client)
			s.Equal(tc.wantURI, st.URI())
		})
	}
}

func (s *StorageTestSuite) TestRegisterLoadOptionsFunc() {
	s.T().Cleanup(func() { UnregisterLoadOptionsFunc() })

	testCases := []struct {
		caseName    string
		fn          LoadOptionsFunc
		cfg         s2.Config
		wantRegion  string
		wantAccess  string
		wantSession string
	}{
		{
			caseName: "applies registered credentials and region",
			fn: func(cfg s2.Config) ([]func(*awsconfig.LoadOptions) error, error) {
				return []func(*awsconfig.LoadOptions) error{
					awsconfig.WithRegion("ap-northeast-1"),
					awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
						return aws.Credentials{
							AccessKeyID:     "REGISTERED",
							SecretAccessKey: "REGISTERED-SECRET",
							SessionToken:    "REGISTERED-SESSION",
						}, nil
					})),
				}, nil
			},
			cfg:         s2.Config{Type: s2.TypeS3, Root: "my-bucket"},
			wantRegion:  "ap-northeast-1",
			wantAccess:  "REGISTERED",
			wantSession: "REGISTERED-SESSION",
		},
		{
			caseName: "nil, nil leaves AWS SDK defaults in place",
			fn: func(cfg s2.Config) ([]func(*awsconfig.LoadOptions) error, error) {
				return nil, nil
			},
			cfg: s2.Config{Type: s2.TypeS3, Root: "my-bucket"},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			RegisterLoadOptionsFunc(tc.fn)

			strg, err := NewStorage(context.Background(), tc.cfg)
			s.Require().NoError(err)

			st := strg.(*storage)
			client, ok := st.client.(*s3.Client)
			s.Require().True(ok)

			opts := client.Options()
			if tc.wantRegion != "" {
				s.Equal(tc.wantRegion, opts.Region)
			}
			if tc.wantAccess != "" {
				creds, err := opts.Credentials.Retrieve(context.Background())
				s.Require().NoError(err)
				s.Equal(tc.wantAccess, creds.AccessKeyID)
				s.Equal(tc.wantSession, creds.SessionToken)
			}
		})
	}
}

func (s *StorageTestSuite) TestRegisterLoadOptionsFuncError() {
	s.T().Cleanup(func() { UnregisterLoadOptionsFunc() })

	RegisterLoadOptionsFunc(func(cfg s2.Config) ([]func(*awsconfig.LoadOptions) error, error) {
		return nil, fmt.Errorf("boom")
	})

	_, err := NewStorage(context.Background(), s2.Config{Type: s2.TypeS3, Root: "my-bucket"})
	s.Require().Error(err)
	s.ErrorContains(err, "boom")
}

func (s *StorageTestSuite) TestUnregisterLoadOptionsFunc() {
	RegisterLoadOptionsFunc(func(cfg s2.Config) ([]func(*awsconfig.LoadOptions) error, error) {
		return nil, fmt.Errorf("should not be called after unregister")
	})
	UnregisterLoadOptionsFunc()

	_, err := NewStorage(context.Background(), s2.Config{Type: s2.TypeS3, Root: "my-bucket"})
	s.Require().NoError(err)
}

func (s *StorageTestSuite) TestType() {
	_, strg := s.testMockClient()
	s.Equal(s2.TypeS3, strg.Type())
}

func (s *StorageTestSuite) TestGet() {
	testCases := []struct {
		caseName string
		ctx      context.Context
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
			_, strg := s.testMockClient()

			got, err := strg.Get(tc.ctx, tc.name)
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
		m, _ := s.testMockClient()
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
		ctx      context.Context
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
			_, strg := s.testMockClient()

			err := strg.Put(tc.ctx, tc.obj)
			s.Require().NoError(err)

			got, err := strg.Get(tc.ctx, tc.obj.Name())
			s.Require().NoError(err)

			rc, err := got.Open()
			s.Require().NoError(err)
			defer rc.Close()

			body, _ := io.ReadAll(rc)
			s.Equal("new content", string(body))
			v, _ := got.Metadata().Get("key")
			s.Equal("val", v)
		})
	}
}

func (s *StorageTestSuite) TestDelete() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		name     string
	}{
		{
			caseName: "typical",
			name:     "a.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()

			err := strg.Delete(tc.ctx, tc.name)
			s.Require().NoError(err)

			_, err = strg.Get(tc.ctx, tc.name)
			s.Error(err)
		})
	}
}

func (s *StorageTestSuite) TestDeleteRecursive() {
	testCases := []struct {
		caseName     string
		ctx          context.Context
		prefix       string
		refuseDelete string
		wantErr      string
		wantLeft     []string
	}{
		{
			caseName: "typical",
			prefix:   "cc",
			wantLeft: []string{"a.txt", "b.txt"},
		},
		{
			caseName:     "key the batch reports in Errors",
			prefix:       "cc",
			refuseDelete: "cc/c2.txt",
			wantErr:      `"cc/c2.txt": AccessDenied: Access Denied`,
			wantLeft:     []string{"a.txt", "b.txt", "cc/c2.txt"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			m, strg := s.testMockClient()
			m.refuseDelete = tc.refuseDelete

			err := strg.DeleteRecursive(tc.ctx, tc.prefix)
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
			} else {
				s.Require().NoError(err)
			}

			res, err := strg.List(tc.ctx, s2.ListOptions{Limit: 10, Recursive: true})
			s.Require().NoError(err)
			objs := res.Objects
			s.Equal(len(tc.wantLeft), len(objs))
			for i, w := range tc.wantLeft {
				s.Equal(w, objs[i].Name())
			}
		})
	}
	s.Run("empty-page", func() {
		m := newMockS3Client()
		strg := &storage{
			client: m,
			bucket: "mybucket",
		}
		err := strg.DeleteRecursive(context.Background(), "prefix")
		s.Require().NoError(err)
	})
}

func (s *StorageTestSuite) TestSub() {
	_, strg := s.testMockClient()
	ctx := context.Background()

	sub, err := strg.Sub(ctx, "cc")
	s.Require().NoError(err)
	s.Equal(s2.TypeS3, sub.Type())
	s.Equal("s3://mybucket/cc/", sub.URI())
	data, err := strg.Sub(ctx, "data")
	s.Require().NoError(err)
	view, err := data.Sub(ctx, "x y")
	s.Require().NoError(err)
	s.Equal("s3://mybucket/data/x%20y/", view.URI())

	res, err := sub.List(ctx, s2.ListOptions{Limit: 10, Recursive: true})
	s.Require().NoError(err)
	s.Len(res.Objects, 2)
}

func (s *StorageTestSuite) TestExists() {
	testCases := []struct {
		caseName string
		name     string
		want     bool
		wantErr  bool
	}{
		{caseName: "leaf object", name: "a.txt", want: true},
		{caseName: "leaf object missing", name: "not-found.txt", want: false},
		// "cc" is not a leaf object, but the mock has cc/c1.txt and
		// cc/c2.txt under it, so the ListObjectsV2 fallback finds them.
		{caseName: "non-empty prefix", name: "cc", want: true},
		// No object starts with "no-such/", so the fallback returns 0.
		{caseName: "missing prefix", name: "no-such", want: false},
		// The storage root is always considered present: constructing
		// the Storage means the caller has accepted the bucket as
		// existing, and we don't burn a HeadBucket round-trip on every
		// call to confirm what the next read or write would surface
		// anyway.
		{caseName: "storage root", name: "", want: true},
		// "/" spells the same root, but a name is used as written.
		{caseName: "storage root slash", name: "/", wantErr: true},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()
			got, err := strg.Exists(context.Background(), tc.name)
			if tc.wantErr {
				s.Require().ErrorIs(err, s2.ErrInvalidName)
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
}

func (s *StorageTestSuite) TestPutMetadata() {
	_, strg := s.testMockClient()
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
		_, strg := s.testMockClient()
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
		_, strg := s.testMockClient()
		err := strg.Copy(context.Background(), "not-found.txt", "dst.txt")
		s.True(errors.Is(err, s2.ErrNotExist), "got %v, want s2.ErrNotExist", err)
	})
}

func (s *StorageTestSuite) TestMove() {
	_, strg := s.testMockClient()
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

// TestCopyOverCopyObjectLimit shrinks the 5 GiB limit so Copy, Move and PutMetadata take UploadPartCopy ranges.
func (s *StorageTestSuite) TestCopyOverCopyObjectLimit() {
	defer func(limit, part int64, conc int) {
		maxCopyObjectSize, copyPartSize, copyConcurrency = limit, part, conc
	}(maxCopyObjectSize, copyPartSize, copyConcurrency)

	maxCopyObjectSize, copyPartSize = 4, 3
	body := []byte("0123456789")
	expires := time.Date(2026, 10, 21, 7, 28, 0, 0, time.UTC)
	headers := &s3.HeadObjectOutput{
		CacheControl:            aws.String("no-cache"),
		ContentDisposition:      aws.String("attachment"),
		ContentEncoding:         aws.String("gzip"),
		ContentLanguage:         aws.String("ja"),
		WebsiteRedirectLocation: aws.String("/elsewhere"),
		ExpiresString:           aws.String(expires.Format(http.TimeFormat)),
	}
	copyBig := func(ctx context.Context, strg s2.Storage) error { return strg.Copy(ctx, "big", "big-copy") }
	putMetadata := func(ctx context.Context, strg s2.Storage) error {
		return strg.PutMetadata(ctx, "big", s2.Metadata{"new": "w"})
	}
	testCases := []struct {
		caseName string
		run      func(ctx context.Context, strg s2.Storage) error
		// concurrency, when set, replaces copyConcurrency.
		concurrency int
		// replaceAt rewrites the source after that many part copies; cancelAt cancels the caller's context then.
		replaceAt int
		cancelAt  int
		// invalidRequest refuses CopyObject for another reason than the size.
		invalidRequest bool
		// largeCopy accepts CopyObject above the limit; deleteAfterRefusal removes the source once CopyObject is refused.
		largeCopy          bool
		deleteAfterRefusal bool
		// hideTagCount, denyTagging and completeConflict set the mock's switches of the same names.
		hideTagCount     bool
		denyTagging      bool
		completeConflict bool
		// headFailsAfterRefusal makes HeadObject answer SlowDown once CopyObject is refused.
		headFailsAfterRefusal bool
		untagged              bool
		name                  string
		metadata              map[string]string
		// carried is whether the upload carries the source's headers, as a COPY directive keeps them.
		carried         bool
		wantCopyObjects int
		wantParts       int
		wantTaggings    int
		wantErr         bool
		wantErrIs       error
		// wantErrCodes are API error codes the returned error must wrap.
		wantErrCodes []string
	}{
		{
			caseName:        "copy",
			run:             copyBig,
			name:            "big-copy",
			metadata:        map[string]string{"k": "v"},
			carried:         true,
			wantCopyObjects: 1,
			wantParts:       4,
			wantTaggings:    1,
		},
		{
			caseName:        "copy one part at a time",
			run:             copyBig,
			concurrency:     1,
			name:            "big-copy",
			metadata:        map[string]string{"k": "v"},
			carried:         true,
			wantCopyObjects: 1,
			wantParts:       4,
			wantTaggings:    1,
		},
		{
			caseName:        "untagged copy",
			run:             copyBig,
			untagged:        true,
			name:            "big-copy",
			metadata:        map[string]string{"k": "v"},
			carried:         true,
			wantCopyObjects: 1,
			wantParts:       4,
			wantTaggings:    1,
		},
		{
			caseName:        "tagged copy, HEAD reports no tag count",
			run:             copyBig,
			hideTagCount:    true,
			name:            "big-copy",
			metadata:        map[string]string{"k": "v"},
			carried:         true,
			wantCopyObjects: 1,
			wantParts:       4,
			wantTaggings:    1,
		},
		{
			caseName:        "GetObjectTagging refused",
			run:             copyBig,
			denyTagging:     true,
			wantCopyObjects: 1,
			wantTaggings:    1,
			wantErr:         true,
			wantErrCodes:    []string{"AccessDenied"},
		},
		{
			caseName:        "move",
			run:             func(ctx context.Context, strg s2.Storage) error { return s2.Move(ctx, strg, "big", "big-copy") },
			name:            "big-copy",
			metadata:        map[string]string{"k": "v"},
			carried:         true,
			wantCopyObjects: 1,
			wantParts:       4,
			wantTaggings:    1,
		},
		{
			caseName:        "put metadata",
			run:             putMetadata,
			name:            "big",
			metadata:        map[string]string{"new": "w"},
			wantCopyObjects: 1,
			wantParts:       4,
			wantTaggings:    1,
		},
		{
			caseName:        "put metadata where CopyObject has no limit",
			run:             putMetadata,
			largeCopy:       true,
			untagged:        true,
			name:            "big",
			metadata:        map[string]string{"new": "w"},
			wantCopyObjects: 1,
		},
		{
			caseName:           "source deleted after the refused CopyObject",
			run:                copyBig,
			deleteAfterRefusal: true,
			wantCopyObjects:    1,
			wantErr:            true,
			wantErrIs:          s2.ErrNotExist,
		},
		{
			caseName:              "HEAD fails after the refused CopyObject",
			run:                   copyBig,
			headFailsAfterRefusal: true,
			wantCopyObjects:       1,
			wantErr:               true,
			wantErrCodes:          []string{"InvalidRequest", "SlowDown"},
		},
		{
			caseName:        "caller's context canceled between parts",
			run:             copyBig,
			concurrency:     1,
			cancelAt:        1,
			wantCopyObjects: 1,
			wantParts:       1,
			wantTaggings:    1,
			wantErr:         true,
			wantErrIs:       context.Canceled,
		},
		{
			caseName:        "at the limit",
			run:             func(ctx context.Context, strg s2.Storage) error { return strg.Copy(ctx, "small", "small-copy") },
			name:            "small-copy",
			wantCopyObjects: 1,
		},
		{
			caseName:        "invalid request at the limit",
			run:             func(ctx context.Context, strg s2.Storage) error { return strg.Copy(ctx, "small", "small-copy") },
			invalidRequest:  true,
			wantCopyObjects: 1,
			wantErr:         true,
		},
		{
			caseName:        "source replaced between parts",
			run:             copyBig,
			concurrency:     1,
			replaceAt:       1,
			wantCopyObjects: 1,
			wantParts:       1,
			wantTaggings:    1,
			wantErr:         true,
			wantErrIs:       s2.ErrModified,
		},
		{
			caseName:        "put metadata, source replaced before complete",
			run:             putMetadata,
			concurrency:     1,
			replaceAt:       4,
			wantCopyObjects: 1,
			wantParts:       4,
			wantTaggings:    1,
			wantErr:         true,
			wantErrIs:       s2.ErrModified,
		},
		{
			caseName:         "put metadata, complete conflicts with a write",
			run:              putMetadata,
			completeConflict: true,
			wantCopyObjects:  1,
			wantParts:        4,
			wantTaggings:     1,
			wantErr:          true,
			wantErrIs:        s2.ErrModified,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			m, strg := s.testMockClient()
			m.putWithContentType("mybucket", "big", body, map[string]string{"k": "v"}, "text/plain")
			m.putWithContentType("mybucket", "small", body[:4], nil, "text/plain")
			big, _ := m.get("mybucket", "big")
			big.headers = headers
			if !tc.untagged {
				big.tags = map[string]string{"t": "1 2"}
			}
			m.invalidRequest = tc.invalidRequest
			m.largeCopy = tc.largeCopy
			m.hideTagCount, m.denyTagging, m.completeConflict = tc.hideTagCount, tc.denyTagging, tc.completeConflict
			if tc.deleteAfterRefusal {
				m.afterCopyObject = func() { m.delete("mybucket", "big") }
			}
			if tc.headFailsAfterRefusal {
				m.afterCopyObject = func() {
					m.mu.Lock()
					defer m.mu.Unlock()

					m.headErr = &smithy.GenericAPIError{Code: "SlowDown"}
				}
			}
			copyConcurrency = 5
			if tc.concurrency > 0 {
				copyConcurrency = tc.concurrency
			}
			replaced := []byte("abcdefghij")
			if tc.replaceAt > 0 {
				m.afterPartCopy = func(n int) {
					if n == tc.replaceAt {
						m.put("mybucket", "big", replaced, nil)
					}
				}
			}
			if tc.cancelAt > 0 {
				m.afterPartCopy = func(n int) {
					if n == tc.cancelAt {
						cancel()
					}
				}
			}

			err := tc.run(ctx, strg)
			s.Equal(tc.wantCopyObjects, m.copyObjects)
			s.Equal(tc.wantParts, m.partCopies)
			s.Equal(tc.wantTaggings, m.getTaggings)
			s.Empty(m.uploads, "an upload was left behind")
			if tc.wantErr {
				s.Error(err)
				if tc.wantErrIs != nil {
					s.ErrorIs(err, tc.wantErrIs)
				}
				for _, code := range tc.wantErrCodes {
					s.True(hasAPIErrorCode(err, code), "%v does not wrap %s", err, code)
					s.NotErrorIs(err, s2.ErrNotExist)
				}
				for _, name := range []string{"big-copy", "small-copy"} {
					_, ok := m.get("mybucket", name)
					s.False(ok, "a failed copy wrote %s", name)
				}
				if tc.replaceAt > 0 {
					obj, _ := m.get("mybucket", "big")
					s.Equal(replaced, obj.body, "the newer write was lost")
				}
				return
			}
			s.Require().NoError(err)
			obj, ok := m.get("mybucket", tc.name)
			s.Require().True(ok)
			s.Equal(string(body[:len(obj.body)]), string(obj.body))
			s.Equal("text/plain", obj.contentType)
			if tc.metadata == nil {
				return
			}
			s.Equal(tc.metadata, obj.metadata)
			s.Equal(body, obj.body)
			if tc.untagged {
				s.Empty(obj.tags)
			} else {
				s.Equal(map[string]string{"t": "1 2"}, obj.tags)
			}
			in := m.lastCreate
			if tc.wantParts == 0 {
				s.Nil(in, "a single CopyObject started an upload")
				return
			}
			if tc.carried {
				s.Equal(headers.CacheControl, in.CacheControl)
				s.Equal(headers.ContentDisposition, in.ContentDisposition)
				s.Equal(headers.ContentEncoding, in.ContentEncoding)
				s.Equal(headers.ContentLanguage, in.ContentLanguage)
				s.Nil(in.WebsiteRedirectLocation, "CopyObject does not copy the redirect")
				s.Require().NotNil(in.Expires)
				s.True(expires.Equal(*in.Expires), "Expires %v", *in.Expires)
			} else {
				s.Nil(in.CacheControl)
				s.Nil(in.ContentDisposition)
				s.Nil(in.ContentEncoding)
				s.Nil(in.ContentLanguage)
				s.Nil(in.WebsiteRedirectLocation)
				s.Nil(in.Expires)
			}
		})
	}
}

// hasAPIErrorCode reports whether any API error err wraps has code, following both branches of a multi-%w error.
func hasAPIErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == code {
		return true
	}
	if u, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range u.Unwrap() {
			if hasAPIErrorCode(e, code) {
				return true
			}
		}
	}
	return false
}

func (s *StorageTestSuite) TestCopyPartSizeFor() {
	testCases := []struct {
		caseName string
		size     int64
		want     int64
	}{
		{caseName: "just over CopyObject's limit", size: 5<<30 + 1, want: 1 << 30},
		{caseName: "10000 parts of 1 GiB", size: maxCopyParts << 30, want: 1 << 30},
		{caseName: "one byte more", size: maxCopyParts<<30 + 1, want: 1<<30 + 1},
		{caseName: "50 TB", size: 50e12, want: 5e9},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			got := copyPartSizeFor(tc.size)
			s.Equal(tc.want, got)
			s.LessOrEqual((tc.size+got-1)/got, int64(maxCopyParts))
		})
	}
}

func (s *StorageTestSuite) TestSignedURL() {
	testCases := []struct {
		caseName string
		ctx      context.Context
		name     string
		ttl      time.Duration
		want     string
		wantErr  string
	}{
		{
			caseName: "typical",
			name:     "a.txt",
			ttl:      time.Hour,
			want:     "https://s3mock/mybucket/a.txt",
		},
		{
			caseName: "not found",
			name:     "not-found.txt",
			ttl:      time.Hour,
			wantErr:  "not exist: not-found.txt",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			_, strg := s.testMockClient()

			got, err := strg.SignedURL(tc.ctx, s2.SignedURLOptions{Name: tc.name, TTL: tc.ttl})
			if tc.wantErr != "" {
				s.ErrorContains(err, tc.wantErr)
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.want, got)
		})
	}
	s.Run("unknown-client-type", func() {
		strg := &storage{
			client: &mockS3Client{}, // Not a *s3.Client
		}
		_, err := strg.SignedURL(context.Background(), s2.SignedURLOptions{Name: "a.txt", TTL: time.Hour})
		s.Error(err)
		s.ErrorContains(err, "unknown client type")
	})
}

func (s *StorageTestSuite) TestListedObjectOpensItself() {
	m := newMockS3Client()
	// A key ending in "/" written by another tool, beside the object whose
	// name path.Join would fold it onto.
	m.put("mybucket", "photos", []byte("THE NEIGHBOUR"), nil)
	m.put("mybucket", "photos/", []byte("MARKER"), nil)
	strg := &storage{client: m, presignClient: m, bucket: "mybucket"}

	res, err := strg.List(context.Background(), s2.ListOptions{Recursive: true})
	s.Require().NoError(err)
	s.Require().Len(res.Objects, 2)
	for _, obj := range res.Objects {
		rc, err := obj.Open()
		s.Require().NoError(err)
		b, err := io.ReadAll(rc)
		_ = rc.Close()
		s.Require().NoError(err)
		s.Lenf(b, int(obj.Length()), "%q opened a different object", obj.Name())
	}
}

func (s *StorageTestSuite) TestObjectOpensTheKeyStorageWrote() {
	m := newMockS3Client()
	// A prefixed root: Put joins the prefix with the name, Open concatenates
	// it so a listing's name survives verbatim. Both must land on one key.
	strg := &storage{client: m, presignClient: m, bucket: "mybucket", prefix: "data/objects"}
	ctx := context.Background()
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes("a.txt", []byte("BODY"))))

	obj, err := strg.Get(ctx, "a.txt")
	s.Require().NoError(err)
	rc, err := obj.Open()
	s.Require().NoError(err)

	defer func() { _ = rc.Close() }()

	b, err := io.ReadAll(rc)
	s.Require().NoError(err)
	s.Equal("BODY", string(b))
}

package s3api

import (
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is used here only to mirror S3 multipart ETag semantics under test.
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
)

type MultipartTestSuite struct{ s3apiSuite }

func TestMultipartTestSuite(t *testing.T) {
	suite.Run(t, &MultipartTestSuite{})
}

func (s *MultipartTestSuite) TestCreateMultipartUpload() {
	testCases := []struct {
		caseName    string
		setupBucket bool
		bucket      string
		key         string
		wantStatus  int
		wantErrCode string
	}{
		{
			caseName:    "success",
			setupBucket: true,
			bucket:      "mp-bucket",
			key:         "file.bin",
			wantStatus:  http.StatusOK,
		},
		{
			caseName:    "bucket not found",
			bucket:      "no-such",
			key:         "file.bin",
			wantStatus:  http.StatusNotFound,
			wantErrCode: "NoSuchBucket",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			if tc.setupBucket {
				s.createBucket(tc.bucket)
			}
			req := httptest.NewRequest("POST", "/"+tc.bucket+"/"+tc.key+"?uploads", nil)
			req.SetPathValue("bucket", tc.bucket)
			req.SetPathValue("key", tc.key)
			w := httptest.NewRecorder()
			handleCreateMultipartUpload(s.server, w, req)

			s.Equal(tc.wantStatus, w.Code)
			if tc.wantErrCode == "" {
				var result InitiateMultipartUploadResult
				s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
				s.Equal(tc.bucket, result.Bucket)
				s.Equal(tc.key, result.Key)
				s.NotEmpty(result.UploadID)
				return
			}
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal(tc.wantErrCode, errResp.Code)
		})
	}
}

func (s *MultipartTestSuite) TestCompleteMultipartUploadRejects() {
	// Upload a real part so these cases fail on validation, not on a missing part.
	s.createBucket("cmp")
	createReq := httptest.NewRequest("POST", "/cmp/o.txt?uploads", nil)
	createReq.SetPathValue("bucket", "cmp")
	createReq.SetPathValue("key", "o.txt")
	createW := httptest.NewRecorder()
	handleCreateMultipartUpload(s.server, createW, createReq)
	s.Require().Equal(http.StatusOK, createW.Code)

	var created InitiateMultipartUploadResult
	s.Require().NoError(xml.Unmarshal(createW.Body.Bytes(), &created))

	partReq := httptest.NewRequest("PUT", "/cmp/o.txt?partNumber=1&uploadId="+created.UploadID, strings.NewReader("hello"))
	partReq.SetPathValue("bucket", "cmp")
	partReq.SetPathValue("key", "o.txt")
	partReq.ContentLength = 5
	partW := httptest.NewRecorder()
	handleUploadPart(s.server, partW, partReq)
	s.Require().Equal(http.StatusOK, partW.Code)

	part := func(n string) string {
		return "<Part><PartNumber>" + n + "</PartNumber><ETag>x</ETag></Part>"
	}
	testCases := []struct {
		caseName string
		parts    string
		wantCode string
	}{
		{caseName: "duplicate part number", parts: part("1") + part("1"), wantCode: "InvalidPartOrder"},
		{caseName: "descending order", parts: part("2") + part("1"), wantCode: "InvalidPartOrder"},
		{caseName: "part number zero", parts: part("0"), wantCode: "InvalidArgument"},
		{caseName: "part number above the ceiling", parts: part("10001"), wantCode: "InvalidArgument"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			body := "<CompleteMultipartUpload>" + tc.parts + "</CompleteMultipartUpload>"
			req := httptest.NewRequest("POST", "/cmp/o.txt?uploadId="+created.UploadID, strings.NewReader(body))
			req.SetPathValue("bucket", "cmp")
			req.SetPathValue("key", "o.txt")
			w := httptest.NewRecorder()
			handleCompleteMultipartUpload(s.server, w, req)

			s.Equal(http.StatusBadRequest, w.Code)
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal(tc.wantCode, errResp.Code)
		})
	}

	s.Run("body over the cap", func() {
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for b.Len() < 12<<20 { // ~12 MiB, over maxXMLRequestBody (8 MiB)
			b.WriteString(part("1"))
		}
		b.WriteString("</CompleteMultipartUpload>")

		req := httptest.NewRequest("POST", "/cmp/o.txt?uploadId="+created.UploadID, strings.NewReader(b.String()))
		req.SetPathValue("bucket", "cmp")
		req.SetPathValue("key", "o.txt")
		w := httptest.NewRecorder()
		handleCompleteMultipartUpload(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("MaxMessageLengthExceeded", errResp.Code)
	})
}

// initiateUpload returns the upload ID for a create request carrying headers.
func (s *MultipartTestSuite) initiateUpload(bucket, key string, headers http.Header) string {
	s.T().Helper()

	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploads", nil)
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	for name, values := range headers {
		req.Header[name] = values
	}
	w := httptest.NewRecorder()
	handleCreateMultipartUpload(s.server, w, req)
	s.Require().Equal(http.StatusOK, w.Code)

	var created InitiateMultipartUploadResult
	s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &created))
	s.Require().NotEmpty(created.UploadID)
	return created.UploadID
}

func (s *MultipartTestSuite) uploadPart(bucket, key, uploadID string, partNumber int, body string) {
	s.T().Helper()

	target := fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucket, key, partNumber, uploadID)
	req := httptest.NewRequest("PUT", target, strings.NewReader(body))
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	req.ContentLength = int64(len(body))
	w := httptest.NewRecorder()
	handleUploadPart(s.server, w, req)
	s.Require().Equal(http.StatusOK, w.Code)
}

// completeUpload assembles partNumbers into the final object.
func (s *MultipartTestSuite) completeUpload(bucket, key, uploadID string, partNumbers ...int) {
	s.T().Helper()

	var body strings.Builder
	body.WriteString("<CompleteMultipartUpload>")
	for _, n := range partNumbers {
		fmt.Fprintf(&body, "<Part><PartNumber>%d</PartNumber><ETag>x</ETag></Part>", n)
	}
	body.WriteString("</CompleteMultipartUpload>")

	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploadId="+uploadID, strings.NewReader(body.String()))
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()
	handleCompleteMultipartUpload(s.server, w, req)
	s.Require().Equal(http.StatusOK, w.Code, w.Body.String())
}

// getObject reads the object back through the handler that has to surface
// the recorded metadata.
func (s *MultipartTestSuite) getObject(bucket, key string) *httptest.ResponseRecorder {
	s.T().Helper()

	req := httptest.NewRequest("GET", "/"+bucket+"/"+key, nil)
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()
	handleGetObject(s.server, w, req)
	s.Require().Equal(http.StatusOK, w.Code)
	return w
}

func (s *MultipartTestSuite) storage(bucket string) s2.Storage {
	s.T().Helper()

	strg, err := s.server.Buckets.Get(context.Background(), bucket)
	s.Require().NoError(err)
	return strg
}

// smuggledInternalKeys sends every reserved key as an x-amz-meta-* header.
func smuggledInternalKeys() http.Header {
	h := http.Header{}
	for k := range server.InternalMetadataKeys {
		h.Set(metaHeaderPrefix+k, "smuggled")
	}
	return h
}

// An absent Content-Type stays unstored so the console can still guess.
func (s *MultipartTestSuite) TestCreateMultipartUploadLeavesAbsentContentTypeUnstored() {
	testCases := []struct {
		caseName string
		headers  http.Header
	}{
		{caseName: "no header"},
		{caseName: "whitespace-only header", headers: http.Header{"Content-Type": {"   "}}},
		{
			// Derived from the reserved set, so a key added to it is
			// covered here rather than silently going untested.
			caseName: "reserved keys smuggled through x-amz-meta-*",
			headers:  smuggledInternalKeys(),
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.createBucket("mp-noct")
			uploadID := s.initiateUpload("mp-noct", "movie.mp4", tc.headers)

			obj, err := s.storage("mp-noct").Get(context.Background(), manifestKey(uploadID))
			s.Require().NoError(err)
			for k := range server.InternalMetadataKeys {
				s.NotContains(obj.Metadata(), k)
			}
		})
	}
}

// failingStorage fails every Get; uploadMetadata calls nothing else.
type failingStorage struct {
	s2.Storage
	err error
}

func (f failingStorage) Get(context.Context, string) (s2.Object, error) { return nil, f.err }

func (s *MultipartTestSuite) TestUploadMetadata() {
	testCases := []struct {
		caseName string
		err      error
		wantErr  bool
	}{
		{
			caseName: "a missing manifest is not an error",
			err:      fmt.Errorf("%w: manifest", s2.ErrNotExist),
		},
		{
			// Completing anyway would silently drop the caller's metadata.
			caseName: "any other read failure is surfaced",
			err:      errors.New("backend unavailable"),
			wantErr:  true,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			md, err := uploadMetadata(context.Background(), failingStorage{err: tc.err}, "x")

			if tc.wantErr {
				s.Require().ErrorIs(err, tc.err)
				s.Nil(md)
				return
			}
			s.Require().NoError(err)
			s.Empty(md)
		})
	}
}

// An uploadId s2 never issued must not reach the storage layer as a key.
func (s *MultipartTestSuite) TestMalformedUploadID() {
	testCases := []struct {
		caseName string
		method   string
		handler  func(*server.Server, http.ResponseWriter, *http.Request)
	}{
		{caseName: "upload part", method: "PUT", handler: handleUploadPart},
		{caseName: "complete", method: "POST", handler: handleCompleteMultipartUpload},
		{caseName: "abort", method: "DELETE", handler: handleAbortMultipartUpload},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.createBucket("mp-bad")
			target := "/mp-bad/o.txt?partNumber=1&uploadId=" + url.QueryEscape("../evil")
			req := httptest.NewRequest(tc.method, target, strings.NewReader(""))
			req.SetPathValue("bucket", "mp-bad")
			req.SetPathValue("key", "o.txt")
			w := httptest.NewRecorder()
			tc.handler(s.server, w, req)

			s.Equal(http.StatusNotFound, w.Code)
			s.NotContains(w.Body.String(), multipartPrefix)
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal("NoSuchUpload", errResp.Code)
		})
	}
}

func (s *MultipartTestSuite) TestCreateMultipartUploadRecordsMetadata() {
	s.createBucket("mp-manifest")
	uploadID := s.initiateUpload("mp-manifest", "file.bin", http.Header{
		"Content-Type":      {"video/mp4"},
		"X-Amz-Meta-Author": {"uz"},
	})

	obj, err := s.storage("mp-manifest").Get(context.Background(), manifestKey(uploadID))
	s.Require().NoError(err)
	s.Equal("video/mp4", obj.Metadata()[contentTypeMetadataKey])
	s.Equal("uz", obj.Metadata()["author"])
	s.Zero(obj.Length())
}

func (s *MultipartTestSuite) TestCompleteMultipartUploadCarriesInitiateMetadata() {
	testCases := []struct {
		caseName        string
		headers         http.Header
		wantContentType string
		wantMeta        map[string]string
	}{
		{
			caseName: "content type and user metadata",
			headers: http.Header{
				"Content-Type":      {"video/mp4"},
				"X-Amz-Meta-Author": {"uz"},
				"X-Amz-Meta-Origin": {"camera"},
			},
			wantContentType: "video/mp4",
			wantMeta:        map[string]string{"author": "uz", "origin": "camera"},
		},
		{
			caseName:        "no content type falls back to the default",
			wantContentType: defaultContentType,
		},
		{
			caseName:        "whitespace-only content type falls back to the default",
			headers:         http.Header{"Content-Type": {"   "}},
			wantContentType: defaultContentType,
		},
		{
			caseName:        "user metadata without a content type",
			headers:         http.Header{"X-Amz-Meta-Author": {"uz"}},
			wantContentType: defaultContentType,
			wantMeta:        map[string]string{"author": "uz"},
		},
	}
	for i, tc := range testCases {
		s.Run(tc.caseName, func() {
			// A key per row: rows must not read each other's object.
			bucket, key := "mp-meta", fmt.Sprintf("movie-%d.bin", i)
			s.createBucket(bucket)

			uploadID := s.initiateUpload(bucket, key, tc.headers)
			s.uploadPart(bucket, key, uploadID, 1, "hello ")
			s.uploadPart(bucket, key, uploadID, 2, "world")
			s.completeUpload(bucket, key, uploadID, 1, 2)

			w := s.getObject(bucket, key)
			s.Equal("hello world", w.Body.String())
			s.Equal(tc.wantContentType, w.Header().Get("Content-Type"))
			for name, want := range tc.wantMeta {
				s.Equal(want, w.Header().Get("x-amz-meta-"+name))
			}
			// The ETag survives alongside the recorded metadata.
			s.Regexp(`^"[0-9a-f]{32}-2"$`, w.Header().Get("ETag"))
			// s2's own bookkeeping keys must not leak as user metadata.
			s.Empty(w.Header().Get("x-amz-meta-" + contentTypeMetadataKey))
			s.Empty(w.Header().Get("x-amz-meta-" + etagMetadataKey))
		})
	}
}

// An upload in flight across a server upgrade has no manifest.
func (s *MultipartTestSuite) TestCompleteMultipartUploadWithoutManifest() {
	const bucket, key = "mp-nomanifest", "file.bin"
	s.createBucket(bucket)

	uploadID := s.initiateUpload(bucket, key, http.Header{"Content-Type": {"video/mp4"}})
	s.uploadPart(bucket, key, uploadID, 1, "hello")
	s.Require().NoError(s.storage(bucket).Delete(context.Background(), manifestKey(uploadID)))

	s.completeUpload(bucket, key, uploadID, 1)

	w := s.getObject(bucket, key)
	s.Equal("hello", w.Body.String())
	s.Equal(defaultContentType, w.Header().Get("Content-Type"))
}

// Complete used to delete only the parts it was given (#202).
func (s *MultipartTestSuite) TestCompleteMultipartUploadClearsTheUploadTree() {
	const bucket, key = "mp-cleanup", "file.bin"
	s.createBucket(bucket)

	uploadID := s.initiateUpload(bucket, key, nil)
	s.uploadPart(bucket, key, uploadID, 1, "hello")
	s.uploadPart(bucket, key, uploadID, 2, "orphan")
	s.completeUpload(bucket, key, uploadID, 1)

	ctx := context.Background()
	strg := s.storage(bucket)
	for _, name := range []string{
		manifestKey(uploadID),
		partKey(uploadID, 1),
		partKey(uploadID, 2),
		multipartPrefix + uploadID,
	} {
		exists, err := strg.Exists(ctx, name)
		s.Require().NoError(err)
		s.Falsef(exists, "%s should have been removed", name)
	}
}

func TestPartsReader(t *testing.T) {
	testCases := []struct {
		caseName string
		bodies   []string
	}{
		{caseName: "single part", bodies: []string{"hello"}},
		{caseName: "two parts", bodies: []string{"foo", "barbaz"}},
		{caseName: "empty part in middle", bodies: []string{"a", "", "b"}},
		{caseName: "many parts", bodies: []string{"1", "22", "333", "4444", "55555"}},
		{caseName: "no parts", bodies: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			parts := make([]s2.Object, len(tc.bodies))
			var want string
			var wantMD5s []byte
			for i, body := range tc.bodies {
				parts[i] = s2.NewObjectBytes("part", []byte(body))
				want += body
				h := md5.Sum([]byte(body)) // #nosec G401
				wantMD5s = append(wantMD5s, h[:]...)
			}

			pr := &partsReader{parts: parts}
			got, err := io.ReadAll(pr)
			require.NoError(t, err)
			assert.Equal(t, want, string(got))
			assert.Equal(t, wantMD5s, pr.partMD5s)
			assert.NoError(t, pr.Close())
		})
	}
}

func TestPartsReader_CloseMidStream(t *testing.T) {
	parts := []s2.Object{
		s2.NewObjectBytes("a", []byte("hello world")),
	}
	pr := &partsReader{parts: parts}

	// Read 1 byte to open the underlying part.
	buf := make([]byte, 1)
	_, err := pr.Read(buf)
	require.NoError(t, err)
	require.NotNil(t, pr.current, "current should be open after a partial read")

	// Closing mid-stream releases current and reports no error.
	require.NoError(t, pr.Close())
	assert.Nil(t, pr.current)

	// A subsequent Close is a no-op on the now-empty reader.
	require.NoError(t, pr.Close())
}

func TestPartsReader_SmallBuffer(t *testing.T) {
	// Read byte-by-byte to exercise the "part exhausted mid-buffer" path.
	parts := []s2.Object{
		s2.NewObjectBytes("a", []byte("abc")),
		s2.NewObjectBytes("b", []byte("de")),
	}
	pr := &partsReader{parts: parts}

	var got []byte
	buf := make([]byte, 1)
	for {
		n, err := pr.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	assert.Equal(t, "abcde", string(got))
	assert.Len(t, pr.partMD5s, 2*md5.Size)
}

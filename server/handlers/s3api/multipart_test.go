package s3api

import (
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is used here only to mirror S3 multipart ETag semantics under test.
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		{caseName: "empty parts list", parts: "", wantCode: "InvalidRequest"},
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

// uploadPart returns the entry Complete needs for the part.
func (s *MultipartTestSuite) uploadPart(bucket, key, uploadID string, partNumber int, body string) CompletePart {
	s.T().Helper()

	target := fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucket, key, partNumber, uploadID)
	req := httptest.NewRequest("PUT", target, strings.NewReader(body))
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	req.ContentLength = int64(len(body))
	w := httptest.NewRecorder()
	handleUploadPart(s.server, w, req)
	s.Require().Equal(http.StatusOK, w.Code, w.Body.String())
	return CompletePart{PartNumber: partNumber, ETag: w.Header().Get("ETag")}
}

func completeBody(parts ...CompletePart) string {
	var body strings.Builder
	body.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&body, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", p.PartNumber, p.ETag)
	}
	body.WriteString("</CompleteMultipartUpload>")
	return body.String()
}

func (s *MultipartTestSuite) complete(bucket, key, uploadID string, parts ...CompletePart) *httptest.ResponseRecorder {
	s.T().Helper()

	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploadId="+uploadID, strings.NewReader(completeBody(parts...)))
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()
	handleCompleteMultipartUpload(s.server, w, req)
	return w
}

// completeUpload assembles parts into the final object.
func (s *MultipartTestSuite) completeUpload(bucket, key, uploadID string, parts ...CompletePart) {
	s.T().Helper()

	w := s.complete(bucket, key, uploadID, parts...)
	s.Require().Equal(http.StatusOK, w.Code, w.Body.String())
}

func (s *MultipartTestSuite) abortUpload(bucket, key, uploadID string) {
	s.T().Helper()

	req := httptest.NewRequest("DELETE", "/"+bucket+"/"+key+"?uploadId="+uploadID, nil)
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()
	handleAbortMultipartUpload(s.server, w, req)
	s.Require().Equal(http.StatusNoContent, w.Code, w.Body.String())
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

// multipartETag is S3's ETag for parts: the MD5 of their MD5s, then "-N".
func multipartETag(parts ...string) string {
	var digests []byte
	for _, p := range parts {
		h := md5.Sum([]byte(p)) // #nosec G401
		digests = append(digests, h[:]...)
	}
	sum := md5.Sum(digests) // #nosec G401
	return fmt.Sprintf(`"%x-%d"`, sum, len(parts))
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

			md, err := s.server.Multipart.Metadata(context.Background(), uploadID, "mp-noct", "movie.mp4", s.generation("mp-noct"))
			s.Require().NoError(err)
			for k := range server.InternalMetadataKeys {
				s.NotContains(md, k)
			}
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
			s.NotContains(w.Body.String(), ".multipart")
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal("NoSuchUpload", errResp.Code)
		})
	}
}

// generation returns bucket's current generation.
func (s *MultipartTestSuite) generation(bucket string) int64 {
	s.T().Helper()

	created, err := s.server.Buckets.CreatedAt(context.Background(), bucket)
	s.Require().NoError(err)
	return created.UnixNano()
}

// An upload of a deleted bucket must not complete into its same-named successor.
func (s *MultipartTestSuite) TestUploadOfRecreatedBucket() {
	root := s.T().TempDir()
	cfg := server.DefaultConfig()
	cfg.Root = root
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
	s.createBucket("mp-reborn")
	uploadID := s.initiateUpload("mp-reborn", "file.bin", nil)
	p1 := s.uploadPart("mp-reborn", "file.bin", uploadID, 1, "hello")
	// Backdated so the successor's marker cannot share its time.
	at := time.Now().Add(-time.Hour)
	s.Require().NoError(os.Chtimes(filepath.Join(root, "mp-reborn", ".keep"), at, at))
	s.Require().NoError(s.server.Buckets.Delete(context.Background(), "mp-reborn"))
	s.createBucket("mp-reborn")

	testCases := []struct {
		caseName string
		method   string
		handler  func(*server.Server, http.ResponseWriter, *http.Request)
		body     string
	}{
		{caseName: "upload part", method: "PUT", handler: handleUploadPart, body: "world"},
		{caseName: "complete", method: "POST", handler: handleCompleteMultipartUpload, body: fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, p1.ETag)},
		{caseName: "abort", method: "DELETE", handler: handleAbortMultipartUpload},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req := httptest.NewRequest(tc.method, "/mp-reborn/file.bin?partNumber=2&uploadId="+uploadID, strings.NewReader(tc.body))
			req.SetPathValue("bucket", "mp-reborn")
			req.SetPathValue("key", "file.bin")
			w := httptest.NewRecorder()
			tc.handler(s.server, w, req)

			s.Equal(http.StatusNotFound, w.Code, w.Body.String())
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal("NoSuchUpload", errResp.Code)
		})
	}
	exists, err := s.storage("mp-reborn").Exists(context.Background(), "file.bin")
	s.Require().NoError(err)
	s.False(exists)
}

// An ensure-exists PUT of the bucket must not orphan its uploads.
func (s *MultipartTestSuite) TestUploadSurvivesBucketRePut() {
	testCases := []struct {
		caseName string
		create   func(root string)
	}{
		{caseName: "bucket created by s2", create: func(string) { s.createBucket("mp-reput") }},
		{
			caseName: "directory placed under Root by hand",
			create: func(root string) {
				s.Require().NoError(os.MkdirAll(filepath.Join(root, "mp-reput"), 0o750))
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			root := s.T().TempDir()
			cfg := server.DefaultConfig()
			cfg.Root = root
			srv, err := server.NewServer(context.Background(), cfg)
			s.Require().NoError(err)
			s.server = srv
			tc.create(root)
			uploadID := s.initiateUpload("mp-reput", "file.bin", nil)

			req := httptest.NewRequest("PUT", "/mp-reput", nil)
			req.SetPathValue("bucket", "mp-reput")
			w := httptest.NewRecorder()
			handleCreateBucket(s.server, w, req)
			s.Require().Equal(http.StatusOK, w.Code, w.Body.String())

			p1 := s.uploadPart("mp-reput", "file.bin", uploadID, 1, "hello")
			s.completeUpload("mp-reput", "file.bin", uploadID, p1)
		})
	}
}

// An expired upload is refused before the sweep frees it.
func (s *MultipartTestSuite) TestExpiredUpload() {
	root := s.T().TempDir()
	cfg := server.DefaultConfig()
	cfg.Root = root
	cfg.MultipartMaxAge = int64(time.Hour.Seconds())
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
	s.createBucket("mp-expired")
	uploadID := s.initiateUpload("mp-expired", "file.bin", nil)
	at := time.Now().Add(-2 * time.Hour)
	s.Require().NoError(os.Chtimes(filepath.Join(root, ".multipart", uploadID, "meta"), at, at))

	testCases := []struct {
		caseName string
		method   string
		handler  func(*server.Server, http.ResponseWriter, *http.Request)
	}{
		{caseName: "upload part", method: "PUT", handler: handleUploadPart},
		{caseName: "complete", method: "POST", handler: handleCompleteMultipartUpload},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"x"</ETag></Part></CompleteMultipartUpload>`
			req := httptest.NewRequest(tc.method, "/mp-expired/file.bin?partNumber=1&uploadId="+uploadID, strings.NewReader(body))
			req.SetPathValue("bucket", "mp-expired")
			req.SetPathValue("key", "file.bin")
			w := httptest.NewRecorder()
			tc.handler(s.server, w, req)

			s.Equal(http.StatusNotFound, w.Code, w.Body.String())
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal("NoSuchUpload", errResp.Code)
		})
	}

	// Abort still reclaims it.
	s.abortUpload("mp-expired", "file.bin", uploadID)
}

func (s *MultipartTestSuite) TestCreateMultipartUploadRecordsMetadata() {
	s.createBucket("mp-manifest")
	uploadID := s.initiateUpload("mp-manifest", "file.bin", http.Header{
		"Content-Type":      {"video/mp4"},
		"X-Amz-Meta-Author": {"uz"},
	})

	md, err := s.server.Multipart.Metadata(context.Background(), uploadID, "mp-manifest", "file.bin", s.generation("mp-manifest"))
	s.Require().NoError(err)
	s.Equal("video/mp4", md[contentTypeMetadataKey])
	s.Equal("uz", md["author"])
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
			p1 := s.uploadPart(bucket, key, uploadID, 1, "hello ")
			p2 := s.uploadPart(bucket, key, uploadID, 2, "world")
			s.completeUpload(bucket, key, uploadID, p1, p2)

			w := s.getObject(bucket, key)
			s.Equal("hello world", w.Body.String())
			s.Equal(tc.wantContentType, w.Header().Get("Content-Type"))
			for name, want := range tc.wantMeta {
				s.Equal(want, w.Header().Get("x-amz-meta-"+name))
			}
			// The ETag survives alongside the recorded metadata.
			s.Equal(multipartETag("hello ", "world"), w.Header().Get("ETag"))
			// s2's own bookkeeping keys must not leak as user metadata.
			s.Empty(w.Header().Get("x-amz-meta-" + contentTypeMetadataKey))
			s.Empty(w.Header().Get("x-amz-meta-" + etagMetadataKey))
		})
	}
}

// Uploads are bound to their bucket/key; pre-v0.16 IDs have no record.
func (s *MultipartTestSuite) TestUnknownOrMismatchedUpload() {
	s.createBucket("mp-bind")
	s.createBucket("mp-other")
	uploadID := s.initiateUpload("mp-bind", "file.bin", nil)
	p1 := s.uploadPart("mp-bind", "file.bin", uploadID, 1, "hello")

	handlers := []struct {
		name    string
		method  string
		handler func(*server.Server, http.ResponseWriter, *http.Request)
	}{
		{name: "upload part", method: "PUT", handler: handleUploadPart},
		{name: "complete", method: "POST", handler: handleCompleteMultipartUpload},
		{name: "abort", method: "DELETE", handler: handleAbortMultipartUpload},
	}
	testCases := []struct {
		caseName string
		bucket   string
		key      string
		uploadID string
	}{
		{caseName: "never issued", bucket: "mp-bind", key: "file.bin", uploadID: strings.Repeat("0", 32)},
		{caseName: "another key", bucket: "mp-bind", key: "other.bin", uploadID: uploadID},
		{caseName: "another bucket", bucket: "mp-other", key: "file.bin", uploadID: uploadID},
	}
	for _, tc := range testCases {
		for _, h := range handlers {
			s.Run(tc.caseName+"/"+h.name, func() {
				target := fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", tc.bucket, tc.key, tc.uploadID)
				req := httptest.NewRequest(h.method, target, strings.NewReader(completeBody(p1)))
				req.SetPathValue("bucket", tc.bucket)
				req.SetPathValue("key", tc.key)
				w := httptest.NewRecorder()
				h.handler(s.server, w, req)

				s.Equal(http.StatusNotFound, w.Code, w.Body.String())
				var errResp ErrorResponse
				s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
				s.Equal("NoSuchUpload", errResp.Code)
			})
		}
	}

	// None of the rejected requests touched the real upload.
	s.completeUpload("mp-bind", "file.bin", uploadID, p1)
	s.Equal("hello", s.getObject("mp-bind", "file.bin").Body.String())
}

func (s *MultipartTestSuite) TestCompleteMultipartUploadChecksPartETags() {
	s.createBucket("mp-etag")
	uploadID := s.initiateUpload("mp-etag", "file.bin", nil)
	p1 := s.uploadPart("mp-etag", "file.bin", uploadID, 1, "hello")
	hexETag := strings.Trim(p1.ETag, `"`)

	testCases := []struct {
		caseName string
		etag     string
		wantCode int
	}{
		{caseName: "as returned", etag: p1.ETag, wantCode: http.StatusOK},
		{caseName: "without quotes", etag: hexETag, wantCode: http.StatusOK},
		{caseName: "upper case hex", etag: strings.ToUpper(hexETag), wantCode: http.StatusOK},
		{caseName: "pretty-printed element", etag: "\n  " + p1.ETag + "\n", wantCode: http.StatusOK},
		{caseName: "another part's etag", etag: `"` + strings.Repeat("0", 32) + `"`, wantCode: http.StatusBadRequest},
		{caseName: "empty", etag: "", wantCode: http.StatusBadRequest},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			// A fresh upload per row: a successful Complete consumes the parts.
			uploadID := s.initiateUpload("mp-etag", "file.bin", nil)
			s.uploadPart("mp-etag", "file.bin", uploadID, 1, "hello")

			w := s.complete("mp-etag", "file.bin", uploadID, CompletePart{PartNumber: 1, ETag: tc.etag})

			s.Equal(tc.wantCode, w.Code, w.Body.String())
			if tc.wantCode != http.StatusOK {
				var errResp ErrorResponse
				s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
				s.Equal("InvalidPart", errResp.Code)
			}
		})
	}
}

// abortDuring aborts the upload the first time the request body is read.
type abortDuring struct {
	abort func()
	done  bool
}

func (a *abortDuring) Read([]byte) (int, error) {
	if !a.done {
		a.done = true
		a.abort()
	}
	return 0, io.EOF
}

// An Abort that lands while a part streams in must not leave that part behind.
func (s *MultipartTestSuite) TestUploadPartRacingAbort() {
	ctx := context.Background()
	s.createBucket("mp-race")
	uploadID := s.initiateUpload("mp-race", "file.bin", nil)
	s.uploadPart("mp-race", "file.bin", uploadID, 1, "hello")

	trigger := &abortDuring{abort: func() { s.abortUpload("mp-race", "file.bin", uploadID) }}
	req := httptest.NewRequest("PUT", "/mp-race/file.bin?partNumber=2&uploadId="+uploadID, io.MultiReader(trigger, strings.NewReader("world")))
	req.SetPathValue("bucket", "mp-race")
	req.SetPathValue("key", "file.bin")
	req.ContentLength = 5
	w := httptest.NewRecorder()
	handleUploadPart(s.server, w, req)

	s.True(trigger.done)
	s.Equal(http.StatusNotFound, w.Code, w.Body.String())
	var errResp ErrorResponse
	s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
	s.Equal("NoSuchUpload", errResp.Code)

	exists, err := s.server.Multipart.Storage().Exists(ctx, uploadID)
	s.Require().NoError(err)
	s.False(exists, "the late part must not outlive the abort")
}

// A part that exists but cannot be read is the server's fault, not the client's.
func (s *MultipartTestSuite) TestCompleteMultipartUploadPartReadFailure() {
	s.createBucket("mp-eio")
	uploadID := s.initiateUpload("mp-eio", "file.bin", nil)
	p1 := s.uploadPart("mp-eio", "file.bin", uploadID, 1, "hello")
	// A corrupt sidecar makes the fs backend's Get fail with a decode error.
	s.Require().NoError(s.server.Multipart.Storage().Put(context.Background(), s2.NewObjectBytes(uploadID+"/.meta/00001", []byte("{"))))

	w := s.complete("mp-eio", "file.bin", uploadID, p1)
	s.Equal(http.StatusInternalServerError, w.Code, w.Body.String())
}

// Parts left without a record are freed by repeating Abort, as on S3.
func (s *MultipartTestSuite) TestAbortRemovesLeftoverParts() {
	ctx := context.Background()
	s.createBucket("mp-leftover")
	uploadID := s.initiateUpload("mp-leftover", "file.bin", nil)
	s.abortUpload("mp-leftover", "file.bin", uploadID)
	// Written straight to the store: a part left by a cleanup that failed.
	s.Require().NoError(s.server.Multipart.Storage().Put(ctx, s2.NewObjectBytes(uploadID+"/00001", []byte("late"))))

	s.abortUpload("mp-leftover", "file.bin", uploadID)

	exists, err := s.server.Multipart.Storage().Exists(ctx, uploadID)
	s.Require().NoError(err)
	s.False(exists)
}

// State lives outside the bucket and is fully removed on Complete and Abort.
func (s *MultipartTestSuite) TestMultipartStateLifecycle() {
	testCases := []struct {
		caseName string
		finish   func(bucket, key, uploadID string, p1 CompletePart)
	}{
		{caseName: "complete", finish: func(bucket, key, uploadID string, p1 CompletePart) { s.completeUpload(bucket, key, uploadID, p1) }},
		{caseName: "abort", finish: func(bucket, key, uploadID string, _ CompletePart) { s.abortUpload(bucket, key, uploadID) }},
	}
	for i, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			bucket, key := fmt.Sprintf("mp-life-%d", i), "file.bin"
			s.createBucket(bucket)

			uploadID := s.initiateUpload(bucket, key, nil)
			p1 := s.uploadPart(bucket, key, uploadID, 1, "hello")
			s.uploadPart(bucket, key, uploadID, 2, "unlisted")

			res, err := s.storage(bucket).List(ctx, s2.ListOptions{Recursive: true})
			s.Require().NoError(err)
			s.Empty(server.FilterKeep(res.Objects))

			tc.finish(bucket, key, uploadID, p1)

			exists, err := s.server.Multipart.Storage().Exists(ctx, uploadID)
			s.Require().NoError(err)
			s.False(exists)
		})
	}
}

// A part without a recorded ETag cannot contribute to the multipart ETag.
func (s *MultipartTestSuite) TestCompleteMultipartUploadRejectsPartWithoutETag() {
	s.createBucket("mp-noetag")
	uploadID := s.initiateUpload("mp-noetag", "file.bin", nil)
	s.Require().NoError(s.server.Multipart.PutPart(context.Background(), uploadID, 1, []byte("hello"), ""))

	w := s.complete("mp-noetag", "file.bin", uploadID, CompletePart{PartNumber: 1, ETag: ""})

	s.Equal(http.StatusBadRequest, w.Code)
	var errResp ErrorResponse
	s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
	s.Equal("InvalidPart", errResp.Code)
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
			for i, body := range tc.bodies {
				parts[i] = s2.NewObjectBytes("part", []byte(body))
				want += body
			}

			pr := &partsReader{parts: parts}
			got, err := io.ReadAll(pr)
			require.NoError(t, err)
			assert.Equal(t, want, string(got))
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
}

// listUploads runs ListMultipartUploads through the bucket GET dispatch.
func (s *MultipartTestSuite) listUploads(bucket, query string) (*httptest.ResponseRecorder, ListMultipartUploadsResult) {
	s.T().Helper()

	req := httptest.NewRequest("GET", "/"+bucket+"?uploads&"+query, nil)
	req.SetPathValue("bucket", bucket)
	w := httptest.NewRecorder()
	handleBucketGET(s.server, w, req)
	var result ListMultipartUploadsResult
	if w.Code == http.StatusOK {
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
	}
	return w, result
}

func (s *MultipartTestSuite) TestListMultipartUploads() {
	root := s.T().TempDir()
	cfg := server.DefaultConfig()
	cfg.Root = root
	cfg.MultipartMaxAge = int64(time.Hour.Seconds())
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
	s.createBucket("mp-list")
	s.createBucket("mp-other")
	backdate := func(id string, age time.Duration) {
		at := time.Now().Add(-age)
		s.Require().NoError(os.Chtimes(filepath.Join(root, ".multipart", id, "meta"), at, at))
	}
	// Same-key uploads list in ID order.
	a1, a2 := s.initiateUpload("mp-list", "a", nil), s.initiateUpload("mp-list", "a", nil)
	if a1 > a2 {
		a1, a2 = a2, a1
	}
	ids := map[string]string{
		"a1":  a1,
		"a2":  a2,
		"b":   s.initiateUpload("mp-list", "b", nil),
		"dx":  s.initiateUpload("mp-list", "dir/x", nil),
		"dy":  s.initiateUpload("mp-list", "dir/y", nil),
		"old": s.initiateUpload("mp-list", "expired", nil),
	}
	s.initiateUpload("mp-other", "a", nil)
	backdate(ids["old"], 2*time.Hour)

	testCases := []struct {
		caseName      string
		query         string
		wantUploads   []string
		wantPrefixes  []string
		wantTruncated bool
		wantNext      string
	}{
		{caseName: "everything in key then upload ID order", wantUploads: []string{"a1", "a2", "b", "dx", "dy"}},
		{caseName: "prefix", query: "prefix=dir/", wantUploads: []string{"dx", "dy"}},
		{caseName: "delimiter rolls keys into common prefixes", query: "delimiter=/", wantUploads: []string{"a1", "a2", "b"}, wantPrefixes: []string{"dir/"}},
		{caseName: "key-marker alone skips that key", query: "key-marker=a", wantUploads: []string{"b", "dx", "dy"}},
		{caseName: "upload-id-marker resumes within the key", query: "key-marker=a&upload-id-marker=" + ids["a1"], wantUploads: []string{"a2", "b", "dx", "dy"}},
		{caseName: "max-uploads truncates", query: "max-uploads=2", wantUploads: []string{"a1", "a2"}, wantTruncated: true, wantNext: "a2"},
		{caseName: "the next page follows the markers", query: "key-marker=a&max-uploads=2&upload-id-marker=" + ids["a2"], wantUploads: []string{"b", "dx"}, wantTruncated: true, wantNext: "dx"},
		{caseName: "a common prefix counts toward the page", query: "delimiter=/&max-uploads=4", wantUploads: []string{"a1", "a2", "b"}, wantPrefixes: []string{"dir/"}},
		{caseName: "a common prefix not past key-marker is dropped", query: "delimiter=/&key-marker=dir/"},
		{caseName: "max-uploads=0 is an empty untruncated page", query: "max-uploads=0"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			w, got := s.listUploads("mp-list", tc.query)
			s.Require().Equal(http.StatusOK, w.Code, w.Body.String())

			gotIDs := make([]string, 0, len(got.Uploads))
			for _, u := range got.Uploads {
				s.Equal(s2OwnerID, u.Owner.ID)
				s.Equal("STANDARD", u.StorageClass)
				s.False(u.Initiated.IsZero())
				gotIDs = append(gotIDs, u.UploadID)
			}
			wantIDs := make([]string, 0, len(tc.wantUploads))
			for _, name := range tc.wantUploads {
				wantIDs = append(wantIDs, ids[name])
			}
			s.Equal(wantIDs, gotIDs)
			gotPrefixes := make([]string, 0, len(got.CommonPrefixes))
			for _, p := range got.CommonPrefixes {
				gotPrefixes = append(gotPrefixes, p.Prefix)
			}
			s.Equal(append([]string{}, tc.wantPrefixes...), gotPrefixes)
			s.Equal(tc.wantTruncated, got.IsTruncated)
			s.Equal(ids[tc.wantNext], got.NextUploadIDMarker)
		})
	}

	s.Run("the next page survives aborting the marker upload", func() {
		_, first := s.listUploads("mp-list", "max-uploads=1")
		s.Require().True(first.IsTruncated)
		s.Require().Equal(ids["a1"], first.NextUploadIDMarker)
		s.abortUpload("mp-list", "a", ids["a1"])

		_, next := s.listUploads("mp-list", "key-marker="+first.NextKeyMarker+"&upload-id-marker="+first.NextUploadIDMarker)
		got := make([]string, 0, len(next.Uploads))
		for _, u := range next.Uploads {
			got = append(got, u.UploadID)
		}
		s.Equal([]string{ids["a2"], ids["b"], ids["dx"], ids["dy"]}, got)
	})
	s.Run("invalid max-uploads", func() {
		w, _ := s.listUploads("mp-list", "max-uploads=abc")
		s.Equal(http.StatusBadRequest, w.Code)
	})
	s.Run("missing bucket", func() {
		w, _ := s.listUploads("mp-nope", "")
		s.Equal(http.StatusNotFound, w.Code)
	})
	s.Run("a recreated bucket does not list its predecessor's uploads", func() {
		at := time.Now().Add(-time.Hour)
		s.Require().NoError(os.Chtimes(filepath.Join(root, "mp-other", ".keep"), at, at))
		s.Require().NoError(s.server.Buckets.Delete(context.Background(), "mp-other"))
		s.createBucket("mp-other")
		w, got := s.listUploads("mp-other", "")
		s.Require().Equal(http.StatusOK, w.Code)
		s.Empty(got.Uploads)
	})
}

// listParts runs ListParts through the object GET dispatch.
func (s *MultipartTestSuite) listParts(bucket, key, uploadID, query string) (*httptest.ResponseRecorder, ListPartsResult) {
	s.T().Helper()

	req := httptest.NewRequest("GET", "/"+bucket+"/"+key+"?uploadId="+url.QueryEscape(uploadID)+"&"+query, nil)
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()
	handleGetObject(s.server, w, req)
	var result ListPartsResult
	if w.Code == http.StatusOK {
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
	}
	return w, result
}

func (s *MultipartTestSuite) TestListParts() {
	s.createBucket("mp-parts")
	uploadID := s.initiateUpload("mp-parts", "file.bin", nil)
	etags := map[int]string{}
	for n, body := range map[int]string{1: "one", 2: "two!", 3: "three"} {
		etags[n] = s.uploadPart("mp-parts", "file.bin", uploadID, n, body).ETag
	}
	sizes := map[int]uint64{1: 3, 2: 4, 3: 5}

	testCases := []struct {
		caseName      string
		query         string
		wantParts     []int
		wantTruncated bool
		wantNext      int
	}{
		{caseName: "all parts in order", wantParts: []int{1, 2, 3}},
		{caseName: "part-number-marker", query: "part-number-marker=1", wantParts: []int{2, 3}},
		{caseName: "max-parts truncates", query: "max-parts=2", wantParts: []int{1, 2}, wantTruncated: true, wantNext: 2},
		{caseName: "the next page follows the marker", query: "max-parts=2&part-number-marker=2", wantParts: []int{3}},
		{caseName: "max-parts=0 is an empty untruncated page", query: "max-parts=0"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			w, got := s.listParts("mp-parts", "file.bin", uploadID, tc.query)
			s.Require().Equal(http.StatusOK, w.Code, w.Body.String())

			gotParts := make([]int, 0, len(got.Parts))
			for _, p := range got.Parts {
				s.Equal(etags[p.PartNumber], p.ETag)
				s.Equal(sizes[p.PartNumber], p.Size)
				s.False(p.LastModified.IsZero())
				gotParts = append(gotParts, p.PartNumber)
			}
			s.Equal(append([]int{}, tc.wantParts...), gotParts)
			s.Equal(tc.wantTruncated, got.IsTruncated)
			s.Equal(tc.wantNext, got.NextPartNumberMarker)
			s.Equal(uploadID, got.UploadID)
		})
	}

	errorCases := []struct {
		caseName   string
		key        string
		uploadID   string
		query      string
		wantStatus int
		wantCode   string
	}{
		{caseName: "unknown upload", key: "file.bin", uploadID: strings.Repeat("0", 32), wantStatus: http.StatusNotFound, wantCode: "NoSuchUpload"},
		{caseName: "another key", key: "other.bin", uploadID: uploadID, wantStatus: http.StatusNotFound, wantCode: "NoSuchUpload"},
		{caseName: "malformed upload id", key: "file.bin", uploadID: "../evil", wantStatus: http.StatusNotFound, wantCode: "NoSuchUpload"},
		{caseName: "invalid max-parts", key: "file.bin", uploadID: uploadID, query: "max-parts=abc", wantStatus: http.StatusBadRequest, wantCode: "InvalidArgument"},
		{caseName: "invalid part-number-marker", key: "file.bin", uploadID: uploadID, query: "part-number-marker=-1", wantStatus: http.StatusBadRequest, wantCode: "InvalidArgument"},
	}
	for _, tc := range errorCases {
		s.Run(tc.caseName, func() {
			w, _ := s.listParts("mp-parts", tc.key, tc.uploadID, tc.query)
			s.Equal(tc.wantStatus, w.Code, w.Body.String())
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal(tc.wantCode, errResp.Code)
		})
	}

	s.Run("head with an upload id still reads the object", func() {
		s.putObject("mp-parts", "plain.txt", "hello")
		req := httptest.NewRequest("HEAD", "/mp-parts/plain.txt?uploadId="+uploadID, nil)
		req.SetPathValue("bucket", "mp-parts")
		req.SetPathValue("key", "plain.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)
		s.Equal(http.StatusOK, w.Code)
	})
}

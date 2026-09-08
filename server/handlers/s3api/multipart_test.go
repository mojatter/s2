package s3api

import (
	"crypto/md5" // #nosec G501 -- MD5 is used here only to mirror S3 multipart ETag semantics under test.
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/mojatter/s2"
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

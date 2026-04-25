package s3api

import (
	"crypto/md5" // #nosec G501 -- MD5 is used here only to mirror S3 multipart ETag semantics under test.
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
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

package s3api

import (
	"bufio"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"
)

type UtilsTestSuite struct{ s3apiSuite }

func TestUtilsTestSuite(t *testing.T) {
	suite.Run(t, &UtilsTestSuite{})
}

func (s *UtilsTestSuite) TestS2ErrorToS3Error() {
	testCases := []struct {
		caseName    string
		err         error
		wantCode    string
		wantMessage string
		wantStatus  int
	}{
		{
			caseName:    "not exist",
			err:         fmt.Errorf("%w: key", s2.ErrNotExist),
			wantCode:    "NoSuchKey",
			wantMessage: msgNoSuchKey,
			wantStatus:  http.StatusNotFound,
		},
		{
			caseName:    "no such bucket",
			err:         &ErrNoSuchBucket{Name: "b"},
			wantCode:    "NoSuchBucket",
			wantMessage: msgNoSuchBucket,
			wantStatus:  http.StatusNotFound,
		},
		{
			caseName:    "wrapped not exist",
			err:         fmt.Errorf("wrap: %w", fmt.Errorf("%w: key", s2.ErrNotExist)),
			wantCode:    "NoSuchKey",
			wantMessage: msgNoSuchKey,
			wantStatus:  http.StatusNotFound,
		},
		{
			caseName:    "bucket not found",
			err:         &server.ErrBucketNotFound{Name: "b"},
			wantCode:    "NoSuchBucket",
			wantMessage: msgNoSuchBucket,
			wantStatus:  http.StatusNotFound,
		},
		{
			caseName:    "wrapped bucket not found",
			err:         fmt.Errorf("wrap: %w", &server.ErrBucketNotFound{Name: "b"}),
			wantCode:    "NoSuchBucket",
			wantMessage: msgNoSuchBucket,
			wantStatus:  http.StatusNotFound,
		},
		{
			caseName:    "invalid name",
			err:         fmt.Errorf("%w: ../other", s2.ErrInvalidName),
			wantCode:    "InvalidArgument",
			wantMessage: "s2: invalid name: ../other",
			wantStatus:  http.StatusBadRequest,
		},
		{
			caseName:    "reserved bucket name",
			err:         fmt.Errorf("%w: \"healthz\" is served by the health endpoint", server.ErrReservedBucketName),
			wantCode:    "InvalidBucketName",
			wantMessage: `bucket name is reserved: "healthz" is served by the health endpoint`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			caseName:    "no such upload",
			err:         fmt.Errorf("%w: abc", server.ErrNoSuchUpload),
			wantCode:    "NoSuchUpload",
			wantMessage: msgNoSuchUpload,
			wantStatus:  http.StatusNotFound,
		},
		{
			caseName:    "modified since Get",
			err:         fmt.Errorf("wrap: %w", fmt.Errorf("%w: /tmp/root/key", s2.ErrModified)),
			wantCode:    "InternalError",
			wantMessage: "The object was modified while it was being read. Please try again.",
			wantStatus:  http.StatusInternalServerError,
		},
		{
			caseName:    "unknown error",
			err:         fmt.Errorf("something broke"),
			wantCode:    "InternalError",
			wantMessage: msgInternalError,
			wantStatus:  http.StatusInternalServerError,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			code, message, status := s2ErrorToS3Error(tc.err)
			s.Equal(tc.wantCode, code)
			s.Equal(tc.wantStatus, status)
			s.Equal(tc.wantMessage, message)
		})
	}
}

func (s *UtilsTestSuite) TestParseMetadataHeaders() {
	testCases := []struct {
		caseName string
		headers  map[string]string
		want     s2.Metadata
	}{
		{
			caseName: "typical",
			headers:  map[string]string{"X-Amz-Meta-Key": "val"},
			want:     s2.Metadata{"key": "val"},
		},
		{
			caseName: "multiple",
			headers: map[string]string{
				"X-Amz-Meta-A": "1",
				"X-Amz-Meta-B": "2",
			},
			want: s2.Metadata{"a": "1", "b": "2"},
		},
		{
			caseName: "non-meta headers ignored",
			headers: map[string]string{
				"Content-Type":  "text/plain",
				"X-Amz-Meta-Ok": "yes",
			},
			want: s2.Metadata{"ok": "yes"},
		},
		{
			caseName: "empty",
			headers:  map[string]string{},
			want:     s2.Metadata{},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req := httptest.NewRequest("PUT", "/", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			got := parseMetadataHeaders(req)
			s.Equal(tc.want, got)
		})
	}
}

func (s *UtilsTestSuite) TestUploadErrorToS3Error() {
	testCases := []struct {
		caseName    string
		err         error
		wantCode    string
		wantMessage string
		wantStatus  int
	}{
		{caseName: "incomplete request body", err: fmt.Errorf("failed to write temp file: %w", errIncompleteBody), wantCode: "IncompleteBody", wantMessage: msgIncompleteBody, wantStatus: http.StatusBadRequest},
		// A storage read cut short, as in CopyObject or CompleteMultipartUpload, is the server's and stays retryable.
		{caseName: "storage read cut short", err: fmt.Errorf("failed to write temp file: %w", io.ErrUnexpectedEOF), wantCode: "InternalError", wantMessage: msgInternalError, wantStatus: http.StatusInternalServerError},
		{caseName: "etag unknown", err: fmt.Errorf("%w: key", s2.ErrUnknownETag), wantCode: "InternalError", wantMessage: msgInternalError, wantStatus: http.StatusInternalServerError},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			code, message, status := uploadErrorToS3Error(tc.err)
			s.Equal(tc.wantCode, code)
			s.Equal(tc.wantMessage, message)
			s.Equal(tc.wantStatus, status)
		})
	}
}

func (s *UtilsTestSuite) TestAWSChunkedReader() {
	testCases := []struct {
		caseName string
		body     string
		declared int64
		bufSize  int    // if non-zero, the bufio buffer size instead of the default
		readErr  error  // if set, the source fails with it after body, and the read must pass it through
		want     string // the decoded body, when the read must succeed
		wantErr  bool   // if set, the read must fail with errIncompleteBody
	}{
		{caseName: "two chunks", body: "3;chunk-signature=x\r\nabc\r\n2;chunk-signature=x\r\nde\r\n0;chunk-signature=x\r\n\r\n", declared: 5, want: "abcde"},
		{caseName: "negative size", body: "-1;chunk-signature=x\r\nabc\r\n0;chunk-signature=x\r\n\r\n", declared: 3, wantErr: true},
		{caseName: "non-hex size", body: "zz;chunk-signature=x\r\n", declared: 3, wantErr: true},
		{caseName: "size past what was declared", body: "7fffffffffffffff;chunk-signature=x\r\nabc\r\n", declared: 3, wantErr: true},
		{caseName: "cut off mid-chunk", body: "a;chunk-signature=x\r\nabc", declared: 10, wantErr: true},
		{caseName: "no final chunk", body: "3;chunk-signature=x\r\nabc\r\n", declared: 3, wantErr: true},
		{caseName: "less than declared", body: "3;chunk-signature=x\r\nabc\r\n0;chunk-signature=x\r\n\r\n", declared: 5, wantErr: true},
		{caseName: "more than declared", body: "3;chunk-signature=x\r\nabc\r\n2;chunk-signature=x\r\nde\r\n0;chunk-signature=x\r\n\r\n", declared: 3, wantErr: true},
		{caseName: "chunk data not followed by CRLF", body: "3;chunk-signature=x\r\nabcX0;chunk-signature=x\r\n\r\n", declared: 3, wantErr: true},
		{caseName: "chunk data followed by one byte", body: "3;chunk-signature=x\r\nabc\r", declared: 3, wantErr: true},
		{caseName: "chunk header longer than the buffer", body: strings.Repeat("a", 65) + ";chunk-signature=x\r\n", declared: 3, bufSize: 64, wantErr: true},
		{caseName: "a read error is not an incomplete body", body: "3;chunk-signature=x\r\nabc", declared: 3, readErr: errors.New("connection reset")},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			var src io.Reader = strings.NewReader(tc.body)
			if tc.readErr != nil {
				src = io.MultiReader(src, iotest.ErrReader(tc.readErr))
			}
			br := bufio.NewReader(src)
			if tc.bufSize != 0 {
				br = bufio.NewReaderSize(src, tc.bufSize)
			}
			got, err := io.ReadAll(&awsChunkedReader{br: br, declared: tc.declared})
			if tc.readErr != nil {
				s.ErrorIs(err, tc.readErr)
				s.NotErrorIs(err, errIncompleteBody)
				return
			}
			if tc.wantErr {
				s.ErrorIs(err, errIncompleteBody)
				return
			}
			s.Require().NoError(err)
			s.Equal(tc.want, string(got))
		})
	}
}

func (s *UtilsTestSuite) TestWriteS2Error() {
	testCases := []struct {
		caseName    string
		write       func(w http.ResponseWriter, r *http.Request, err error)
		err         error
		wantStatus  int
		wantMessage string
		wantLog     []string
	}{
		{
			caseName:    "server error logs its detail at error",
			write:       writeS2Error,
			err:         errors.New("open /root/b/k: permission denied"),
			wantStatus:  http.StatusInternalServerError,
			wantMessage: msgInternalError,
			wantLog:     []string{"level=ERROR", "method=GET", "path=/b/k", "code=InternalError", "status=500", `error="open /root/b/k: permission denied"`},
		},
		{
			caseName:    "client error logs its detail at debug",
			write:       writeS2Error,
			err:         fmt.Errorf("%w: k", s2.ErrNotExist),
			wantStatus:  http.StatusNotFound,
			wantMessage: msgNoSuchKey,
			wantLog:     []string{"level=DEBUG", "path=/b/k", "code=NoSuchKey", "status=404", `error="s2: object not exist: k"`},
		},
		{
			caseName:    "client gone logs at debug",
			write:       writeS2Error,
			err:         context.Canceled,
			wantStatus:  http.StatusInternalServerError,
			wantMessage: msgInternalError,
			wantLog:     []string{"level=DEBUG", "code=InternalError", "status=500", `error="context canceled"`},
		},
		{
			caseName:    "client gone during read-back",
			write:       writeUploadError,
			err:         fmt.Errorf("%w: %w", s2.ErrUnknownETag, context.Canceled),
			wantStatus:  http.StatusInternalServerError,
			wantMessage: msgInternalError,
			wantLog:     []string{"level=DEBUG", "code=InternalError", "status=500"},
		},
		{
			caseName:    "upload error",
			write:       writeUploadError,
			err:         fmt.Errorf("failed to write temp file: %w: 3 of 10 bytes", errIncompleteBody),
			wantStatus:  http.StatusBadRequest,
			wantMessage: msgIncompleteBody,
			wantLog:     []string{"level=DEBUG", "code=IncompleteBody", "status=400", "3 of 10 bytes"},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			buf := captureSlog(s.T())
			w := httptest.NewRecorder()
			tc.write(w, httptest.NewRequest(http.MethodGet, "/b/k", nil), tc.err)
			s.Equal(tc.wantStatus, w.Code)
			var resp server.S3ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &resp))
			s.Equal(tc.wantMessage, resp.Message)
			for _, want := range tc.wantLog {
				s.Contains(buf.String(), want)
			}
		})
	}
}

func (s *UtilsTestSuite) TestLogS3Error() {
	testCases := []struct {
		caseName string
		attrs    []any
		wantLog  string
		denyLog  string
	}{
		{caseName: "no attrs", denyLog: "key="},
		{caseName: "key attr", attrs: []any{"key", "a/b"}, wantLog: "key=a/b"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			buf := captureSlog(s.T())
			logS3Error(httptest.NewRequest(http.MethodPost, "/b?delete", nil), "NoSuchKey", http.StatusNotFound, s2.ErrNotExist, tc.attrs...)
			s.Contains(buf.String(), "code=NoSuchKey")
			if tc.wantLog != "" {
				s.Contains(buf.String(), tc.wantLog)
			}
			if tc.denyLog != "" {
				s.NotContains(buf.String(), tc.denyLog)
			}
		})
	}
}

func (s *UtilsTestSuite) TestLogS3ErrorRequestID() {
	s.putObject("b", "k", "v")
	buf := captureSlog(s.T())
	w := httptest.NewRecorder()
	s.server.S3Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/b/missing", nil))
	s.Equal(http.StatusNotFound, w.Code)
	id := w.Header().Get("x-amz-request-id")
	s.Len(id, 26)
	s.Contains(w.Body.String(), "<RequestId>"+id+"</RequestId>")
	s.Contains(buf.String(), `msg="S3 API error" request_id=`+id+" ")
}

func (s *UtilsTestSuite) TestRequestLogOperation() {
	testCases := []struct {
		caseName string
		method   string
		target   string
		header   map[string]string
		body     string
		wantLog  string
	}{
		{caseName: "ListBuckets", method: http.MethodGet, target: "/", wantLog: "operation=ListBuckets status="},
		{caseName: "CreateBucket", method: http.MethodPut, target: "/nb", wantLog: "operation=CreateBucket status="},
		{caseName: "HeadBucket", method: http.MethodHead, target: "/b", wantLog: "operation=HeadBucket status="},
		{caseName: "GetBucketLocation", method: http.MethodGet, target: "/b?location", wantLog: "operation=GetBucketLocation status="},
		{caseName: "ListObjectsV2", method: http.MethodGet, target: "/b?list-type=2", wantLog: "operation=ListObjectsV2 status="},
		{caseName: "ListMultipartUploads", method: http.MethodGet, target: "/b?uploads", wantLog: "operation=ListMultipartUploads status="},
		{caseName: "DeleteObjects", method: http.MethodPost, target: "/b?delete", body: "<Delete></Delete>", wantLog: "operation=DeleteObjects status="},
		{caseName: "GetObject", method: http.MethodGet, target: "/b/k", wantLog: "operation=GetObject status=200"},
		{caseName: "HeadObject", method: http.MethodHead, target: "/b/k", wantLog: "operation=HeadObject status=200"},
		{caseName: "PutObject", method: http.MethodPut, target: "/b/k2", body: "v", wantLog: "operation=PutObject status=200"},
		{caseName: "CopyObject", method: http.MethodPut, target: "/b/k3", header: map[string]string{"x-amz-copy-source": "/b/k"}, wantLog: "operation=CopyObject copy_source=/b/k status=200"},
		{caseName: "DeleteObject", method: http.MethodDelete, target: "/b/k4", wantLog: "operation=DeleteObject status="},
		{caseName: "CreateMultipartUpload", method: http.MethodPost, target: "/b/k?uploads", wantLog: "operation=CreateMultipartUpload status=200"},
		{caseName: "UploadPart", method: http.MethodPut, target: "/b/k?partNumber=3&uploadId=u1", body: "v", wantLog: "operation=UploadPart upload_id=u1 part_number=3 status="},
		{caseName: "CompleteMultipartUpload", method: http.MethodPost, target: "/b/k?uploadId=u1", wantLog: "operation=CompleteMultipartUpload upload_id=u1 status="},
		{caseName: "AbortMultipartUpload", method: http.MethodDelete, target: "/b/k?uploadId=u1", wantLog: "operation=AbortMultipartUpload upload_id=u1 status="},
		{caseName: "ListParts", method: http.MethodGet, target: "/b/k?uploadId=u1", wantLog: "operation=ListParts upload_id=u1 status="},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			s.putObject("b", "k", "v")
			buf := captureSlog(s.T())
			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			s.server.S3Handler().ServeHTTP(httptest.NewRecorder(), req)
			s.Contains(buf.String(), tc.wantLog)
			s.Equal(1, strings.Count(buf.String(), "operation="), "one operation per request line")
		})
	}
}

func (s *UtilsTestSuite) TestRequestLogOperationRejectedBySigV4() {
	cfg := server.DefaultConfig()
	cfg.Root = s.T().TempDir()
	cfg.User, cfg.Password = "AKID", "secret"
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	buf := captureSlog(s.T())
	w := httptest.NewRecorder()
	srv.S3Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/b/k", nil))
	s.Equal(http.StatusForbidden, w.Code)
	s.Contains(buf.String(), "S3 API request")
	s.NotContains(buf.String(), "operation=")
}

// captureSlog sends slog to a Debug-level buffer until t ends; SetDefault alone does not restore the log package.
func captureSlog(t *testing.T) *strings.Builder {
	buf := &strings.Builder{}
	prev, prevW, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevW)
		log.SetFlags(prevFlags)
	})
	return buf
}

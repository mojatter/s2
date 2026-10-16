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

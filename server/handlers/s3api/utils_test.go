package s3api

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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
		caseName   string
		err        error
		wantCode   string
		wantStatus int
	}{
		{
			caseName:   "not exist",
			err:        fmt.Errorf("%w: key", s2.ErrNotExist),
			wantCode:   "NoSuchKey",
			wantStatus: http.StatusNotFound,
		},
		{
			caseName:   "no such bucket",
			err:        &ErrNoSuchBucket{Name: "b"},
			wantCode:   "NoSuchBucket",
			wantStatus: http.StatusNotFound,
		},
		{
			caseName:   "wrapped not exist",
			err:        fmt.Errorf("wrap: %w", fmt.Errorf("%w: key", s2.ErrNotExist)),
			wantCode:   "NoSuchKey",
			wantStatus: http.StatusNotFound,
		},
		{
			caseName:   "bucket not found",
			err:        &server.ErrBucketNotFound{Name: "b"},
			wantCode:   "NoSuchBucket",
			wantStatus: http.StatusNotFound,
		},
		{
			caseName:   "wrapped bucket not found",
			err:        fmt.Errorf("wrap: %w", &server.ErrBucketNotFound{Name: "b"}),
			wantCode:   "NoSuchBucket",
			wantStatus: http.StatusNotFound,
		},
		{
			caseName:   "invalid name",
			err:        fmt.Errorf("%w: ../other", s2.ErrInvalidName),
			wantCode:   "InvalidArgument",
			wantStatus: http.StatusBadRequest,
		},
		{
			caseName:   "unknown error",
			err:        fmt.Errorf("something broke"),
			wantCode:   "InternalError",
			wantStatus: http.StatusInternalServerError,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			code, _, status := s2ErrorToS3Error(tc.err)
			s.Equal(tc.wantCode, code)
			s.Equal(tc.wantStatus, status)
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

func (s *UtilsTestSuite) TestAWSChunkedReader() {
	testCases := []struct {
		caseName string
		body     string
		declared int64
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
		{caseName: "chunk header longer than the buffer", body: strings.Repeat("a", 4097) + ";chunk-signature=x\r\n", declared: 3, wantErr: true},
		{caseName: "a read error is not an incomplete body", body: "3;chunk-signature=x\r\nabc", declared: 3, readErr: errors.New("connection reset")},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			var src io.Reader = strings.NewReader(tc.body)
			if tc.readErr != nil {
				src = io.MultiReader(src, iotest.ErrReader(tc.readErr))
			}
			got, err := io.ReadAll(&awsChunkedReader{br: bufio.NewReader(src), declared: tc.declared})
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

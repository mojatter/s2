package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

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

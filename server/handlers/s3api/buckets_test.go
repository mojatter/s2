package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"
)

type BucketsTestSuite struct{ s3apiSuite }

func TestBucketsTestSuite(t *testing.T) {
	suite.Run(t, &BucketsTestSuite{})
}

// --- ListBuckets ---

func (s *BucketsTestSuite) TestListBuckets() {
	s.Run("empty", func() {
		req := httptest.NewRequest("GET", "/s3api", nil)
		w := httptest.NewRecorder()
		HandleListBuckets(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListAllMyBucketsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Buckets)
		s.Equal(s2OwnerID, result.Owner.ID)
	})

	s.Run("with buckets", func() {
		s.createBucket("alpha")
		s.createBucket("beta")

		req := httptest.NewRequest("GET", "/s3api", nil)
		w := httptest.NewRecorder()
		HandleListBuckets(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListAllMyBucketsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Buckets, 2)

		names := []string{result.Buckets[0].Name, result.Buckets[1].Name}
		s.Contains(names, "alpha")
		s.Contains(names, "beta")

		for _, b := range result.Buckets {
			s.False(b.CreationDate.IsZero(), "CreationDate should not be zero")
			s.True(b.CreationDate.Year() >= 2025, "CreationDate should be a recent timestamp")
		}
	})
}

// --- CreateBucket ---

func (s *BucketsTestSuite) TestCreateBucket() {
	req := httptest.NewRequest("PUT", "/s3api/new-bucket", nil)
	req.SetPathValue("bucket", "new-bucket")
	w := httptest.NewRecorder()
	handleCreateBucket(s.server, w, req)

	s.Equal(http.StatusOK, w.Code)

	exists, err := s.server.Buckets.Exists("new-bucket")
	s.Require().NoError(err)
	s.True(exists)
}

// --- DeleteBucket ---

func (s *BucketsTestSuite) TestDeleteBucket() {
	s.Run("existing", func() {
		s.createBucket("to-delete")

		req := httptest.NewRequest("DELETE", "/s3api/to-delete", nil)
		req.SetPathValue("bucket", "to-delete")
		w := httptest.NewRecorder()
		handleDeleteBucket(s.server, w, req)

		s.Equal(http.StatusNoContent, w.Code)

		exists, err := s.server.Buckets.Exists("to-delete")
		s.Require().NoError(err)
		s.False(exists)
	})

	s.Run("not found", func() {
		req := httptest.NewRequest("DELETE", "/s3api/nonexistent", nil)
		req.SetPathValue("bucket", "nonexistent")
		w := httptest.NewRecorder()
		handleDeleteBucket(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("NoSuchBucket", errResp.Code)
	})
}

// --- GetBucketLocation ---

func (s *BucketsTestSuite) TestGetBucketLocation() {
	testCases := []struct {
		caseName     string
		bucket       string
		createBucket bool
		handler      server.HandlerFunc
		wantStatus   int
		wantLocation string
		wantErrCode  string
	}{
		{
			caseName:     "existing bucket",
			bucket:       "loc",
			createBucket: true,
			handler:      handleGetBucketLocation,
			wantStatus:   http.StatusOK,
			wantLocation: s2Region,
		},
		{
			caseName:    "nonexistent bucket",
			bucket:      "no-such",
			handler:     handleGetBucketLocation,
			wantStatus:  http.StatusNotFound,
			wantErrCode: "NoSuchBucket",
		},
		{
			caseName:     "dispatched via handleBucketGET",
			bucket:       "disp",
			createBucket: true,
			handler:      handleBucketGET,
			wantStatus:   http.StatusOK,
			wantLocation: s2Region,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			if tc.createBucket {
				s.createBucket(tc.bucket)
			}
			req := httptest.NewRequest("GET", "/s3api/"+tc.bucket+"?location", nil)
			req.SetPathValue("bucket", tc.bucket)
			w := httptest.NewRecorder()
			tc.handler(s.server, w, req)

			s.Equal(tc.wantStatus, w.Code)
			if tc.wantLocation != "" {
				var result LocationConstraint
				s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
				s.Equal(tc.wantLocation, result.Location)
			}
			if tc.wantErrCode != "" {
				var errResp ErrorResponse
				s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
				s.Equal(tc.wantErrCode, errResp.Code)
			}
		})
	}
}

// --- HeadBucket ---

func (s *BucketsTestSuite) TestHeadBucket() {
	testCases := []struct {
		caseName     string
		bucket       string
		createBucket bool
		wantStatus   int
	}{
		{
			caseName:     "existing",
			bucket:       "exists",
			createBucket: true,
			wantStatus:   http.StatusOK,
		},
		{
			caseName:   "not found",
			bucket:     "nope",
			wantStatus: http.StatusNotFound,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			if tc.createBucket {
				s.createBucket(tc.bucket)
			}
			req := httptest.NewRequest("HEAD", "/s3api/"+tc.bucket, nil)
			req.SetPathValue("bucket", tc.bucket)
			w := httptest.NewRecorder()
			handleHeadBucket(s.server, w, req)

			s.Equal(tc.wantStatus, w.Code)
		})
	}
}

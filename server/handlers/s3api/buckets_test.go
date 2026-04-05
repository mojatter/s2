package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

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

		// CreationDate should be a real timestamp, not a hardcoded mock
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
	s.Run("existing bucket", func() {
		s.createBucket("loc")

		req := httptest.NewRequest("GET", "/s3api/loc?location", nil)
		req.SetPathValue("bucket", "loc")
		w := httptest.NewRecorder()
		handleGetBucketLocation(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result LocationConstraint
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Equal(s2Region, result.Location)
	})

	s.Run("nonexistent bucket", func() {
		req := httptest.NewRequest("GET", "/s3api/no-such?location", nil)
		req.SetPathValue("bucket", "no-such")
		w := httptest.NewRecorder()
		handleGetBucketLocation(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("NoSuchBucket", errResp.Code)
	})

	s.Run("dispatched via handleBucketGET", func() {
		s.createBucket("disp")

		req := httptest.NewRequest("GET", "/s3api/disp?location", nil)
		req.SetPathValue("bucket", "disp")
		w := httptest.NewRecorder()
		handleBucketGET(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result LocationConstraint
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Equal("us-east-1", result.Location)
	})
}

// --- HeadBucket ---

func (s *BucketsTestSuite) TestHeadBucket() {
	s.Run("existing", func() {
		s.createBucket("exists")

		req := httptest.NewRequest("HEAD", "/s3api/exists", nil)
		req.SetPathValue("bucket", "exists")
		w := httptest.NewRecorder()
		handleHeadBucket(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
	})

	s.Run("not found", func() {
		req := httptest.NewRequest("HEAD", "/s3api/nope", nil)
		req.SetPathValue("bucket", "nope")
		w := httptest.NewRecorder()
		handleHeadBucket(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})
}

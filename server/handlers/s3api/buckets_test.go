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
		req := httptest.NewRequest("GET", "/", nil)
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

		req := httptest.NewRequest("GET", "/", nil)
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

	s.Run("filtered by policy", func() {
		s.createBucket("visible")
		s.createBucket("denied-bucket")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{"arn:aws:s3:::visible"}},
		}}}

		req := httptest.NewRequest("GET", "/", nil)
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		HandleListBuckets(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListAllMyBucketsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Buckets, 1)
		s.Equal("visible", result.Buckets[0].Name)
	})

	s.Run("explicit deny on s3:ListAllMyBuckets blocks the endpoint entirely", func() {
		s.createBucket("visible2")

		user := &server.User{Policy: &server.Policy{Statement: []server.Statement{
			{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{"arn:aws:s3:::visible2"}},
			{Effect: "Deny", Action: []string{"s3:ListAllMyBuckets"}, Resource: []string{"arn:aws:s3:::*"}},
		}}}

		req := httptest.NewRequest("GET", "/", nil)
		req = req.WithContext(server.WithUser(req.Context(), user))
		w := httptest.NewRecorder()
		HandleListBuckets(s.server, w, req)

		s.Equal(http.StatusForbidden, w.Code)
		s.Contains(w.Body.String(), "AccessDenied")
	})

	// Once s3:ListBucket became grantable to the anonymous ("*") principal
	// (see server.AnonymousAccessKeyID), FilterBucketNames -- shared by every
	// principal -- started including anonymously-listable buckets in
	// unauthenticated GET / too. These two cases pin that behavior down
	// explicitly for the anonymous principal, plus the Deny s3:ListAllMyBuckets
	// escape hatch documented in docs/users-policy.md's "Allowing anonymous
	// directory listing" section.
	s.Run("anonymous principal's ListBucket grant is disclosed via GET /", func() {
		s.createBucket("anon-visible")

		anon := &server.User{
			AccessKeyID: server.AnonymousAccessKeyID,
			Policy: &server.Policy{Statement: []server.Statement{
				{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{"arn:aws:s3:::anon-visible"}},
			}},
		}

		req := httptest.NewRequest("GET", "/", nil)
		req = req.WithContext(server.WithUser(req.Context(), anon))
		w := httptest.NewRecorder()
		HandleListBuckets(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListAllMyBucketsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Buckets, 1)
		s.Equal("anon-visible", result.Buckets[0].Name)
	})

	s.Run("Deny s3:ListAllMyBuckets suppresses that disclosure for the anonymous principal", func() {
		s.createBucket("anon-visible2")

		anon := &server.User{
			AccessKeyID: server.AnonymousAccessKeyID,
			Policy: &server.Policy{Statement: []server.Statement{
				{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{"arn:aws:s3:::anon-visible2"}},
				{Effect: "Deny", Action: []string{"s3:ListAllMyBuckets"}, Resource: []string{"arn:aws:s3:::*"}},
			}},
		}

		req := httptest.NewRequest("GET", "/", nil)
		req = req.WithContext(server.WithUser(req.Context(), anon))
		w := httptest.NewRecorder()
		HandleListBuckets(s.server, w, req)

		s.Equal(http.StatusForbidden, w.Code)
		s.Contains(w.Body.String(), "AccessDenied")
	})
}

// --- CreateBucket ---

func (s *BucketsTestSuite) TestCreateBucket() {
	testCases := []struct {
		caseName    string
		bucket      string
		wantStatus  int
		wantErrCode string
	}{
		{
			caseName:   "success",
			bucket:     "new-bucket",
			wantStatus: http.StatusOK,
		},
		{
			// DefaultConfig.HealthPath = "/healthz" reserves the bucket name "healthz".
			// Buckets.Create returns ErrReservedBucketName, which s2ErrorToS3Error
			// maps to InvalidBucketName + 400.
			caseName:    "reserved name",
			bucket:      "healthz",
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "InvalidBucketName",
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req := httptest.NewRequest("PUT", "/"+tc.bucket, nil)
			req.SetPathValue("bucket", tc.bucket)
			w := httptest.NewRecorder()
			handleCreateBucket(s.server, w, req)

			s.Equal(tc.wantStatus, w.Code)
			if tc.wantErrCode == "" {
				exists, err := s.server.Buckets.Exists(req.Context(), tc.bucket)
				s.Require().NoError(err)
				s.True(exists)
				return
			}
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal(tc.wantErrCode, errResp.Code)
		})
	}
}

// --- DeleteBucket ---

func (s *BucketsTestSuite) TestDeleteBucket() {
	s.Run("existing", func() {
		s.createBucket("to-delete")

		req := httptest.NewRequest("DELETE", "/to-delete", nil)
		req.SetPathValue("bucket", "to-delete")
		w := httptest.NewRecorder()
		handleDeleteBucket(s.server, w, req)

		s.Equal(http.StatusNoContent, w.Code)

		exists, err := s.server.Buckets.Exists(req.Context(), "to-delete")
		s.Require().NoError(err)
		s.False(exists)
	})

	s.Run("not found", func() {
		req := httptest.NewRequest("DELETE", "/nonexistent", nil)
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
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?location", nil)
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
			req := httptest.NewRequest("HEAD", "/"+tc.bucket, nil)
			req.SetPathValue("bucket", tc.bucket)
			w := httptest.NewRecorder()
			handleHeadBucket(s.server, w, req)

			s.Equal(tc.wantStatus, w.Code)
		})
	}
}

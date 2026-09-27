package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestTrailingSlashBucket goes through the router, which sends "PUT /b/" and "DELETE /b/" to the object handlers.
func (s *BucketsTestSuite) TestTrailingSlashBucket() {
	resp := s.roundTrip(s.server, http.MethodPut, "/slash-bucket/")
	s.Equal(http.StatusOK, resp.StatusCode)
	exists, err := s.server.Buckets.Exists(context.Background(), "slash-bucket")
	s.Require().NoError(err)
	s.True(exists)

	resp = s.roundTrip(s.server, http.MethodDelete, "/slash-bucket/")
	s.Equal(http.StatusNoContent, resp.StatusCode)
	exists, err = s.server.Buckets.Exists(context.Background(), "slash-bucket")
	s.Require().NoError(err)
	s.False(exists)
}

// TestBucketSubresource checks that a bucket request naming a subresource neither creates nor deletes the bucket.
func (s *BucketsTestSuite) TestBucketSubresource() {
	testCases := []struct {
		caseName string
		method   string
		// target's bucket holds the object "k" first when seed is true.
		target     string
		seed       bool
		copySource string
		wantStatus int
		wantExists bool
	}{
		{caseName: "DeleteBucketTagging", method: http.MethodDelete, target: "/del-tag?tagging", seed: true, wantStatus: http.StatusNotImplemented, wantExists: true},
		{caseName: "DeleteBucketTagging on an empty bucket", method: http.MethodDelete, target: "/del-tag-empty?tagging", wantStatus: http.StatusNotImplemented, wantExists: true},
		{caseName: "DeleteBucketTagging with a trailing slash", method: http.MethodDelete, target: "/del-tag-slash/?tagging", seed: true, wantStatus: http.StatusNotImplemented, wantExists: true},
		{caseName: "DeleteBucketTagging as aws-sdk-go-v2 sends it", method: http.MethodDelete, target: "/del-tag-sdk/?tagging&x-id=DeleteBucketTagging", seed: true, wantStatus: http.StatusNotImplemented, wantExists: true},
		{caseName: "abort with an empty key", method: http.MethodDelete, target: "/del-upload/?uploadId=x", seed: true, wantStatus: http.StatusNotImplemented, wantExists: true},
		{caseName: "PutBucketVersioning", method: http.MethodPut, target: "/put-ver?versioning", seed: true, wantStatus: http.StatusNotImplemented, wantExists: true},
		{caseName: "PutBucketTagging on a missing bucket", method: http.MethodPut, target: "/put-tag-missing/?tagging", wantStatus: http.StatusNotImplemented, wantExists: false},
		{caseName: "copy with no destination key", method: http.MethodPut, target: "/put-copy/", copySource: "/src/k", wantStatus: http.StatusNotImplemented, wantExists: false},
		{caseName: "CreateBucket with the SDK operation hint", method: http.MethodPut, target: "/put-xid/?x-id=CreateBucket", wantStatus: http.StatusOK, wantExists: true},
		{caseName: "DeleteBucket with the SDK operation hint", method: http.MethodDelete, target: "/del-xid/?x-id=DeleteBucket", wantStatus: http.StatusNoContent, wantExists: false},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			bucket := strings.Split(strings.TrimPrefix(tc.target, "/"), "/")[0]
			bucket, _, _ = strings.Cut(bucket, "?")
			if tc.seed {
				s.putObject(bucket, "k", "body")
			} else if tc.method == http.MethodDelete {
				s.createBucket(bucket)
			}
			req := httptest.NewRequest(tc.method, tc.target, nil)
			if tc.copySource != "" {
				req.Header.Set("x-amz-copy-source", tc.copySource)
			}
			w := httptest.NewRecorder()
			s.server.S3Handler().ServeHTTP(w, req)

			s.Equal(tc.wantStatus, w.Code)
			if tc.wantStatus == http.StatusNotImplemented {
				var errResp ErrorResponse
				s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
				s.Equal("NotImplemented", errResp.Code)
			}
			exists, err := s.server.Buckets.Exists(ctx, bucket)
			s.Require().NoError(err)
			s.Equal(tc.wantExists, exists)
			if tc.seed {
				strg, err := s.server.Buckets.Get(ctx, bucket)
				s.Require().NoError(err)
				ok, err := strg.Exists(ctx, "k")
				s.Require().NoError(err)
				s.True(ok, "the object survives")
			}
		})
	}
}

// TestDeleteBucketNotEmpty checks that DeleteBucket refuses a bucket holding an object an S3 client can see.
func (s *BucketsTestSuite) TestDeleteBucketNotEmpty() {
	testCases := []struct {
		caseName string
		bucket   string
		// keys go into the bucket first; one ending in "/" is a console folder.
		keys          []string
		dropMarker    bool
		trailingSlash bool
		wantStatus    int
	}{
		{caseName: "empty", bucket: "nb-empty", wantStatus: http.StatusNoContent},
		{caseName: "one object", bucket: "nb-one", keys: []string{"k"}, wantStatus: http.StatusConflict},
		{caseName: "nested object", bucket: "nb-nested", keys: []string{"dir/sub/k"}, wantStatus: http.StatusConflict},
		{caseName: "console folders only", bucket: "nb-folders", keys: []string{"dir/", "dir/sub/"}, wantStatus: http.StatusNoContent},
		{caseName: "a .keep written under a folder key", bucket: "nb-userkeep", keys: []string{"docs/.keep"}, wantStatus: http.StatusNoContent},
		{caseName: "object without the bucket marker", bucket: "nb-nomarker", keys: []string{"k"}, dropMarker: true, wantStatus: http.StatusConflict},
		{caseName: "trailing slash", bucket: "nb-slash", keys: []string{"k"}, trailingSlash: true, wantStatus: http.StatusConflict},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			ctx := context.Background()
			s.createBucket(tc.bucket)
			strg, err := s.server.Buckets.Get(ctx, tc.bucket)
			s.Require().NoError(err)
			for _, key := range tc.keys {
				if folder, ok := strings.CutSuffix(key, "/"); ok {
					s.Require().NoError(s.server.Buckets.CreateFolder(ctx, tc.bucket, folder))
				} else {
					s.putObject(tc.bucket, key, "body")
				}
			}
			if tc.dropMarker {
				s.Require().NoError(strg.Delete(ctx, ".keep"))
			}
			target := "/" + tc.bucket
			if tc.trailingSlash {
				target += "/"
			}
			w := httptest.NewRecorder()
			s.server.S3Handler().ServeHTTP(w, httptest.NewRequest(http.MethodDelete, target, nil))

			s.Equal(tc.wantStatus, w.Code)
			exists, err := s.server.Buckets.Exists(ctx, tc.bucket)
			s.Require().NoError(err)
			if tc.wantStatus == http.StatusNoContent {
				s.False(exists)
				return
			}
			var errResp ErrorResponse
			s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
			s.Equal("BucketNotEmpty", errResp.Code)
			s.True(exists)
			for _, key := range tc.keys {
				ok, err := strg.Exists(ctx, key)
				s.Require().NoError(err)
				s.True(ok, "%s survives", key)
			}
		})
	}
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

func TestNamesBucketSubresource(t *testing.T) {
	testCases := []struct {
		caseName string
		target   string
		want     bool
	}{
		{caseName: "no query", target: "/b", want: false},
		{caseName: "presigned", target: "/b?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=c&X-Amz-Date=d&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=s", want: false},
		{caseName: "subresource", target: "/b?policy", want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, namesBucketSubresource(httptest.NewRequest(http.MethodDelete, tc.target, nil)))
		})
	}
}

// TestDeleteBucketNotEmptyPaged finds an object past a first page of .keep markers alone, on memfs as 1001 folders are slow on disk.
func TestDeleteBucketNotEmptyPaged(t *testing.T) {
	ctx := context.Background()
	cfg := server.DefaultConfig()
	cfg.Type = s2.TypeMemFS
	srv, err := server.NewServer(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Buckets.Create(ctx, "paged"))
	for i := range maxObjectKeys + 1 {
		require.NoError(t, srv.Buckets.CreateFolder(ctx, "paged", fmt.Sprintf("f%05d", i)))
	}
	strg, err := srv.Buckets.Get(ctx, "paged")
	require.NoError(t, err)
	// "zzz/k" sorts after every folder, so only the second page holds it.
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("zzz/k", []byte("body"))))

	del := func() int {
		w := httptest.NewRecorder()
		srv.S3Handler().ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/paged", nil))
		return w.Code
	}
	require.Equal(t, http.StatusConflict, del())
	require.NoError(t, strg.Delete(ctx, "zzz/k"))
	require.Equal(t, http.StatusNoContent, del())
	exists, err := srv.Buckets.Exists(ctx, "paged")
	require.NoError(t, err)
	require.False(t, exists)
}

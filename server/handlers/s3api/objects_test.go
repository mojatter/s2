package s3api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/suite"
)

type S3APISuite struct {
	suite.Suite
	server *server.Server
}

func TestS3APISuite(t *testing.T) {
	suite.Run(t, &S3APISuite{})
}

func (s *S3APISuite) SetupTest() {
	cfg := server.DefaultConfig()
	cfg.Root = s.T().TempDir()
	srv, err := server.NewServer(context.Background(), cfg)
	s.Require().NoError(err)
	s.server = srv
}

func (s *S3APISuite) putObject(bucket, key, content string) {
	s.T().Helper()
	ctx := context.Background()
	if ok, _ := s.server.Buckets.Exists(bucket); !ok {
		s.Require().NoError(s.server.Buckets.Create(ctx, bucket))
	}
	strg, err := s.server.Buckets.Get(ctx, bucket)
	s.Require().NoError(err)
	s.Require().NoError(strg.Put(ctx, s2.NewObjectBytes(key, []byte(content))))
}

func (s *S3APISuite) createBucket(name string) {
	s.T().Helper()
	s.Require().NoError(s.server.Buckets.Create(context.Background(), name))
}

// --- Error mapping ---

func (s *S3APISuite) TestS2ErrorToS3Error() {
	testCases := []struct {
		caseName   string
		err        error
		wantCode   string
		wantStatus int
	}{
		{
			caseName:   "not exist",
			err:        &s2.ErrNotExist{Name: "key"},
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
			err:        fmt.Errorf("wrap: %w", &s2.ErrNotExist{Name: "key"}),
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

// --- ListBuckets ---

func (s *S3APISuite) TestListBuckets() {
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

func (s *S3APISuite) TestCreateBucket() {
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

func (s *S3APISuite) TestDeleteBucket() {
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

// --- HeadBucket ---

func (s *S3APISuite) TestHeadBucket() {
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

// --- ListObjects ---

func (s *S3APISuite) TestListObjects() {
	s.Run("with delimiter", func() {
		s.putObject("b", "file.txt", "hello")
		s.putObject("b", "dir/nested.txt", "world")

		req := httptest.NewRequest("GET", "/s3api/b?delimiter=/&prefix=", nil)
		req.SetPathValue("bucket", "b")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Equal("b", result.Name)
		s.Equal("/", result.Delimiter)
		s.Equal(defaultMaxKeys, result.MaxKeys)
		s.False(result.IsTruncated)
		s.Len(result.Contents, 1)
		s.Equal("file.txt", result.Contents[0].Key)
		s.Len(result.CommonPrefixes, 1)
		s.Equal("dir/", result.CommonPrefixes[0].Prefix)
	})

	s.Run("without delimiter (recursive)", func() {
		s.putObject("r", "a.txt", "1")
		s.putObject("r", "sub/b.txt", "2")

		req := httptest.NewRequest("GET", "/s3api/r", nil)
		req.SetPathValue("bucket", "r")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Contents, 2)
		s.Equal("a.txt", result.Contents[0].Key)
		s.Equal("sub/b.txt", result.Contents[1].Key)
		s.Empty(result.CommonPrefixes)
	})

	s.Run("with prefix", func() {
		s.putObject("p", "images/a.png", "a")
		s.putObject("p", "images/b.png", "b")
		s.putObject("p", "docs/c.txt", "c")

		req := httptest.NewRequest("GET", "/s3api/p?prefix=images/", nil)
		req.SetPathValue("bucket", "p")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Contents, 2)
		s.Equal("images/", result.Prefix)
	})

	s.Run("empty bucket", func() {
		s.createBucket("empty")

		req := httptest.NewRequest("GET", "/s3api/empty", nil)
		req.SetPathValue("bucket", "empty")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Contents)
	})

	s.Run("nonexistent bucket", func() {
		req := httptest.NewRequest("GET", "/s3api/no-such-bucket", nil)
		req.SetPathValue("bucket", "no-such-bucket")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("NoSuchBucket", errResp.Code)
	})
}

// --- ListObjects pagination ---

func (s *S3APISuite) TestListObjects_Pagination() {
	s.putObject("pg", "a.txt", "1")
	s.putObject("pg", "b.txt", "2")
	s.putObject("pg", "c.txt", "3")

	s.Run("max-keys truncates", func() {
		req := httptest.NewRequest("GET", "/s3api/pg?max-keys=2", nil)
		req.SetPathValue("bucket", "pg")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.True(result.IsTruncated)
		s.Equal(2, result.MaxKeys)
		s.Len(result.Contents, 2)
		s.NotEmpty(result.NextContinuationToken)
	})

	s.Run("continuation-token fetches rest", func() {
		// First page
		req1 := httptest.NewRequest("GET", "/s3api/pg?max-keys=2", nil)
		req1.SetPathValue("bucket", "pg")
		w1 := httptest.NewRecorder()
		handleListObjects(s.server, w1, req1)
		var page1 ListBucketResult
		s.Require().NoError(xml.Unmarshal(w1.Body.Bytes(), &page1))

		// Second page
		req2 := httptest.NewRequest("GET", "/s3api/pg?continuation-token="+page1.NextContinuationToken, nil)
		req2.SetPathValue("bucket", "pg")
		w2 := httptest.NewRecorder()
		handleListObjects(s.server, w2, req2)

		var page2 ListBucketResult
		s.Require().NoError(xml.Unmarshal(w2.Body.Bytes(), &page2))
		s.False(page2.IsTruncated)
		s.Len(page2.Contents, 1)
		s.Equal("c.txt", page2.Contents[0].Key)
		s.Equal(page1.NextContinuationToken, page2.ContinuationToken)
	})

	s.Run("start-after", func() {
		req := httptest.NewRequest("GET", "/s3api/pg?start-after=a.txt", nil)
		req.SetPathValue("bucket", "pg")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Contents, 2)
		s.Equal("b.txt", result.Contents[0].Key)
		s.Equal("c.txt", result.Contents[1].Key)
	})

	s.Run("max-keys=0 returns empty", func() {
		req := httptest.NewRequest("GET", "/s3api/pg?max-keys=0", nil)
		req.SetPathValue("bucket", "pg")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Contents)
	})

	s.Run("invalid max-keys uses default", func() {
		req := httptest.NewRequest("GET", "/s3api/pg?max-keys=abc", nil)
		req.SetPathValue("bucket", "pg")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Equal(defaultMaxKeys, result.MaxKeys)
	})

	s.Run("delimiter with start-after", func() {
		s.putObject("pgd", "a.txt", "1")
		s.putObject("pgd", "b.txt", "2")
		s.putObject("pgd", "dir/c.txt", "3")

		req := httptest.NewRequest("GET", "/s3api/pgd?delimiter=/&start-after=a.txt", nil)
		req.SetPathValue("bucket", "pgd")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Contents, 1)
		s.Equal("b.txt", result.Contents[0].Key)
	})
}

// --- GetObject ---

func (s *S3APISuite) TestGetObject() {
	s.Run("typical", func() {
		s.putObject("b", "hello.txt", "Hello, S2!")

		req := httptest.NewRequest("GET", "/s3api/b/hello.txt", nil)
		req.SetPathValue("bucket", "b")
		req.SetPathValue("key", "hello.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("Hello, S2!", w.Body.String())
		s.Equal("10", w.Header().Get("Content-Length"))
		s.NotEmpty(w.Header().Get("Last-Modified"))
	})

	s.Run("not found", func() {
		s.createBucket("miss")

		req := httptest.NewRequest("GET", "/s3api/miss/missing.txt", nil)
		req.SetPathValue("bucket", "miss")
		req.SetPathValue("key", "missing.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("NoSuchKey", errResp.Code)
	})

	s.Run("nested key", func() {
		s.putObject("b2", "a/b/c.txt", "nested")

		req := httptest.NewRequest("GET", "/s3api/b2/a/b/c.txt", nil)
		req.SetPathValue("bucket", "b2")
		req.SetPathValue("key", "a/b/c.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("nested", w.Body.String())
	})
}

// --- HeadObject ---

func (s *S3APISuite) TestHeadObject() {
	s.putObject("hb", "file.txt", "content")

	s.Run("returns headers without body", func() {
		req := httptest.NewRequest("HEAD", "/s3api/hb/file.txt", nil)
		req.SetPathValue("bucket", "hb")
		req.SetPathValue("key", "file.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("7", w.Header().Get("Content-Length"))
		s.Empty(w.Body.String())
	})

	s.Run("not found", func() {
		s.createBucket("hb2")

		req := httptest.NewRequest("HEAD", "/s3api/hb2/nope.txt", nil)
		req.SetPathValue("bucket", "hb2")
		req.SetPathValue("key", "nope.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})
}

// --- PutObject ---

func (s *S3APISuite) TestPutObject() {
	s.Run("typical", func() {
		s.createBucket("pb")

		body := "hello world"
		req := httptest.NewRequest("PUT", "/s3api/pb/new.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "pb")
		req.SetPathValue("key", "new.txt")
		req.ContentLength = int64(len(body))
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)

		// Verify written content
		strg, err := s.server.Buckets.Get(context.Background(), "pb")
		s.Require().NoError(err)
		obj, err := strg.Get(context.Background(), "new.txt")
		s.Require().NoError(err)
		rc, err := obj.Open()
		s.Require().NoError(err)
		defer rc.Close()
		data, _ := io.ReadAll(rc)
		s.Equal(body, string(data))
	})

	s.Run("overwrites existing", func() {
		s.putObject("ow", "f.txt", "old")

		body := "new"
		req := httptest.NewRequest("PUT", "/s3api/ow/f.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "ow")
		req.SetPathValue("key", "f.txt")
		req.ContentLength = int64(len(body))
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)

		strg, _ := s.server.Buckets.Get(context.Background(), "ow")
		obj, _ := strg.Get(context.Background(), "f.txt")
		rc, _ := obj.Open()
		defer rc.Close()
		data, _ := io.ReadAll(rc)
		s.Equal("new", string(data))
	})
}

// --- PutObject ETag ---

func (s *S3APISuite) TestPutObject_ETag() {
	s.createBucket("etag")

	testCases := []struct {
		caseName string
		key      string
		body     string
	}{
		{caseName: "short content", key: "short.bin", body: "abc"},
		{caseName: "empty content", key: "empty.bin", body: ""},
		{caseName: "binary-like", key: "binary.bin", body: "\x00\x01\x02\xff"},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req := httptest.NewRequest("PUT", "/s3api/etag/"+tc.key, strings.NewReader(tc.body))
			req.SetPathValue("bucket", "etag")
			req.SetPathValue("key", tc.key)
			req.ContentLength = int64(len(tc.body))
			w := httptest.NewRecorder()
			handlePutObject(s.server, w, req)

			s.Equal(http.StatusOK, w.Code)

			h := md5.Sum([]byte(tc.body))
			expectedETag := `"` + hex.EncodeToString(h[:]) + `"`
			s.Equal(expectedETag, w.Header().Get("ETag"))
		})
	}
}

// --- PutObject ETag persists for GetObject ---

func (s *S3APISuite) TestETag_RoundTrip() {
	s.createBucket("ert")

	body := "roundtrip"
	h := md5.Sum([]byte(body))
	expectedETag := `"` + hex.EncodeToString(h[:]) + `"`

	// Put via API handler
	putReq := httptest.NewRequest("PUT", "/s3api/ert/rt.txt", strings.NewReader(body))
	putReq.SetPathValue("bucket", "ert")
	putReq.SetPathValue("key", "rt.txt")
	putReq.ContentLength = int64(len(body))
	putW := httptest.NewRecorder()
	handlePutObject(s.server, putW, putReq)
	s.Equal(expectedETag, putW.Header().Get("ETag"))

	// GetObject should return the same ETag
	getReq := httptest.NewRequest("GET", "/s3api/ert/rt.txt", nil)
	getReq.SetPathValue("bucket", "ert")
	getReq.SetPathValue("key", "rt.txt")
	getW := httptest.NewRecorder()
	handleGetObject(s.server, getW, getReq)
	s.Equal(expectedETag, getW.Header().Get("ETag"))

	// ListObjects returns objects without metadata (by design in fs backend),
	// so ETags in list results use the fallback value.
	listReq := httptest.NewRequest("GET", "/s3api/ert", nil)
	listReq.SetPathValue("bucket", "ert")
	listW := httptest.NewRecorder()
	handleListObjects(s.server, listW, listReq)
	var listResult ListBucketResult
	s.Require().NoError(xml.Unmarshal(listW.Body.Bytes(), &listResult))
	var found bool
	for _, c := range listResult.Contents {
		if c.Key == "rt.txt" {
			s.NotEmpty(c.ETag)
			found = true
			break
		}
	}
	s.True(found, "rt.txt should be in list results")
}

// --- Metadata ---

func (s *S3APISuite) TestMetadata() {
	s.Run("put and get metadata", func() {
		s.createBucket("md")

		body := "data"
		req := httptest.NewRequest("PUT", "/s3api/md/meta.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "md")
		req.SetPathValue("key", "meta.txt")
		req.ContentLength = int64(len(body))
		req.Header.Set("X-Amz-Meta-Author", "alice")
		req.Header.Set("X-Amz-Meta-Version", "42")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)
		s.Equal(http.StatusOK, w.Code)

		getReq := httptest.NewRequest("GET", "/s3api/md/meta.txt", nil)
		getReq.SetPathValue("bucket", "md")
		getReq.SetPathValue("key", "meta.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)

		s.Equal(http.StatusOK, getW.Code)
		s.Equal("alice", getW.Header().Get("x-amz-meta-author"))
		s.Equal("42", getW.Header().Get("x-amz-meta-version"))
		// Internal metadata key should not leak
		s.Empty(getW.Header().Get("x-amz-meta-"+etagMetadataKey))
	})

	s.Run("no metadata headers", func() {
		s.createBucket("nmd")

		body := "data"
		req := httptest.NewRequest("PUT", "/s3api/nmd/plain.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "nmd")
		req.SetPathValue("key", "plain.txt")
		req.ContentLength = int64(len(body))
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)
		s.Equal(http.StatusOK, w.Code)

		getReq := httptest.NewRequest("GET", "/s3api/nmd/plain.txt", nil)
		getReq.SetPathValue("bucket", "nmd")
		getReq.SetPathValue("key", "plain.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)

		s.Equal(http.StatusOK, getW.Code)
		// ETag metadata should exist but not exposed as x-amz-meta-*
		s.Empty(getW.Header().Get("x-amz-meta-" + etagMetadataKey))
	})

	s.Run("head returns metadata", func() {
		s.createBucket("hmd")

		body := "data"
		req := httptest.NewRequest("PUT", "/s3api/hmd/file.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "hmd")
		req.SetPathValue("key", "file.txt")
		req.ContentLength = int64(len(body))
		req.Header.Set("X-Amz-Meta-Foo", "bar")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		headReq := httptest.NewRequest("HEAD", "/s3api/hmd/file.txt", nil)
		headReq.SetPathValue("bucket", "hmd")
		headReq.SetPathValue("key", "file.txt")
		headW := httptest.NewRecorder()
		handleGetObject(s.server, headW, headReq)

		s.Equal(http.StatusOK, headW.Code)
		s.Equal("bar", headW.Header().Get("x-amz-meta-foo"))
		s.Empty(headW.Body.String())
	})
}

// --- parseMetadataHeaders ---

func (s *S3APISuite) TestParseMetadataHeaders() {
	testCases := []struct {
		caseName string
		headers  map[string]string
		want     s2.MetadataMap
	}{
		{
			caseName: "typical",
			headers:  map[string]string{"X-Amz-Meta-Key": "val"},
			want:     s2.MetadataMap{"key": "val"},
		},
		{
			caseName: "multiple",
			headers: map[string]string{
				"X-Amz-Meta-A": "1",
				"X-Amz-Meta-B": "2",
			},
			want: s2.MetadataMap{"a": "1", "b": "2"},
		},
		{
			caseName: "non-meta headers ignored",
			headers: map[string]string{
				"Content-Type":  "text/plain",
				"X-Amz-Meta-Ok": "yes",
			},
			want: s2.MetadataMap{"ok": "yes"},
		},
		{
			caseName: "empty",
			headers:  map[string]string{},
			want:     s2.MetadataMap{},
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

// --- CopyObject ---

func (s *S3APISuite) TestCopyObject() {
	s.Run("same bucket", func() {
		s.putObject("cb", "src.txt", "source data")

		req := httptest.NewRequest("PUT", "/s3api/cb/dst.txt", nil)
		req.SetPathValue("bucket", "cb")
		req.SetPathValue("key", "dst.txt")
		req.Header.Set("x-amz-copy-source", "/cb/src.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result CopyObjectResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.NotEmpty(result.ETag)
		s.False(result.LastModified.IsZero())

		// Verify content
		getReq := httptest.NewRequest("GET", "/s3api/cb/dst.txt", nil)
		getReq.SetPathValue("bucket", "cb")
		getReq.SetPathValue("key", "dst.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal("source data", getW.Body.String())
	})

	s.Run("cross bucket", func() {
		s.putObject("src-bkt", "file.txt", "cross-bucket")
		s.createBucket("dst-bkt")

		req := httptest.NewRequest("PUT", "/s3api/dst-bkt/copied.txt", nil)
		req.SetPathValue("bucket", "dst-bkt")
		req.SetPathValue("key", "copied.txt")
		req.Header.Set("x-amz-copy-source", "/src-bkt/file.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)

		getReq := httptest.NewRequest("GET", "/s3api/dst-bkt/copied.txt", nil)
		getReq.SetPathValue("bucket", "dst-bkt")
		getReq.SetPathValue("key", "copied.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal("cross-bucket", getW.Body.String())
	})

	s.Run("without leading slash", func() {
		s.putObject("ns", "a.txt", "no-slash")

		req := httptest.NewRequest("PUT", "/s3api/ns/b.txt", nil)
		req.SetPathValue("bucket", "ns")
		req.SetPathValue("key", "b.txt")
		req.Header.Set("x-amz-copy-source", "ns/a.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
	})

	s.Run("url-encoded source", func() {
		s.putObject("enc", "my file.txt", "encoded")

		req := httptest.NewRequest("PUT", "/s3api/enc/copy.txt", nil)
		req.SetPathValue("bucket", "enc")
		req.SetPathValue("key", "copy.txt")
		req.Header.Set("x-amz-copy-source", "/enc/my%20file.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
	})

	s.Run("invalid source", func() {
		req := httptest.NewRequest("PUT", "/s3api/b/dst.txt", nil)
		req.SetPathValue("bucket", "b")
		req.SetPathValue("key", "dst.txt")
		req.Header.Set("x-amz-copy-source", "no-slash-no-key")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("InvalidArgument", errResp.Code)
	})

	s.Run("source not found", func() {
		s.createBucket("snf")

		req := httptest.NewRequest("PUT", "/s3api/snf/dst.txt", nil)
		req.SetPathValue("bucket", "snf")
		req.SetPathValue("key", "dst.txt")
		req.Header.Set("x-amz-copy-source", "/snf/nonexistent.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})
}

// --- DeleteObject ---

func (s *S3APISuite) TestDeleteObject() {
	s.Run("existing", func() {
		s.putObject("db", "to-delete.txt", "bye")

		req := httptest.NewRequest("DELETE", "/s3api/db/to-delete.txt", nil)
		req.SetPathValue("bucket", "db")
		req.SetPathValue("key", "to-delete.txt")
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusNoContent, w.Code)

		// Verify deleted
		getReq := httptest.NewRequest("GET", "/s3api/db/to-delete.txt", nil)
		getReq.SetPathValue("bucket", "db")
		getReq.SetPathValue("key", "to-delete.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal(http.StatusNotFound, getW.Code)
	})

	s.Run("non-existing key returns error", func() {
		s.createBucket("di")

		req := httptest.NewRequest("DELETE", "/s3api/di/ghost.txt", nil)
		req.SetPathValue("bucket", "di")
		req.SetPathValue("key", "ghost.txt")
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		// fs backend returns error for missing keys
		s.NotEqual(http.StatusNoContent, w.Code)
	})
}

// --- XML response format ---

func (s *S3APISuite) TestXMLResponseFormat() {
	s.createBucket("xf")

	req := httptest.NewRequest("GET", "/s3api/xf", nil)
	req.SetPathValue("bucket", "xf")
	w := httptest.NewRecorder()
	handleListObjects(s.server, w, req)

	s.Equal("application/xml", w.Header().Get("Content-Type"))
	s.Contains(w.Body.String(), `<?xml version="1.0" encoding="UTF-8"?>`)
	s.Contains(w.Body.String(), `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`)
}

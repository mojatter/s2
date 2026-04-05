package s3api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ObjectsTestSuite struct{ s3apiSuite }

func TestObjectsTestSuite(t *testing.T) {
	suite.Run(t, &ObjectsTestSuite{})
}

// --- ListObjects ---

func (s *ObjectsTestSuite) TestListObjects() {
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

func (s *ObjectsTestSuite) TestListObjects_Pagination() {
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

func (s *ObjectsTestSuite) TestGetObject() {
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

func (s *ObjectsTestSuite) TestHeadObject() {
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

func (s *ObjectsTestSuite) TestPutObject() {
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

func (s *ObjectsTestSuite) TestPutObject_ETag() {
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

func (s *ObjectsTestSuite) TestETag_RoundTrip() {
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

func (s *ObjectsTestSuite) TestMetadata() {
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

// --- CopyObject ---

func (s *ObjectsTestSuite) TestCopyObject() {
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

func (s *ObjectsTestSuite) TestDeleteObject() {
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

// --- DeleteObjects ---

func (s *ObjectsTestSuite) TestDeleteObjects() {
	s.Run("delete multiple objects", func() {
		s.putObject("do", "a.txt", "1")
		s.putObject("do", "b.txt", "2")
		s.putObject("do", "c.txt", "3")

		body := `<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>`
		req := httptest.NewRequest("POST", "/s3api/do?delete", strings.NewReader(body))
		req.SetPathValue("bucket", "do")
		w := httptest.NewRecorder()
		handleDeleteObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result DeleteObjectsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Deleted, 2)
		s.Empty(result.Errors)

		// c.txt should still exist
		getReq := httptest.NewRequest("GET", "/s3api/do/c.txt", nil)
		getReq.SetPathValue("bucket", "do")
		getReq.SetPathValue("key", "c.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal(http.StatusOK, getW.Code)
	})

	s.Run("quiet mode omits deleted", func() {
		s.putObject("dq", "x.txt", "data")

		body := `<Delete><Quiet>true</Quiet><Object><Key>x.txt</Key></Object></Delete>`
		req := httptest.NewRequest("POST", "/s3api/dq?delete", strings.NewReader(body))
		req.SetPathValue("bucket", "dq")
		w := httptest.NewRecorder()
		handleDeleteObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result DeleteObjectsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Deleted)
		s.Empty(result.Errors)
	})

	s.Run("nonexistent key reports error", func() {
		s.createBucket("de")

		body := `<Delete><Object><Key>ghost.txt</Key></Object></Delete>`
		req := httptest.NewRequest("POST", "/s3api/de?delete", strings.NewReader(body))
		req.SetPathValue("bucket", "de")
		w := httptest.NewRecorder()
		handleDeleteObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result DeleteObjectsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Deleted)
		s.Len(result.Errors, 1)
		s.Equal("ghost.txt", result.Errors[0].Key)
	})

	s.Run("nonexistent bucket", func() {
		body := `<Delete><Object><Key>a.txt</Key></Object></Delete>`
		req := httptest.NewRequest("POST", "/s3api/no-bucket?delete", strings.NewReader(body))
		req.SetPathValue("bucket", "no-bucket")
		w := httptest.NewRecorder()
		handleDeleteObjects(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})

	s.Run("malformed XML", func() {
		s.createBucket("dm")

		req := httptest.NewRequest("POST", "/s3api/dm?delete", strings.NewReader("not xml"))
		req.SetPathValue("bucket", "dm")
		w := httptest.NewRecorder()
		handleDeleteObjects(s.server, w, req)

		s.Equal(http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("MalformedXML", errResp.Code)
	})

	s.Run("dispatched via handleBucketPOST", func() {
		s.putObject("dp", "f.txt", "data")

		body := `<Delete><Object><Key>f.txt</Key></Object></Delete>`
		req := httptest.NewRequest("POST", "/s3api/dp?delete", strings.NewReader(body))
		req.SetPathValue("bucket", "dp")
		w := httptest.NewRecorder()
		handleBucketPOST(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result DeleteObjectsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Deleted, 1)
	})
}

// --- Range requests ---

func (s *ObjectsTestSuite) TestGetObject_Range() {
	s.putObject("rng", "data.txt", "Hello, World!")
	// "Hello, World!" is 13 bytes: H(0) e(1) l(2) l(3) o(4) ,(5) (6) W(7) o(8) r(9) l(10) d(11) !(12)

	s.Run("mid range", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=0-4")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusPartialContent, w.Code)
		s.Equal("Hello", w.Body.String())
		s.Equal("bytes 0-4/13", w.Header().Get("Content-Range"))
		s.Equal("5", w.Header().Get("Content-Length"))
	})

	s.Run("open-ended range", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=7-")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusPartialContent, w.Code)
		s.Equal("World!", w.Body.String())
		s.Equal("bytes 7-12/13", w.Header().Get("Content-Range"))
	})

	s.Run("suffix range", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=-6")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusPartialContent, w.Code)
		s.Equal("World!", w.Body.String())
		s.Equal("bytes 7-12/13", w.Header().Get("Content-Range"))
	})

	s.Run("end clamped to file size", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=7-999")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusPartialContent, w.Code)
		s.Equal("World!", w.Body.String())
		s.Equal("bytes 7-12/13", w.Header().Get("Content-Range"))
	})

	s.Run("start beyond file size returns 416", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=999-")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
		s.Equal("bytes */13", w.Header().Get("Content-Range"))
	})

	s.Run("no Range header returns full content", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("Hello, World!", w.Body.String())
	})

	s.Run("invalid scheme", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "pages=1-2")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("suffix zero returns 416", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=-0")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("suffix exceeding file size returns entire file", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=-999")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusPartialContent, w.Code)
		s.Equal("Hello, World!", w.Body.String())
		s.Equal("bytes 0-12/13", w.Header().Get("Content-Range"))
	})

	s.Run("non-numeric start returns 416", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=abc-5")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("non-numeric end returns 416", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=0-xyz")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("start greater than end returns 416", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=10-5")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("non-numeric suffix returns 416", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=-abc")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("single byte range", func() {
		req := httptest.NewRequest("GET", "/s3api/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=0-0")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusPartialContent, w.Code)
		s.Equal("H", w.Body.String())
		s.Equal("bytes 0-0/13", w.Header().Get("Content-Range"))
		s.Equal("1", w.Header().Get("Content-Length"))
	})
}

// --- Multipart Upload ---

func (s *ObjectsTestSuite) TestMultipartUpload() {
	s.Run("full lifecycle", func() {
		s.createBucket("mp")

		// CreateMultipartUpload
		createReq := httptest.NewRequest("POST", "/s3api/mp/large.txt?uploads", nil)
		createReq.SetPathValue("bucket", "mp")
		createReq.SetPathValue("key", "large.txt")
		createW := httptest.NewRecorder()
		handleCreateMultipartUpload(s.server, createW, createReq)

		s.Equal(http.StatusOK, createW.Code)
		var initResult InitiateMultipartUploadResult
		s.Require().NoError(xml.Unmarshal(createW.Body.Bytes(), &initResult))
		s.Equal("mp", initResult.Bucket)
		s.Equal("large.txt", initResult.Key)
		s.NotEmpty(initResult.UploadID)
		uploadID := initResult.UploadID

		// UploadPart 1
		part1 := "Hello, "
		req1 := httptest.NewRequest("PUT", "/s3api/mp/large.txt?partNumber=1&uploadId="+uploadID, strings.NewReader(part1))
		req1.SetPathValue("bucket", "mp")
		req1.SetPathValue("key", "large.txt")
		req1.ContentLength = int64(len(part1))
		w1 := httptest.NewRecorder()
		handleUploadPart(s.server, w1, req1)
		s.Equal(http.StatusOK, w1.Code)
		s.NotEmpty(w1.Header().Get("ETag"))

		// UploadPart 2
		part2 := "World!"
		req2 := httptest.NewRequest("PUT", "/s3api/mp/large.txt?partNumber=2&uploadId="+uploadID, strings.NewReader(part2))
		req2.SetPathValue("bucket", "mp")
		req2.SetPathValue("key", "large.txt")
		req2.ContentLength = int64(len(part2))
		w2 := httptest.NewRecorder()
		handleUploadPart(s.server, w2, req2)
		s.Equal(http.StatusOK, w2.Code)

		// CompleteMultipartUpload
		completeBody := `<CompleteMultipartUpload>` +
			`<Part><PartNumber>1</PartNumber><ETag>` + w1.Header().Get("ETag") + `</ETag></Part>` +
			`<Part><PartNumber>2</PartNumber><ETag>` + w2.Header().Get("ETag") + `</ETag></Part>` +
			`</CompleteMultipartUpload>`
		completeReq := httptest.NewRequest("POST", "/s3api/mp/large.txt?uploadId="+uploadID, strings.NewReader(completeBody))
		completeReq.SetPathValue("bucket", "mp")
		completeReq.SetPathValue("key", "large.txt")
		completeW := httptest.NewRecorder()
		handleCompleteMultipartUpload(s.server, completeW, completeReq)

		s.Equal(http.StatusOK, completeW.Code)
		var completeResult CompleteMultipartUploadResult
		s.Require().NoError(xml.Unmarshal(completeW.Body.Bytes(), &completeResult))
		s.Equal("mp", completeResult.Bucket)
		s.Equal("large.txt", completeResult.Key)
		s.Contains(completeResult.ETag, "-2") // multipart ETag ends with -<partCount>

		// Verify assembled content
		getReq := httptest.NewRequest("GET", "/s3api/mp/large.txt", nil)
		getReq.SetPathValue("bucket", "mp")
		getReq.SetPathValue("key", "large.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal(http.StatusOK, getW.Code)
		s.Equal("Hello, World!", getW.Body.String())

		// Parts must be cleaned up — they should not appear in list results
		listReq := httptest.NewRequest("GET", "/s3api/mp", nil)
		listReq.SetPathValue("bucket", "mp")
		listW := httptest.NewRecorder()
		handleListObjects(s.server, listW, listReq)
		var listResult ListBucketResult
		s.Require().NoError(xml.Unmarshal(listW.Body.Bytes(), &listResult))
		s.Len(listResult.Contents, 1)
		s.Equal("large.txt", listResult.Contents[0].Key)
	})

	s.Run("abort cleans up parts", func() {
		s.createBucket("ab")

		createReq := httptest.NewRequest("POST", "/s3api/ab/f.txt?uploads", nil)
		createReq.SetPathValue("bucket", "ab")
		createReq.SetPathValue("key", "f.txt")
		createW := httptest.NewRecorder()
		handleCreateMultipartUpload(s.server, createW, createReq)
		var initResult InitiateMultipartUploadResult
		s.Require().NoError(xml.Unmarshal(createW.Body.Bytes(), &initResult))
		uploadID := initResult.UploadID

		// Upload a part
		partReq := httptest.NewRequest("PUT", "/s3api/ab/f.txt?partNumber=1&uploadId="+uploadID, strings.NewReader("data"))
		partReq.SetPathValue("bucket", "ab")
		partReq.SetPathValue("key", "f.txt")
		partReq.ContentLength = 4
		partW := httptest.NewRecorder()
		handleUploadPart(s.server, partW, partReq)
		s.Equal(http.StatusOK, partW.Code)

		// Abort
		abortReq := httptest.NewRequest("DELETE", "/s3api/ab/f.txt?uploadId="+uploadID, nil)
		abortReq.SetPathValue("bucket", "ab")
		abortReq.SetPathValue("key", "f.txt")
		abortW := httptest.NewRecorder()
		handleAbortMultipartUpload(s.server, abortW, abortReq)
		s.Equal(http.StatusNoContent, abortW.Code)

		// List should be empty (final object was never written)
		listReq := httptest.NewRequest("GET", "/s3api/ab", nil)
		listReq.SetPathValue("bucket", "ab")
		listW := httptest.NewRecorder()
		handleListObjects(s.server, listW, listReq)
		var listResult ListBucketResult
		s.Require().NoError(xml.Unmarshal(listW.Body.Bytes(), &listResult))
		s.Empty(listResult.Contents)
	})

	s.Run("parts hidden from list", func() {
		s.createBucket("hid")

		createReq := httptest.NewRequest("POST", "/s3api/hid/obj.txt?uploads", nil)
		createReq.SetPathValue("bucket", "hid")
		createReq.SetPathValue("key", "obj.txt")
		createW := httptest.NewRecorder()
		handleCreateMultipartUpload(s.server, createW, createReq)
		var initResult InitiateMultipartUploadResult
		s.Require().NoError(xml.Unmarshal(createW.Body.Bytes(), &initResult))
		uploadID := initResult.UploadID

		// Upload part but don't complete
		partReq := httptest.NewRequest("PUT", "/s3api/hid/obj.txt?partNumber=1&uploadId="+uploadID, strings.NewReader("partial"))
		partReq.SetPathValue("bucket", "hid")
		partReq.SetPathValue("key", "obj.txt")
		partReq.ContentLength = 7
		handleUploadPart(s.server, httptest.NewRecorder(), partReq)

		// Parts must not appear in list
		listReq := httptest.NewRequest("GET", "/s3api/hid", nil)
		listReq.SetPathValue("bucket", "hid")
		listW := httptest.NewRecorder()
		handleListObjects(s.server, listW, listReq)
		var listResult ListBucketResult
		s.Require().NoError(xml.Unmarshal(listW.Body.Bytes(), &listResult))
		s.Empty(listResult.Contents)
	})
}

// --- XML response format ---

func (s *ObjectsTestSuite) TestXMLResponseFormat() {
	s.createBucket("xf")

	req := httptest.NewRequest("GET", "/s3api/xf", nil)
	req.SetPathValue("bucket", "xf")
	w := httptest.NewRecorder()
	handleListObjects(s.server, w, req)

	s.Equal("application/xml", w.Header().Get("Content-Type"))
	s.Contains(w.Body.String(), `<?xml version="1.0" encoding="UTF-8"?>`)
	s.Contains(w.Body.String(), `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`)
}

package s3api

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/fs"
	"github.com/mojatter/s2/server"
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

		req := httptest.NewRequest("GET", "/b?delimiter=/&prefix=", nil)
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

		req := httptest.NewRequest("GET", "/r", nil)
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

		req := httptest.NewRequest("GET", "/p?prefix=images/", nil)
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

		req := httptest.NewRequest("GET", "/empty", nil)
		req.SetPathValue("bucket", "empty")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Contents)
	})

	s.Run("nonexistent bucket", func() {
		req := httptest.NewRequest("GET", "/no-such-bucket", nil)
		req.SetPathValue("bucket", "no-such-bucket")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
		var errResp ErrorResponse
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &errResp))
		s.Equal("NoSuchBucket", errResp.Code)
	})

	s.Run("prefix with trailing slash and delimiter", func() {
		s.putObject("ts", "dir/a.txt", "a")
		s.putObject("ts", "dir/b.txt", "b")
		s.putObject("ts", "other.txt", "x")

		req := httptest.NewRequest("GET", "/ts?delimiter=/&prefix=dir/", nil)
		req.SetPathValue("bucket", "ts")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Contents, 2)
		s.Equal("dir/a.txt", result.Contents[0].Key)
		s.Equal("dir/b.txt", result.Contents[1].Key)
	})

	s.Run("nonexistent prefix with delimiter returns empty", func() {
		s.createBucket("np")
		s.putObject("np", "real.txt", "x")

		req := httptest.NewRequest("GET", "/np?delimiter=/&prefix=nope/", nil)
		req.SetPathValue("bucket", "np")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Contents)
		s.Empty(result.CommonPrefixes)
	})

	s.Run("non-directory-aligned prefix with delimiter", func() {
		s.putObject("nda", "images/a.png", "a")
		s.putObject("nda", "images/b.png", "b")
		s.putObject("nda", "docs/c.txt", "c")

		testCases := []struct {
			caseName       string
			prefix         string
			wantContents   []string
			wantPrefixes   []string
		}{
			{
				caseName:     "partial prefix matches subdir as common prefix",
				prefix:       "im",
				wantContents: nil,
				wantPrefixes: []string{"images/"},
			},
			{
				caseName:     "partial prefix matching nothing",
				prefix:       "z",
				wantContents: nil,
				wantPrefixes: nil,
			},
			{
				caseName:     "directory + partial filename",
				prefix:       "images/a",
				wantContents: []string{"images/a.png"},
				wantPrefixes: nil,
			},
			{
				caseName:     "directory + nonexistent partial filename",
				prefix:       "images/z",
				wantContents: nil,
				wantPrefixes: nil,
			},
			{
				caseName:     "directory with trailing slash lists contents",
				prefix:       "images/",
				wantContents: []string{"images/a.png", "images/b.png"},
				wantPrefixes: nil,
			},
		}
		for _, tc := range testCases {
			s.Run(tc.caseName, func() {
				url := "/nda?delimiter=/&prefix=" + tc.prefix
				req := httptest.NewRequest("GET", url, nil)
				req.SetPathValue("bucket", "nda")
				w := httptest.NewRecorder()
				handleListObjects(s.server, w, req)

				s.Equal(http.StatusOK, w.Code)
				var result ListBucketResult
				s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))

				gotContents := make([]string, len(result.Contents))
				for i, c := range result.Contents {
					gotContents[i] = c.Key
				}
				gotPrefixes := make([]string, len(result.CommonPrefixes))
				for i, p := range result.CommonPrefixes {
					gotPrefixes[i] = p.Prefix
				}
				if len(gotContents) == 0 {
					gotContents = nil
				}
				if len(gotPrefixes) == 0 {
					gotPrefixes = nil
				}
				s.Equal(tc.wantContents, gotContents)
				s.Equal(tc.wantPrefixes, gotPrefixes)
			})
		}
	})

	s.Run("non-directory-aligned prefix without delimiter (recursive)", func() {
		s.putObject("ndr", "images/a.png", "a")
		s.putObject("ndr", "images/b.png", "b")

		req := httptest.NewRequest("GET", "/ndr?prefix=im", nil)
		req.SetPathValue("bucket", "ndr")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Contents, 2)
		s.Equal("images/a.png", result.Contents[0].Key)
		s.Equal("images/b.png", result.Contents[1].Key)
	})

	s.Run("nonexistent prefix without trailing slash returns empty", func() {
		s.createBucket("np2")
		s.putObject("np2", "real.txt", "x")

		req := httptest.NewRequest("GET", "/np2?delimiter=/&prefix=nope", nil)
		req.SetPathValue("bucket", "np2")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Contents)
		s.Empty(result.CommonPrefixes)
	})
}

// --- ListObjects pagination ---

func (s *ObjectsTestSuite) TestListObjects_Pagination() {
	s.putObject("pg", "a.txt", "1")
	s.putObject("pg", "b.txt", "2")
	s.putObject("pg", "c.txt", "3")

	s.Run("max-keys truncates", func() {
		req := httptest.NewRequest("GET", "/pg?max-keys=2", nil)
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
		req1 := httptest.NewRequest("GET", "/pg?max-keys=2", nil)
		req1.SetPathValue("bucket", "pg")
		w1 := httptest.NewRecorder()
		handleListObjects(s.server, w1, req1)
		var page1 ListBucketResult
		s.Require().NoError(xml.Unmarshal(w1.Body.Bytes(), &page1))

		// Second page
		req2 := httptest.NewRequest("GET", "/pg?continuation-token="+page1.NextContinuationToken, nil)
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
		req := httptest.NewRequest("GET", "/pg?start-after=a.txt", nil)
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
		req := httptest.NewRequest("GET", "/pg?max-keys=0", nil)
		req.SetPathValue("bucket", "pg")
		w := httptest.NewRecorder()
		handleListObjects(s.server, w, req)

		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Empty(result.Contents)
	})

	s.Run("invalid max-keys uses default", func() {
		req := httptest.NewRequest("GET", "/pg?max-keys=abc", nil)
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

		req := httptest.NewRequest("GET", "/pgd?delimiter=/&start-after=a.txt", nil)
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

		req := httptest.NewRequest("GET", "/b/hello.txt", nil)
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

		req := httptest.NewRequest("GET", "/miss/missing.txt", nil)
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

		req := httptest.NewRequest("GET", "/b2/a/b/c.txt", nil)
		req.SetPathValue("bucket", "b2")
		req.SetPathValue("key", "a/b/c.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("nested", w.Body.String())
	})

	s.Run("trailing-slash GET dispatches to ListObjects", func() {
		s.putObject("ts", "x.txt", "x")

		req := httptest.NewRequest("GET", "/ts/", nil)
		req.SetPathValue("bucket", "ts")
		req.SetPathValue("key", "")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result ListBucketResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Equal("ts", result.Name)
	})

	s.Run("trailing-slash HEAD dispatches to HeadBucket", func() {
		s.createBucket("ts2")

		req := httptest.NewRequest("HEAD", "/ts2/", nil)
		req.SetPathValue("bucket", "ts2")
		req.SetPathValue("key", "")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Empty(w.Body.String())
	})
}

// --- HeadObject ---

func (s *ObjectsTestSuite) TestHeadObject() {
	s.putObject("hb", "file.txt", "content")

	s.Run("returns headers without body", func() {
		req := httptest.NewRequest("HEAD", "/hb/file.txt", nil)
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

		req := httptest.NewRequest("HEAD", "/hb2/nope.txt", nil)
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
		req := httptest.NewRequest("PUT", "/pb/new.txt", strings.NewReader(body))
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
		req := httptest.NewRequest("PUT", "/ow/f.txt", strings.NewReader(body))
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
			req := httptest.NewRequest("PUT", "/etag/"+tc.key, strings.NewReader(tc.body))
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
	putReq := httptest.NewRequest("PUT", "/ert/rt.txt", strings.NewReader(body))
	putReq.SetPathValue("bucket", "ert")
	putReq.SetPathValue("key", "rt.txt")
	putReq.ContentLength = int64(len(body))
	putW := httptest.NewRecorder()
	handlePutObject(s.server, putW, putReq)
	s.Equal(expectedETag, putW.Header().Get("ETag"))

	// GetObject should return the same ETag
	getReq := httptest.NewRequest("GET", "/ert/rt.txt", nil)
	getReq.SetPathValue("bucket", "ert")
	getReq.SetPathValue("key", "rt.txt")
	getW := httptest.NewRecorder()
	handleGetObject(s.server, getW, getReq)
	s.Equal(expectedETag, getW.Header().Get("ETag"))

	// ListObjects returns objects without metadata (by design in fs backend),
	// so ETags in list results use the fallback value.
	listReq := httptest.NewRequest("GET", "/ert", nil)
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

// --- PutObject aws-chunked streaming body ---

// buildAWSChunkedBody encodes payload as an AWS SigV4 streaming body:
//
//	<hex-size>;chunk-signature=<64 hex>\r\n
//	<data>\r\n
//	...
//	0;chunk-signature=<64 hex>\r\n
//	\r\n
//
// The chunk signatures are not validated by the parser, so we use a fixed
// placeholder here.
func buildAWSChunkedBody(payload []byte, chunkSize int) []byte {
	fakeSig := strings.Repeat("0", 64)
	var buf strings.Builder
	for i := 0; i < len(payload); i += chunkSize {
		end := min(i+chunkSize, len(payload))
		chunk := payload[i:end]
		buf.WriteString(strconv.FormatInt(int64(len(chunk)), 16))
		buf.WriteString(";chunk-signature=")
		buf.WriteString(fakeSig)
		buf.WriteString("\r\n")
		buf.Write(chunk)
		buf.WriteString("\r\n")
	}
	buf.WriteString("0;chunk-signature=")
	buf.WriteString(fakeSig)
	buf.WriteString("\r\n\r\n")
	return []byte(buf.String())
}

func (s *ObjectsTestSuite) TestPutObject_AWSChunked() {
	s.createBucket("chunked")

	payload := make([]byte, 1024*1024) // 1 MiB
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	const chunkSize = 64 * 1024

	testCases := []struct {
		caseName    string
		setHeaders  func(r *http.Request)
		key         string
	}{
		{
			caseName: "Content-Encoding aws-chunked",
			key:      "ce.bin",
			setHeaders: func(r *http.Request) {
				r.Header.Set("Content-Encoding", "aws-chunked")
			},
		},
		{
			// Reproduces the warp/minio-go behavior: streaming signed payload
			// is signaled via X-Amz-Content-Sha256 alone, without
			// Content-Encoding: aws-chunked.
			caseName: "X-Amz-Content-Sha256 STREAMING only",
			key:      "streaming.bin",
			setHeaders: func(r *http.Request) {
				r.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			},
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			chunked := buildAWSChunkedBody(payload, chunkSize)
			req := httptest.NewRequest("PUT", "/chunked/"+tc.key, strings.NewReader(string(chunked)))
			req.SetPathValue("bucket", "chunked")
			req.SetPathValue("key", tc.key)
			req.ContentLength = int64(len(chunked))
			req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(payload)))
			tc.setHeaders(req)
			w := httptest.NewRecorder()
			handlePutObject(s.server, w, req)
			s.Require().Equal(http.StatusOK, w.Code)

			// Storage layer should see exactly the decoded payload.
			strg, err := s.server.Buckets.Get(context.Background(), "chunked")
			s.Require().NoError(err)
			obj, err := strg.Get(context.Background(), tc.key)
			s.Require().NoError(err)
			s.Equal(uint64(len(payload)), obj.Length(), "object length should match decoded payload size")
			rc, err := obj.Open()
			s.Require().NoError(err)
			defer rc.Close()

			data, err := io.ReadAll(rc)
			s.Require().NoError(err)
			s.Equal(len(payload), len(data), "stored size should match decoded payload size")
			s.Equal(payload, data, "stored bytes should match decoded payload")

			// GET via handler should return the same bytes and advertise the
			// correct Content-Length (this is what warp's minio-go validates).
			getReq := httptest.NewRequest("GET", "/chunked/"+tc.key, nil)
			getReq.SetPathValue("bucket", "chunked")
			getReq.SetPathValue("key", tc.key)
			getW := httptest.NewRecorder()
			handleGetObject(s.server, getW, getReq)
			s.Require().Equal(http.StatusOK, getW.Code)
			s.Equal(strconv.Itoa(len(payload)), getW.Header().Get("Content-Length"))
			s.Equal(len(payload), getW.Body.Len())
		})
	}
}

// --- Metadata ---

func (s *ObjectsTestSuite) TestMetadata() {
	s.Run("put and get metadata", func() {
		s.createBucket("md")

		body := "data"
		req := httptest.NewRequest("PUT", "/md/meta.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "md")
		req.SetPathValue("key", "meta.txt")
		req.ContentLength = int64(len(body))
		req.Header.Set("X-Amz-Meta-Author", "alice")
		req.Header.Set("X-Amz-Meta-Version", "42")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)
		s.Equal(http.StatusOK, w.Code)

		getReq := httptest.NewRequest("GET", "/md/meta.txt", nil)
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
		req := httptest.NewRequest("PUT", "/nmd/plain.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "nmd")
		req.SetPathValue("key", "plain.txt")
		req.ContentLength = int64(len(body))
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)
		s.Equal(http.StatusOK, w.Code)

		getReq := httptest.NewRequest("GET", "/nmd/plain.txt", nil)
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
		req := httptest.NewRequest("PUT", "/hmd/file.txt", strings.NewReader(body))
		req.SetPathValue("bucket", "hmd")
		req.SetPathValue("key", "file.txt")
		req.ContentLength = int64(len(body))
		req.Header.Set("X-Amz-Meta-Foo", "bar")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		headReq := httptest.NewRequest("HEAD", "/hmd/file.txt", nil)
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

		req := httptest.NewRequest("PUT", "/cb/dst.txt", nil)
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
		getReq := httptest.NewRequest("GET", "/cb/dst.txt", nil)
		getReq.SetPathValue("bucket", "cb")
		getReq.SetPathValue("key", "dst.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal("source data", getW.Body.String())
	})

	s.Run("cross bucket", func() {
		s.putObject("src-bkt", "file.txt", "cross-bucket")
		s.createBucket("dst-bkt")

		req := httptest.NewRequest("PUT", "/dst-bkt/copied.txt", nil)
		req.SetPathValue("bucket", "dst-bkt")
		req.SetPathValue("key", "copied.txt")
		req.Header.Set("x-amz-copy-source", "/src-bkt/file.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)

		getReq := httptest.NewRequest("GET", "/dst-bkt/copied.txt", nil)
		getReq.SetPathValue("bucket", "dst-bkt")
		getReq.SetPathValue("key", "copied.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal("cross-bucket", getW.Body.String())
	})

	s.Run("without leading slash", func() {
		s.putObject("ns", "a.txt", "no-slash")

		req := httptest.NewRequest("PUT", "/ns/b.txt", nil)
		req.SetPathValue("bucket", "ns")
		req.SetPathValue("key", "b.txt")
		req.Header.Set("x-amz-copy-source", "ns/a.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
	})

	s.Run("url-encoded source", func() {
		s.putObject("enc", "my file.txt", "encoded")

		req := httptest.NewRequest("PUT", "/enc/copy.txt", nil)
		req.SetPathValue("bucket", "enc")
		req.SetPathValue("key", "copy.txt")
		req.Header.Set("x-amz-copy-source", "/enc/my%20file.txt")
		w := httptest.NewRecorder()
		handlePutObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
	})

	s.Run("invalid source", func() {
		req := httptest.NewRequest("PUT", "/b/dst.txt", nil)
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

		req := httptest.NewRequest("PUT", "/snf/dst.txt", nil)
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

		req := httptest.NewRequest("DELETE", "/db/to-delete.txt", nil)
		req.SetPathValue("bucket", "db")
		req.SetPathValue("key", "to-delete.txt")
		w := httptest.NewRecorder()
		handleDeleteObject(s.server, w, req)

		s.Equal(http.StatusNoContent, w.Code)

		// Verify deleted
		getReq := httptest.NewRequest("GET", "/db/to-delete.txt", nil)
		getReq.SetPathValue("bucket", "db")
		getReq.SetPathValue("key", "to-delete.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal(http.StatusNotFound, getW.Code)
	})

	s.Run("non-existing key returns error", func() {
		s.createBucket("di")

		req := httptest.NewRequest("DELETE", "/di/ghost.txt", nil)
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
		req := httptest.NewRequest("POST", "/do?delete", strings.NewReader(body))
		req.SetPathValue("bucket", "do")
		w := httptest.NewRecorder()
		handleDeleteObjects(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		var result DeleteObjectsResult
		s.Require().NoError(xml.Unmarshal(w.Body.Bytes(), &result))
		s.Len(result.Deleted, 2)
		s.Empty(result.Errors)

		// c.txt should still exist
		getReq := httptest.NewRequest("GET", "/do/c.txt", nil)
		getReq.SetPathValue("bucket", "do")
		getReq.SetPathValue("key", "c.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal(http.StatusOK, getW.Code)
	})

	s.Run("quiet mode omits deleted", func() {
		s.putObject("dq", "x.txt", "data")

		body := `<Delete><Quiet>true</Quiet><Object><Key>x.txt</Key></Object></Delete>`
		req := httptest.NewRequest("POST", "/dq?delete", strings.NewReader(body))
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
		req := httptest.NewRequest("POST", "/de?delete", strings.NewReader(body))
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
		req := httptest.NewRequest("POST", "/no-bucket?delete", strings.NewReader(body))
		req.SetPathValue("bucket", "no-bucket")
		w := httptest.NewRecorder()
		handleDeleteObjects(s.server, w, req)

		s.Equal(http.StatusNotFound, w.Code)
	})

	s.Run("malformed XML", func() {
		s.createBucket("dm")

		req := httptest.NewRequest("POST", "/dm?delete", strings.NewReader("not xml"))
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
		req := httptest.NewRequest("POST", "/dp?delete", strings.NewReader(body))
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
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
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
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
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
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
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
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
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
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=999-")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
		s.Equal("bytes */13", w.Header().Get("Content-Range"))
	})

	s.Run("no Range header returns full content", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusOK, w.Code)
		s.Equal("Hello, World!", w.Body.String())
	})

	s.Run("invalid scheme", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "pages=1-2")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("suffix zero returns 416", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=-0")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("suffix exceeding file size returns entire file", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
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
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=abc-5")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("non-numeric end returns 416", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=0-xyz")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("start greater than end returns 416", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=10-5")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("non-numeric suffix returns 416", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
		req.SetPathValue("bucket", "rng")
		req.SetPathValue("key", "data.txt")
		req.Header.Set("Range", "bytes=-abc")
		w := httptest.NewRecorder()
		handleGetObject(s.server, w, req)

		s.Equal(http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	s.Run("single byte range", func() {
		req := httptest.NewRequest("GET", "/rng/data.txt", nil)
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
		createReq := httptest.NewRequest("POST", "/mp/large.txt?uploads", nil)
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
		req1 := httptest.NewRequest("PUT", "/mp/large.txt?partNumber=1&uploadId="+uploadID, strings.NewReader(part1))
		req1.SetPathValue("bucket", "mp")
		req1.SetPathValue("key", "large.txt")
		req1.ContentLength = int64(len(part1))
		w1 := httptest.NewRecorder()
		handleUploadPart(s.server, w1, req1)
		s.Equal(http.StatusOK, w1.Code)
		s.NotEmpty(w1.Header().Get("ETag"))

		// UploadPart 2
		part2 := "World!"
		req2 := httptest.NewRequest("PUT", "/mp/large.txt?partNumber=2&uploadId="+uploadID, strings.NewReader(part2))
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
		completeReq := httptest.NewRequest("POST", "/mp/large.txt?uploadId="+uploadID, strings.NewReader(completeBody))
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
		getReq := httptest.NewRequest("GET", "/mp/large.txt", nil)
		getReq.SetPathValue("bucket", "mp")
		getReq.SetPathValue("key", "large.txt")
		getW := httptest.NewRecorder()
		handleGetObject(s.server, getW, getReq)
		s.Equal(http.StatusOK, getW.Code)
		s.Equal("Hello, World!", getW.Body.String())

		// Parts must be cleaned up — they should not appear in list results
		listReq := httptest.NewRequest("GET", "/mp", nil)
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

		createReq := httptest.NewRequest("POST", "/ab/f.txt?uploads", nil)
		createReq.SetPathValue("bucket", "ab")
		createReq.SetPathValue("key", "f.txt")
		createW := httptest.NewRecorder()
		handleCreateMultipartUpload(s.server, createW, createReq)
		var initResult InitiateMultipartUploadResult
		s.Require().NoError(xml.Unmarshal(createW.Body.Bytes(), &initResult))
		uploadID := initResult.UploadID

		// Upload a part
		partReq := httptest.NewRequest("PUT", "/ab/f.txt?partNumber=1&uploadId="+uploadID, strings.NewReader("data"))
		partReq.SetPathValue("bucket", "ab")
		partReq.SetPathValue("key", "f.txt")
		partReq.ContentLength = 4
		partW := httptest.NewRecorder()
		handleUploadPart(s.server, partW, partReq)
		s.Equal(http.StatusOK, partW.Code)

		// Abort
		abortReq := httptest.NewRequest("DELETE", "/ab/f.txt?uploadId="+uploadID, nil)
		abortReq.SetPathValue("bucket", "ab")
		abortReq.SetPathValue("key", "f.txt")
		abortW := httptest.NewRecorder()
		handleAbortMultipartUpload(s.server, abortW, abortReq)
		s.Equal(http.StatusNoContent, abortW.Code)

		// List should be empty (final object was never written)
		listReq := httptest.NewRequest("GET", "/ab", nil)
		listReq.SetPathValue("bucket", "ab")
		listW := httptest.NewRecorder()
		handleListObjects(s.server, listW, listReq)
		var listResult ListBucketResult
		s.Require().NoError(xml.Unmarshal(listW.Body.Bytes(), &listResult))
		s.Empty(listResult.Contents)
	})

	s.Run("parts hidden from list", func() {
		s.createBucket("hid")

		createReq := httptest.NewRequest("POST", "/hid/obj.txt?uploads", nil)
		createReq.SetPathValue("bucket", "hid")
		createReq.SetPathValue("key", "obj.txt")
		createW := httptest.NewRecorder()
		handleCreateMultipartUpload(s.server, createW, createReq)
		var initResult InitiateMultipartUploadResult
		s.Require().NoError(xml.Unmarshal(createW.Body.Bytes(), &initResult))
		uploadID := initResult.UploadID

		// Upload part but don't complete
		partReq := httptest.NewRequest("PUT", "/hid/obj.txt?partNumber=1&uploadId="+uploadID, strings.NewReader("partial"))
		partReq.SetPathValue("bucket", "hid")
		partReq.SetPathValue("key", "obj.txt")
		partReq.ContentLength = 7
		handleUploadPart(s.server, httptest.NewRecorder(), partReq)

		// Parts must not appear in list
		listReq := httptest.NewRequest("GET", "/hid", nil)
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

	req := httptest.NewRequest("GET", "/xf", nil)
	req.SetPathValue("bucket", "xf")
	w := httptest.NewRecorder()
	handleListObjects(s.server, w, req)

	s.Equal("application/xml", w.Header().Get("Content-Type"))
	s.Contains(w.Body.String(), `<?xml version="1.0" encoding="UTF-8"?>`)
	s.Contains(w.Body.String(), `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`)
}

// --- HTTP Benchmarks ---
//
// End-to-end HTTP-layer benchmarks against the S3 handler with a 1 KiB
// payload. PUT rotates the object key each iteration (so no caching
// hides work); GET repeatedly reads a single pre-populated object. The
// server runs with authentication disabled (cfg.User == "") so the
// benchmark measures transport + handler + storage cost without
// SigV4 signature verification noise.

// setupBenchServer brings up an in-process S3 listener for benchmarks.
// The backend is selected by the caller; osfs is rooted at b.TempDir()
// while memfs is a pristine in-process filesystem. The "benchbucket"
// bucket is pre-created.
func setupBenchServer(b *testing.B, typ s2.Type) *httptest.Server {
	b.Helper()
	cfg := server.DefaultConfig()
	cfg.Type = typ
	if typ == s2.TypeOSFS {
		cfg.Root = b.TempDir()
	}
	cfg.User = ""
	cfg.HealthPath = ""
	cfg.ConsoleListen = ""
	srv, err := server.NewServer(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := srv.Buckets.Create(context.Background(), "benchbucket"); err != nil {
		b.Fatal(err)
	}
	ts := httptest.NewServer(srv.S3Handler())
	b.Cleanup(ts.Close)
	return ts
}

func benchHTTPPutObject(b *testing.B, typ s2.Type) {
	srv := setupBenchServer(b, typ)
	payload := bytes.Repeat([]byte("a"), 1024) // 1 KiB
	client := srv.Client()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := fmt.Sprintf("file-%d.txt", i)
		req, err := http.NewRequest(http.MethodPut, srv.URL+"/benchbucket/"+key, bytes.NewReader(payload))
		if err != nil {
			b.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("PUT status %d", resp.StatusCode)
		}
	}
}

func benchHTTPGetObject(b *testing.B, typ s2.Type) {
	srv := setupBenchServer(b, typ)

	// Pre-populate one object that every iteration reads.
	payload := bytes.Repeat([]byte("a"), 1024)
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/benchbucket/read.txt", bytes.NewReader(payload))
	if err != nil {
		b.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		b.Fatal(err)
	}
	_ = resp.Body.Close()

	client := srv.Client()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/benchbucket/read.txt", nil)
		if err != nil {
			b.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("GET status %d", resp.StatusCode)
		}
	}
}

func BenchmarkHTTPPutObject(b *testing.B)      { benchHTTPPutObject(b, s2.TypeOSFS) }
func BenchmarkHTTPGetObject(b *testing.B)      { benchHTTPGetObject(b, s2.TypeOSFS) }
func BenchmarkHTTPPutObjectMemFS(b *testing.B) { benchHTTPPutObject(b, s2.TypeMemFS) }
func BenchmarkHTTPGetObjectMemFS(b *testing.B) { benchHTTPGetObject(b, s2.TypeMemFS) }

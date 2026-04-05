package s3api

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
)

const (
	etagMetadataKey = "s2-etag"
	defaultMaxKeys  = 1000
)

func handleListObjects(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	continuationToken := query.Get("continuation-token")
	startAfter := query.Get("start-after")

	maxKeys := defaultMaxKeys
	if v := query.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxKeys = n
		}
	}

	// continuation-token takes precedence over start-after
	after := continuationToken
	if after == "" {
		after = startAfter
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	// Fetch extra to detect truncation (+1) and account for hidden .keep files (+1)
	fetchLimit := maxKeys + 2

	var objs []s2.Object
	var prefixes []string
	if delimiter == "" {
		if after != "" {
			objs, err = strg.ListRecursiveAfter(ctx, prefix, fetchLimit, after)
		} else {
			objs, err = strg.ListRecursive(ctx, prefix, fetchLimit)
		}
	} else {
		if after != "" {
			objs, prefixes, err = strg.ListAfter(ctx, prefix, fetchLimit, after)
		} else {
			objs, prefixes, err = strg.List(ctx, prefix, fetchLimit)
		}
	}
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	objs = server.FilterKeep(objs)

	isTruncated := false
	var nextToken string
	if maxKeys > 0 && len(objs) > maxKeys {
		isTruncated = true
		nextToken = objs[maxKeys-1].Name()
		objs = objs[:maxKeys]
	} else if maxKeys == 0 {
		objs = nil
	}

	contents := make([]Content, 0, len(objs))
	for _, obj := range objs {
		contents = append(contents, Content{
			Key:          obj.Name(),
			LastModified: obj.LastModified().UTC(),
			ETag:         objectETag(obj),
			Size:         obj.Length(),
			StorageClass: "STANDARD",
		})
	}
	commonPrefixes := make([]CommonPrefix, 0, len(prefixes))
	for _, p := range prefixes {
		prefixWithDelimiter := p
		if delimiter != "" && p[len(p)-1] != delimiter[0] {
			prefixWithDelimiter += delimiter
		}
		commonPrefixes = append(commonPrefixes, CommonPrefix{
			Prefix: prefixWithDelimiter,
		})
	}

	result := ListBucketResult{
		Name:                  bucketName,
		Prefix:                prefix,
		Delimiter:             delimiter,
		KeyCount:              len(objs) + len(prefixes),
		MaxKeys:               maxKeys,
		IsTruncated:           isTruncated,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextToken,
	}

	writeXML(w, http.StatusOK, result)
}

func handleGetObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	obj, err := strg.Get(ctx, key)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	rc, err := obj.Open()
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Length", strconv.FormatUint(obj.Length(), 10))
	w.Header().Set("Last-Modified", obj.LastModified().Format(http.TimeFormat))
	w.Header().Set("ETag", objectETag(obj))

	// Write user metadata as x-amz-meta-* headers
	if md := obj.Metadata(); md != nil {
		for _, k := range md.Keys() {
			if k == etagMetadataKey {
				continue
			}
			v, _ := md.Get(k)
			w.Header().Set("x-amz-meta-"+k, v)
		}
	}

	w.WriteHeader(http.StatusOK)

	if r.Method != http.MethodHead {
		io.Copy(w, rc)
	}
}

func handlePutObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	// If x-amz-copy-source is present, this is a CopyObject request
	if copySource := r.Header.Get("x-amz-copy-source"); copySource != "" {
		handleCopyObject(s, w, r, copySource)
		return
	}

	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	// Enforce upload size limit
	maxSize := s.Config.MaxUploadSize
	if r.ContentLength > maxSize {
		writeError(w, r, "EntityTooLarge", fmt.Sprintf("Your proposed upload exceeds the maximum allowed size (%d bytes)", maxSize), http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	// Wrap body with MD5 hash calculation
	hash := md5.New()
	body := io.TeeReader(r.Body, hash)
	obj := s2.NewObjectReader(key, io.NopCloser(body), uint64(r.ContentLength))

	if err := strg.Put(ctx, obj); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	etag := `"` + hex.EncodeToString(hash.Sum(nil)) + `"`

	// Store ETag and user metadata
	md := parseMetadataHeaders(r)
	md[etagMetadataKey] = etag
	if err := strg.PutMetadata(ctx, key, md); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

// objectETag returns the ETag for an object. If the object has a stored ETag
// in metadata, it is used. Otherwise, a fallback based on length is returned.
func objectETag(obj s2.Object) string {
	if md := obj.Metadata(); md != nil {
		if etag, ok := md.Get(etagMetadataKey); ok {
			return etag
		}
	}
	return `"` + hex.EncodeToString(md5.New().Sum(nil)) + `"`
}

const metaHeaderPrefix = "X-Amz-Meta-"

func parseMetadataHeaders(r *http.Request) s2.MetadataMap {
	md := make(s2.MetadataMap)
	for key, values := range r.Header {
		if strings.HasPrefix(key, metaHeaderPrefix) && len(values) > 0 {
			metaKey := strings.ToLower(key[len(metaHeaderPrefix):])
			md[metaKey] = values[0]
		}
	}
	return md
}

func handleCopyObject(s *server.Server, w http.ResponseWriter, r *http.Request, copySource string) {
	ctx := r.Context()
	dstBucket := r.PathValue("bucket")
	dstKey := r.PathValue("key")

	// Parse copy source: /bucket/key or bucket/key (URL-encoded)
	copySource, _ = url.PathUnescape(copySource)
	copySource = strings.TrimPrefix(copySource, "/")
	slashIdx := strings.Index(copySource, "/")
	if slashIdx < 0 {
		writeError(w, r, "InvalidArgument", "Invalid x-amz-copy-source", http.StatusBadRequest)
		return
	}
	srcBucket := copySource[:slashIdx]
	srcKey := copySource[slashIdx+1:]

	srcStrg, err := s.Buckets.Get(ctx, srcBucket)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	// Read source object
	srcObj, err := srcStrg.Get(ctx, srcKey)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	rc, err := srcObj.Open()
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	defer rc.Close()

	// Write to destination
	dstStrg, err := s.Buckets.Get(ctx, dstBucket)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	dstObj := s2.NewObjectReader(dstKey, rc, srcObj.Length())
	if err := dstStrg.Put(ctx, dstObj); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	result := CopyObjectResult{
		LastModified: time.Now().UTC(),
		ETag:         objectETag(srcObj),
	}
	writeXML(w, http.StatusOK, result)
}

func handleDeleteObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	if err := strg.Delete(ctx, key); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func init() {
	server.RegisterHandleFunc("GET /s3api/{bucket}", handleListObjects)
	server.RegisterHandleFunc("GET /s3api/{bucket}/{key...}", handleGetObject)
	server.RegisterHandleFunc("HEAD /s3api/{bucket}/{key...}", handleGetObject)
	server.RegisterHandleFunc("PUT /s3api/{bucket}/{key...}", handlePutObject)
	server.RegisterHandleFunc("DELETE /s3api/{bucket}/{key...}", handleDeleteObject)
}

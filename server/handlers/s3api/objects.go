package s3api

import (
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is required for S3-compatible ETag
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

const (
	etagMetadataKey = "s2-etag"
	defaultMaxKeys  = 1000
)

// splitS3Prefix splits an S3 prefix at the last "/" so the directory portion
// can be passed to a directory-oriented List call and the remainder used as a
// basename filter on the entries.
//
//	"images/a"  -> ("images/", "a")
//	"images/"   -> ("images/", "")
//	"im"        -> ("",        "im")
//	""          -> ("",        "")
func splitS3Prefix(prefix string) (listDir, baseFilter string) {
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		return prefix[:i+1], prefix[i+1:]
	}
	return "", prefix
}

// entryBasename returns the portion of a full key after the listDir, which is
// the basename of the entry inside the listed directory.
func entryBasename(key, listDir string) string {
	return strings.TrimPrefix(key, listDir)
}

func filterObjectsByBasename(objs []s2.Object, listDir, baseFilter string) []s2.Object {
	out := objs[:0]
	for _, obj := range objs {
		if strings.HasPrefix(entryBasename(obj.Name(), listDir), baseFilter) {
			out = append(out, obj)
		}
	}
	return out
}

func filterPrefixesByBasename(prefixes []string, listDir, baseFilter string) []string {
	out := prefixes[:0]
	for _, p := range prefixes {
		if strings.HasPrefix(entryBasename(p, listDir), baseFilter) {
			out = append(out, p)
		}
	}
	return out
}

// listObjectsParams holds the query parameters for ListObjectsV2.
type listObjectsParams struct {
	bucketName        string
	prefix            string
	delimiter         string
	continuationToken string
	startAfter        string
	maxKeys           int
}

// after returns the continuation point: continuation-token wins over start-after.
func (p listObjectsParams) after() string {
	if p.continuationToken != "" {
		return p.continuationToken
	}
	return p.startAfter
}

func parseListObjectsParams(r *http.Request) listObjectsParams {
	query := r.URL.Query()
	p := listObjectsParams{
		bucketName:        r.PathValue("bucket"),
		prefix:            query.Get("prefix"),
		delimiter:         query.Get("delimiter"),
		continuationToken: query.Get("continuation-token"),
		startAfter:        query.Get("start-after"),
		maxKeys:           defaultMaxKeys,
	}
	if v := query.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.maxKeys = n
		}
	}
	return p
}

// listObjects fetches objects (and common prefixes for delimited requests)
// from storage according to params.
func listObjects(ctx context.Context, strg s2.Storage, p listObjectsParams) ([]s2.Object, []string, error) {
	// Fetch extra to detect truncation (+1) and account for hidden .keep files (+1)
	fetchLimit := p.maxKeys + 2

	if p.delimiter == "" {
		// Recursive: List already does string-prefix matching, so an
		// arbitrary S3 prefix (e.g. "im" matching "images/a.png") works as-is.
		res, err := strg.List(ctx, s2.ListOptions{
			Prefix:    p.prefix,
			After:     p.after(),
			Limit:     fetchLimit,
			Recursive: true,
		})
		if err != nil {
			return nil, nil, err
		}
		return res.Objects, nil, nil
	}

	// Delimited: S3 prefixes are arbitrary strings, but storage.List has
	// directory semantics. Split the prefix at the last "/" so we list the
	// directory portion and filter the entries by the remaining basename.
	listDir, baseFilter := splitS3Prefix(p.prefix)
	res, err := strg.List(ctx, s2.ListOptions{
		Prefix: listDir,
		After:  p.after(),
		Limit:  fetchLimit,
	})
	if err != nil {
		return nil, nil, err
	}
	objs := res.Objects
	prefixes := res.CommonPrefixes
	if baseFilter != "" {
		objs = filterObjectsByBasename(objs, listDir, baseFilter)
		prefixes = filterPrefixesByBasename(prefixes, listDir, baseFilter)
	}
	return objs, prefixes, nil
}

// applyMaxKeys clips objs to maxKeys, returning the cursor for the next page
// when the input was truncated.
func applyMaxKeys(objs []s2.Object, maxKeys int) (out []s2.Object, nextToken string, isTruncated bool) {
	if maxKeys == 0 {
		return nil, "", false
	}
	if len(objs) > maxKeys {
		return objs[:maxKeys], objs[maxKeys-1].Name(), true
	}
	return objs, "", false
}

func buildListBucketResult(p listObjectsParams, objs []s2.Object, prefixes []string, nextToken string, isTruncated bool) ListBucketResult {
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
	for _, prefix := range prefixes {
		prefixWithDelimiter := prefix
		if p.delimiter != "" && prefix[len(prefix)-1] != p.delimiter[0] {
			prefixWithDelimiter += p.delimiter
		}
		commonPrefixes = append(commonPrefixes, CommonPrefix{
			Prefix: prefixWithDelimiter,
		})
	}
	return ListBucketResult{
		Name:                  p.bucketName,
		Prefix:                p.prefix,
		Delimiter:             p.delimiter,
		KeyCount:              len(objs) + len(prefixes),
		MaxKeys:               p.maxKeys,
		IsTruncated:           isTruncated,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
		ContinuationToken:     p.continuationToken,
		NextContinuationToken: nextToken,
	}
}

func handleListObjects(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p := parseListObjectsParams(r)

	strg, err := s.Buckets.Get(ctx, p.bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	objs, prefixes, err := listObjects(ctx, strg, p)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	objs = server.FilterKeep(objs)
	objs = filterMultipart(objs)
	objs, nextToken, isTruncated := applyMaxKeys(objs, p.maxKeys)

	writeXML(w, http.StatusOK, buildListBucketResult(p, objs, prefixes, nextToken, isTruncated))
}

func handleGetObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	// A trailing-slash request (e.g. "GET /my-bucket/?location") routes
	// to the "{key...}" pattern with an empty key because Go 1.22's
	// ServeMux matches that wildcard against zero-or-more segments.
	// Delegate to the bucket-level handlers so GetBucketLocation /
	// ListObjectsV2 / HeadBucket continue to work for SDKs (notably
	// minio-go / warp) that always emit a trailing slash on bucket
	// operations.
	if key == "" {
		if r.Method == http.MethodHead {
			handleHeadBucket(s, w, r)
			return
		}
		handleBucketGET(s, w, r)
		return
	}

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

	// Write user metadata as x-amz-meta-* headers
	for k, v := range obj.Metadata() {
		if k == etagMetadataKey {
			continue
		}
		w.Header().Set("x-amz-meta-"+k, v)
	}
	w.Header().Set("Last-Modified", obj.LastModified().Format(http.TimeFormat))
	w.Header().Set("ETag", objectETag(obj))

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" && r.Method != http.MethodHead {
		handleRangeRequest(w, r, obj, rangeHeader)
		return
	}

	rc, err := obj.Open()
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Length", strconv.FormatUint(obj.Length(), 10))
	w.WriteHeader(http.StatusOK)

	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, rc)
	}
}

// parseRangeHeader parses an RFC 7233 byte-range header against a total
// content size and returns the resolved inclusive [start, end] bounds.
// The third return is false when the header is malformed or cannot be satisfied.
func parseRangeHeader(header string, total uint64) (uint64, uint64, bool) {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, false
	}
	before, after, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}

	if before == "" {
		// Suffix range: bytes=-N (last N bytes)
		n, err := strconv.ParseUint(after, 10, 64)
		if err != nil || n == 0 {
			return 0, 0, false
		}
		if n > total {
			n = total
		}
		return total - n, total - 1, true
	}

	s, err := strconv.ParseUint(before, 10, 64)
	if err != nil || s >= total {
		return 0, 0, false
	}
	if after == "" {
		return s, total - 1, true
	}
	e, err := strconv.ParseUint(after, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if e >= total {
		e = total - 1
	}
	if s > e {
		return 0, 0, false
	}
	return s, e, true
}

func writeRangeNotSatisfiable(w http.ResponseWriter, r *http.Request, total uint64) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
	writeError(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
}

func handleRangeRequest(w http.ResponseWriter, r *http.Request, obj s2.Object, rangeHeader string) {
	total := obj.Length()
	start, end, ok := parseRangeHeader(rangeHeader, total)
	if !ok {
		writeRangeNotSatisfiable(w, r, total)
		return
	}

	length := end - start + 1
	rc, err := obj.OpenRange(start, length)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.Copy(w, rc)
}

func handlePutObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	// UploadPart: PUT /{bucket}/{key}?partNumber=N&uploadId=X
	if r.URL.Query().Get("uploadId") != "" {
		handleUploadPart(s, w, r)
		return
	}
	// If x-amz-copy-source is present, this is a CopyObject request
	if copySource := r.Header.Get("x-amz-copy-source"); copySource != "" {
		handleCopyObject(s, w, r, copySource)
		return
	}

	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	// Enforce upload size limit
	maxSize := s.Config.EffectiveMaxUploadSize()
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
	hash := md5.New() // #nosec G401 -- MD5 is required for S3-compatible ETag
	decodedBody := unwrapAWSChunkedBody(r)
	body := io.TeeReader(decodedBody, hash)
	contentLength := r.ContentLength
	if v := r.Header.Get("X-Amz-Decoded-Content-Length"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			contentLength = n
		}
	}
	obj := s2.NewObjectReader(key, io.NopCloser(body), s2.MustUint64(contentLength))

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
	return `"` + hex.EncodeToString(md5.New().Sum(nil)) + `"` // #nosec G401 -- MD5 is required for S3-compatible ETag
}

const metaHeaderPrefix = "X-Amz-Meta-"

func parseMetadataHeaders(r *http.Request) s2.Metadata {
	md := make(s2.Metadata)
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
	defer func() { _ = rc.Close() }()

	// Determine metadata for the destination object.
	var md s2.Metadata
	if strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
		md = parseMetadataHeaders(r)
	} else {
		md = srcObj.Metadata().Clone()
	}

	// Write to destination
	dstStrg, err := s.Buckets.Get(ctx, dstBucket)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	dstObj := s2.NewObjectReader(dstKey, rc, srcObj.Length(), s2.WithMetadata(md))
	if err := dstStrg.Put(ctx, dstObj); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	// Persist ETag (carried from source or recomputed) alongside user metadata.
	md[etagMetadataKey] = objectETag(srcObj)
	if err := dstStrg.PutMetadata(ctx, dstKey, md); err != nil {
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

	// AbortMultipartUpload: DELETE /{bucket}/{key}?uploadId=X
	if r.URL.Query().Get("uploadId") != "" {
		handleAbortMultipartUpload(s, w, r)
		return
	}

	if err := strg.Delete(ctx, key); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleDeleteObjects(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")

	var req DeleteObjectsRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	result := DeleteObjectsResult{}
	for _, obj := range req.Objects {
		if err := strg.Delete(ctx, obj.Key); err != nil {
			code, msg, _ := s2ErrorToS3Error(err)
			result.Errors = append(result.Errors, DeleteError{
				Key:     obj.Key,
				Code:    code,
				Message: msg,
			})
			continue
		}
		if !req.Quiet {
			result.Deleted = append(result.Deleted, DeletedObject(obj))
		}
	}

	writeXML(w, http.StatusOK, result)
}

func handleBucketPOST(s *server.Server, w http.ResponseWriter, r *http.Request) {
	if _, ok := r.URL.Query()["delete"]; ok {
		handleDeleteObjects(s, w, r)
		return
	}
	writeError(w, r, "NotImplemented", "This operation is not implemented", http.StatusNotImplemented)
}

func handleBucketGET(s *server.Server, w http.ResponseWriter, r *http.Request) {
	if _, ok := r.URL.Query()["location"]; ok {
		handleGetBucketLocation(s, w, r)
		return
	}
	handleListObjects(s, w, r)
}

func init() {
	server.RegisterS3HandleFunc("GET /{bucket}", middleware.SigV4(handleBucketGET))
	server.RegisterS3HandleFunc("POST /{bucket}", middleware.SigV4(handleBucketPOST))
	server.RegisterS3HandleFunc("GET /{bucket}/{key...}", middleware.SigV4(handleGetObject))
	server.RegisterS3HandleFunc("HEAD /{bucket}/{key...}", middleware.SigV4(handleGetObject))
	server.RegisterS3HandleFunc("PUT /{bucket}/{key...}", middleware.SigV4(handlePutObject))
	server.RegisterS3HandleFunc("DELETE /{bucket}/{key...}", middleware.SigV4(handleDeleteObject))
}

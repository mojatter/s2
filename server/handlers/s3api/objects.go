package s3api

import (
	"context"
	"errors"
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
	// defaultContentType aliases the server package's value for readability.
	defaultContentType = server.DefaultContentType

	// maxObjectKeys is S3's per-request key ceiling: ListObjects' default
	// (and maximum) max-keys, and DeleteObjects' <Object> limit.
	maxObjectKeys = 1000

	// maxListFetches bounds the work one ListObjects request may do while
	// refilling a page. Stopping early yields a short page with a token.
	maxListFetches = 8
)

// requestContentType returns r's Content-Type, or "" if it sent none -- or
// only whitespace, which some HTTP clients emit for "no MIME type resolved".
func requestContentType(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("Content-Type"))
}

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

func parseListObjectsParams(r *http.Request) (listObjectsParams, error) {
	query := r.URL.Query()
	p := listObjectsParams{
		bucketName:        r.PathValue("bucket"),
		prefix:            query.Get("prefix"),
		delimiter:         query.Get("delimiter"),
		continuationToken: query.Get("continuation-token"),
		startAfter:        query.Get("start-after"),
		maxKeys:           maxObjectKeys,
	}
	if v := query.Get("max-keys"); v != "" {
		// bitSize 32: Atoi would accept up to int64 on a 64-bit build.
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 0 {
			return p, fmt.Errorf("max-keys must be an integer between 0 and 2147483647, got %q", v)
		}
		// S3 caps max-keys rather than rejecting an over-large one.
		p.maxKeys = min(int(n), maxObjectKeys)
	}
	return p, nil
}

// renderPrefix is a common prefix as S3 shows it, delimiter included.
func renderPrefix(prefix, delimiter string) string {
	if delimiter == "" || strings.HasSuffix(prefix, delimiter) {
		return prefix
	}
	return prefix + delimiter
}

// refusedCursor reports a start-after the storage will not take as a name.
func refusedCursor(cursor string) error {
	return fmt.Errorf("%w: start-after %s", s2.ErrInvalidName, cursor)
}

// pastRange reports whether name sorts beyond every key the basename filter
// can still match.
func pastRange(name, rangeEnd string) bool {
	return name > rangeEnd && !strings.HasPrefix(name, rangeEnd)
}

// listObjects collects one page, refilling it from the backend when hidden
// entries thin a fetch out. The token returned is the backend's own
// NextAfter: only the backend knows how to resume itself (#215).
func listObjects(ctx context.Context, strg s2.Storage, p listObjectsParams) (objs []s2.Object, prefixes []string, nextToken string, err error) {
	// S3 answers max-keys=0 with an empty, untruncated page, whatever the
	// request resumes from -- echoing the token back would never terminate.
	if p.maxKeys == 0 {
		return nil, nil, "", nil
	}

	// Delimited: S3 prefixes are arbitrary strings, but storage.List has
	// directory semantics. Split the prefix at the last "/" so we list the
	// directory portion and filter the entries by the remaining basename.
	listDir, baseFilter := splitS3Prefix(p.prefix)
	// A directory portion the storage refuses as a prefix begins no name s2 can address, so the answer is the empty page, not the refusal.
	if s2.ValidatePrefix(listDir) != nil {
		return nil, nil, "", nil
	}
	// The cursor is a key, not a filter: it names where a previous page
	// stopped, and no page ever ended on a name the storage would refuse. One
	// that is not a name at all is the caller's error, not an empty listing.
	if s2.ValidatePrefix(p.startAfter) != nil {
		return nil, nil, "", refusedCursor(p.startAfter)
	}

	// A recursive List does string-prefix matching of its own, so an S3
	// prefix ("im" matching "images/a.png") pushes down as-is -- as long as
	// the storage takes it. One it refuses stays on the directory portion,
	// and baseFilter does the rest.
	pushPrefix := p.delimiter == "" && s2.ValidatePrefix(p.prefix) == nil

	after := p.continuationToken
	seen := make(map[string]struct{})
	visible := 0
	for fetches := 0; visible < p.maxKeys && fetches < maxListFetches; fetches++ {
		opts := s2.ListOptions{
			Prefix:     listDir,
			After:      after,
			StartAfter: p.startAfter,
			Limit:      max(1, p.maxKeys-visible),
			Recursive:  p.delimiter == "",
		}
		if pushPrefix {
			opts.Prefix = p.prefix
		}
		res, err := strg.List(ctx, opts)
		if errors.Is(err, s2.ErrInvalidName) && pushPrefix {
			// A backend reserves names of its own, which ValidatePrefix
			// cannot know about. Blame the prefix before the cursor: falling
			// back to the directory portion loses nothing, since baseFilter
			// keeps the keys beginning with the rest -- ".meta" must still
			// find ".metadata.json" -- where a cursor compared here costs the
			// scan up to it. Cleared rather than retried per page.
			pushPrefix = false
			opts.Prefix = listDir
			res, err = strg.List(ctx, opts)
		}
		if errors.Is(err, s2.ErrInvalidName) && opts.StartAfter != "" {
			// A cursor the storage reserves is refused like any other, but
			// the same error would come from the directory portion. Ask once
			// without the cursor to tell the two apart; the page that answers
			// is not the one the caller asked for, so it is discarded.
			probe := opts
			probe.StartAfter = ""
			if _, perr := strg.List(ctx, probe); perr == nil {
				return nil, nil, "", refusedCursor(p.startAfter)
			}
		}
		if errors.Is(err, s2.ErrInvalidName) {
			// Then the directory portion itself is refused, so it holds no
			// object the prefix could match.
			return objs, prefixes, "", nil
		}
		if err != nil {
			return nil, nil, "", err
		}
		if res.NextAfter != "" && res.NextAfter == after {
			return nil, nil, "", fmt.Errorf("storage returned a list token that does not advance: %q", after)
		}

		// Read the raw page's bounds before the basename filters, which
		// compact their input in place.
		lastObj, lastPrefix := "", ""
		if n := len(res.Objects); n > 0 {
			lastObj = res.Objects[n-1].Name()
		}
		if n := len(res.CommonPrefixes); n > 0 {
			lastPrefix = res.CommonPrefixes[n-1]
		}

		pageObjs := server.FilterKeep(res.Objects)
		pagePrefixes := res.CommonPrefixes
		if baseFilter != "" {
			pageObjs = filterObjectsByBasename(pageObjs, listDir, baseFilter)
			pagePrefixes = filterPrefixesByBasename(pagePrefixes, listDir, baseFilter)
		}
		// The contract forbids a common prefix repeated across fetches; this
		// guards against a backend that breaks it.
		kept := pagePrefixes[:0]
		for _, prefix := range pagePrefixes {
			if _, dup := seen[prefix]; dup {
				continue
			}
			seen[prefix] = struct{}{}
			kept = append(kept, prefix)
		}
		pagePrefixes = kept

		objs = append(objs, pageObjs...)
		prefixes = append(prefixes, pagePrefixes...)
		visible += len(pageObjs) + len(pagePrefixes)
		after = res.NextAfter

		// Nothing past the basename's range can match. Only an object-less
		// page may be judged by its prefixes, which a backend breaking Limit
		// could return past its last object.
		if baseFilter != "" {
			rangeEnd := listDir + baseFilter
			switch {
			case lastObj != "" && pastRange(lastObj, rangeEnd):
				after = ""
			case lastObj == "" && lastPrefix != "" && pastRange(renderPrefix(lastPrefix, "/"), rangeEnd):
				after = ""
			}
		}
		if after == "" {
			break
		}
	}
	return objs, prefixes, after, nil
}

func buildListBucketResult(p listObjectsParams, objs []s2.Object, prefixes []string, nextToken string, isTruncated bool) ListBucketResult {
	contents := make([]Content, 0, len(objs))
	for _, obj := range objs {
		contents = append(contents, Content{
			Key:          obj.Name(),
			LastModified: obj.LastModified().UTC(),
			ETag:         obj.ETag(),
			Size:         obj.Length(),
			StorageClass: "STANDARD",
		})
	}
	commonPrefixes := make([]CommonPrefix, 0, len(prefixes))
	for _, prefix := range prefixes {
		commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: renderPrefix(prefix, p.delimiter)})
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
	server.SetS3Operation(r, "ListObjectsV2")
	ctx := r.Context()
	p, err := parseListObjectsParams(r)
	if err != nil {
		writeError(w, r, "InvalidArgument", err.Error(), http.StatusBadRequest)
		return
	}

	strg, err := s.Buckets.Get(ctx, p.bucketName)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}

	objs, prefixes, nextToken, err := listObjects(ctx, strg, p)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}

	writeXML(w, http.StatusOK, buildListBucketResult(p, objs, prefixes, nextToken, nextToken != ""))
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
	// ListParts shares the object route; HEAD has no such operation.
	if r.Method == http.MethodGet && r.URL.Query().Get("uploadId") != "" {
		handleListParts(s, w, r)
		return
	}
	// The AWS CLI reads the source's tags before a multipart server-side copy.
	if r.Method == http.MethodGet && r.URL.Query().Has("tagging") {
		handleGetObjectTagging(s, w, r)
		return
	}
	if r.Method == http.MethodHead {
		server.SetS3Operation(r, "HeadObject")
	} else {
		server.SetS3Operation(r, "GetObject")
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}

	obj, err := strg.Get(ctx, key)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}
	// The conditions are judged on the version Get saw, which Open then refuses to swap (#349).
	switch evalPreconditions(r.Header, "", obj) {
	case http.StatusNotModified:
		setValidatorHeaders(w, obj)
		w.WriteHeader(http.StatusNotModified)
		return
	case http.StatusPreconditionFailed:
		writeError(w, r, "PreconditionFailed", msgPreconditionFailed, http.StatusPreconditionFailed)
		return
	}

	// HEAD sends no body, so it does not open one: an Open refusing a replaced body (#349) must not fail it.
	if r.Method == http.MethodHead {
		setObjectHeaders(w, obj, key)
		w.WriteHeader(http.StatusOK)
		return
	}

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		handleRangeRequest(w, r, obj, key, rangeHeader)
		return
	}

	rc, err := obj.Open()
	if err != nil {
		writeS2Error(w, r, err)
		return
	}
	defer func() { _ = rc.Close() }()

	setObjectHeaders(w, obj, key)
	w.WriteHeader(http.StatusOK)
	server.CopyBody(w, r, rc)
}

// evalPreconditions judges the conditional headers named prefix+If-* against obj, in RFC 7232's order; 0 means none failed.
func evalPreconditions(h http.Header, prefix string, obj s2.Object) int {
	etag := obj.ETag()
	modified := obj.LastModified().Truncate(time.Second)
	// Without a modification time the date conditions say nothing, as in net/http.
	dated := !modified.IsZero()
	// Both are lists, which a client may split over several lines.
	if v := strings.Join(h.Values(prefix+"If-Match"), ","); v != "" {
		if !etagListMatches(v, etag, false) {
			return http.StatusPreconditionFailed
		}
	} else if t, ok := headerTime(h, prefix+"If-Unmodified-Since"); ok && dated && modified.After(t) {
		return http.StatusPreconditionFailed
	}
	if v := strings.Join(h.Values(prefix+"If-None-Match"), ","); v != "" {
		if etagListMatches(v, etag, true) {
			return http.StatusNotModified
		}
	} else if t, ok := headerTime(h, prefix+"If-Modified-Since"); ok && dated && !modified.After(t) {
		return http.StatusNotModified
	}
	return 0
}

// etagListMatches reports whether list, "*" or comma-separated ETags, names etag; weak ignores W/, and a bare ETag counts as quoted.
func etagListMatches(list, etag string, weak bool) bool {
	want := strings.Trim(strings.TrimPrefix(etag, "W/"), `"`)
	if !weak && strings.HasPrefix(etag, "W/") {
		want = "" // A weak stored tag has no strong match; only "*" can name it.
	}
	for c := range strings.SplitSeq(list, ",") {
		c = strings.TrimSpace(c)
		if c == "*" {
			return true
		}
		if c == "" || want == "" {
			continue
		}
		if strings.HasPrefix(c, "W/") {
			if !weak {
				continue
			}
			c = c[len("W/"):]
		}
		if strings.Trim(c, `"`) == want {
			return true
		}
	}
	return false
}

// headerTime parses an HTTP date header; a missing or malformed one is ignored, as RFC 7232 asks.
func headerTime(h http.Header, name string) (time.Time, bool) {
	v := h.Get(name)
	if v == "" {
		return time.Time{}, false
	}
	t, err := http.ParseTime(v)
	return t, err == nil
}

// setValidatorHeaders sets what a 304 carries too: obj's ETag and its Last-Modified in GMT.
func setValidatorHeaders(w http.ResponseWriter, obj s2.Object) {
	w.Header().Set("Last-Modified", obj.LastModified().UTC().Format(http.TimeFormat))
	w.Header().Set("ETag", obj.ETag())
}

// setObjectHeaders describes obj, once its body has opened, so an error answered for a body replaced since Get carries none of them (#349).
func setObjectHeaders(w http.ResponseWriter, obj s2.Object, key string) {
	for k, v := range obj.Metadata() {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	setValidatorHeaders(w, obj)
	w.Header().Set("Content-Type", server.ResolveContentType(obj, key))
	w.Header().Set("Content-Length", strconv.FormatUint(obj.Length(), 10))
}

// handleGetObjectTagging answers the empty tag set of an existing object, since s2 keeps no tags.
func handleGetObjectTagging(s *server.Server, w http.ResponseWriter, r *http.Request) {
	server.SetS3Operation(r, "GetObjectTagging")
	ctx := r.Context()
	strg, err := s.Buckets.Get(ctx, r.PathValue("bucket"))
	if err != nil {
		writeS2Error(w, r, err)
		return
	}
	if _, err := strg.Get(ctx, r.PathValue("key")); err != nil {
		writeS2Error(w, r, err)
		return
	}
	writeXML(w, http.StatusOK, Tagging{})
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

func handleRangeRequest(w http.ResponseWriter, r *http.Request, obj s2.Object, key, rangeHeader string) {
	total := obj.Length()
	start, end, ok := parseRangeHeader(rangeHeader, total)
	if !ok {
		writeRangeNotSatisfiable(w, r, total)
		return
	}

	length := end - start + 1
	rc, err := obj.OpenRange(start, length)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}
	defer func() { _ = rc.Close() }()

	setObjectHeaders(w, obj, key)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	server.CopyBody(w, r, rc)
}

func handlePutObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	// A trailing-slash CreateBucket ("PUT /my-bucket/") routes here with an empty key.
	if r.PathValue("key") == "" {
		handleCreateBucket(s, w, r)
		return
	}
	if namesSubresource(r, "uploadId", "partNumber") {
		writeError(w, r, "NotImplemented", "This operation is not implemented", http.StatusNotImplemented)
		return
	}
	// UploadPart: PUT /{bucket}/{key}?partNumber=N&uploadId=X; an empty one fails there rather than overwriting key.
	if q := r.URL.Query(); q.Has("uploadId") || q.Has("partNumber") {
		handleUploadPart(s, w, r)
		return
	}
	// If x-amz-copy-source is present, this is a CopyObject request
	if copySource := r.Header.Get(copySourceHeader); copySource != "" {
		handleCopyObject(s, w, r, copySource)
		return
	}
	server.SetS3Operation(r, "PutObject")

	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	maxSize := s.Config.EffectiveMaxUploadSize()
	body, contentLength, ok := uploadBody(w, r, maxSize)
	if !ok {
		return
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}

	// An absent Content-Type stays unstored; readers resolve it (#210).
	obj := s2.NewObjectReader(key, io.NopCloser(body), s2.MustUint64(contentLength),
		s2.WithContentType(requestContentType(r)), s2.WithMetadata(parseMetadataHeaders(r)))
	res, err := s2.Upload(ctx, strg, obj, uploadConditions(r))
	if tooLarge := (*http.MaxBytesError)(nil); errors.As(err, &tooLarge) {
		writeEntityTooLarge(w, r, maxSize)
		return
	}
	if err != nil {
		writeUploadError(w, r, err)
		return
	}

	if res.ETag != "" {
		// A backend that cannot report one is better served by no header than
		// by an empty one, which is not a valid entity tag.
		w.Header().Set("ETag", res.ETag)
	}
	w.WriteHeader(http.StatusOK)
}

const metaHeaderPrefix = "X-Amz-Meta-"

// copySourceHeader names the object a CopyObject or UploadPartCopy request copies from.
const copySourceHeader = "X-Amz-Copy-Source"

// reservedETagKey marks an object a pre-v0.18 s2-server wrote on s3/gcs roots, so clients cannot set it until v1.0 (#247).
const reservedETagKey = "s2-etag"

// uploadConditions reads a write's If-Match and If-None-Match, quoting a bare ETag as Object.ETag reports it.
func uploadConditions(r *http.Request) s2.UploadOptions {
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	if ifMatch != "" && ifMatch != "*" && !strings.HasPrefix(ifMatch, `"`) && !strings.HasPrefix(ifMatch, "W/") {
		ifMatch = `"` + ifMatch + `"`
	}
	return s2.UploadOptions{IfMatch: ifMatch, IfNoneMatch: strings.TrimSpace(r.Header.Get("If-None-Match"))}
}

func parseMetadataHeaders(r *http.Request) s2.Metadata {
	md := make(s2.Metadata)
	for key, values := range r.Header {
		if strings.HasPrefix(key, metaHeaderPrefix) && len(values) > 0 {
			metaKey := strings.ToLower(key[len(metaHeaderPrefix):])
			if metaKey != reservedETagKey {
				md[metaKey] = values[0]
			}
		}
	}
	return md
}

// parseCopySource reads x-amz-copy-source and authorizes reading it, writing the error when it fails.
func parseCopySource(w http.ResponseWriter, r *http.Request, copySource string) (bucket, key string, ok bool) {
	// S3 appends ?versionId=<id> unencoded; any other ? belongs to a key a client left unencoded.
	if i := strings.LastIndex(copySource, "?"); i >= 0 {
		if v, ok := strings.CutPrefix(copySource[i+1:], "versionId="); ok && !strings.Contains(v, "&") {
			// s2 keeps no versions, so only null names an object.
			if v != "null" {
				writeError(w, r, "NotImplemented", "This operation is not implemented", http.StatusNotImplemented)
				return "", "", false
			}
			copySource = copySource[:i]
		}
	}
	// Parse copy source: /bucket/key or bucket/key (URL-encoded)
	copySource, _ = url.PathUnescape(copySource)
	copySource = strings.TrimPrefix(copySource, "/")
	slashIdx := strings.Index(copySource, "/")
	if slashIdx < 0 {
		writeError(w, r, "InvalidArgument", "Invalid x-amz-copy-source", http.StatusBadRequest)
		return "", "", false
	}
	bucket, key = copySource[:slashIdx], copySource[slashIdx+1:]
	// The header is not a URL path, so the mux's traversal check never saw it.
	// An empty bucket would resolve to the root holding every bucket, and
	// the policy check below would run against a bucket named "".
	if s2.ValidateName(bucket) != nil || s2.ValidateName(key) != nil {
		writeError(w, r, "InvalidArgument", "Invalid x-amz-copy-source", http.StatusBadRequest)
		return "", "", false
	}

	// S3Action authorized only the destination; without this check a PutObject-only policy could read any object.
	if !server.AllowedS3Action(server.UserFromContext(r.Context()), server.ActionGetObject, bucket, key) {
		writeError(w, r, "AccessDenied", "Access Denied", http.StatusForbidden)
		return "", "", false
	}
	return bucket, key, true
}

// copySourceObject gets the copy source and holds its x-amz-copy-source-if-* conditions, writing the error when it fails.
func copySourceObject(s *server.Server, w http.ResponseWriter, r *http.Request, bucket, key string) (s2.Object, bool) {
	ctx := r.Context()
	srcStrg, err := s.Buckets.Get(ctx, bucket)
	if err != nil {
		writeS2Error(w, r, err)
		return nil, false
	}
	srcObj, err := srcStrg.Get(ctx, key)
	if err != nil {
		writeS2Error(w, r, err)
		return nil, false
	}
	// A copy answers 412 where a GET would answer 304, as S3 does.
	if evalPreconditions(r.Header, "x-amz-copy-source-", srcObj) != 0 {
		writeError(w, r, "PreconditionFailed", msgPreconditionFailed, http.StatusPreconditionFailed)
		return nil, false
	}
	return srcObj, true
}

func handleCopyObject(s *server.Server, w http.ResponseWriter, r *http.Request, copySource string) {
	server.SetS3Operation(r, "CopyObject", "copy_source", copySource)
	ctx := r.Context()
	dstBucket := r.PathValue("bucket")
	dstKey := r.PathValue("key")

	srcBucket, srcKey, ok := parseCopySource(w, r, copySource)
	if !ok {
		return
	}

	// The destination bucket is checked first, so a failed source condition cannot hide its absence.
	dstStrg, err := s.Buckets.Get(ctx, dstBucket)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}

	srcObj, ok := copySourceObject(s, w, r, srcBucket, srcKey)
	if !ok {
		return
	}

	rc, err := srcObj.Open()
	if err != nil {
		writeS2Error(w, r, err)
		return
	}
	defer func() { _ = rc.Close() }()

	// AWS keeps the source's metadata and Content-Type unless MetadataDirective=REPLACE.
	md, contentType := srcObj.Metadata().Clone(), srcObj.ContentType()
	delete(md, reservedETagKey)
	if strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
		md, contentType = parseMetadataHeaders(r), requestContentType(r)
	}

	// NopCloser: the deferred Close above owns rc, not the backend's Put.
	dstObj := s2.NewObjectReader(dstKey, io.NopCloser(rc), srcObj.Length(),
		s2.WithMetadata(md), s2.WithContentType(contentType))
	res, err := s2.Upload(ctx, dstStrg, dstObj, uploadConditions(r))
	if err != nil {
		writeUploadError(w, r, err)
		return
	}

	result := CopyObjectResult{
		LastModified: time.Now().UTC(),
		ETag:         res.ETag,
	}
	writeXML(w, http.StatusOK, result)
}

func handleDeleteObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	// A trailing-slash DeleteBucket ("DELETE /my-bucket/") routes here with an empty key.
	if key == "" {
		handleDeleteBucket(s, w, r)
		return
	}
	if namesSubresource(r, "uploadId") {
		writeError(w, r, "NotImplemented", "This operation is not implemented", http.StatusNotImplemented)
		return
	}
	// Named before Buckets.Get, which can fail before the abort is dispatched.
	if q := r.URL.Query(); q.Has("uploadId") {
		server.SetS3Operation(r, "AbortMultipartUpload", "upload_id", q.Get("uploadId"))
	} else {
		server.SetS3Operation(r, "DeleteObject")
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}

	// AbortMultipartUpload: DELETE /{bucket}/{key}?uploadId=X; an empty one fails there rather than deleting key.
	if r.URL.Query().Has("uploadId") {
		handleAbortMultipartUpload(s, w, r)
		return
	}

	if err := strg.Delete(ctx, key); err != nil {
		writeS2Error(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleDeleteObjects(s *server.Server, w http.ResponseWriter, r *http.Request) {
	server.SetS3Operation(r, "DeleteObjects")
	ctx := r.Context()
	bucketName := r.PathValue("bucket")

	var req DeleteObjectsRequest
	if !decodeXMLBody(w, r, &req) {
		return
	}
	if len(req.Objects) > maxObjectKeys {
		writeError(w, r, "MalformedXML", fmt.Sprintf("The XML you provided was not well-formed: a request may contain at most %d keys", maxObjectKeys), http.StatusBadRequest)
		return
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		writeS2Error(w, r, err)
		return
	}

	// The keys to delete only become known once the body above is decoded,
	// so SigV4's up-front check (against the coarse bucket/* resource) is
	// too imprecise for a policy scoped to a narrower prefix -- check each
	// key individually here.
	user := server.UserFromContext(ctx)

	// Unlike handleCopyObject or the console's recursive-delete handler,
	// a denied key here is reported as a per-key DeleteError and the loop
	// continues instead of aborting the whole request -- this mirrors S3's
	// documented multi-status DeleteObjects semantics (a batch delete
	// always returns partial results, not an all-or-nothing failure), so
	// don't change it to abort-on-first-denial to match those other sites.
	result := DeleteObjectsResult{}
	for _, obj := range req.Objects {
		if !server.AllowedS3Action(user, server.ActionDeleteObject, bucketName, obj.Key) {
			result.Errors = append(result.Errors, DeleteError{
				Key:     obj.Key,
				Code:    "AccessDenied",
				Message: "Access Denied",
			})
			continue
		}
		if err := strg.Delete(ctx, obj.Key); err != nil {
			code, msg, status := s2ErrorToS3Error(err)
			// status is the key's mapped class, not the response's 200 multi-status.
			logS3Error(r, code, status, err, "key", obj.Key)
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
	if _, ok := r.URL.Query()["uploads"]; ok {
		handleListMultipartUploads(s, w, r)
		return
	}
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

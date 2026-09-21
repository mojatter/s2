package s3api

import (
	"crypto/md5" // #nosec G501 -- MD5 is required for S3-compatible multipart ETag
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

// newUploadID generates a 16-byte upload ID: 4 bytes of Unix seconds, so IDs sort by initiation, then 12 random bytes.
func newUploadID() (string, error) {
	b := make([]byte, 16)
	binary.BigEndian.PutUint32(b[:4], uint32(time.Now().Unix())) //nolint:gosec // G115 -- fits until 2106
	if _, err := rand.Read(b[4:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// validUploadID reports whether id could have come from newUploadID.
func validUploadID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16
}

// bucketGeneration returns the bucket's generation; false once an error is written.
func bucketGeneration(s *server.Server, w http.ResponseWriter, r *http.Request, bucket string) (int64, bool) {
	gen, err := s.Buckets.Generation(r.Context(), bucket)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return 0, false
	}
	return gen, true
}

func handleCreateMultipartUpload(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	// The key is not written until Complete, so ask the backend now whether it
	// would take it, rather than after the client has uploaded every part.
	// Only a refused name stops us; a stat that fails for any other reason is
	// the write's problem, not the initiate's.
	if _, err := strg.Exists(ctx, key); errors.Is(err, s2.ErrInvalidName) {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	gen, ok := bucketGeneration(s, w, r, bucketName)
	if !ok {
		return
	}

	uploadID, err := newUploadID()
	if err != nil {
		writeError(w, r, "InternalError", "Failed to generate upload ID", http.StatusInternalServerError)
		return
	}

	// S3 takes the object's headers from the initiate request.
	if err := s.Multipart.Create(ctx, uploadID, bucketName, key, gen, parseMetadataHeaders(r), requestContentType(r)); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      key,
		UploadID: uploadID,
	})
}

func handleUploadPart(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	if uploadID == "" || partNumberStr == "" {
		writeError(w, r, "InvalidArgument", "Missing uploadId or partNumber", http.StatusBadRequest)
		return
	}
	if !validUploadID(uploadID) {
		writeError(w, r, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > maxUploadParts {
		writeError(w, r, "InvalidArgument", "Part number must be between 1 and 10000", http.StatusBadRequest)
		return
	}

	if _, err := s.Buckets.Get(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	gen, ok := bucketGeneration(s, w, r, bucketName)
	if !ok {
		return
	}
	if _, _, err := s.Multipart.Metadata(ctx, uploadID, bucketName, key, gen); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	maxSize := s.Config.EffectiveMaxUploadSize()
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	data, err := io.ReadAll(unwrapAWSChunkedBody(r))
	if err != nil {
		writeError(w, r, "InternalError", "Failed to read part data", http.StatusInternalServerError)
		return
	}

	etag, err := s.Multipart.PutPart(ctx, uploadID, partNumber, data)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func handleCompleteMultipartUpload(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	uploadID := r.URL.Query().Get("uploadId")
	if uploadID == "" {
		writeError(w, r, "InvalidArgument", "Missing uploadId", http.StatusBadRequest)
		return
	}
	if !validUploadID(uploadID) {
		writeError(w, r, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}

	var req CompleteMultipartUploadRequest
	if !decodeXMLBody(w, r, &req) {
		return
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	// An empty list would Put a zero-byte object, truncating the key.
	if len(req.Parts) == 0 {
		writeError(w, r, "InvalidRequest", "You must specify at least one part", http.StatusBadRequest)
		return
	}

	// S3 rejects an unordered parts list rather than sorting it; sorting also
	// hides a repeated part number, which assembles that part many times over.
	for i, p := range req.Parts {
		if p.PartNumber < 1 || p.PartNumber > maxUploadParts {
			writeError(w, r, "InvalidArgument", "Part number must be between 1 and 10000", http.StatusBadRequest)
			return
		}
		if i > 0 && p.PartNumber <= req.Parts[i-1].PartNumber {
			writeError(w, r, "InvalidPartOrder", "The list of parts was not in ascending order. Parts list must be specified in order by part number.", http.StatusBadRequest)
			return
		}
	}

	// Complete carries no headers; they were recorded at initiate time.
	gen, ok := bucketGeneration(s, w, r, bucketName)
	if !ok {
		return
	}
	md, contentType, err := s.Multipart.Metadata(ctx, uploadID, bucketName, key, gen)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	// Stat parts up front for length and ETag; bodies stream below.
	partObjs := make([]s2.Object, len(req.Parts))
	var totalLen uint64
	for i, p := range req.Parts {
		obj, err := s.Multipart.Part(ctx, uploadID, p.PartNumber)
		if errors.Is(err, s2.ErrNotExist) {
			writeError(w, r, "InvalidPart", fmt.Sprintf("Part %d not found", p.PartNumber), http.StatusBadRequest)
			return
		}
		if err != nil {
			code, msg, status := s2ErrorToS3Error(err)
			writeError(w, r, code, msg, status)
			return
		}
		partETag := strings.Trim(obj.ETag(), `"`)
		if partETag == "" {
			writeError(w, r, "InvalidPart", fmt.Sprintf("Part %d has no ETag", p.PartNumber), http.StatusBadRequest)
			return
		}
		if !strings.EqualFold(strings.Trim(strings.TrimSpace(p.ETag), `"`), partETag) {
			writeError(w, r, "InvalidPart", fmt.Sprintf("Part %d: the specified ETag did not match the uploaded part's ETag", p.PartNumber), http.StatusBadRequest)
			return
		}
		partObjs[i] = obj
		totalLen += obj.Length()
	}

	// The object's ETag is its body MD5, as for PutObject, not S3's md5-of-md5s-N form.
	hash := md5.New() // #nosec G401 -- MD5 is required for S3-compatible ETag
	pr := &partsReader{parts: partObjs}
	body := struct {
		io.Reader
		io.Closer
	}{io.TeeReader(pr, hash), pr}
	if err := strg.Put(ctx, s2.NewObjectReader(key, body, totalLen, s2.WithMetadata(md), s2.WithContentType(contentType))); err != nil {
		_ = pr.Close()
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	_ = s.Multipart.Remove(ctx, uploadID)
	etag := `"` + hex.EncodeToString(hash.Sum(nil)) + `"`

	writeXML(w, http.StatusOK, CompleteMultipartUploadResult{
		Location: "/" + bucketName + "/" + key,
		Bucket:   bucketName,
		Key:      key,
		ETag:     etag,
	})
}

func handleAbortMultipartUpload(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	uploadID := r.URL.Query().Get("uploadId")
	if uploadID == "" {
		writeError(w, r, "InvalidArgument", "Missing uploadId", http.StatusBadRequest)
		return
	}
	if !validUploadID(uploadID) {
		writeError(w, r, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}

	if _, err := s.Buckets.Get(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	gen, ok := bucketGeneration(s, w, r, bucketName)
	if !ok {
		return
	}
	if err := s.Multipart.Abort(ctx, uploadID, bucketName, key, gen); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseListLimit reads a max-uploads/max-parts value, capped at maxObjectKeys as S3 does.
func parseListLimit(r *http.Request, name string) (int, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return maxObjectKeys, true
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 0 {
		return 0, false
	}
	return min(int(n), maxObjectKeys), true
}

var s2Owner = Owner{ID: s2OwnerID, DisplayName: s2OwnerDisplayName}

func handleListMultipartUploads(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	q := r.URL.Query()
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	keyMarker, uploadIDMarker := q.Get("key-marker"), q.Get("upload-id-marker")
	maxUploads, ok := parseListLimit(r, "max-uploads")
	if !ok {
		writeError(w, r, "InvalidArgument", "max-uploads must be an integer between 0 and 2147483647", http.StatusBadRequest)
		return
	}

	if _, err := s.Buckets.Get(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	gen, ok := bucketGeneration(s, w, r, bucketName)
	if !ok {
		return
	}
	all, err := s.Multipart.Uploads(ctx)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	var uploads []server.Upload
	for _, u := range all {
		if u.Bucket != bucketName || u.Generation != gen || !strings.HasPrefix(u.Key, prefix) {
			continue
		}
		// S3 resumes after key-marker, or within it after upload-id-marker, by plain comparison.
		if keyMarker != "" && (u.Key < keyMarker || u.Key == keyMarker && (uploadIDMarker == "" || u.ID <= uploadIDMarker)) {
			continue
		}
		uploads = append(uploads, u)
	}
	// IDs lead with Unix seconds, so ID order within a key is initiation order.
	slices.SortFunc(uploads, func(a, b server.Upload) int {
		if c := strings.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})

	result := ListMultipartUploadsResult{
		Bucket:         bucketName,
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxUploads:     maxUploads,
	}
	seen := map[string]bool{}
	entries := 0
	for _, u := range uploads {
		common := ""
		if delimiter != "" {
			if i := strings.Index(u.Key[len(prefix):], delimiter); i >= 0 {
				common = u.Key[:len(prefix)+i+len(delimiter)]
			}
		}
		if common != "" && (seen[common] || common <= keyMarker) {
			continue
		}
		if entries == maxUploads {
			result.IsTruncated = maxUploads > 0
			break
		}
		entries++
		if common != "" {
			seen[common] = true
			result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{Prefix: common})
			result.NextKeyMarker, result.NextUploadIDMarker = common, ""
			continue
		}
		result.Uploads = append(result.Uploads, MultipartUpload{
			Key:          u.Key,
			UploadID:     u.ID,
			Initiator:    s2Owner,
			Owner:        s2Owner,
			StorageClass: "STANDARD",
			Initiated:    u.Initiated.UTC(),
		})
		result.NextKeyMarker, result.NextUploadIDMarker = u.Key, u.ID
	}
	if !result.IsTruncated {
		result.NextKeyMarker, result.NextUploadIDMarker = "", ""
	}
	writeXML(w, http.StatusOK, result)
}

func handleListParts(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")
	uploadID := r.URL.Query().Get("uploadId")
	if !validUploadID(uploadID) {
		writeError(w, r, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	marker := 0
	if v := r.URL.Query().Get("part-number-marker"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, "InvalidArgument", "part-number-marker must be a non-negative integer", http.StatusBadRequest)
			return
		}
		marker = n
	}
	maxParts, ok := parseListLimit(r, "max-parts")
	if !ok {
		writeError(w, r, "InvalidArgument", "max-parts must be an integer between 0 and 2147483647", http.StatusBadRequest)
		return
	}

	if _, err := s.Buckets.Get(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	gen, ok := bucketGeneration(s, w, r, bucketName)
	if !ok {
		return
	}
	if _, _, err := s.Multipart.Metadata(ctx, uploadID, bucketName, key, gen); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	parts, err := s.Multipart.Parts(ctx, uploadID)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	result := ListPartsResult{
		Bucket:           bucketName,
		Key:              key,
		UploadID:         uploadID,
		PartNumberMarker: marker,
		MaxParts:         maxParts,
		Initiator:        s2Owner,
		Owner:            s2Owner,
		StorageClass:     "STANDARD",
	}
	for _, p := range parts {
		if p.Number <= marker {
			continue
		}
		if len(result.Parts) == maxParts {
			result.IsTruncated = maxParts > 0
			break
		}
		result.Parts = append(result.Parts, Part{PartNumber: p.Number, LastModified: p.LastModified.UTC(), ETag: p.ETag, Size: p.Size})
	}
	if result.IsTruncated && len(result.Parts) > 0 {
		result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].PartNumber
	}
	writeXML(w, http.StatusOK, result)
}

// partsReader concatenates the bodies of parts, opening each one lazily.
type partsReader struct {
	parts   []s2.Object
	idx     int
	current io.ReadCloser
}

func (p *partsReader) Read(buf []byte) (int, error) {
	for {
		if p.current == nil {
			if p.idx >= len(p.parts) {
				return 0, io.EOF
			}
			rc, err := p.parts[p.idx].Open()
			if err != nil {
				return 0, err
			}
			p.current = rc
		}
		n, err := p.current.Read(buf)
		if err == io.EOF {
			_ = p.current.Close()
			p.current = nil
			p.idx++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (p *partsReader) Close() error {
	if p.current != nil {
		err := p.current.Close()
		p.current = nil
		return err
	}
	return nil
}

func handleObjectPOST(s *server.Server, w http.ResponseWriter, r *http.Request) {
	// A trailing-slash bucket request (e.g. "POST /my-bucket/?delete")
	// routes to this pattern with an empty key. Delegate to the
	// bucket-level POST handler so DeleteObjects continues to work.
	if r.PathValue("key") == "" {
		handleBucketPOST(s, w, r)
		return
	}

	q := r.URL.Query()
	if _, ok := q["uploads"]; ok {
		handleCreateMultipartUpload(s, w, r)
		return
	}
	if q.Get("uploadId") != "" {
		handleCompleteMultipartUpload(s, w, r)
		return
	}
	writeError(w, r, "NotImplemented", "This operation is not implemented", http.StatusNotImplemented)
}

func init() {
	server.RegisterS3HandleFunc("POST /{bucket}/{key...}", middleware.SigV4(handleObjectPOST))
}

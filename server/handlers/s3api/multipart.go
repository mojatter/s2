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
	"strconv"
	"strings"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

// multipartPrefix is the pre-v0.16 in-bucket layout, still hidden from listings.
const multipartPrefix = "__s2mp__/"

func filterMultipart(objs []s2.Object) []s2.Object {
	out := make([]s2.Object, 0, len(objs))
	for _, o := range objs {
		if !strings.HasPrefix(o.Name(), multipartPrefix) {
			out = append(out, o)
		}
	}
	return out
}

// newUploadID generates a 16-byte upload ID: 4 bytes of elapsed seconds
// since the server started followed by 12 bytes of random data.
func newUploadID(started time.Time) (string, error) {
	b := make([]byte, 16)
	binary.BigEndian.PutUint32(b[:4], uint32(time.Since(started).Seconds()))
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

func handleCreateMultipartUpload(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")
	key := r.PathValue("key")

	if _, err := s.Buckets.Get(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	uploadID, err := newUploadID(s.StartedAt)
	if err != nil {
		writeError(w, r, "InternalError", "Failed to generate upload ID", http.StatusInternalServerError)
		return
	}

	// S3 takes the object's headers from the initiate request.
	md := parseMetadataHeaders(r)
	// The store below is conditional, so x-amz-meta-s2-content-type would
	// otherwise reach the reserved key it shares a namespace with (#192).
	dropInternalMetadata(md)
	// An absent Content-Type stays unstored: GetObject answers with the
	// default either way, and the console can still guess from the key.
	if ct := requestContentType(r); ct != "" {
		md[contentTypeMetadataKey] = ct
	}
	if err := s.Multipart.Create(ctx, uploadID, bucketName, key, md); err != nil {
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
	if _, err := s.Multipart.Metadata(ctx, uploadID, bucketName, key); err != nil {
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

	h := md5.Sum(data) // #nosec G401 -- MD5 is required for S3-compatible ETag
	etag := `"` + hex.EncodeToString(h[:]) + `"`
	if err := s.Multipart.PutPart(ctx, uploadID, partNumber, data, etag); err != nil {
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
	md, err := s.Multipart.Metadata(ctx, uploadID, bucketName, key)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	// Stat parts up front for length and digest; bodies stream below.
	partObjs := make([]s2.Object, len(req.Parts))
	digests := make([]byte, 0, len(req.Parts)*md5.Size)
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
		digest, err := hex.DecodeString(strings.Trim(obj.Metadata()[etagMetadataKey], `"`))
		if err != nil || len(digest) != md5.Size {
			writeError(w, r, "InvalidPart", fmt.Sprintf("Part %d has no ETag", p.PartNumber), http.StatusBadRequest)
			return
		}
		if !strings.EqualFold(strings.Trim(strings.TrimSpace(p.ETag), `"`), hex.EncodeToString(digest)) {
			writeError(w, r, "InvalidPart", fmt.Sprintf("Part %d: the specified ETag did not match the uploaded part's ETag", p.PartNumber), http.StatusBadRequest)
			return
		}
		digests = append(digests, digest...)
		partObjs[i] = obj
		totalLen += obj.Length()
	}

	combined := md5.Sum(digests) // #nosec G401 -- MD5 is required for S3-compatible multipart ETag
	etag := `"` + hex.EncodeToString(combined[:]) + `-` + strconv.Itoa(len(req.Parts)) + `"`
	md[etagMetadataKey] = etag

	pr := &partsReader{parts: partObjs}
	if err := strg.Put(ctx, s2.NewObjectReader(key, pr, totalLen, s2.WithMetadata(md))); err != nil {
		_ = pr.Close()
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	_ = s.Multipart.Remove(ctx, uploadID)

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
	if err := s.Multipart.Abort(ctx, uploadID, bucketName, key); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

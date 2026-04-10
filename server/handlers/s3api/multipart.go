package s3api

import (
	"crypto/md5" // #nosec G501 -- MD5 is required for S3-compatible multipart ETag
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

// multipartPrefix is the reserved key prefix used to store in-progress part data.
// Keys with this prefix are hidden from ListObjects results.
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

func partKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s%s/%05d", multipartPrefix, uploadID, partNumber)
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

	writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      key,
		UploadID: uploadID,
	})
}

func handleUploadPart(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")

	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	if uploadID == "" || partNumberStr == "" {
		writeError(w, r, "InvalidArgument", "Missing uploadId or partNumber", http.StatusBadRequest)
		return
	}
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		writeError(w, r, "InvalidArgument", "Part number must be between 1 and 10000", http.StatusBadRequest)
		return
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
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

	partObj := s2.NewObjectBytes(partKey(uploadID, partNumber), data)
	if err := strg.Put(ctx, partObj); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	h := md5.Sum(data) // #nosec G401 -- MD5 is required for S3-compatible ETag
	etag := `"` + hex.EncodeToString(h[:]) + `"`
	_ = strg.PutMetadata(ctx, partKey(uploadID, partNumber), s2.Metadata{etagMetadataKey: etag})

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

	var req CompleteMultipartUploadRequest
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

	sort.Slice(req.Parts, func(i, j int) bool {
		return req.Parts[i].PartNumber < req.Parts[j].PartNumber
	})

	// Stat each part once up front: verify existence and compute the total
	// length required by NewObjectReader. We intentionally do NOT read part
	// bodies here — they are streamed lazily by partsReader below.
	partObjs := make([]s2.Object, len(req.Parts))
	var totalLen uint64
	for i, p := range req.Parts {
		obj, err := strg.Get(ctx, partKey(uploadID, p.PartNumber))
		if err != nil {
			writeError(w, r, "InvalidPart", fmt.Sprintf("Part %d not found", p.PartNumber), http.StatusBadRequest)
			return
		}
		partObjs[i] = obj
		totalLen += obj.Length()
	}

	// Stream all parts through a single reader, tee-ing each part into its own
	// MD5 hash as it flows by. This avoids buffering the assembled object in
	// memory — critical for the memfs backend, and a peak-memory win for all
	// backends. The multipart ETag is MD5(concat of each part's raw MD5 bytes)
	// + "-" + partCount; we collect the per-part MD5s after Put has drained
	// the reader.
	pr := &partsReader{parts: partObjs}
	finalObj := s2.NewObjectReader(key, pr, totalLen)
	if err := strg.Put(ctx, finalObj); err != nil {
		_ = pr.Close()
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	combined := md5.Sum(pr.partMD5s) // #nosec G401 -- MD5 is required for S3-compatible multipart ETag
	etag := `"` + hex.EncodeToString(combined[:]) + `-` + strconv.Itoa(len(req.Parts)) + `"`
	_ = strg.PutMetadata(ctx, key, s2.Metadata{etagMetadataKey: etag})

	// Clean up part objects
	for _, p := range req.Parts {
		_ = strg.Delete(ctx, partKey(uploadID, p.PartNumber))
	}

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

	uploadID := r.URL.Query().Get("uploadId")
	if uploadID == "" {
		writeError(w, r, "InvalidArgument", "Missing uploadId", http.StatusBadRequest)
		return
	}

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	_ = strg.DeleteRecursive(ctx, multipartPrefix+uploadID+"/")
	w.WriteHeader(http.StatusNoContent)
}

// partsReader is an io.ReadCloser that concatenates the bodies of a slice of
// s2.Object parts, opening each one lazily. As each part's body flows through
// Read, it is also hashed into a per-part MD5; after the reader is fully
// drained, partMD5s holds the concatenation of those digests in part order —
// exactly what the S3 multipart ETag formula requires.
type partsReader struct {
	parts    []s2.Object
	idx      int
	current  io.ReadCloser
	currentH hash.Hash
	partMD5s []byte
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
			p.currentH = md5.New() // #nosec G401 -- MD5 is required for S3-compatible multipart ETag
		}
		n, err := p.current.Read(buf)
		if n > 0 {
			_, _ = p.currentH.Write(buf[:n])
		}
		if err == io.EOF {
			p.partMD5s = append(p.partMD5s, p.currentH.Sum(nil)...)
			_ = p.current.Close()
			p.current = nil
			p.currentH = nil
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

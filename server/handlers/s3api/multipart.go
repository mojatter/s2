package s3api

import (
	"bytes"
	"crypto/md5" // #nosec G501 -- MD5 is required for S3-compatible multipart ETag
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

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

func newUploadID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
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

	uploadID, err := newUploadID()
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

	maxSize := s.Config.MaxUploadSize
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	data, err := io.ReadAll(r.Body)
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
	_ = strg.PutMetadata(ctx, partKey(uploadID, partNumber), s2.MetadataMap{etagMetadataKey: etag})

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

	// Assemble parts into a buffer, computing the multipart ETag as we go.
	// The multipart ETag is MD5(concat of each part's raw MD5 bytes) + "-" + partCount.
	var buf bytes.Buffer
	var partMD5s []byte
	for _, p := range req.Parts {
		obj, err := strg.Get(ctx, partKey(uploadID, p.PartNumber))
		if err != nil {
			writeError(w, r, "InvalidPart", fmt.Sprintf("Part %d not found", p.PartNumber), http.StatusBadRequest)
			return
		}
		rc, err := obj.Open()
		if err != nil {
			code, msg, status := s2ErrorToS3Error(err)
			writeError(w, r, code, msg, status)
			return
		}
		h := md5.New() // #nosec G401 -- MD5 is required for S3-compatible multipart ETag
		if _, err := io.Copy(io.MultiWriter(&buf, h), rc); err != nil {
			_ = rc.Close()
			writeError(w, r, "InternalError", "Failed to assemble parts", http.StatusInternalServerError)
			return
		}
		_ = rc.Close()
		partMD5s = append(partMD5s, h.Sum(nil)...)
	}

	finalObj := s2.NewObjectBytes(key, buf.Bytes())
	if err := strg.Put(ctx, finalObj); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	combined := md5.Sum(partMD5s) // #nosec G401 -- MD5 is required for S3-compatible multipart ETag
	etag := `"` + hex.EncodeToString(combined[:]) + `-` + strconv.Itoa(len(req.Parts)) + `"`
	_ = strg.PutMetadata(ctx, key, s2.MetadataMap{etagMetadataKey: etag})

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

func handleObjectPOST(s *server.Server, w http.ResponseWriter, r *http.Request) {
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
	server.RegisterHandleFunc("POST /s3api/{bucket}/{key...}", middleware.SigV4(handleObjectPOST))
}

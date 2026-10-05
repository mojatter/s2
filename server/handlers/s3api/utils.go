package s3api

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
)

const (
	// S3 constants
	s2OwnerID          = "s2-id"
	s2OwnerDisplayName = "s2-user"
	s2Region           = "us-east-1"

	// maxXMLRequestBody caps the size of an S3 XML request body (DeleteObjects,
	// CompleteMultipartUpload). These carry a bounded key/part list -- S3 limits
	// DeleteObjects to 1000 keys and a multipart upload to 10000 parts -- so a
	// few MiB is generous, while an uncapped xml.Decoder would let a client grow
	// the decoded slice until the process runs out of memory.
	maxXMLRequestBody = 8 << 20 // 8 MiB

	maxUploadParts = 10000 // S3's per-upload part ceiling
)

// decodeXMLBody decodes a capped XML request body, writing the S3 error
// response itself on failure. It reports whether v was decoded.
func decodeXMLBody(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxXMLRequestBody)
	if err := xml.NewDecoder(r.Body).Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, r, "MaxMessageLengthExceeded", fmt.Sprintf("Your request was too big (maximum %d bytes)", maxXMLRequestBody), http.StatusBadRequest)
			return false
		}
		writeError(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return false
	}
	return true
}

func writeXML(w http.ResponseWriter, status int, v interface{}) {
	server.WriteXML(w, status, v)
}

// namesSubresource reports whether r's query holds a key other than allowed; s2 implements no other subresource.
func namesSubresource(r *http.Request, allowed ...string) bool {
	for k := range r.URL.Query() {
		// x-id is the SDK's operation hint; X-Amz-* carries a presigned URL's signature.
		if !slices.Contains(allowed, k) && !strings.EqualFold(k, "x-id") && !strings.HasPrefix(strings.ToLower(k), "x-amz-") {
			return true
		}
	}
	return false
}

func writeError(w http.ResponseWriter, r *http.Request, code string, message string, status int) {
	server.WriteS3Error(w, r, code, message, status)
}

// ErrNoSuchBucket is returned when a bucket does not exist.
type ErrNoSuchBucket struct {
	Name string
}

func (e *ErrNoSuchBucket) Error() string {
	return "no such bucket: " + e.Name
}

// unwrapAWSChunkedBody checks for AWS chunked transfer encoding and returns
// a reader that decodes the chunked payload. If the request does not use
// AWS chunked encoding, the body is returned as-is.
//
// AWS chunked format:
//
//	<hex-size>;chunk-signature=<sig>\r\n
//	<data>\r\n
//	...
//	0;chunk-signature=<sig>\r\n
//	\r\n
//
// Different SDKs signal streaming-signed uploads differently: some set
// Content-Encoding: aws-chunked explicitly, while others (including
// minio-go, used by warp) rely solely on X-Amz-Content-Sha256 being set
// to one of the STREAMING-* payload markers. We accept both so the raw
// chunk framing never leaks into stored object bodies.
func unwrapAWSChunkedBody(r *http.Request, declared int64) io.ReadCloser {
	if !isAWSChunkedRequest(r) {
		return r.Body
	}
	return io.NopCloser(&awsChunkedReader{br: bufio.NewReader(r.Body), declared: declared})
}

// chunkedOverhead bounds aws-chunked framing: under 1/32 of the payload in 8 KiB chunks, AWS's minimum, plus the trailer.
func chunkedOverhead(maxSize int64) int64 {
	return maxSize/32 + 64<<10
}

// uploadBody caps r's payload at maxSize, counted after aws-chunked decoding, and returns its declared size; it answers and returns false when that size is absent or past the limit.
func uploadBody(w http.ResponseWriter, r *http.Request, maxSize int64) (io.Reader, int64, bool) {
	chunked := isAWSChunkedRequest(r)
	declared, bodyLimit := r.ContentLength, maxSize
	if chunked {
		bodyLimit = maxSize + min(chunkedOverhead(maxSize), math.MaxInt64-maxSize)
		declared = -1
		if n, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64); err == nil {
			declared = n
		}
	}
	if declared < 0 {
		writeError(w, r, "MissingContentLength", "You must provide the Content-Length HTTP header (X-Amz-Decoded-Content-Length for an aws-chunked body).", http.StatusLengthRequired)
		return nil, 0, false
	}
	if declared > maxSize || r.ContentLength > bodyLimit {
		writeEntityTooLarge(w, r, maxSize)
		return nil, 0, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, bodyLimit)
	if !chunked {
		return r.Body, declared, true
	}
	return http.MaxBytesReader(w, unwrapAWSChunkedBody(r, declared), maxSize), declared, true
}

// incompleteBodyError is S3's answer to a body that does not match its declared length.
func incompleteBodyError(err error) (string, string, int) {
	return "IncompleteBody", err.Error(), http.StatusBadRequest
}

func writeEntityTooLarge(w http.ResponseWriter, r *http.Request, maxSize int64) {
	writeError(w, r, "EntityTooLarge", fmt.Sprintf("Your proposed upload exceeds the maximum allowed size (%d bytes)", maxSize), http.StatusBadRequest)
}

func isAWSChunkedRequest(r *http.Request) bool {
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "aws-chunked") {
		return true
	}
	// X-Amz-Content-Sha256 values that indicate a chunked body:
	//   STREAMING-AWS4-HMAC-SHA256-PAYLOAD
	//   STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
	//   STREAMING-UNSIGNED-PAYLOAD-TRAILER
	//   STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD (SigV4a)
	if sha := r.Header.Get("X-Amz-Content-Sha256"); strings.HasPrefix(sha, "STREAMING-") {
		return true
	}
	return false
}

// errIncompleteBody reports a body that is malformed, cut short, or not the length it declared.
var errIncompleteBody = errors.New("incomplete body")

type awsChunkedReader struct {
	br        *bufio.Reader
	declared  int64 // X-Amz-Decoded-Content-Length; the decoded bytes must add up to it
	decoded   int64
	remaining int64
	done      bool
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		// Read chunk header: "<hex-size>;chunk-signature=<sig>\r\n"
		// ReadSlice, not ReadString: a header is bounded by the buffer, so a body without a newline is not held in memory.
		raw, err := r.br.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return 0, fmt.Errorf("%w: chunk header longer than %d bytes", errIncompleteBody, r.br.Size())
		}
		if err != nil {
			return 0, chunkedReadError(err)
		}
		line := strings.TrimRight(string(raw), "\r\n")
		// Extract hex size before the semicolon
		sizeStr, _, _ := strings.Cut(line, ";")
		size, err := strconv.ParseInt(sizeStr, 16, 64)
		if err != nil || size < 0 {
			return 0, fmt.Errorf("%w: invalid chunk size %q", errIncompleteBody, sizeStr)
		}
		if size > r.declared-r.decoded {
			return 0, fmt.Errorf("%w: chunk size %d with %d of %d bytes decoded", errIncompleteBody, size, r.decoded, r.declared)
		}
		if size == 0 {
			if r.decoded != r.declared {
				return 0, fmt.Errorf("%w: %d of %d bytes decoded", errIncompleteBody, r.decoded, r.declared)
			}
			r.done = true
			return 0, io.EOF
		}
		r.remaining = size
	}

	toRead := len(p)
	if int64(toRead) > r.remaining {
		toRead = int(r.remaining)
	}
	n, err := r.br.Read(p[:toRead])
	r.remaining -= int64(n)
	r.decoded += int64(n)
	if err != nil {
		return n, chunkedReadError(err)
	}
	if r.remaining == 0 {
		var crlf [2]byte
		if _, err := io.ReadFull(r.br, crlf[:]); err != nil {
			return n, chunkedReadError(err)
		}
		if crlf != [2]byte{'\r', '\n'} {
			return n, fmt.Errorf("%w: chunk data not followed by CRLF", errIncompleteBody)
		}
	}
	return n, nil
}

// chunkedReadError turns the body ending before the zero-size chunk into errIncompleteBody and passes other errors through.
func chunkedReadError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: body ended before the final chunk", errIncompleteBody)
	}
	return err
}

// uploadErrorToS3Error maps a failure from s2.Upload. ErrUnknownETag means the
// object was stored and only the read-back failed, so it must not answer
// NoSuchKey: that would tell the client its write did not land. A retry of the
// same request converges, since the write is idempotent.
func uploadErrorToS3Error(err error) (string, string, int) {
	if errors.Is(err, errIncompleteBody) {
		return incompleteBodyError(err)
	}
	if errors.Is(err, s2.ErrUnknownETag) {
		return "InternalError", err.Error(), http.StatusInternalServerError
	}
	// net/http reports a body that ends before its Content-Length this way.
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return incompleteBodyError(err)
	}
	return s2ErrorToS3Error(err)
}

func s2ErrorToS3Error(err error) (string, string, int) {
	if errors.Is(err, s2.ErrNotExist) {
		return "NoSuchKey", err.Error(), http.StatusNotFound
	}
	var bucketErr *ErrNoSuchBucket
	if errors.As(err, &bucketErr) {
		return "NoSuchBucket", err.Error(), http.StatusNotFound
	}
	var bucketNotFound *server.ErrBucketNotFound
	if errors.As(err, &bucketNotFound) {
		return "NoSuchBucket", err.Error(), http.StatusNotFound
	}
	if errors.Is(err, s2.ErrInvalidName) {
		return "InvalidArgument", err.Error(), http.StatusBadRequest
	}
	if errors.Is(err, server.ErrReservedBucketName) {
		return "InvalidBucketName", err.Error(), http.StatusBadRequest
	}
	if errors.Is(err, server.ErrNoSuchUpload) {
		return "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound
	}
	return "InternalError", err.Error(), http.StatusInternalServerError
}

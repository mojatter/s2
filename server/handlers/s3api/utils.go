package s3api

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
)

func writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, xml.Header)
	enc := xml.NewEncoder(w)
	if err := enc.Encode(v); err != nil {
		slog.Error("Failed to encode XML", "error", err)
	}
}

func writeError(w http.ResponseWriter, r *http.Request, code string, message string, status int) {
	resp := ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: "s2-request-id",
	}
	writeXML(w, status, resp)
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
func unwrapAWSChunkedBody(r *http.Request) io.ReadCloser {
	ce := r.Header.Get("Content-Encoding")
	if ce != "aws-chunked" {
		return r.Body
	}
	return io.NopCloser(&awsChunkedReader{br: bufio.NewReader(r.Body)})
}

type awsChunkedReader struct {
	br        *bufio.Reader
	remaining int
	done      bool
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		// Read chunk header: "<hex-size>;chunk-signature=<sig>\r\n"
		line, err := r.br.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")
		// Extract hex size before the semicolon
		sizeStr, _, _ := strings.Cut(line, ";")
		size, err := strconv.ParseInt(sizeStr, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid aws-chunked size: %q", sizeStr)
		}
		if size == 0 {
			r.done = true
			return 0, io.EOF
		}
		r.remaining = int(size)
	}

	toRead := len(p)
	if toRead > r.remaining {
		toRead = r.remaining
	}
	n, err := r.br.Read(p[:toRead])
	r.remaining -= n
	if r.remaining == 0 {
		// Consume trailing \r\n after chunk data
		_, _ = r.br.ReadString('\n')
	}
	return n, err
}

func s2ErrorToS3Error(err error) (string, string, int) {
	if s2.IsNotExist(err) {
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
	return "InternalError", err.Error(), http.StatusInternalServerError
}

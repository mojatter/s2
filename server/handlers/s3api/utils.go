package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
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
		fmt.Printf("Error encoding XML: %v\n", err)
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

func handleRangeRequest(w http.ResponseWriter, r *http.Request, obj s2.Object, rangeHeader string) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Length()))
		writeError(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	spec := rangeHeader[len("bytes="):]
	var start, end uint64
	total := obj.Length()

	if before, after, ok := strings.Cut(spec, "-"); ok {
		if before == "" {
			// Suffix range: bytes=-N (last N bytes)
			n, err := strconv.ParseUint(after, 10, 64)
			if err != nil || n == 0 {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
				writeError(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if n > total {
				n = total
			}
			start = total - n
			end = total - 1
		} else {
			s, err := strconv.ParseUint(before, 10, 64)
			if err != nil || s >= total {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
				writeError(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			start = s
			if after == "" {
				end = total - 1
			} else {
				e, err := strconv.ParseUint(after, 10, 64)
				if err != nil {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
					writeError(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
					return
				}
				if e >= total {
					e = total - 1
				}
				end = e
			}
		}
	} else {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
		writeError(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if start > end {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
		writeError(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	length := end - start + 1
	rc, err := obj.OpenRange(start, length)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.Copy(w, rc)
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

package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"

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

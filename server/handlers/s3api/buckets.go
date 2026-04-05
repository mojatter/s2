package s3api

import (
	"net/http"

	"github.com/mojatter/s2/server"
)

func HandleListBuckets(s *server.Server, w http.ResponseWriter, r *http.Request) {
	names, err := s.Buckets.Names()
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	buckets := make([]Bucket, 0, len(names))
	for _, name := range names {
		buckets = append(buckets, Bucket{
			Name:         name,
			CreationDate: s.Buckets.CreatedAt(r.Context(), name),
		})
	}

	result := ListAllMyBucketsResult{
		Owner: Owner{
			ID:          s2OwnerID,
			DisplayName: s2OwnerDisplayName,
		},
		Buckets: buckets,
	}

	writeXML(w, http.StatusOK, result)
}

func handleCreateBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")

	if err := s.Buckets.Create(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func handleDeleteBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("bucket")

	exists, err := s.Buckets.Exists(bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	if !exists {
		writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	if err := s.Buckets.Delete(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleHeadBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("bucket")

	exists, err := s.Buckets.Exists(bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func init() {
	server.RegisterHandleFunc("GET /s3api", HandleListBuckets)
	server.RegisterHandleFunc("GET /s3api/", HandleListBuckets)
	server.RegisterHandleFunc("PUT /s3api/{bucket}", handleCreateBucket)
	server.RegisterHandleFunc("DELETE /s3api/{bucket}", handleDeleteBucket)
	server.RegisterHandleFunc("HEAD /s3api/{bucket}", handleHeadBucket)
}

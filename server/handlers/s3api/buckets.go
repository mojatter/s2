package s3api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

func HandleListBuckets(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// S3Action defers this route entirely so FilterBucketNames can filter
	// per-bucket instead of gating the whole request on an up-front
	// s3:ListAllMyBuckets Allow (see S3Action's doc comment). But an
	// explicit Deny on that action, as a policy author porting an
	// AWS-style policy would expect, must still block the endpoint.
	if server.ExplicitlyDeniedListAllMyBuckets(server.UserFromContext(ctx)) {
		writeError(w, r, "AccessDenied", "Access Denied", http.StatusForbidden)
		return
	}
	names, err := s.Buckets.Names(ctx)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	names = server.FilterBucketNames(server.UserFromContext(ctx), names)

	buckets := make([]Bucket, 0, len(names))
	for _, name := range names {
		// One unreadable marker must not fail the whole listing.
		created, err := s.Buckets.CreatedAt(ctx, name)
		if err != nil || created.IsZero() {
			created = time.Now()
		}
		buckets = append(buckets, Bucket{Name: name, CreationDate: created})
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

// namesBucketSubresource reports whether r asks for something other than the bucket itself; s2 implements no bucket subresource.
func namesBucketSubresource(r *http.Request) bool {
	return namesSubresource(r) || r.Header.Get(copySourceHeader) != ""
}

func handleCreateBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	if namesBucketSubresource(r) {
		writeError(w, r, "NotImplemented", "This operation is not implemented", http.StatusNotImplemented)
		return
	}
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
	if namesBucketSubresource(r) {
		writeError(w, r, "NotImplemented", "This operation is not implemented", http.StatusNotImplemented)
		return
	}
	ctx := r.Context()
	bucketName := r.PathValue("bucket")

	exists, err := s.Buckets.Exists(ctx, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	if !exists {
		writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}
	// The check and the delete are not atomic; an object put in between goes with the bucket.
	holds, err := bucketHoldsObjects(ctx, s, bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	if holds {
		writeError(w, r, "BucketNotEmpty", "The bucket you tried to delete is not empty", http.StatusConflict)
		return
	}

	if err := s.Buckets.Delete(ctx, bucketName); err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// bucketHoldsObjects reports whether the bucket holds an object an S3 client can see; .keep markers do not count.
func bucketHoldsObjects(ctx context.Context, s *server.Server, name string) (bool, error) {
	strg, err := s.Buckets.Get(ctx, name)
	if err != nil {
		return false, err
	}
	opts := s2.ListOptions{Recursive: true, Limit: maxObjectKeys}
	for {
		res, err := strg.List(ctx, opts)
		if err != nil {
			return false, err
		}
		if len(server.FilterKeep(res.Objects)) > 0 {
			return true, nil
		}
		if res.NextAfter == "" {
			return false, nil
		}
		if res.NextAfter == opts.After {
			return false, fmt.Errorf("storage returned a list token that does not advance: %q", opts.After)
		}
		opts.After = res.NextAfter
	}
}

func handleHeadBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("bucket")

	exists, err := s.Buckets.Exists(r.Context(), bucketName)
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

func handleGetBucketLocation(s *server.Server, w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("bucket")

	exists, err := s.Buckets.Exists(r.Context(), bucketName)
	if err != nil {
		code, msg, status := s2ErrorToS3Error(err)
		writeError(w, r, code, msg, status)
		return
	}
	if !exists {
		writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	writeXML(w, http.StatusOK, LocationConstraint{Location: s2Region})
}

func init() {
	server.RegisterS3HandleFunc("GET /{$}", middleware.SigV4(HandleListBuckets))
	server.RegisterS3HandleFunc("PUT /{bucket}", middleware.SigV4(handleCreateBucket))
	server.RegisterS3HandleFunc("DELETE /{bucket}", middleware.SigV4(handleDeleteBucket))
	server.RegisterS3HandleFunc("HEAD /{bucket}", middleware.SigV4(handleHeadBucket))
}

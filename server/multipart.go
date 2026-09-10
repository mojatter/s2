package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/mojatter/s2"
)

// multipartDir holds in-progress multipart state beside the buckets.
const multipartDir = ".multipart"

// isHiddenBucketEntry reports whether name is s2's own state, not a bucket.
func isHiddenBucketEntry(name string) bool {
	return strings.HasPrefix(name, ".")
}

// MultipartStore stores in-progress uploads under <Root>/.multipart/<uploadId>/.
type MultipartStore struct {
	strg s2.Storage
}

func newMultipartStore(ctx context.Context, root s2.Storage) (*MultipartStore, error) {
	strg, err := root.Sub(ctx, multipartDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open multipart storage: %w", err)
	}
	return &MultipartStore{strg: strg}, nil
}

// Storage returns the storage rooted at the multipart directory.
func (ms *MultipartStore) Storage() s2.Storage {
	return ms.strg
}

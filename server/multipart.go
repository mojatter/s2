package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/mojatter/s2"
)

// multipartDir holds in-progress multipart state beside the buckets.
const multipartDir = ".multipart"

// ErrNoSuchUpload is an unknown upload, or one bound elsewhere.
var ErrNoSuchUpload = errors.New("no such upload")

// errNoRecord distinguishes a missing meta from one bound elsewhere.
var errNoRecord = errors.New("no upload record")

// isHiddenBucketEntry reports whether name is s2's own state, not a bucket.
func isHiddenBucketEntry(name string) bool {
	return strings.HasPrefix(name, ".")
}

// uploadRecord binds an upload ID to its target object.
type uploadRecord struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
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

const uploadMetaName = "meta"

func uploadPartName(n int) string {
	return fmt.Sprintf("%05d", n)
}

// upload returns id's own storage, keeping fs sidecars inside it.
func (ms *MultipartStore) upload(ctx context.Context, id string) (s2.Storage, error) {
	return ms.strg.Sub(ctx, id)
}

func isNotExist(err error) bool {
	return errors.Is(err, s2.ErrNotExist) || errors.Is(err, fs.ErrNotExist)
}

// Create records an upload of bucket/key; md is applied at completion.
func (ms *MultipartStore) Create(ctx context.Context, id, bucket, key string, md s2.Metadata) error {
	body, err := json.Marshal(uploadRecord{Bucket: bucket, Key: key})
	if err != nil {
		return err
	}
	u, err := ms.upload(ctx, id)
	if err != nil {
		return err
	}
	return u.Put(ctx, s2.NewObjectBytes(uploadMetaName, body, s2.WithMetadata(md)))
}

// load reads id's record; a meta lost before Open counts as missing.
func (ms *MultipartStore) load(ctx context.Context, id string) (uploadRecord, s2.Metadata, error) {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return uploadRecord{}, nil, err
	}
	obj, err := u.Get(ctx, uploadMetaName)
	if isNotExist(err) {
		return uploadRecord{}, nil, errNoRecord
	}
	if err != nil {
		return uploadRecord{}, nil, err
	}
	rc, err := obj.Open()
	if isNotExist(err) {
		return uploadRecord{}, nil, errNoRecord
	}
	if err != nil {
		return uploadRecord{}, nil, err
	}

	defer rc.Close() //nolint:errcheck // read-only

	var rec uploadRecord
	if err := json.NewDecoder(rc).Decode(&rec); err != nil {
		return uploadRecord{}, nil, fmt.Errorf("failed to read upload %s: %w", id, err)
	}
	md := obj.Metadata().Clone()
	if md == nil {
		md = make(s2.Metadata)
	}
	return rec, md, nil
}

// record is load plus the bucket/key binding check.
func (ms *MultipartStore) record(ctx context.Context, id, bucket, key string) (s2.Metadata, error) {
	rec, md, err := ms.load(ctx, id)
	if err != nil {
		return nil, err
	}
	if rec.Bucket != bucket || rec.Key != key {
		return nil, ErrNoSuchUpload
	}
	return md, nil
}

// Metadata returns id's headers; ErrNoSuchUpload unless it targets bucket/key.
func (ms *MultipartStore) Metadata(ctx context.Context, id, bucket, key string) (s2.Metadata, error) {
	md, err := ms.record(ctx, id, bucket, key)
	if errors.Is(err, errNoRecord) {
		return nil, ErrNoSuchUpload
	}
	return md, err
}

// Abort removes id; only a missing record skips the binding check.
func (ms *MultipartStore) Abort(ctx context.Context, id, bucket, key string) error {
	if _, err := ms.record(ctx, id, bucket, key); err != nil && !errors.Is(err, errNoRecord) {
		return err
	}
	exists, err := ms.strg.Exists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNoSuchUpload
	}
	return ms.Remove(ctx, id)
}

// PutPart stores part n of id with its ETag, dropping it if an Abort took the record.
func (ms *MultipartStore) PutPart(ctx context.Context, id string, n int, data []byte, etag string) error {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return err
	}
	putErr := u.Put(ctx, s2.NewObjectBytes(uploadPartName(n), data, s2.WithMetadata(s2.Metadata{EtagMetadataKey: etag})))
	// Only a record known to be gone drops the part; a failed probe leaves it.
	if exists, err := u.Exists(ctx, uploadMetaName); err == nil && !exists {
		_ = ms.Remove(ctx, id)
		return ErrNoSuchUpload
	}
	return putErr
}

// Part returns part n of id.
func (ms *MultipartStore) Part(ctx context.Context, id string, n int) (s2.Object, error) {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return nil, err
	}
	return u.Get(ctx, uploadPartName(n))
}

// Remove deletes everything under id, walking only id's own directory.
func (ms *MultipartStore) Remove(ctx context.Context, id string) error {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return err
	}
	return u.DeleteRecursive(ctx, "")
}

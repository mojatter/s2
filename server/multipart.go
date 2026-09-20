package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mojatter/s2"
)

// multipartDir holds in-progress multipart state beside the buckets.
const multipartDir = ".multipart"

// ErrNoSuchUpload is an unknown upload, or one bound elsewhere.
var ErrNoSuchUpload = errors.New("no such upload")

// errNoRecord distinguishes a missing meta from one bound elsewhere.
var errNoRecord = errors.New("no upload record")

// errBadRecord is a meta that exists but does not decode.
var errBadRecord = errors.New("corrupt upload record")

// isHiddenBucketEntry reports whether name is s2's own state, not a bucket.
func isHiddenBucketEntry(name string) bool {
	return strings.HasPrefix(name, ".")
}

// uploadMeta binds an upload ID to its target object in one bucket generation.
type uploadMeta struct {
	Bucket     string `json:"bucket"`
	Key        string `json:"key"`
	Generation int64  `json:"generation,omitempty"`
	// ContentType lives in the record body: a backend may assign the record object a type of its own.
	// Nil marks a v0.17 record, which kept the type under legacyContentTypeKey.
	ContentType *string `json:"content_type"`
}

// legacyContentTypeKey is where a v0.17 record kept the initiate Content-Type.
const legacyContentTypeKey = "s2-content-type"

// MultipartStore stores in-progress uploads under <Root>/.multipart/<uploadId>/.
type MultipartStore struct {
	strg s2.Storage
	// maxAge is the upload lifetime; 0 means forever.
	maxAge time.Duration
}

func newMultipartStore(ctx context.Context, root s2.Storage, maxAge time.Duration) (*MultipartStore, error) {
	strg, err := root.Sub(ctx, multipartDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open multipart storage: %w", err)
	}
	return &MultipartStore{strg: strg, maxAge: maxAge}, nil
}

// expired reports whether initiated is past maxAge; an undated record never expires.
func (ms *MultipartStore) expired(initiated time.Time) bool {
	return ms.maxAge > 0 && !initiated.IsZero() && time.Since(initiated) > ms.maxAge
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

// Create records an upload of bucket/key in generation gen; md and contentType are applied at completion.
func (ms *MultipartStore) Create(ctx context.Context, id, bucket, key string, gen int64, md s2.Metadata, contentType string) error {
	body, err := json.Marshal(uploadMeta{Bucket: bucket, Key: key, Generation: gen, ContentType: &contentType})
	if err != nil {
		return err
	}
	u, err := ms.upload(ctx, id)
	if err != nil {
		return err
	}
	return u.Put(ctx, s2.NewObjectBytes(uploadMetaName, body, s2.WithMetadata(md)))
}

// load reads id's record and initiation time; a meta lost before Open counts as missing.
func (ms *MultipartStore) load(ctx context.Context, id string) (uploadMeta, s2.Metadata, time.Time, error) {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return uploadMeta{}, nil, time.Time{}, err
	}
	obj, err := u.Get(ctx, uploadMetaName)
	if isNotExist(err) {
		return uploadMeta{}, nil, time.Time{}, errNoRecord
	}
	if err != nil {
		return uploadMeta{}, nil, time.Time{}, err
	}
	rc, err := obj.Open()
	if isNotExist(err) {
		return uploadMeta{}, nil, time.Time{}, errNoRecord
	}
	if err != nil {
		return uploadMeta{}, nil, time.Time{}, err
	}

	defer rc.Close() //nolint:errcheck // read-only

	var rec uploadMeta
	if err := json.NewDecoder(rc).Decode(&rec); err != nil {
		return uploadMeta{}, nil, time.Time{}, fmt.Errorf("%w: upload %s: %w", errBadRecord, id, err)
	}
	md := obj.Metadata().Clone()
	if md == nil {
		md = make(s2.Metadata)
	}
	if rec.ContentType == nil {
		rec.ContentType = ms.legacyContentType(obj, md)
	}
	return rec, md, obj.LastModified(), nil
}

// legacyContentType takes a v0.17 record's Content-Type out of md, or from the attribute fs lifted it into.
func (ms *MultipartStore) legacyContentType(obj s2.Object, md s2.Metadata) *string {
	ct, ok := md[legacyContentTypeKey]
	delete(md, legacyContentTypeKey)
	if !ok && isFSType(ms.strg.Type()) {
		// Only fs lifts the key; a cloud record object has a type of its own.
		ct = obj.ContentType()
	}
	return &ct
}

func isFSType(typ s2.Type) bool {
	return typ == s2.TypeOSFS || typ == s2.TypeMemFS
}

// record is load plus the bucket/key/generation binding check; age is left to the caller.
func (ms *MultipartStore) record(ctx context.Context, id, bucket, key string, gen int64) (uploadMeta, s2.Metadata, time.Time, error) {
	rec, md, initiated, err := ms.load(ctx, id)
	if err != nil {
		return uploadMeta{}, nil, time.Time{}, err
	}
	if rec.Bucket != bucket || rec.Key != key || rec.Generation != gen {
		return uploadMeta{}, nil, time.Time{}, ErrNoSuchUpload
	}
	return rec, md, initiated, nil
}

// Metadata returns id's metadata and Content-Type; ErrNoSuchUpload unless it targets bucket/key in gen and is unexpired.
func (ms *MultipartStore) Metadata(ctx context.Context, id, bucket, key string, gen int64) (s2.Metadata, string, error) {
	rec, md, initiated, err := ms.record(ctx, id, bucket, key, gen)
	if errors.Is(err, errNoRecord) {
		return nil, "", ErrNoSuchUpload
	}
	if err != nil {
		return nil, "", err
	}
	if ms.expired(initiated) {
		return nil, "", ErrNoSuchUpload
	}
	return md, *rec.ContentType, nil
}

// Abort removes id, expired or not; only a missing record skips the binding check.
func (ms *MultipartStore) Abort(ctx context.Context, id, bucket, key string, gen int64) error {
	if _, _, _, err := ms.record(ctx, id, bucket, key, gen); err != nil && !errors.Is(err, errNoRecord) {
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

// PutPart stores part n of id and returns the ETag the storage assigned, dropping it if an Abort took the record.
func (ms *MultipartStore) PutPart(ctx context.Context, id string, n int, data []byte) (string, error) {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return "", err
	}
	// The storage's ETag, not the part's MD5, is what Complete checks against.
	res, putErr := s2.Upload(ctx, u, s2.NewObjectBytes(uploadPartName(n), data), s2.UploadOptions{})
	// Only a record known to be gone drops the part; a failed probe leaves it.
	if exists, err := u.Exists(ctx, uploadMetaName); err == nil && !exists {
		_ = ms.Remove(ctx, id)
		return "", ErrNoSuchUpload
	}
	if isNotExist(putErr) {
		// A backend without s2.Uploader reads the part back, so an Abort
		// between the write and that read surfaces here.
		return "", ErrNoSuchUpload
	}
	if putErr != nil {
		return "", putErr
	}
	return res.ETag, nil
}

// Part returns part n of id.
func (ms *MultipartStore) Part(ctx context.Context, id string, n int) (s2.Object, error) {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return nil, err
	}
	return u.Get(ctx, uploadPartName(n))
}

// Sweep frees uploads past maxAge, aged by their record rather than their newest part.
func (ms *MultipartStore) Sweep(ctx context.Context) error {
	if ms.maxAge <= 0 {
		return nil
	}
	return ms.forEachUpload(ctx, ms.sweepUpload)
}

// forEachUpload calls fn with every upload ID, paging through the store.
func (ms *MultipartStore) forEachUpload(ctx context.Context, fn func(ctx context.Context, id string)) error {
	for after := ""; ; {
		res, err := ms.strg.List(ctx, s2.ListOptions{After: after})
		if err != nil {
			return fmt.Errorf("failed to list multipart uploads: %w", err)
		}
		for _, id := range res.CommonPrefixes {
			fn(ctx, id)
		}
		if res.NextAfter == "" {
			return nil
		}
		after = res.NextAfter
	}
}

// Upload is one in-progress upload as listed.
type Upload struct {
	ID         string
	Bucket     string
	Key        string
	Generation int64
	Initiated  time.Time
}

// Uploads lists live uploads in storage order; missing, corrupt and expired ones are skipped.
func (ms *MultipartStore) Uploads(ctx context.Context) ([]Upload, error) {
	var (
		uploads []Upload
		loadErr error
	)
	err := ms.forEachUpload(ctx, func(ctx context.Context, id string) {
		if loadErr != nil {
			return
		}
		rec, _, initiated, err := ms.load(ctx, id)
		if errors.Is(err, errNoRecord) || errors.Is(err, errBadRecord) {
			return
		}
		// Anything else is the backend failing; a partial list would pass as complete.
		if err != nil {
			loadErr = err
			return
		}
		if !ms.expired(initiated) {
			uploads = append(uploads, Upload{ID: id, Bucket: rec.Bucket, Key: rec.Key, Generation: rec.Generation, Initiated: initiated})
		}
	})
	if err != nil {
		return nil, err
	}
	return uploads, loadErr
}

// Part is one uploaded part as listed.
type Part struct {
	Number       int
	ETag         string
	Size         uint64
	LastModified time.Time
}

// Parts lists id's parts in number order.
func (ms *MultipartStore) Parts(ctx context.Context, id string) ([]Part, error) {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return nil, err
	}
	var parts []Part
	for after := ""; ; {
		res, err := u.List(ctx, s2.ListOptions{After: after})
		if err != nil {
			return nil, err
		}
		for _, obj := range res.Objects {
			n, err := strconv.Atoi(obj.Name())
			if err != nil || uploadPartName(n) != obj.Name() {
				continue
			}
			parts = append(parts, Part{Number: n, ETag: obj.ETag(), Size: obj.Length(), LastModified: obj.LastModified()})
		}
		if res.NextAfter == "" {
			break
		}
		after = res.NextAfter
	}
	slices.SortFunc(parts, func(a, b Part) int { return a.Number - b.Number })
	return parts, nil
}

// sweepUpload removes id if it has expired.
func (ms *MultipartStore) sweepUpload(ctx context.Context, id string) {
	started, ok, err := ms.startedAt(ctx, id)
	if err != nil {
		slog.Warn("Failed to age multipart upload", "uploadId", id, "error", err)
		return
	}
	if !ok || !ms.expired(started) {
		return
	}
	if err := ms.Remove(ctx, id); err != nil {
		slog.Warn("Failed to remove stale multipart upload", "uploadId", id, "error", err)
		return
	}
	slog.Info("Removed stale multipart upload", "uploadId", id)
}

// startedAt dates id by its record, else its newest object; ok is false when undatable.
func (ms *MultipartStore) startedAt(ctx context.Context, id string) (time.Time, bool, error) {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return time.Time{}, false, err
	}
	if obj, err := u.Get(ctx, uploadMetaName); err == nil {
		return obj.LastModified(), true, nil
	}
	return newestObject(ctx, u)
}

// newestObject returns the latest LastModified under u.
func newestObject(ctx context.Context, u s2.Storage) (time.Time, bool, error) {
	var (
		newest time.Time
		found  bool
	)
	for after := ""; ; {
		res, err := u.List(ctx, s2.ListOptions{Recursive: true, After: after})
		if err != nil {
			return time.Time{}, false, err
		}
		for _, obj := range res.Objects {
			if t := obj.LastModified(); t.After(newest) {
				newest, found = t, true
			}
		}
		if res.NextAfter == "" {
			return newest, found, nil
		}
		after = res.NextAfter
	}
}

// Remove deletes everything under id, walking only id's own directory.
func (ms *MultipartStore) Remove(ctx context.Context, id string) error {
	u, err := ms.upload(ctx, id)
	if err != nil {
		return err
	}
	return u.DeleteRecursive(ctx, "")
}

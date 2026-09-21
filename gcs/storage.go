package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	storagev1 "google.golang.org/api/storage/v1"

	"github.com/mojatter/s2"
)

// ErrRequiredConfigRoot is kept for backwards compatibility.
// Deprecated: Use s2.ErrRequiredConfigRoot instead.
var ErrRequiredConfigRoot = s2.ErrRequiredConfigRoot

type gcsStorage struct {
	client gcsClient
	bucket string
	prefix string
}

func init() {
	s2.RegisterNewStorageFunc(s2.TypeGCS, NewStorage)
}

// NewStorage creates a new GCS storage.
// cfg.Root must be set to "<bucket>" or "<bucket>/<prefix>".
// If cfg.GCS is non-nil, its fields override the default credential chain.
func NewStorage(ctx context.Context, cfg s2.Config) (s2.Storage, error) {
	if cfg.Root == "" {
		return nil, ErrRequiredConfigRoot
	}

	var opts []option.ClientOption
	if gc := cfg.GCS; gc != nil {
		if gc.CredentialsFile != "" {
			opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, gc.CredentialsFile))
		}
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs: failed to create client: %w", err)
	}
	// The JSON API client shares the credentials; PutMetadata needs its per-key metadata deletes.
	jsonOpts, err := jsonAPIOptions(opts)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	svc, err := storagev1.NewService(ctx, jsonOpts...)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("gcs: failed to create JSON API client: %w", err)
	}

	bucket, prefix := s2.ParseRoot(cfg.Root)

	return &gcsStorage{
		client: &sdkClient{c: client, svc: svc},
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (s *gcsStorage) Type() s2.Type {
	return s2.TypeGCS
}

func (s *gcsStorage) Sub(_ context.Context, prefix string) (s2.Storage, error) {
	if err := s2.ValidatePrefix(prefix); err != nil {
		return nil, err
	}
	return &gcsStorage{
		client: s.client,
		bucket: s.bucket,
		prefix: path.Join(s.prefix, prefix),
	}, nil
}

const defaultListLimit = 1000

func (s *gcsStorage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
	if err := s2.ValidatePrefix(opts.Prefix); err != nil {
		return s2.ListResult{}, err
	}
	// StartAfter is a key, and every backend joins it with the storage prefix.
	if err := s2.ValidatePrefix(opts.StartAfter); err != nil {
		return s2.ListResult{}, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}

	q := &storage.Query{
		Prefix: s.fullPrefix(opts.Prefix),
	}
	if !opts.Recursive {
		q.Delimiter = "/"
	}
	if opts.After == "" && opts.StartAfter != "" {
		q.StartOffset = joinKeepSlash(s.prefix, opts.StartAfter)
	}

	it := s.client.bucket(s.bucket).objects(ctx, q)
	// One s2 page is one SDK page, so the token the SDK hands back resumes
	// where this call stops.
	it.setMaxSize(limit)
	it.setPageToken(opts.After)

	out := s2.ListResult{
		Objects:        make([]s2.Object, 0),
		CommonPrefixes: make([]string, 0),
	}
	for {
		attrs, err := it.next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return s2.ListResult{}, fmt.Errorf("gcs: list objects: %w", err)
		}

		// A common prefix is a directory marker in a non-recursive listing.
		// StartOffset is inclusive, so objects skip the match itself.
		if attrs.Prefix != "" {
			out.CommonPrefixes = append(out.CommonPrefixes, s2.RelName(s.prefix, attrs.Prefix))
		} else if name := s2.RelName(s.prefix, attrs.Name); q.StartOffset == "" || name != opts.StartAfter {
			md, contentType := objectMetadata(attrs)
			out.Objects = append(out.Objects, &object{
				client:       s.client,
				bucket:       s.bucket,
				prefix:       s.prefix,
				name:         name,
				length:       s2.MustUint64(attrs.Size),
				lastModified: attrs.Updated,
				metadata:     md,
				contentType:  contentType,
				etag:         objectETag(attrs),
			})
		}

		// Once the buffered page is drained, the token the SDK holds resumes
		// at the next one.
		if it.remaining() == 0 {
			out.NextAfter = it.nextPageToken()
			break
		}
	}
	return out, nil
}

func (s *gcsStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	if err := s2.ValidateName(name); err != nil {
		return nil, err
	}
	obj := s.client.bucket(s.bucket).object(s.key(name))
	attrs, err := obj.attrs(ctx)
	if err != nil {
		return nil, mapNotExist(err, name)
	}
	md, contentType := objectMetadata(attrs)
	return &object{
		client:       s.client,
		bucket:       s.bucket,
		prefix:       s.prefix,
		name:         name,
		length:       s2.MustUint64(attrs.Size),
		lastModified: attrs.Updated,
		metadata:     md,
		contentType:  contentType,
		etag:         objectETag(attrs),
	}, nil
}

func (s *gcsStorage) Exists(ctx context.Context, name string) (bool, error) {
	// "" is the root; "/" is a spelling of it that no name may take.
	if name == "" {
		return true, nil
	}
	if err := s2.ValidateName(name); err != nil {
		return false, err
	}

	obj := s.client.bucket(s.bucket).object(s.key(name))
	_, err := obj.attrs(ctx)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, storage.ErrObjectNotExist) {
		return false, err
	}

	// Fallback: probe for any object under "<name>/".
	q := &storage.Query{Prefix: s.key(name) + "/"}
	it := s.client.bucket(s.bucket).objects(ctx, q)
	_, err = it.next()
	if errors.Is(err, iterator.Done) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *gcsStorage) Put(ctx context.Context, obj s2.Object) error {
	if err := s2.ValidateName(obj.Name()); err != nil {
		return err
	}
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	w := s.client.bucket(s.bucket).object(s.key(obj.Name())).newWriter(ctx, obj.Metadata(), obj.ContentType())

	if _, err := io.Copy(w, rc); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs: put %q: %w", obj.Name(), err)
	}
	return w.Close()
}

// PutMetadata replaces the user metadata; a Content-Type under the legacy s2-content-type key moves to the object's own.
// The patch answers the provider's 412 when the object changed between the read and the write, leaving it untouched.
func (s *gcsStorage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	if err := s2.ValidateName(name); err != nil {
		return err
	}
	obj := s.client.bucket(s.bucket).object(s.key(name))
	attrs, err := obj.attrs(ctx)
	if err != nil {
		return mapNotExist(err, name)
	}
	// The generation guards against an overwrite, which resets the metageneration to 1.
	p := objectPatch{
		metadata:       metadata,
		deleteKeys:     droppedKeys(attrs.Metadata, metadata),
		generation:     attrs.Generation,
		metageneration: attrs.Metageneration,
	}
	if _, contentType, legacy := liftLegacy(attrs.Metadata); legacy {
		// An empty type clears the sniffed one the SDK stored before v0.18.
		p.contentType, p.clearContentType = contentType, contentType == ""
	}
	return mapJSONNotExist(obj.patch(ctx, p), name)
}

// droppedKeys returns the keys of md that next does not carry; a merging patch can only delete them by name.
func droppedKeys(md map[string]string, next s2.Metadata) []string {
	var keys []string
	for k := range md {
		if _, ok := next[k]; !ok {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *gcsStorage) Copy(ctx context.Context, src, dst string) error {
	for _, name := range []string{src, dst} {
		if err := s2.ValidateName(name); err != nil {
			return err
		}
	}
	srcObj := s.client.bucket(s.bucket).object(s.key(src))
	dstObj := s.client.bucket(s.bucket).object(s.key(dst))
	return mapNotExist(srcObj.copyTo(ctx, dstObj), src)
}

func (s *gcsStorage) Delete(_ context.Context, name string) error {
	if err := s2.ValidateName(name); err != nil {
		return err
	}
	obj := s.client.bucket(s.bucket).object(s.key(name))
	err := obj.delete(context.Background())
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return err
}

func (s *gcsStorage) DeleteRecursive(ctx context.Context, prefix string) error {
	if err := s2.ValidatePrefix(prefix); err != nil {
		return err
	}
	q := &storage.Query{Prefix: joinKeepSlash(s.prefix, prefix)}
	it := s.client.bucket(s.bucket).objects(ctx, q)

	for {
		attrs, err := it.next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("gcs: delete recursive list: %w", err)
		}

		obj := s.client.bucket(s.bucket).object(attrs.Name)
		if err := obj.delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("gcs: delete %q: %w", attrs.Name, err)
		}
	}
	return nil
}

func (s *gcsStorage) SignedURL(_ context.Context, opts s2.SignedURLOptions) (string, error) {
	if err := s2.ValidateName(opts.Name); err != nil {
		return "", err
	}
	method := opts.Method
	if method == "" {
		method = s2.SignedURLGet
	}
	if method != s2.SignedURLGet && method != s2.SignedURLPut {
		return "", fmt.Errorf("gcs: unsupported signed URL method %q", method)
	}

	gcsOpts := &storage.SignedURLOptions{
		Method:  string(method),
		Expires: time.Now().Add(opts.TTL),
	}
	return s.client.bucket(s.bucket).signedURL(s.key(opts.Name), gcsOpts)
}

// --- helpers ---

func (s *gcsStorage) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return path.Join(s.prefix, name)
}

func (s *gcsStorage) fullPrefix(prefix string) string {
	full := path.Join(s.prefix, prefix)
	if full != "" && !strings.HasSuffix(full, "/") {
		full += "/"
	}
	return full
}

func mapNotExist(err error, name string) error {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("%w: %s", s2.ErrNotExist, name)
	}
	return err
}

// mapJSONNotExist is mapNotExist for the JSON API client, whose 404 is a googleapi error of its own.
func mapJSONNotExist(err error, name string) error {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
		return fmt.Errorf("%w: %s", s2.ErrNotExist, name)
	}
	return mapNotExist(err, name)
}

// jsonAPIOptions points the JSON API client at STORAGE_EMULATOR_HOST, which only the SDK client honours on its own.
// An emulator takes no credentials, so the caller's options are dropped along with them.
func jsonAPIOptions(opts []option.ClientOption) ([]option.ClientOption, error) {
	host := os.Getenv("STORAGE_EMULATOR_HOST")
	if host == "" {
		return opts, nil
	}
	endpoint, err := emulatorEndpoint(host)
	if err != nil {
		return nil, err
	}
	return []option.ClientOption{option.WithoutAuthentication(), option.WithEndpoint(endpoint)}, nil
}

// emulatorEndpoint returns the JSON API endpoint for host, which may carry a scheme.
func emulatorEndpoint(host string) (string, error) {
	u := &url.URL{Scheme: "http", Host: host}
	if strings.Contains(host, "://") {
		parsed, err := url.Parse(host)
		if err != nil {
			return "", fmt.Errorf("gcs: failed to parse STORAGE_EMULATOR_HOST %q: %w", host, err)
		}
		u = parsed
	}
	u.Path = "storage/v1/"
	return u.String(), nil
}

// joinKeepSlash is path.Join that keeps the trailing slash confining a listing
// to one directory: prefix's own, or the storage's when prefix is empty and so
// means everything inside it -- a Sub of "photos" must not reach "photos-old/".
func joinKeepSlash(base, prefix string) string {
	p := path.Join(base, prefix)
	if p == "" || strings.HasSuffix(p, "/") {
		return p
	}
	if prefix == "" || strings.HasSuffix(prefix, "/") {
		p += "/"
	}
	return p
}

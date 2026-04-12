package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/internal/numconv"
)

var ErrRequiredConfigRoot = errors.New("required config.root")

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

	bucket, prefix := parseRoot(cfg.Root)

	return &gcsStorage{
		client: &sdkClient{c: client},
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (s *gcsStorage) Type() s2.Type {
	return s2.TypeGCS
}

func (s *gcsStorage) Sub(_ context.Context, prefix string) (s2.Storage, error) {
	return &gcsStorage{
		client: s.client,
		bucket: s.bucket,
		prefix: path.Join(s.prefix, prefix),
	}, nil
}

const defaultListLimit = 1000

func (s *gcsStorage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
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
	if opts.After != "" {
		q.StartOffset = opts.After
	}

	it := s.client.bucket(s.bucket).objects(ctx, q)

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

		// Common prefix (directory marker in non-recursive listing).
		if attrs.Prefix != "" {
			out.CommonPrefixes = append(out.CommonPrefixes, attrs.Prefix)
			continue
		}

		name := attrs.Name
		if s.prefix != "" {
			name = name[len(s.prefix)+1:]
		}

		// StartOffset is inclusive; skip the exact match for "after" semantics.
		if opts.After != "" && name == opts.After {
			continue
		}

		if len(out.Objects) >= limit {
			out.NextAfter = name
			break
		}

		out.Objects = append(out.Objects, &object{
			client:       s.client,
			bucket:       s.bucket,
			prefix:       s.prefix,
			name:         name,
			length:       numconv.MustUint64(attrs.Size),
			lastModified: attrs.Updated,
			metadata:     s2.Metadata(attrs.Metadata),
		})
	}
	return out, nil
}

func (s *gcsStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	obj := s.client.bucket(s.bucket).object(s.key(name))
	attrs, err := obj.attrs(ctx)
	if err != nil {
		return nil, mapNotExist(err, name)
	}
	return &object{
		client:       s.client,
		bucket:       s.bucket,
		prefix:       s.prefix,
		name:         name,
		length:       numconv.MustUint64(attrs.Size),
		lastModified: attrs.Updated,
		metadata:     s2.Metadata(attrs.Metadata),
	}, nil
}

func (s *gcsStorage) Exists(ctx context.Context, name string) (bool, error) {
	if name == "" || name == "/" {
		return true, nil
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
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	w := s.client.bucket(s.bucket).object(s.key(obj.Name())).newWriter(ctx, obj.Metadata())

	if _, err := io.Copy(w, rc); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs: put %q: %w", obj.Name(), err)
	}
	return w.Close()
}

func (s *gcsStorage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	obj := s.client.bucket(s.bucket).object(s.key(name))
	_, err := obj.update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: metadata,
	})
	return err
}

func (s *gcsStorage) Copy(ctx context.Context, src, dst string) error {
	srcObj := s.client.bucket(s.bucket).object(s.key(src))
	dstObj := s.client.bucket(s.bucket).object(s.key(dst))
	return srcObj.copyTo(ctx, dstObj)
}

func (s *gcsStorage) Delete(_ context.Context, name string) error {
	obj := s.client.bucket(s.bucket).object(s.key(name))
	err := obj.delete(context.Background())
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return err
}

func (s *gcsStorage) DeleteRecursive(ctx context.Context, prefix string) error {
	q := &storage.Query{Prefix: s.key(prefix)}
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

func parseRoot(root string) (bucket, prefix string) {
	roots := strings.SplitN(strings.Trim(root, "/"), "/", 2)
	bucket = roots[0]
	if len(roots) > 1 {
		prefix = roots[1]
	}
	return
}

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

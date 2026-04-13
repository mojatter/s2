package azure

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/internal/numconv"
)

var (
	ErrRequiredConfigRoot         = errors.New("required config.root")
	ErrRequiredAccountName        = errors.New("azure: account_name or connection_string is required")
	ErrSignedURLRequiresSharedKey = errors.New("azure: signed URL requires account_name and account_key")
)

type azureStorage struct {
	client    azureClient
	container string
	prefix    string
}

func init() {
	s2.RegisterNewStorageFunc(s2.TypeAzure, NewStorage)
}

// NewStorage creates a new Azure Blob Storage backend.
// cfg.Root must be set to "<container>" or "<container>/<prefix>".
func NewStorage(ctx context.Context, cfg s2.Config) (s2.Storage, error) {
	if cfg.Root == "" {
		return nil, ErrRequiredConfigRoot
	}

	client, err := newSDKClient(cfg.Azure)
	if err != nil {
		return nil, err
	}

	roots := strings.SplitN(strings.Trim(cfg.Root, "/"), "/", 2)
	ctr := roots[0]
	prefix := ""
	if len(roots) > 1 {
		prefix = roots[1]
	}

	return &azureStorage{
		client:    client,
		container: ctr,
		prefix:    prefix,
	}, nil
}

func newSDKClient(ac *AzureConfig) (*sdkClient, error) {
	if ac == nil {
		ac = &AzureConfig{}
	}

	// 1. Connection string
	if ac.ConnectionString != "" {
		c, err := azblob.NewClientFromConnectionString(ac.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("azure: failed to create client from connection string: %w", err)
		}
		return &sdkClient{client: c}, nil
	}

	if ac.AccountName == "" {
		return nil, ErrRequiredAccountName
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", ac.AccountName)

	// 2. Shared key
	if ac.AccountKey != "" {
		sharedKey, err := azblob.NewSharedKeyCredential(ac.AccountName, ac.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("azure: failed to create shared key credential: %w", err)
		}

		c, err := azblob.NewClientWithSharedKeyCredential(serviceURL, sharedKey, nil)
		if err != nil {
			return nil, fmt.Errorf("azure: failed to create client: %w", err)
		}
		return &sdkClient{client: c, sharedKey: sharedKey}, nil
	}

	// 3. DefaultAzureCredential
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("azure: failed to create default credential: %w", err)
	}

	c, err := azblob.NewClient(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azure: failed to create client: %w", err)
	}
	return &sdkClient{client: c}, nil
}

// AzureConfig is re-exported here for use by newSDKClient.
// The canonical definition lives in the s2 package.
type AzureConfig = s2.AzureConfig

func (s *azureStorage) Type() s2.Type {
	return s2.TypeAzure
}

func (s *azureStorage) Sub(_ context.Context, prefix string) (s2.Storage, error) {
	return &azureStorage{
		client:    s.client,
		container: s.container,
		prefix:    path.Join(s.prefix, prefix),
	}, nil
}

const defaultListLimit = 1000

func (s *azureStorage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}

	delimiter := "/"
	if opts.Recursive {
		delimiter = ""
	}

	prefix := s.fullPrefix(opts.Prefix)

	res, err := s.client.listBlobs(ctx, s.container, prefix, delimiter, int32(limit), opts.After)
	if err != nil {
		return s2.ListResult{}, fmt.Errorf("azure: list blobs: %w", err)
	}

	out := s2.ListResult{
		Objects:        make([]s2.Object, 0, len(res.items)),
		CommonPrefixes: make([]string, 0, len(res.prefixes)),
	}

	for _, item := range res.items {
		name := item.name
		if s.prefix != "" {
			name = name[len(s.prefix)+1:]
		}

		out.Objects = append(out.Objects, &object{
			client:    s.client,
			container: s.container,
			prefix:    s.prefix,
			name:      name,
			length:    numconv.MustUint64(item.contentLength),
			modified:  item.lastModified,
			metadata:  s2.Metadata(fromAzureMetadata(item.metadata)),
		})
	}

	out.CommonPrefixes = append(out.CommonPrefixes, res.prefixes...)

	if res.nextMarker != "" {
		out.NextAfter = res.nextMarker
	}
	return out, nil
}

func (s *azureStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	props, err := s.client.getProperties(ctx, s.container, s.key(name))
	if err != nil {
		return nil, mapNotExist(err, name)
	}
	return &object{
		client:    s.client,
		container: s.container,
		prefix:    s.prefix,
		name:      name,
		length:    numconv.MustUint64(props.contentLength),
		modified:  props.lastModified,
		metadata:  s2.Metadata(fromAzureMetadata(props.metadata)),
	}, nil
}

func (s *azureStorage) Exists(ctx context.Context, name string) (bool, error) {
	if name == "" || name == "/" {
		return true, nil
	}

	_, err := s.client.getProperties(ctx, s.container, s.key(name))
	if err == nil {
		return true, nil
	}
	if !isBlobNotFound(err) {
		return false, err
	}

	// Fallback: probe for any blob under "<name>/".
	res, err := s.client.listBlobs(ctx, s.container, s.key(name)+"/", "", 1, "")
	if err != nil {
		return false, err
	}
	return len(res.items) > 0, nil
}

func (s *azureStorage) Put(ctx context.Context, obj s2.Object) error {
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	return s.client.upload(ctx, s.container, s.key(obj.Name()), rc, toAzureMetadata(obj.Metadata()))
}

func (s *azureStorage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	return s.client.setMetadata(ctx, s.container, s.key(name), toAzureMetadata(metadata))
}

func (s *azureStorage) Copy(ctx context.Context, src, dst string) error {
	return s.client.copyBlob(ctx, s.container, s.key(src), s.key(dst))
}

func (s *azureStorage) Delete(_ context.Context, name string) error {
	err := s.client.deleteBlob(context.Background(), s.container, s.key(name))
	if isBlobNotFound(err) {
		return nil
	}
	return err
}

func (s *azureStorage) DeleteRecursive(ctx context.Context, prefix string) error {
	fullPrefix := s.key(prefix)
	for {
		res, err := s.client.listBlobs(ctx, s.container, fullPrefix, "", int32(defaultListLimit), "")
		if err != nil {
			return fmt.Errorf("azure: delete recursive list: %w", err)
		}
		if len(res.items) == 0 {
			break
		}

		for _, item := range res.items {
			if err := s.client.deleteBlob(ctx, s.container, item.name); err != nil && !isBlobNotFound(err) {
				return fmt.Errorf("azure: delete %q: %w", item.name, err)
			}
		}
	}
	return nil
}

func (s *azureStorage) SignedURL(_ context.Context, opts s2.SignedURLOptions) (string, error) {
	method := opts.Method
	if method == "" {
		method = s2.SignedURLGet
	}
	if method != s2.SignedURLGet && method != s2.SignedURLPut {
		return "", fmt.Errorf("azure: unsupported signed URL method %q", method)
	}
	return s.client.signedURL(s.container, s.key(opts.Name), string(method), time.Now().Add(opts.TTL))
}

// --- helpers ---

func (s *azureStorage) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return path.Join(s.prefix, name)
}

func (s *azureStorage) fullPrefix(prefix string) string {
	full := path.Join(s.prefix, prefix)
	if full != "" && !strings.HasSuffix(full, "/") {
		full += "/"
	}
	return full
}

func mapNotExist(err error, name string) error {
	if isBlobNotFound(err) {
		return fmt.Errorf("%w: %s", s2.ErrNotExist, name)
	}
	return err
}

func isBlobNotFound(err error) bool {
	if err == nil {
		return false
	}
	return bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound, bloberror.ResourceNotFound)
}

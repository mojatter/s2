package azblob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/mojatter/s2"
)

// ErrRequiredConfigRoot is kept for backwards compatibility.
// Deprecated: Use s2.ErrRequiredConfigRoot instead.
var ErrRequiredConfigRoot = s2.ErrRequiredConfigRoot

var (
	ErrRequiredAccountName        = errors.New("azblob: account_name or connection_string is required")
	ErrSignedURLRequiresSharedKey = errors.New("azblob: signed URL requires account_name and account_key")
)

type azblobStorage struct {
	client    azblobClient
	container string
	prefix    string
	uri       string
}

func init() {
	s2.RegisterNewStorageFunc(s2.TypeAzblob, NewStorage)
}

// NewStorage creates a new Azure Blob Storage backend (azblob).
// cfg.Root must be set to "<container>" or "<container>/<prefix>".
func NewStorage(ctx context.Context, cfg s2.Config) (s2.Storage, error) {
	if cfg.Root == "" {
		return nil, ErrRequiredConfigRoot
	}

	client, err := newSDKClient(cfg.Azblob)
	if err != nil {
		return nil, err
	}

	ctr, prefix := s2.ParseRoot(cfg.Root)

	return &azblobStorage{
		client:    client,
		container: ctr,
		prefix:    prefix,
		uri:       s2.JoinURI(s2.JoinURI(client.serviceURL(), ctr), prefix),
	}, nil
}

func newSDKClient(ac *AzblobConfig) (*sdkClient, error) {
	if ac == nil {
		ac = &AzblobConfig{}
	}

	// 1. Connection string
	if ac.ConnectionString != "" {
		c, err := azsdk.NewClientFromConnectionString(ac.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("azblob: failed to create client from connection string: %w", err)
		}
		return &sdkClient{client: c}, nil
	}

	if ac.AccountName == "" {
		return nil, ErrRequiredAccountName
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", ac.AccountName)

	// 2. Shared key
	if ac.AccountKey != "" {
		sharedKey, err := azsdk.NewSharedKeyCredential(ac.AccountName, ac.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("azblob: failed to create shared key credential: %w", err)
		}

		c, err := azsdk.NewClientWithSharedKeyCredential(serviceURL, sharedKey, nil)
		if err != nil {
			return nil, fmt.Errorf("azblob: failed to create client: %w", err)
		}
		return &sdkClient{client: c, sharedKey: sharedKey}, nil
	}

	// 3. DefaultAzureCredential
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("azblob: failed to create default credential: %w", err)
	}

	c, err := azsdk.NewClient(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azblob: failed to create client: %w", err)
	}
	return &sdkClient{client: c}, nil
}

// AzblobConfig is re-exported here for use by newSDKClient.
// The canonical definition lives in the s2 package.
type AzblobConfig = s2.AzblobConfig

func (s *azblobStorage) Type() s2.Type {
	return s2.TypeAzblob
}

// URI returns where s stores objects, the URL of the container and prefix without its query; empty when unknown.
func (s *azblobStorage) URI() string {
	return s.uri
}

func (s *azblobStorage) Sub(_ context.Context, prefix string) (s2.Storage, error) {
	if err := s2.ValidatePrefix(prefix); err != nil {
		return nil, err
	}
	return &azblobStorage{
		client:    s.client,
		container: s.container,
		prefix:    path.Join(s.prefix, prefix),
		uri:       s2.JoinURI(s.uri, prefix),
	}, nil
}

const defaultListLimit = 1000

// List implements s2.Storage. ListOptions.After is Azure's own list marker.
// Azure has no start-after parameter, so ListOptions.StartAfter is emulated
// by paging from the start and discarding names up to it, one request per
// page skipped; prefer After for pagination.
func (s *azblobStorage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
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

	prefix := s.listPrefix(opts.Prefix, opts.Recursive)
	marker := opts.After
	startAfter := ""
	if opts.After == "" && opts.StartAfter != "" {
		startAfter = joinKeepSlash(s.prefix, opts.StartAfter)
	}

	out := s2.ListResult{
		Objects:        make([]s2.Object, 0),
		CommonPrefixes: make([]string, 0),
	}
	for {
		var res listBlobsResult
		var err error
		if opts.Recursive {
			res, err = s.client.listBlobs(ctx, s.container, prefix, int32(limit), marker)
		} else {
			res, err = s.client.listBlobsHierarchy(ctx, s.container, prefix, "/", int32(limit), marker)
		}
		if err != nil {
			return s2.ListResult{}, fmt.Errorf("azblob: list blobs: %w", err)
		}
		if res.nextMarker != "" && res.nextMarker == marker {
			return s2.ListResult{}, fmt.Errorf("azblob: list blobs: marker %q does not advance", marker)
		}
		marker = res.nextMarker

		for _, item := range res.items {
			// Names are sorted, so this only discards a leading run: what
			// survives is a page tail the marker still points just past.
			if item.name <= startAfter {
				continue
			}
			if s.isRootMarker(item.name) {
				continue
			}
			out.Objects = append(out.Objects, &object{
				client:      s.client,
				container:   s.container,
				prefix:      s.prefix,
				name:        s2.RelName(s.prefix, item.name),
				length:      s2.MustUint64(item.contentLength),
				modified:    item.lastModified,
				metadata:    s2.Metadata(fromPtrMetadata(item.metadata)),
				contentType: item.contentType,
				etag:        blobETag(item.contentMD5, item.etag),
				version:     item.etag,
				class:       storageClassOf(item.accessTier, item.archiveStatus),
			})
		}

		for _, p := range res.prefixes {
			// A prefix is an entry of its own: S3 drops one not past start-after.
			if p <= startAfter {
				continue
			}
			out.CommonPrefixes = append(out.CommonPrefixes, s2.RelName(s.prefix, p))
		}

		if len(out.Objects) > 0 || len(out.CommonPrefixes) > 0 || marker == "" {
			break
		}
	}

	out.NextAfter = marker
	return out, nil
}

func (s *azblobStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	if err := s2.ValidateName(name); err != nil {
		return nil, err
	}
	props, err := s.client.getProperties(ctx, s.container, s.key(name))
	if err != nil {
		return nil, mapNotExist(err, name)
	}
	if props.archived() {
		return nil, errArchived(name)
	}
	return s.objectFromProps(name, props), nil
}

var _ s2.Stater = (*azblobStorage)(nil)

// Stat implements s2.Stater with the properties Get reads, returning an archived blob whose reads fail.
func (s *azblobStorage) Stat(ctx context.Context, name string) (s2.Object, error) {
	if err := s2.ValidateName(name); err != nil {
		return nil, err
	}
	props, err := s.client.getProperties(ctx, s.container, s.key(name))
	if err != nil {
		return nil, mapNotExist(err, name)
	}
	return s.objectFromProps(name, props), nil
}

// objectFromProps builds name's object from its properties.
func (s *azblobStorage) objectFromProps(name string, props blobProps) *object {
	md := s2.Metadata(fromPtrMetadata(props.metadata))
	if md == nil {
		md = make(s2.Metadata)
	}
	return &object{
		client:      s.client,
		container:   s.container,
		prefix:      s.prefix,
		name:        name,
		length:      s2.MustUint64(props.contentLength),
		modified:    props.lastModified,
		metadata:    md,
		contentType: props.contentType,
		etag:        blobETag(props.contentMD5, props.etag),
		version:     props.etag,
		class:       storageClassOf(props.accessTier, props.archiveStatus),
	}
}

var _ s2.Opener = (*azblobStorage)(nil)

// Open implements s2.Opener with one download, no properties call first.
func (s *azblobStorage) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := s2.ValidateName(name); err != nil {
		return nil, err
	}
	o := &object{client: s.client, container: s.container, prefix: s.prefix, name: name}
	return o.download(ctx, 0, 0)
}

func (s *azblobStorage) Exists(ctx context.Context, name string) (bool, error) {
	// "" is the root; "/" is a spelling of it that no name may take.
	if name == "" {
		return true, nil
	}
	if err := s2.ValidateName(name); err != nil {
		return false, err
	}

	_, err := s.client.getProperties(ctx, s.container, s.key(name))
	if err == nil {
		return true, nil
	}
	if !isBlobNotFound(err) {
		return false, err
	}

	// A marker is an object like any other, so only its own lookup answers for it.
	if strings.HasSuffix(name, "/") {
		return false, nil
	}

	// Fallback: probe for any blob under "<name>/".
	res, err := s.client.listBlobs(ctx, s.container, s.key(name)+"/", 1, "")
	if err != nil {
		return false, err
	}
	return len(res.items) > 0, nil
}

var _ s2.Uploader = (*azblobStorage)(nil)

func (s *azblobStorage) Put(ctx context.Context, obj s2.Object) error {
	_, err := s.Upload(ctx, obj, s2.UploadOptions{})
	return err
}

// Upload implements s2.Uploader. upload reports the Content-MD5 it sent, which
// is the ETag a later Get derives.
func (s *azblobStorage) Upload(ctx context.Context, obj s2.Object, opts s2.UploadOptions) (s2.UploadResult, error) {
	if err := s2.ValidateName(obj.Name()); err != nil {
		return s2.UploadResult{}, err
	}
	ifMatch, ifNoneMatch, err := s.uploadConds(ctx, obj.Name(), opts)
	if err != nil {
		return s2.UploadResult{}, err
	}
	rc, err := obj.Open()
	if err != nil {
		return s2.UploadResult{}, err
	}
	defer func() { _ = rc.Close() }()

	etag, err := s.client.upload(ctx, s.container, s.key(obj.Name()), rc, toPtrMetadata(obj.Metadata()), obj.ContentType(), ifMatch, ifNoneMatch)
	// If-None-Match "*" over a blob answers BlobAlreadyExists; If-Match over one deleted since the read may answer BlobNotFound.
	if opts.Conditional() && bloberror.HasCode(err, bloberror.ConditionNotMet, bloberror.BlobAlreadyExists) || opts.IfMatch != "" && bloberror.HasCode(err, bloberror.BlobNotFound) {
		return s2.UploadResult{}, fmt.Errorf("%w: %s: %w", s2.ErrPreconditionFailed, obj.Name(), err)
	}
	if err != nil {
		return s2.UploadResult{}, err
	}
	return s2.UploadResult{ETag: etag}, nil
}

// uploadConds turns opts into the upload's access conditions: an ETag is judged on the properties read here and held by Azure's own ETag.
func (s *azblobStorage) uploadConds(ctx context.Context, name string, opts s2.UploadOptions) (ifMatch, ifNoneMatch string, err error) {
	if !opts.Conditional() {
		return "", "", nil
	}
	if err := opts.Validate(); err != nil {
		return "", "", err
	}
	if opts.IfMatch == "" {
		return "", "*", nil
	}
	props, err := s.client.getProperties(ctx, s.container, s.key(name))
	if isBlobNotFound(err) {
		return "", "", opts.Check(name, "", false)
	}
	if err != nil {
		return "", "", err
	}
	if err := opts.Check(name, blobETag(props.contentMD5, props.etag), true); err != nil {
		return "", "", err
	}
	// An empty ETag would send no access condition and write unconditionally.
	if props.etag == "" {
		return "", "", fmt.Errorf("azblob: IfMatch: endpoint reports no ETag: %w", errors.ErrUnsupported)
	}
	return props.etag, "", nil
}

func (s *azblobStorage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	if err := s2.ValidateName(name); err != nil {
		return err
	}
	return s.client.setMetadata(ctx, s.container, s.key(name), toPtrMetadata(metadata))
}

func (s *azblobStorage) Copy(ctx context.Context, src, dst string) error {
	for _, name := range []string{src, dst} {
		if err := s2.ValidateName(name); err != nil {
			return err
		}
	}
	err := s.client.copyBlob(ctx, s.container, s.key(src), s.key(dst))
	if bloberror.HasCode(err, bloberror.BlobArchived) {
		return errArchived(src)
	}
	// A rehydrating source answers PendingCopyOperation, as a copy still pending onto dst does.
	if bloberror.HasCode(err, bloberror.PendingCopyOperation) {
		if props, perr := s.client.getProperties(ctx, s.container, s.key(src)); perr == nil && props.archived() {
			return errArchived(src)
		}
	}
	return mapNotExist(err, src)
}

func (s *azblobStorage) Delete(_ context.Context, name string) error {
	if err := s2.ValidateName(name); err != nil {
		return err
	}
	err := s.client.deleteBlob(context.Background(), s.container, s.key(name))
	if isBlobNotFound(err) {
		return nil
	}
	return err
}

func (s *azblobStorage) DeleteRecursive(ctx context.Context, prefix string) error {
	if err := s2.ValidatePrefix(prefix); err != nil {
		return err
	}
	fullPrefix := joinKeepSlash(s.prefix, prefix)
	for {
		res, err := s.client.listBlobs(ctx, s.container, fullPrefix, int32(defaultListLimit), "")
		if err != nil {
			return fmt.Errorf("azblob: delete recursive list: %w", err)
		}
		if len(res.items) == 0 {
			break
		}

		for _, item := range res.items {
			if err := s.client.deleteBlob(ctx, s.container, item.name); err != nil && !isBlobNotFound(err) {
				return fmt.Errorf("azblob: delete %q: %w", item.name, err)
			}
		}
	}
	return nil
}

func (s *azblobStorage) SignedURL(_ context.Context, opts s2.SignedURLOptions) (string, error) {
	if err := s2.ValidateName(opts.Name); err != nil {
		return "", err
	}
	method := opts.Method
	if method == "" {
		method = s2.SignedURLGet
	}
	if method != s2.SignedURLGet && method != s2.SignedURLPut {
		return "", fmt.Errorf("azblob: unsupported signed URL method %q", method)
	}
	return s.client.signedURL(s.container, s.key(opts.Name), string(method), time.Now().Add(opts.TTL))
}

// --- helpers ---

// key is name's full key; not path.Join, which drops a marker's trailing "/" and so names its neighbour.
func (s *azblobStorage) key(name string) string {
	return s2.Key(s.prefix, name)
}

// listPrefix is the query prefix for a listing; only a delimited one treats the caller's prefix as a directory.
func (s *azblobStorage) listPrefix(prefix string, recursive bool) string {
	// joinKeepSlash gives the empty prefix a trailing slash, so a listing that
	// narrows nothing still cannot reach a sibling of the storage prefix.
	full := joinKeepSlash(s.prefix, prefix)
	if full != "" && !recursive && !strings.HasSuffix(full, "/") {
		full += "/"
	}
	return full
}

// archived reports whether the blob is in the Archive tier, where it cannot be read until rehydrated.
func (p blobProps) archived() bool {
	return p.accessTier == string(blob.AccessTierArchive)
}

// storageClassOf names the Archive tier DEEP_ARCHIVE, as a mark that the blob must be rehydrated before reading, and no other tier.
func storageClassOf(accessTier, archiveStatus string) s2.StorageClass {
	if accessTier != string(blob.AccessTierArchive) {
		return s2.StorageClass{}
	}
	return s2.StorageClass{
		Name:      s3DeepArchive,
		Restoring: strings.HasPrefix(archiveStatus, "rehydrate-pending-"),
	}
}

// s3DeepArchive is S3's class name an archived blob reports.
const s3DeepArchive = "DEEP_ARCHIVE"

// errArchived is the error for name when it is archived and not rehydrated.
func errArchived(name string) error {
	return fmt.Errorf("%w: %s is archived", errors.ErrUnsupported, name)
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

// isRootMarker reports the marker of the storage's own prefix, which List hides as its name would be "".
func (s *azblobStorage) isRootMarker(key string) bool {
	return s.prefix != "" && key == s.prefix+"/"
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

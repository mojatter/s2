package s3

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mojatter/s2"
	"github.com/mojatter/s2/internal/numconv"
)

var (
	ErrRequiredConfigRoot = errors.New("required config.root")
)

type clientAPI interface {
	s3.ListObjectsV2APIClient
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
}

type presignClientAPI interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

type storage struct {
	client        clientAPI
	presignClient presignClientAPI
	bucket        string
	prefix        string
}

func init() {
	s2.RegisterNewStorageFunc(s2.TypeS3, NewStorage)
}

// NewStorage creates a new S3 storage with the default AWS configuration.
// If cfg.S3 is non-nil, its fields override the AWS SDK defaults.
func NewStorage(ctx context.Context, cfg s2.Config) (s2.Storage, error) {
	if cfg.Root == "" {
		return nil, ErrRequiredConfigRoot
	}

	var opts []func(*awsconfig.LoadOptions) error
	if sc := cfg.S3; sc != nil {
		if sc.Region != "" {
			opts = append(opts, awsconfig.WithRegion(sc.Region))
		}
		if sc.AccessKeyID != "" && sc.SecretAccessKey != "" {
			opts = append(opts, awsconfig.WithCredentialsProvider(
				aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
					return aws.Credentials{
						AccessKeyID:     sc.AccessKeyID,
						SecretAccessKey: sc.SecretAccessKey,
						Source:          "s2.S3Config",
					}, nil
				}),
			))
		}
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if sc := cfg.S3; sc != nil && sc.EndpointURL != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(sc.EndpointURL)
			o.UsePathStyle = true
		})
	}

	roots := strings.SplitN(strings.Trim(cfg.Root, "/"), "/", 2)
	bucket := roots[0]
	prefix := ""
	if len(roots) > 1 {
		prefix = roots[1]
	}
	return &storage{
		client: s3.NewFromConfig(awsCfg, s3Opts...),
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (s *storage) Type() s2.Type {
	return s2.TypeS3
}

func (s *storage) Sub(ctx context.Context, prefix string) (s2.Storage, error) {
	return &storage{
		client: s.client,
		bucket: s.bucket,
		prefix: path.Join(s.prefix, prefix),
	}, nil
}

// defaultListLimit caps a List call when ListOptions.Limit is unset (0).
// It mirrors S3's default ListObjectsV2 page size.
const defaultListLimit = 1000

func (s *storage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	delimiter := "/"
	if opts.Recursive {
		delimiter = ""
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		MaxKeys: aws.Int32(int32(limit)),
	}
	inputPrefix := path.Join(s.prefix, opts.Prefix)
	if delimiter != "" && inputPrefix != "" && !strings.HasSuffix(inputPrefix, delimiter) {
		inputPrefix += delimiter
	}
	if inputPrefix != "" {
		input.Prefix = aws.String(inputPrefix)
	}
	if opts.After != "" {
		input.StartAfter = aws.String(opts.After)
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}

	res, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return s2.ListResult{}, fmt.Errorf("failed to list objects: %w", err)
	}

	out := s2.ListResult{
		CommonPrefixes: make([]string, 0, len(res.CommonPrefixes)),
		Objects:        make([]s2.Object, 0, len(res.Contents)),
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, aws.ToString(p.Prefix))
	}
	for _, c := range res.Contents {
		key := aws.ToString(c.Key)
		if s.prefix != "" {
			key = key[len(s.prefix)+1:]
		}
		out.Objects = append(out.Objects, &object{
			client:       s.client,
			bucket:       s.bucket,
			prefix:       s.prefix,
			name:         key,
			length:       numconv.MustUint64(aws.ToInt64(c.Size)),
			lastModified: aws.ToTime(c.LastModified),
		})
	}
	if aws.ToBool(res.IsTruncated) {
		out.NextAfter = aws.ToString(res.NextContinuationToken)
	}
	return out, nil
}

func (s *storage) Get(ctx context.Context, name string) (s2.Object, error) {
	params, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, name)),
	})
	if err != nil {
		var noSuchKeyErr *s3types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return nil, fmt.Errorf("%w: %s", s2.ErrNotExist, name)
		}
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	return &object{
		client:       s.client,
		bucket:       s.bucket,
		prefix:       s.prefix,
		name:         name,
		length:       numconv.MustUint64(aws.ToInt64(params.ContentLength)),
		lastModified: aws.ToTime(params.LastModified),
		metadata:     s2.Metadata(params.Metadata),
	}, nil
}

// Exists reports whether name resolves to either a leaf object or a
// non-empty prefix under the storage root.
//
// Implementation: try HeadObject first (the common case — the caller
// is checking a regular object). On a 404 (NotFound) fall back to a
// 1-key ListObjectsV2 probe with the prefix "<name>/" so that
// "directory" semantics behave like the fs backends — anything
// underneath the prefix counts the prefix itself as present.
//
// Caveat carried over from S3's data model: a "directory" with no
// objects underneath cannot be detected (S3 has no standalone
// directory primitive), so a sub-prefix that the user logically
// considers empty will always report Exists == false. The storage
// root itself (name == "") always reports true: constructing this
// Storage means the caller has accepted the underlying S3 bucket as
// existing, and forcing a HeadBucket round-trip on every call adds
// no information that the first real read or write would not.
func (s *storage) Exists(ctx context.Context, name string) (bool, error) {
	if name == "" || name == "/" {
		return true, nil
	}
	key := path.Join(s.prefix, name)

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	if !isNotFoundErr(err) {
		return false, err
	}

	// Fallback: probe for any object under "<name>/" so that prefixes
	// laid down by Buckets.Create (or any caller that writes nested
	// objects) report as present.
	listOut, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(key + "/"),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, err
	}
	return len(listOut.Contents) > 0, nil
}

// isNotFoundErr is the not-found check shared by Exists and any
// caller that needs to map an AWS SDK error back to s2.ErrNotExist.
// The aws-sdk-go-v2 surface returns a typed error for HeadObject 404s,
// but other paths hit string-shaped errors, so we cover both.
func isNotFoundErr(err error) bool {
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "NotFound") || strings.Contains(msg, "404")
}

func (s *storage) Put(ctx context.Context, obj s2.Object) error {
	rc, err := obj.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(path.Join(s.prefix, obj.Name())),
		Body:          rc,
		ContentLength: aws.Int64(numconv.MustInt64(obj.Length())),
		Metadata:      obj.Metadata(),
	})
	return err
}

func (s *storage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	key := path.Join(s.prefix, name)
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(path.Join(s.bucket, key)),
		Metadata:          metadata,
		MetadataDirective: s3types.MetadataDirectiveReplace,
	})
	return err
}

func (s *storage) Copy(ctx context.Context, src, dst string) error {
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(path.Join(s.prefix, dst)),
		CopySource: aws.String(path.Join(s.bucket, s.prefix, src)),
	})
	return err
}

func (s *storage) Delete(ctx context.Context, name string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, name)),
	})
	return err
}

func (s *storage) DeleteRecursive(ctx context.Context, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(path.Join(s.prefix, prefix)),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to next page: %w", err)
		}

		if len(page.Contents) == 0 {
			continue
		}

		objects := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, c := range page.Contents {
			objects = append(objects, s3types.ObjectIdentifier{Key: c.Key})
		}

		_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &s3types.Delete{
				Objects: objects,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
	}
	return nil
}

func (s *storage) SignedURL(ctx context.Context, opts s2.SignedURLOptions) (string, error) {
	method := opts.Method
	if method == "" {
		method = s2.SignedURLGet
	}
	if method != s2.SignedURLGet && method != s2.SignedURLPut {
		return "", fmt.Errorf("s3 storage: unsupported signed URL method %q", method)
	}
	if s.presignClient == nil {
		s3Client, ok := s.client.(*s3.Client)
		if !ok {
			return "", fmt.Errorf("unknown client type: %T", s.client)
		}
		s.presignClient = s3.NewPresignClient(s3Client)
	}
	key := aws.String(path.Join(s.prefix, opts.Name))
	switch method {
	case s2.SignedURLPut:
		req, err := s.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    key,
		}, s3.WithPresignExpires(opts.TTL))
		if err != nil {
			return "", fmt.Errorf("failed to presign put object: %w", err)
		}
		return req.URL, nil
	default: // SignedURLGet
		req, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    key,
		}, s3.WithPresignExpires(opts.TTL))
		if err != nil {
			var noSuchKeyErr *s3types.NoSuchKey
			if errors.As(err, &noSuchKeyErr) {
				return "", fmt.Errorf("%w: %s", s2.ErrNotExist, opts.Name)
			}
			return "", fmt.Errorf("failed to presign get object: %w", err)
		}
		return req.URL, nil
	}
}

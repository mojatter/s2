package s3

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mojatter/s2"
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

func (s *storage) List(ctx context.Context, prefix string, limit int) ([]s2.Object, []string, error) {
	return s.list(ctx, prefix, limit, "", "/")
}

func (s *storage) ListAfter(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, []string, error) {
	return s.list(ctx, prefix, limit, after, "/")
}

func (s *storage) ListRecursive(ctx context.Context, prefix string, limit int) ([]s2.Object, error) {
	objs, _, err := s.list(ctx, prefix, limit, "", "")
	return objs, err
}

func (s *storage) ListRecursiveAfter(ctx context.Context, prefix string, limit int, after string) ([]s2.Object, error) {
	objs, _, err := s.list(ctx, prefix, limit, after, "")
	return objs, err
}

func (s *storage) list(ctx context.Context, prefix string, limit int, after string, delimiter string) ([]s2.Object, []string, error) {
	if limit <= 0 {
		limit = 1000
	}
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		MaxKeys: aws.Int32(int32(limit)),
	}
	inputPrefix := path.Join(s.prefix, prefix)
	if delimiter != "" && inputPrefix != "" && !strings.HasSuffix(inputPrefix, delimiter) {
		inputPrefix += delimiter
	}
	if inputPrefix != "" {
		input.Prefix = aws.String(inputPrefix)
	}
	if after != "" {
		input.StartAfter = aws.String(after)
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}

	res, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list objects: %w", err)
	}

	prefixes := make([]string, 0, len(res.CommonPrefixes))
	for _, p := range res.CommonPrefixes {
		prefixes = append(prefixes, aws.ToString(p.Prefix))
	}
	objs := make([]s2.Object, 0, len(res.Contents))
	for _, c := range res.Contents {
		key := aws.ToString(c.Key)
		if s.prefix != "" {
			key = key[len(s.prefix)+1:]
		}
		objs = append(objs, &object{
			client:       s.client,
			bucket:       s.bucket,
			prefix:       s.prefix,
			name:         key,
			length:       s2.MustUint64(aws.ToInt64(c.Size)),
			lastModified: aws.ToTime(c.LastModified),
		})
	}
	return objs, prefixes, nil
}

func (s *storage) Get(ctx context.Context, name string) (s2.Object, error) {
	params, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, name)),
	})
	if err != nil {
		var noSuchKeyErr *s3types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return nil, &s2.ErrNotExist{Name: name}
		}
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	return &object{
		client:       s.client,
		bucket:       s.bucket,
		prefix:       s.prefix,
		name:         name,
		length:       s2.MustUint64(aws.ToInt64(params.ContentLength)),
		lastModified: aws.ToTime(params.LastModified),
		metadata:     s2.MetadataMap(params.Metadata),
	}, nil
}

func (s *storage) Exists(ctx context.Context, name string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, name)),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
		ContentLength: aws.Int64(s2.MustInt64(obj.Length())),
		Metadata:      obj.Metadata().ToMap(),
	})
	return err
}

func (s *storage) PutMetadata(ctx context.Context, name string, metadata s2.Metadata) error {
	key := path.Join(s.prefix, name)
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(path.Join(s.bucket, key)),
		Metadata:          metadata.ToMap(),
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

func (s *storage) Move(ctx context.Context, src, dst string) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
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

func (s *storage) SignedURL(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if s.presignClient == nil {
		s3Client, ok := s.client.(*s3.Client)
		if !ok {
			return "", fmt.Errorf("unknown client type: %T", s.client)
		}
		s.presignClient = s3.NewPresignClient(s3Client)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, name)),
	}
	req, err := s.presignClient.PresignGetObject(ctx, input, s3.WithPresignExpires(ttl))
	if err != nil {
		var noSuchKeyErr *s3types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return "", &s2.ErrNotExist{Name: name}
		}
		return "", fmt.Errorf("failed to presign get object: %w", err)
	}
	return req.URL, nil
}

package server

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/fs"
)

// ErrReservedBucketName is returned by Buckets.Create when the requested
// name collides with a path reserved on the S3 listener — currently the
// first segment of cfg.HealthPath.
var ErrReservedBucketName = errors.New("bucket name is reserved")

const keepFile = ".keep"

func isKeepFile(name string) bool {
	return path.Base(name) == keepFile
}

// FilterKeep removes .keep marker files from a list of objects.
func FilterKeep(objs []s2.Object) []s2.Object {
	filtered := make([]s2.Object, 0, len(objs))
	for _, o := range objs {
		if !isKeepFile(o.Name()) {
			filtered = append(filtered, o)
		}
	}
	return filtered
}

// ErrBucketNotFound is returned when a bucket does not exist.
type ErrBucketNotFound struct {
	Name string
}

func (e *ErrBucketNotFound) Error() string {
	return "bucket not found: " + e.Name
}

type Buckets struct {
	strg         s2.Storage
	reservedName string // bucket name that collides with cfg.HealthPath; "" if none
}

func newBuckets(ctx context.Context, cfg *Config) (*Buckets, error) {
	strg, err := s2.NewStorage(ctx, cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}
	return &Buckets{
		strg:         strg,
		reservedName: healthPathReservedBucket(cfg.HealthPath),
	}, nil
}

// healthPathReservedBucket returns the bucket name that would collide
// with the health check endpoint, or "" if the first segment of
// healthPath is not a syntactically valid S3 bucket name (in which case
// no collision is possible). The default "/healthz" reserves the
// bucket name "healthz"; operators who need that name can either
// disable the health endpoint by setting cfg.HealthPath to "" or move
// it onto an unreservable prefix like "/-/healthz".
func healthPathReservedBucket(healthPath string) string {
	if healthPath == "" {
		return ""
	}
	p := strings.TrimPrefix(healthPath, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		p = p[:i]
	}
	if !isValidBucketName(p) {
		return ""
	}
	return p
}

func isValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	first := name[0]
	if (first < 'a' || first > 'z') && (first < '0' || first > '9') {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '.' || c == '-':
		default:
			return false
		}
	}
	return true
}

func (bs *Buckets) Names() ([]string, error) {
	ctx := context.Background()
	res, err := bs.strg.List(ctx, s2.ListOptions{})
	if err != nil {
		return nil, err
	}
	return res.CommonPrefixes, nil
}

func (bs *Buckets) Get(ctx context.Context, name string) (s2.Storage, error) {
	exists, err := bs.Exists(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &ErrBucketNotFound{Name: name}
	}
	return bs.strg.Sub(ctx, name)
}

// CreatedAt returns the creation time of a bucket by reading the .keep marker file.
// If the marker is missing, the current time is returned as a fallback.
func (bs *Buckets) CreatedAt(ctx context.Context, name string) time.Time {
	sub, err := bs.strg.Sub(ctx, name)
	if err != nil {
		return time.Now()
	}
	obj, err := sub.Get(ctx, keepFile)
	if err != nil {
		return time.Now()
	}
	return obj.LastModified()
}

// Exists reports whether a bucket directory exists under the storage
// root. It is implemented as a single Stat against the bucket path
// rather than a directory listing of the storage root, so it stays
// O(1) regardless of how many buckets exist — and, more importantly,
// regardless of how many objects each bucket holds. Every S3 request
// runs this on the hot path through Buckets.Get.
//
// Note: Buckets is tied to the fs-family Storage backends (osfs,
// memfs), which expose a real directory hierarchy. Pairing it with
// the s3 backend would need a different implementation because S3
// has no "directory" primitive; s3 is intended for library-style use
// against a single bucket, not as a multi-bucket server backend.
func (bs *Buckets) Exists(name string) (bool, error) {
	return bs.strg.Exists(context.Background(), name)
}

func (bs *Buckets) Create(ctx context.Context, name string) error {
	if bs.reservedName != "" && name == bs.reservedName {
		return fmt.Errorf("%w: %q is served by the health endpoint", ErrReservedBucketName, name)
	}
	obj := s2.NewObjectBytes(name+"/"+keepFile, []byte{})
	return bs.strg.Put(ctx, obj)
}

func (bs *Buckets) Delete(ctx context.Context, name string) error {
	return bs.strg.DeleteRecursive(ctx, name)
}

func (bs *Buckets) CreateFolder(ctx context.Context, bucket, key string) error {
	sub, err := bs.strg.Sub(ctx, bucket)
	if err != nil {
		return err
	}
	obj := s2.NewObjectBytes(key+"/"+keepFile, []byte{})
	return sub.Put(ctx, obj)
}


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

// ErrReservedBucketName is returned by Buckets.Create for a name reserved
// by the health path or by s2 itself.
var ErrReservedBucketName = errors.New("bucket name is reserved")

const keepFile = ".keep"

// bucketMetaDir holds per-bucket state, borrowing the directory fs already hides.
const bucketMetaDir = ".meta"

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

// Buckets manages buckets; their contents are always written through Sub, never the root storage.
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

// isBucketName reports whether name can name a bucket: one path element that
// is not internal state. The mux fills {bucket} from the escaped path and
// unescapes it afterwards, so "bucket1%2F.meta" arrives here as a name
// spanning two directories, which would scope the request to a prefix the
// policy check never saw.
func isBucketName(name string) bool {
	return name != "" && !strings.Contains(name, "/") && !isHiddenBucketEntry(name)
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

func (bs *Buckets) Names(ctx context.Context) ([]string, error) {
	res, err := bs.strg.List(ctx, s2.ListOptions{})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(res.CommonPrefixes))
	for _, name := range res.CommonPrefixes {
		if isHiddenBucketEntry(name) {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}

func (bs *Buckets) Get(ctx context.Context, name string) (s2.Storage, error) {
	exists, err := bs.Exists(ctx, name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &ErrBucketNotFound{Name: name}
	}
	return bs.strg.Sub(ctx, name)
}

// CreatedAt returns the bucket's .keep time; zero when s2 did not create the bucket.
func (bs *Buckets) CreatedAt(ctx context.Context, name string) (time.Time, error) {
	if !isBucketName(name) {
		return time.Time{}, &ErrBucketNotFound{Name: name}
	}
	sub, err := bs.strg.Sub(ctx, name)
	if err != nil {
		return time.Time{}, err
	}
	obj, err := sub.Get(ctx, keepFile)
	if isNotExist(err) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return obj.LastModified(), nil
}

// Generation returns the bucket's multipart generation, recording one when missing.
func (bs *Buckets) Generation(ctx context.Context, name string) (int64, error) {
	if !isBucketName(name) {
		return 0, &ErrBucketNotFound{Name: name}
	}
	strg, err := bs.strg.Sub(ctx, bucketMetaDir)
	if err != nil {
		return 0, err
	}
	obj, err := strg.Get(ctx, name)
	if isNotExist(err) {
		if err := strg.Put(ctx, s2.NewObjectBytes(name, []byte{})); err != nil {
			return 0, err
		}
		obj, err = strg.Get(ctx, name)
	}
	if err != nil {
		return 0, err
	}
	return obj.LastModified().UnixNano(), nil
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
func (bs *Buckets) Exists(ctx context.Context, name string) (bool, error) {
	if !isBucketName(name) {
		return false, nil
	}
	return bs.strg.Exists(ctx, name)
}

func (bs *Buckets) Create(ctx context.Context, name string) error {
	if bs.reservedName != "" && name == bs.reservedName {
		return fmt.Errorf("%w: %q is served by the health endpoint", ErrReservedBucketName, name)
	}
	if isHiddenBucketEntry(name) {
		return fmt.Errorf("%w: %q is internal state", ErrReservedBucketName, name)
	}
	if !isBucketName(name) {
		return fmt.Errorf("%w: %q is not one path element", ErrReservedBucketName, name)
	}
	// An existing bucket keeps its marker, or its lack of one, so its generation holds.
	marker := name + "/" + keepFile
	for _, p := range []string{marker, name} {
		if exists, err := bs.strg.Exists(ctx, p); err != nil || exists {
			return err
		}
	}
	// A new generation first, so a failed marker write cannot leave a stale one behind.
	if err := bs.strg.Put(ctx, s2.NewObjectBytes(bucketMetaDir+"/"+name, []byte{})); err != nil {
		return err
	}
	sub, err := bs.strg.Sub(ctx, name)
	if err != nil {
		return err
	}
	return sub.Put(ctx, s2.NewObjectBytes(keepFile, []byte{}))
}

func (bs *Buckets) Delete(ctx context.Context, name string) error {
	// The console calls Delete without Exists.
	if !isBucketName(name) {
		return &ErrBucketNotFound{Name: name}
	}
	// The slash keeps the prefix match off buckets whose names merely start with name.
	if err := bs.strg.DeleteRecursive(ctx, name+"/"); err != nil {
		return err
	}
	return bs.strg.Delete(ctx, bucketMetaDir+"/"+name)
}

// FolderMarker is the object a folder actually consists of. Authorize it, not
// the folder's own name: a Deny on "<bucket>/private/*" does not match
// "private", but the marker lands inside it.
func FolderMarker(key string) string {
	return key + "/" + keepFile
}

// CreateFolder writes a folder marker into an existing bucket; it never creates the bucket.
func (bs *Buckets) CreateFolder(ctx context.Context, bucket, key string) error {
	sub, err := bs.Get(ctx, bucket)
	if err != nil {
		return err
	}
	return sub.Put(ctx, s2.NewObjectBytes(FolderMarker(key), []byte{}))
}

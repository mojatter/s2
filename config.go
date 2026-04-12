package s2

type Type string

const (
	TypeOSFS  Type = "osfs"
	TypeMemFS Type = "memfs"
	TypeS3    Type = "s3"
	TypeGCS   Type = "gcs"
)

// KnownTypes returns the list of storage Types that are known to s2.
// The returned slice is a fresh copy; mutating it does not affect future
// calls. Note that this only enumerates compiled-in types; whether a given
// type is *registered* depends on whether the corresponding backend
// package has been imported (e.g. _ "github.com/mojatter/s2/fs").
func KnownTypes() []Type {
	return []Type{TypeOSFS, TypeMemFS, TypeS3, TypeGCS}
}

// S3Config holds S3-specific configuration.
// When fields are empty, the AWS SDK defaults (environment variables, shared
// credentials, IAM role, etc.) are used.
type S3Config struct {
	// EndpointURL is a custom S3-compatible endpoint (e.g. "http://localhost:9000").
	EndpointURL string `json:"endpoint_url,omitempty"`
	// Region is the AWS region (e.g. "ap-northeast-1").
	Region string `json:"region,omitempty"`
	// AccessKeyID is the AWS access key ID.
	AccessKeyID string `json:"access_key_id,omitempty"`
	// SecretAccessKey is the AWS secret access key.
	SecretAccessKey string `json:"secret_access_key,omitempty"`
}

// GCSConfig holds GCS-specific configuration.
// When fields are empty, the GCS SDK defaults (environment variables,
// Application Default Credentials, etc.) are used.
type GCSConfig struct {
	// CredentialsFile is a path to a service account JSON key file.
	CredentialsFile string `json:"credentials_file,omitempty"`
}

// Config is a configuration for a storage.
type Config struct {
	// Type is the type of storage.
	Type Type `json:"type"`
	// Root is the root path of the storage.
	// If Type is TypeOSFS, Root is the root path of the file system.
	// If Type is TypeMemFS, Root is not used.
	// If Type is TypeS3, Root is the S3 bucket name. The string following / in the bucket name is treated as a prefix.
	// If Type is TypeGCS, Root is the GCS bucket name. The string following / is treated as a prefix.
	Root string `json:"root"`
	// SignedURL is the presign URL of the storage. This is used for TypeOSFS and TypeMemFS.
	SignedURL string `json:"signed_url,omitempty"`
	// S3 holds S3-specific settings. Only used when Type is TypeS3.
	S3 *S3Config `json:"s3,omitempty"`
	// GCS holds GCS-specific settings. Only used when Type is TypeGCS.
	GCS *GCSConfig `json:"gcs,omitempty"`
}

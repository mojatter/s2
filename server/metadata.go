package server

// EtagMetadataKey and ContentTypeMetadataKey are s2's own bookkeeping
// fields, stored in an object's s2.Metadata map alongside user-supplied
// x-amz-meta-* entries (there being no separate namespace for them -- see
// the s3api package's PutObject/CopyObject implementation).
const (
	EtagMetadataKey        = "s2-etag"
	ContentTypeMetadataKey = "s2-content-type"
)

// InternalMetadataKeys is the reserved-key set derived from the constants
// above. Anything that exposes an object's raw metadata to a caller -- the
// S3 API's x-amz-meta-* response headers, the Web Console's metadata panel
// -- must filter these out first, or an internal field leaks as if it were
// metadata the client set.
var InternalMetadataKeys = map[string]bool{
	EtagMetadataKey:        true,
	ContentTypeMetadataKey: true,
}

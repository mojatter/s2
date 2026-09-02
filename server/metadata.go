package server

import "github.com/mojatter/s2"

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

// FilterInternalMetadata returns a copy of md with InternalMetadataKeys
// removed, safe to expose to a caller as if it were entirely user-supplied
// -- e.g. GetObject's x-amz-meta-* headers, the Web Console's metadata
// panel. Centralized here so the S3 API and the Web Console can't drift
// apart on what counts as "internal" the way they already have on
// Content-Type resolution (see resolvedContentType in the console's objects
// package).
func FilterInternalMetadata(md s2.Metadata) map[string]string {
	if len(md) == 0 {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		if InternalMetadataKeys[k] {
			continue
		}
		out[k] = v
	}
	return out
}

package s2test

import (
	"fmt"
	"strings"
)

// Option configures a conformance helper.
type Option func(*options)

type options struct {
	opaqueETag bool
}

func newOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithOpaqueETag relaxes the form of the ETag the suite expects, not the
// requirement that there is one: a quoted, non-empty value that Get and List
// agree on and that an overwrite changes. That still admits an ETag derived
// from the object's metadata rather than its bytes, as the fs fallback for a
// missing sidecar is. Use it for a backend whose ETag is not the body MD5,
// such as an s3 root using SSE-KMS or SSE-C.
func WithOpaqueETag() Option {
	return func(o *options) { o.opaqueETag = true }
}

// etagError reports why got is not an acceptable ETag for body, or nil.
func (o options) etagError(label, got string, body []byte) error {
	if o.opaqueETag {
		return quotedETagError(label, got)
	}
	if want := quotedMD5(body); got != want {
		return fmt.Errorf("%s.ETag() = %q, want %q", label, got, want)
	}
	return nil
}

// quotedETagError enforces what Object.ETag() promises whatever the value is:
// a non-empty entity tag in quoted form. An unquoted tag reaches the client
// verbatim in the ETag header and is not a valid RFC 7232 entity-tag.
func quotedETagError(label, got string) error {
	if got == "" {
		return fmt.Errorf("%s.ETag() is empty", label)
	}
	// <= 2 also rejects `""`, a quoted tag with nothing in it.
	if len(got) <= 2 || !strings.HasPrefix(got, `"`) || !strings.HasSuffix(got, `"`) {
		return fmt.Errorf("%s.ETag() = %q, want a non-empty quoted entity tag", label, got)
	}
	// etagc excludes DQUOTE, so an inner one truncates the tag at the client.
	if strings.Contains(got[1:len(got)-1], `"`) {
		return fmt.Errorf("%s.ETag() = %q, want no quote inside the entity tag", label, got)
	}
	return nil
}

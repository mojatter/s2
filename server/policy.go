package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// S3 IAM action names, shared between S3Action and ConsoleAction so a typo
// in one mapping can't silently drift from the other.
const (
	actionListAllMyBuckets  = "s3:ListAllMyBuckets"
	actionCreateBucket      = "s3:CreateBucket"
	actionDeleteBucket      = "s3:DeleteBucket"
	actionListBucket        = "s3:ListBucket"
	actionGetBucketLocation = "s3:GetBucketLocation"
	// ActionGetObject is exported for handlers that must authorize a
	// resource distinct from the request's own bucket/key (e.g.
	// handleCopyObject's read of its copy-source object), which S3Action
	// can't check generically since it only derives the destination.
	ActionGetObject = "s3:GetObject"
	ActionPutObject = "s3:PutObject"
	// ActionDeleteObject is exported for handlers that must authorize
	// individual resources parsed from a request body (e.g.
	// handleDeleteObjects' per-key batch delete), which SigV4 can't check
	// generically since the keys aren't known until the body is decoded.
	ActionDeleteObject = "s3:DeleteObject"
)

// Policy is an AWS-IAM-shaped authorization policy attached to a User. It
// grants or denies S3 actions (e.g. "s3:GetObject") against S3 resource
// ARNs (e.g. "arn:aws:s3:::bucket/key").
//
// Condition blocks are intentionally not evaluated (see Statement.Condition)
// -- Validate rejects any statement that sets one, so a policy that appears
// to restrict by IP/time/etc. never silently grants more than it looks like
// it does.
type Policy struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// Statement is a single Allow/Deny rule within a Policy.
type Statement struct {
	// Sid is a statement identifier. Documentation-only; it has no effect
	// on matching.
	Sid string `json:"Sid,omitempty"`
	// Effect is exactly "Allow" or "Deny" (case-sensitive -- Validate
	// rejects any other value, including different casing, rather than
	// silently normalizing it).
	Effect string `json:"Effect"`
	// Action is one or more "s3:*"-style action names. Wildcards ("*")
	// are supported; no other glob syntax is.
	Action stringOrSlice `json:"Action"`
	// Resource is one or more "arn:aws:s3:::..."-style resource ARNs.
	// Wildcards ("*") are supported; no other glob syntax is.
	Resource stringOrSlice `json:"Resource"`
	// Condition is parsed and preserved for forward compatibility but
	// never evaluated. Validate rejects any statement that sets it.
	Condition json.RawMessage `json:"Condition,omitempty"`
}

// stringOrSlice unmarshals from either a bare JSON string or a JSON array
// of strings, matching the grammar AWS IAM policies actually use for
// Action/Resource.
type stringOrSlice []string

func (s *stringOrSlice) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = stringOrSlice{single}
		return nil
	}
	var multi []string
	if err := json.Unmarshal(data, &multi); err != nil {
		return err
	}
	*s = multi
	return nil
}

// Allowed reports whether the policy permits action on resource, using
// AWS's evaluation semantics: an explicit Deny on any matching statement
// wins immediately; otherwise the result is true iff at least one matching
// statement has Effect "Allow"; the default is deny.
func (p *Policy) Allowed(action, resource string) bool {
	allowed := false
	for _, stmt := range p.Statement {
		if !stmt.matches(action, resource) {
			continue
		}
		if stmt.Effect == "Deny" {
			return false
		}
		if stmt.Effect == "Allow" {
			allowed = true
		}
	}
	return allowed
}

func (stmt Statement) matches(action, resource string) bool {
	return matchAny(stmt.Action, action) && matchAny(stmt.Resource, resource)
}

func matchAny(patterns []string, s string) bool {
	for _, p := range patterns {
		if wildcardMatch(p, s) {
			return true
		}
	}
	return false
}

// wildcardMatch reports whether s matches pattern, where "*" in pattern
// matches any sequence of characters (including "/" -- IAM Action/Resource
// wildcards, unlike filepath.Match, do not stop at path separators).
func wildcardMatch(pattern, s string) bool {
	segments := strings.Split(pattern, "*")
	if len(segments) == 1 {
		return pattern == s
	}
	if !strings.HasPrefix(s, segments[0]) {
		return false
	}
	s = s[len(segments[0]):]
	if !strings.HasSuffix(s, segments[len(segments)-1]) {
		return false
	}
	s = s[:len(s)-len(segments[len(segments)-1])]
	for _, seg := range segments[1 : len(segments)-1] {
		if seg == "" {
			continue
		}
		idx := strings.Index(s, seg)
		if idx < 0 {
			return false
		}
		s = s[idx+len(seg):]
	}
	return true
}

// isEmptyCondition reports whether c represents "no condition" -- either
// the field was absent (len 0) or it was present but semantically empty
// (e.g. "{}" or "null", as commonly written in hand-authored policies,
// including the example in https://github.com/mojatter/s2/issues/162).
// Any other value, including a non-empty object, is treated as a real
// Condition and rejected by Validate.
func isEmptyCondition(c json.RawMessage) bool {
	if len(c) == 0 {
		return true
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(c, &m); err != nil {
		return false
	}
	return len(m) == 0
}

// Validate checks the policy for obvious configuration mistakes. It does
// not validate ARN syntax deeply.
func (p *Policy) Validate() error {
	if p.Version != "" && p.Version != "2012-10-17" {
		return fmt.Errorf("policy: unsupported Version %q", p.Version)
	}
	for i, stmt := range p.Statement {
		if stmt.Effect != "Allow" && stmt.Effect != "Deny" {
			return fmt.Errorf("policy: statement[%d]: Effect must be %q or %q, got %q", i, "Allow", "Deny", stmt.Effect)
		}
		if len(stmt.Action) == 0 {
			return fmt.Errorf("policy: statement[%d]: Action must not be empty", i)
		}
		if len(stmt.Resource) == 0 {
			return fmt.Errorf("policy: statement[%d]: Resource must not be empty", i)
		}
		if !isEmptyCondition(stmt.Condition) {
			return fmt.Errorf("policy: statement[%d]: Condition is not supported", i)
		}
	}
	return nil
}

// bucketARN returns the ARN for a bucket-level S3 resource.
func bucketARN(bucket string) string {
	return "arn:aws:s3:::" + bucket
}

// objectARN returns the ARN for an object-level S3 resource.
func objectARN(bucket, key string) string {
	return "arn:aws:s3:::" + bucket + "/" + key
}

// AllowedS3Action reports whether user may perform action on the given
// bucket/key object resource. A nil user or a user with no Policy attached
// (full access, including the legacy single-credential user) is always
// allowed.
//
// This is for handlers that authorize a resource SigV4 couldn't check
// generically -- e.g. batch operations where the affected keys are only
// known after the request body is decoded, so the coarse bucket-level
// check S3Action performs up front isn't precise enough.
func AllowedS3Action(user *User, action, bucket, key string) bool {
	return Authorized(user, action, objectARN(bucket, key))
}

// Authorized reports whether user may perform action on resource. A nil
// user or a user with no Policy attached (full access, including the
// legacy single-credential user) is always allowed. An empty action means
// S3Action/ConsoleAction couldn't determine a single resource to check up
// front -- e.g. batch operations whose affected keys are only known once
// the request body is decoded -- so it also passes, deferring enforcement
// to the handler itself (see AllowedS3Action). This is the single place
// SigV4 and BasicAuth both consult after resolving the matched user, so
// the fail-open convention for an empty action lives in one spot.
func Authorized(user *User, action, resource string) bool {
	if user == nil || user.Policy == nil || action == "" {
		return true
	}
	return user.Policy.Allowed(action, resource)
}

// S3Action derives the s3:* IAM action name and the arn:aws:s3:::bucket/key
// resource for an S3 API request, given its already-routed method and the
// bucket/key path values populated by the ServeMux before middleware runs.
// The mapping mirrors the query-parameter dispatch already implemented in
// server/handlers/s3api/{buckets,objects,multipart}.go.
//
// s3:ListAllMyBuckets has no single resource to check -- it is authorized
// as a whole against a synthetic "arn:aws:s3:::*" resource by the caller,
// and the returned bucket list is filtered separately via FilterBucketNames.
func S3Action(r *http.Request, bucket, key string) (action, resource string) {
	q := r.URL.Query()

	if bucket == "" {
		return actionListAllMyBuckets, "arn:aws:s3:::*"
	}

	if key == "" {
		switch r.Method {
		case http.MethodPut:
			return actionCreateBucket, bucketARN(bucket)
		case http.MethodDelete:
			return actionDeleteBucket, bucketARN(bucket)
		case http.MethodHead:
			return actionListBucket, bucketARN(bucket)
		case http.MethodGet:
			if _, ok := q["location"]; ok {
				return actionGetBucketLocation, bucketARN(bucket)
			}
			return actionListBucket, bucketARN(bucket)
		}
		// Batch delete (POST /{bucket}?delete) falls through to here: the
		// affected keys are only known once the request body is decoded,
		// so no single resource can be checked up front -- a policy
		// scoped to a narrower prefix than the whole bucket would
		// otherwise be denied even for keys it does cover, since a
		// Resource pattern only matches the literal resource it's tested
		// against, not a broader placeholder standing in for "any key".
		// handleDeleteObjects checks each key individually via
		// AllowedS3Action instead.
		return "", ""
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return ActionGetObject, objectARN(bucket, key)
	case http.MethodPut:
		// Plain PutObject and UploadPart (?uploadId=...&partNumber=...)
		// share the same IAM action, matching AWS's own permission model.
		return ActionPutObject, objectARN(bucket, key)
	case http.MethodDelete:
		// AbortMultipartUpload (?uploadId=...) shares s3:PutObject with the
		// rest of the upload lifecycle; a plain delete is s3:DeleteObject.
		if q.Get("uploadId") != "" {
			return ActionPutObject, objectARN(bucket, key)
		}
		return ActionDeleteObject, objectARN(bucket, key)
	case http.MethodPost:
		// CreateMultipartUpload (?uploads) and CompleteMultipartUpload
		// (?uploadId=...) both fall under s3:PutObject.
		return ActionPutObject, objectARN(bucket, key)
	}
	return "", ""
}

// ConsoleAction derives the s3:* IAM action name and resource ARN for a Web
// Console request, given its already-routed method and PathValue("name")/
// PathValue("object") as populated by the ServeMux before middleware runs.
// It mirrors S3Action for the console's non-S3-shaped routes (see
// server/handlers/console/{index,buckets/objects,buckets/objects/view}.go).
//
// An empty action means "no authorization check applies" -- used for the
// static asset route, which serves no user data.
func ConsoleAction(r *http.Request) (action, resource string) {
	if strings.HasPrefix(r.URL.Path, "/static/") {
		return "", ""
	}

	name := r.PathValue("name")
	if name == "" {
		if r.Method == http.MethodPost && r.URL.Path == "/buckets" {
			// The bucket name lives in the form body, not the URL, and
			// doesn't exist yet -- authorize against the wildcard bucket
			// resource rather than parsing the body here.
			return actionCreateBucket, "arn:aws:s3:::*"
		}
		return actionListAllMyBuckets, "arn:aws:s3:::*"
	}

	arn := bucketARN(name)
	rest := strings.TrimPrefix(r.URL.Path, "/buckets/"+name)

	switch {
	case rest == "" && r.Method == http.MethodGet:
		return actionListBucket, arn
	case rest == "" && r.Method == http.MethodDelete:
		return actionDeleteBucket, arn
	case rest == "/folders", rest == "/upload":
		// The target key lives in the form body and isn't known here. A
		// wildcard placeholder resource (bucket/*) would be wrong in both
		// directions -- it can't see a Deny scoped to one specific key,
		// and it doesn't match an Allow scoped to a narrower prefix than
		// the whole bucket -- so defer entirely to the handler, which
		// checks the real key once the body is parsed (handleCreateFolder,
		// handleUploadFile in server/handlers/console/buckets/objects.go).
		return "", ""
	case rest == "/objects" && r.Method == http.MethodDelete:
		// Unlike /folders and /upload, the target key is a query
		// parameter here, so it's available without consuming the body --
		// except for a recursive folder delete (key ends in "/"), whose
		// affected keys are only known once the handler lists the prefix;
		// that case also defers to the handler (handleDeleteObject).
		key := r.URL.Query().Get("key")
		if strings.HasSuffix(key, "/") {
			return "", ""
		}
		return ActionDeleteObject, objectARN(name, key)
	case strings.HasPrefix(rest, "/view/"), strings.HasPrefix(rest, "/meta/"), strings.HasPrefix(rest, "/preview/"):
		return ActionGetObject, objectARN(name, r.PathValue("object"))
	}
	return "", ""
}

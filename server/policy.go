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
	// ActionListAllMyBuckets is exported: HandleListBuckets (S3 API GET /)
	// checks it explicitly via ExplicitlyDeniedListAllMyBuckets. It is not
	// returned by S3Action itself -- see that function's doc comment.
	ActionListAllMyBuckets = "s3:ListAllMyBuckets"
	// ActionCreateBucket is exported: the console's handleCreateBucket
	// checks a form-parsed bucket name directly via AllowedS3BucketAction.
	ActionCreateBucket      = "s3:CreateBucket"
	actionDeleteBucket      = "s3:DeleteBucket"
	actionGetBucketLocation = "s3:GetBucketLocation"
	// ActionListBucket is exported for FilterBucketNames' per-bucket check.
	ActionListBucket = "s3:ListBucket"
	// ActionGetObject is exported: handleCopyObject checks its copy-source
	// object, which S3Action's destination-only mapping doesn't cover.
	ActionGetObject = "s3:GetObject"
	ActionPutObject = "s3:PutObject"
	// ActionDeleteObject is exported for per-key checks on resources
	// S3Action/ConsoleAction can't see up front (batch delete, recursive
	// folder delete).
	ActionDeleteObject = "s3:DeleteObject"

	// actionSkip and actionDeferred are sentinels S3Action/ConsoleAction
	// return instead of a bare "" when no single resource can be checked
	// up front. Authorized passes both through; any other "" means a
	// request matched no dispatch case, so a route added without
	// updating either function fails closed instead of silently open.
	//   - actionSkip: no user data, no check needed (static assets).
	//   - actionDeferred: the resource isn't known until the handler
	//     parses the body/prefix, or the response is filtered rather
	//     than allow/deny'd as a whole (e.g. the console home page). The
	//     handler checks itself via AllowedS3Action/AllowedS3BucketAction
	//     or FilterBucketNames.
	actionSkip     = "s2:Skip"
	actionDeferred = "s2:Deferred"
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
	// A JSON null unmarshals into a string as a silent no-op, not an
	// error, which would let "Action": null through as stringOrSlice{""}
	// -- non-empty, so Validate's len check wouldn't catch it, and an
	// inert Deny would silently never apply. Reject null explicitly.
	if string(data) == "null" {
		return fmt.Errorf("must not be null")
	}
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

// explicitlyDenied reports whether policy has a Deny statement matching
// action and resource, ignoring any Allow. Unlike Allowed, whose default
// (no matching statement) is deny, the default here is "not denied" -- for
// callers where the baseline is already permit and an explicit Deny should
// layer a hard block on top of that baseline.
func (p *Policy) explicitlyDenied(action, resource string) bool {
	for _, stmt := range p.Statement {
		if stmt.Effect == "Deny" && stmt.matches(action, resource) {
			return true
		}
	}
	return false
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

// resourceARNPrefix is the ARN prefix every real S3 resource this server
// checks a policy against (see bucketARN/objectARN) starts with.
const resourceARNPrefix = "arn:aws:s3:::"

// Validate checks the policy for obvious configuration mistakes. It does
// not validate ARN syntax deeply -- only that each Resource entry is
// either the bare wildcard "*" or starts with the ARN prefix this server
// actually matches against. wildcardMatch does plain string comparison,
// so a Resource written without that prefix (e.g. "mybucket/*" instead of
// "arn:aws:s3:::mybucket/*") would never match anything: an intended Deny
// would silently become a permanent no-op instead of failing to load.
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
		for _, r := range stmt.Resource {
			if r != "*" && !strings.HasPrefix(r, resourceARNPrefix) {
				return fmt.Errorf("policy: statement[%d]: Resource %q must be %q or start with %q", i, r, "*", resourceARNPrefix)
			}
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
	return bucketARN(bucket) + "/" + key
}

// AllowedS3BucketAction reports whether user may perform action on a
// bucket-level resource. A nil user or a user with no Policy (full
// access, including the legacy single-credential user) always passes.
// For handlers authorizing a bucket name only known after parsing the
// request body (e.g. the console's handleCreateBucket).
func AllowedS3BucketAction(user *User, action, bucket string) bool {
	return Authorized(user, action, bucketARN(bucket))
}

// AllowedS3Action is AllowedS3BucketAction for an object-level (bucket+key)
// resource -- e.g. per-key checks in batch/recursive delete handlers,
// whose affected keys S3Action/ConsoleAction can't see up front.
func AllowedS3Action(user *User, action, bucket, key string) bool {
	return Authorized(user, action, objectARN(bucket, key))
}

// ExplicitlyDeniedListAllMyBuckets reports whether user has an explicit
// Deny statement for s3:ListAllMyBuckets. A nil user or a user with no
// Policy is never denied.
//
// S3Action doesn't gate GET / (ListBuckets) on s3:ListAllMyBuckets at all
// -- it always defers to FilterBucketNames, so a policy scoped to only a
// few buckets isn't locked out of the endpoint entirely (see S3Action's
// doc comment). But that means an explicit Deny on s3:ListAllMyBuckets,
// which a policy author porting an AWS-style policy would expect to hard
// -block the endpoint, would otherwise be silently ignored. HandleListBuckets
// calls this to layer that specific block back on top of the deferred
// default, without requiring every caller to hold an explicit Allow.
func ExplicitlyDeniedListAllMyBuckets(user *User) bool {
	if user == nil || user.Policy == nil {
		return false
	}
	return user.Policy.explicitlyDenied(ActionListAllMyBuckets, "arn:aws:s3:::*")
}

// DenyUnlessAllowedS3BucketAction writes a 403 Forbidden response and
// reports false if user may not perform action on bucket; otherwise it
// reports true and writes nothing. Consolidates the repeated
// "if !AllowedS3BucketAction(...) { http.Error(...); return }" idiom
// duplicated across console handlers whose resource is only known once
// the request body/query is parsed.
func DenyUnlessAllowedS3BucketAction(w http.ResponseWriter, user *User, action, bucket string) bool {
	if !AllowedS3BucketAction(user, action, bucket) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	return true
}

// DenyUnlessAllowedS3Action is DenyUnlessAllowedS3BucketAction for an
// object-level (bucket+key) resource.
func DenyUnlessAllowedS3Action(w http.ResponseWriter, user *User, action, bucket, key string) bool {
	if !AllowedS3Action(user, action, bucket, key) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	return true
}

// Authorized reports whether user may perform action on resource. A nil
// user or a user with no Policy attached (full access, including the
// legacy single-credential user) is always allowed.
//
// action == actionSkip or actionDeferred also passes, for the reasons
// documented on those constants. Any other empty action is treated as
// deny -- see the doc comment on actionSkip/actionDeferred for why an
// unclassified action must fail closed rather than open. This is the
// single place SigV4 and BasicAuth both consult after resolving the
// matched user, so this convention lives in one spot.
func Authorized(user *User, action, resource string) bool {
	if action == actionSkip || action == actionDeferred {
		return true
	}
	if action == "" {
		return false
	}
	if user == nil || user.Policy == nil {
		return true
	}
	return user.Policy.Allowed(action, resource)
}

// S3Action derives the s3:* IAM action name and the arn:aws:s3:::bucket/key
// resource for an S3 API request, given its already-routed method and the
// bucket/key path values populated by the ServeMux before middleware runs.
// The mapping mirrors the query-parameter dispatch already implemented in
// server/handlers/s3api/{buckets,objects,multipart}.go.
func S3Action(r *http.Request, bucket, key string) (action, resource string) {
	q := r.URL.Query()

	if bucket == "" {
		// GET / (ListBuckets) has no single resource to gate on up front;
		// HandleListBuckets filters the result per-bucket via
		// FilterBucketNames, mirroring ConsoleAction's GET / handling.
		// Hard-gating the whole request on s3:ListAllMyBuckets would lock
		// out a user who only holds narrower per-bucket grants, even
		// though FilterBucketNames would otherwise show them a (possibly
		// empty) filtered list. HandleListBuckets separately layers an
		// explicit-Deny-only check on top via
		// ExplicitlyDeniedListAllMyBuckets, so a Deny statement targeting
		// this action still has an effect.
		return actionDeferred, ""
	}

	// POST with an empty key covers both "POST /{bucket}?delete" (routed
	// bucket-level, no trailing slash) and "POST /{bucket}/?delete"
	// (routed to "/{bucket}/{key...}" with an empty key, then delegated
	// back to handleBucketPOST by handleObjectPOST -- see the comment on
	// handleObjectPOST in server/handlers/s3api/multipart.go). Both forms
	// reach handleDeleteObjects, whose affected keys are only known once
	// the body is decoded; handleDeleteObjects checks each key via
	// AllowedS3Action instead. Handling this before the trailing-slash
	// check below means both URL forms defer identically, rather than the
	// trailing-slash form falling through to the object-level switch and
	// being checked as s3:PutObject on the wrong (empty-key) resource.
	if key == "" && r.Method == http.MethodPost {
		return actionDeferred, ""
	}

	// GET/HEAD with an empty key covers both "GET /{bucket}" (routed
	// bucket-level, no trailing slash) and "GET /{bucket}/" (routed to
	// "/{bucket}/{key...}" with an empty key). handleGetObject delegates
	// key=="" straight back to handleBucketGET/handleHeadBucket regardless
	// of the trailing slash (see the comment on handleGetObject in
	// server/handlers/s3api/objects.go) -- it always runs ListObjectsV2,
	// GetBucketLocation, or HeadBucket, never a GetObject read. Handling
	// this before the trailing-slash check below means both URL forms are
	// checked against the same bucket-level resource, rather than the
	// trailing-slash form falling through to the object-level switch and
	// being checked as s3:GetObject on the wrong (empty-key) resource --
	// which would let a GetObject-only grant enumerate the whole bucket.
	if key == "" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		if r.Method == http.MethodHead {
			return ActionListBucket, bucketARN(bucket)
		}
		if _, ok := q["location"]; ok {
			return actionGetBucketLocation, bucketARN(bucket)
		}
		return ActionListBucket, bucketARN(bucket)
	}

	// key == "" is ambiguous on its own for PUT/DELETE: "PUT /{bucket}"
	// (truly bucket-level, no key wildcard in that pattern) and
	// "PUT /{bucket}/" (matches "/{bucket}/{key...}" with an empty key)
	// both populate PathValue("key") as "". Go's ServeMux always prefers
	// the more specific "/{bucket}/{key...}" pattern once there's a
	// trailing slash, so a trailing-slash request is actually dispatched
	// to the object handler (handlePutObject/handleDeleteObject, neither
	// of which delegates to a bucket-level handler for an empty key) --
	// checking it as a bucket-level action here would authorize a
	// different operation than the one that actually runs. Only a path
	// with no trailing slash is genuinely bucket-level for these methods.
	if key == "" && !strings.HasSuffix(r.URL.Path, "/") {
		switch r.Method {
		case http.MethodPut:
			return ActionCreateBucket, bucketARN(bucket)
		case http.MethodDelete:
			return actionDeleteBucket, bucketARN(bucket)
		}
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
	return "", "" // unreachable given the registered routes; fails closed if it ever isn't.
}

// ConsoleAction derives the s3:* IAM action name and resource ARN for a Web
// Console request, given its already-routed method and PathValue("name")/
// PathValue("object") as populated by the ServeMux before middleware runs.
// It mirrors S3Action for the console's non-S3-shaped routes (see
// server/handlers/console/{index,buckets/objects,buckets/objects/view}.go).
// See actionSkip/actionDeferred for what a non-normal action return means.
func ConsoleAction(r *http.Request) (action, resource string) {
	if strings.HasPrefix(r.URL.Path, "/static/") {
		return actionSkip, "" // no user data
	}

	name := r.PathValue("name")
	if name == "" {
		if r.Method == http.MethodPost && r.URL.Path == "/buckets" {
			// Bucket name lives in the form body; handleCreateBucket
			// checks it via AllowedS3BucketAction.
			return actionDeferred, ""
		}
		// GET / is the console's only entry point (login + bucket
		// sidebar); gating it on s3:ListAllMyBuckets would lock out any
		// user with only narrower per-bucket grants. FilterBucketNames
		// determines what the sidebar actually shows.
		return actionDeferred, ""
	}

	arn := bucketARN(name)
	rest := strings.TrimPrefix(r.URL.Path, "/buckets/"+name)

	switch {
	case rest == "" && r.Method == http.MethodGet:
		return ActionListBucket, arn
	case rest == "" && r.Method == http.MethodDelete:
		return actionDeleteBucket, arn
	case rest == "/folders", rest == "/upload":
		// Target key lives in the form body; the handler checks the real
		// key via AllowedS3Action once parsed (handleCreateFolder,
		// handleUploadFile).
		return actionDeferred, ""
	case rest == "/objects" && r.Method == http.MethodDelete:
		// The key is a query param, available without parsing the body --
		// except for a recursive folder delete (key ends in "/"), whose
		// affected keys are only known once handleDeleteObject lists the
		// prefix.
		key := r.URL.Query().Get("key")
		if strings.HasSuffix(key, "/") {
			return actionDeferred, ""
		}
		return ActionDeleteObject, objectARN(name, key)
	case strings.HasPrefix(rest, "/view/"), strings.HasPrefix(rest, "/meta/"), strings.HasPrefix(rest, "/preview/"):
		return ActionGetObject, objectARN(name, r.PathValue("object"))
	}
	return "", "" // unreachable given the registered routes; fails closed if it ever isn't.
}

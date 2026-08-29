package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func allowStatement(action, resource string) Statement {
	return Statement{Effect: "Allow", Action: stringOrSlice{action}, Resource: stringOrSlice{resource}}
}

func denyStatement(action, resource string) Statement {
	return Statement{Effect: "Deny", Action: stringOrSlice{action}, Resource: stringOrSlice{resource}}
}

func TestPolicyAllowed(t *testing.T) {
	testCases := []struct {
		caseName string
		policy   Policy
		action   string
		resource string
		want     bool
	}{
		{
			caseName: "explicit deny beats allow",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:*", "*"),
				denyStatement("s3:DeleteObject", "arn:aws:s3:::bucket/*"),
			}},
			action:   "s3:DeleteObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     false,
		},
		{
			caseName: "deny order independent",
			policy: Policy{Statement: []Statement{
				denyStatement("s3:DeleteObject", "arn:aws:s3:::bucket/*"),
				allowStatement("s3:*", "*"),
			}},
			action:   "s3:DeleteObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     false,
		},
		{
			caseName: "no matching statement denies",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "*"),
			}},
			action:   "s3:PutObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     false,
		},
		{
			caseName: "empty statement list denies everything",
			policy:   Policy{},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     false,
		},
		{
			caseName: "action wildcard matches",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:Get*", "*"),
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     true,
		},
		{
			caseName: "action wildcard does not match unrelated action",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:Get*", "*"),
			}},
			action:   "s3:PutObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     false,
		},
		{
			caseName: "full wildcard action",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:*", "*"),
			}},
			action:   "s3:DeleteBucket",
			resource: "arn:aws:s3:::bucket",
			want:     true,
		},
		{
			caseName: "resource bucket wildcard matches key in bucket",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "arn:aws:s3:::mybucket/*"),
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::mybucket/foo.txt",
			want:     true,
		},
		{
			caseName: "resource bucket wildcard does not match other bucket",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "arn:aws:s3:::mybucket/*"),
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::otherbucket/foo.txt",
			want:     false,
		},
		{
			caseName: "resource all-buckets wildcard matches any bucket",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:ListBucket", "arn:aws:s3:::*"),
			}},
			action:   "s3:ListBucket",
			resource: "arn:aws:s3:::mybucket",
			want:     true,
		},
		{
			caseName: "multiple allow statements cover different actions",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "*"),
				allowStatement("s3:ListBucket", "*"),
			}},
			action:   "s3:ListBucket",
			resource: "arn:aws:s3:::bucket",
			want:     true,
		},
		{
			caseName: "multiple allow statements deny unrelated action",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "*"),
				allowStatement("s3:ListBucket", "*"),
			}},
			action:   "s3:DeleteObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     false,
		},
		{
			caseName: "sid does not affect matching",
			policy: Policy{Statement: []Statement{
				{Sid: "Anything", Effect: "Allow", Action: stringOrSlice{"s3:GetObject"}, Resource: stringOrSlice{"*"}},
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::bucket/key",
			want:     true,
		},
		{
			caseName: "bucket-level resource does not match object-level resource",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "arn:aws:s3:::mybucket"),
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::mybucket/foo.txt",
			want:     false,
		},
		{
			caseName: "bucket-level resource matches bucket-level action",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:ListBucket", "arn:aws:s3:::mybucket"),
			}},
			action:   "s3:ListBucket",
			resource: "arn:aws:s3:::mybucket",
			want:     true,
		},
		{
			caseName: "multiple resource patterns: matches one of them",
			policy: Policy{Statement: []Statement{
				{Effect: "Allow", Action: stringOrSlice{"s3:GetObject"}, Resource: stringOrSlice{
					"arn:aws:s3:::bucket-a/*", "arn:aws:s3:::bucket-b/*",
				}},
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::bucket-b/file.txt",
			want:     true,
		},
		{
			caseName: "multiple resource patterns: matches none of them",
			policy: Policy{Statement: []Statement{
				{Effect: "Allow", Action: stringOrSlice{"s3:GetObject"}, Resource: stringOrSlice{
					"arn:aws:s3:::bucket-a/*", "arn:aws:s3:::bucket-b/*",
				}},
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::bucket-c/file.txt",
			want:     false,
		},
		{
			caseName: "resource pattern with multiple wildcards matches",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "arn:aws:s3:::backup-*-2024/*"),
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::backup-jan-2024/report.pdf",
			want:     true,
		},
		{
			caseName: "resource pattern with multiple wildcards does not match wrong middle segment",
			policy: Policy{Statement: []Statement{
				allowStatement("s3:GetObject", "arn:aws:s3:::backup-*-2024/*"),
			}},
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::backup-jan-2023/report.pdf",
			want:     false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.policy.Allowed(tc.action, tc.resource))
		})
	}
}

func TestAuthorized(t *testing.T) {
	scopedUser := &User{Policy: &Policy{Statement: []Statement{
		allowStatement("s3:GetObject", "arn:aws:s3:::bucket/*"),
	}}}

	testCases := []struct {
		caseName string
		user     *User
		action   string
		resource string
		want     bool
	}{
		{caseName: "nil user is always allowed", user: nil, action: "s3:DeleteBucket", resource: "arn:aws:s3:::bucket", want: true},
		{caseName: "user with no policy is always allowed", user: &User{}, action: "s3:DeleteBucket", resource: "arn:aws:s3:::bucket", want: true},
		{caseName: "actionSkip passes even for a restrictive policy", user: scopedUser, action: actionSkip, resource: "", want: true},
		{caseName: "actionDeferred passes even for a restrictive policy", user: scopedUser, action: actionDeferred, resource: "", want: true},
		{caseName: "unclassified empty action is denied, not treated as skip/defer", user: scopedUser, action: "", resource: "", want: false},
		{caseName: "unclassified empty action is denied even for a full-access user", user: nil, action: "", resource: "", want: false},
		{caseName: "scoped policy allows a matching action/resource", user: scopedUser, action: "s3:GetObject", resource: "arn:aws:s3:::bucket/key", want: true},
		{caseName: "scoped policy denies a non-matching action", user: scopedUser, action: "s3:DeleteObject", resource: "arn:aws:s3:::bucket/key", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, Authorized(tc.user, tc.action, tc.resource))
		})
	}
}

func TestAllowedS3Action(t *testing.T) {
	scopedUser := &User{Policy: &Policy{Statement: []Statement{
		allowStatement(ActionDeleteObject, "arn:aws:s3:::bucket/tmp/*"),
	}}}

	testCases := []struct {
		caseName string
		user     *User
		action   string
		bucket   string
		key      string
		want     bool
	}{
		{caseName: "nil user is always allowed", user: nil, action: ActionDeleteObject, bucket: "bucket", key: "tmp/a.txt", want: true},
		{caseName: "user with no policy is always allowed", user: &User{}, action: ActionDeleteObject, bucket: "bucket", key: "anything", want: true},
		{caseName: "scoped policy allows a matching key", user: scopedUser, action: ActionDeleteObject, bucket: "bucket", key: "tmp/a.txt", want: true},
		{caseName: "scoped policy denies a non-matching key", user: scopedUser, action: ActionDeleteObject, bucket: "bucket", key: "keep/b.txt", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, AllowedS3Action(tc.user, tc.action, tc.bucket, tc.key))
		})
	}
}

func TestExplicitlyDeniedListAllMyBuckets(t *testing.T) {
	deniedUser := &User{Policy: &Policy{Statement: []Statement{
		denyStatement(ActionListAllMyBuckets, "arn:aws:s3:::*"),
	}}}
	scopedUser := &User{Policy: &Policy{Statement: []Statement{
		allowStatement(ActionListBucket, "arn:aws:s3:::mybucket"),
	}}}

	testCases := []struct {
		caseName string
		user     *User
		want     bool
	}{
		{caseName: "nil user is never denied", user: nil, want: false},
		{caseName: "user with no policy is never denied", user: &User{}, want: false},
		{caseName: "explicit Deny on the action blocks it", user: deniedUser, want: true},
		{caseName: "a policy with no matching statement at all is not denied", user: scopedUser, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, ExplicitlyDeniedListAllMyBuckets(tc.user))
		})
	}
}

func TestPolicyValidate(t *testing.T) {
	testCases := []struct {
		caseName string
		policy   Policy
		wantErr  bool
	}{
		{
			caseName: "valid minimal policy",
			policy: Policy{
				Version:   "2012-10-17",
				Statement: []Statement{allowStatement("s3:GetObject", "*")},
			},
			wantErr: false,
		},
		{
			caseName: "empty version accepted",
			policy: Policy{
				Statement: []Statement{allowStatement("s3:GetObject", "*")},
			},
			wantErr: false,
		},
		{
			caseName: "unsupported version rejected",
			policy: Policy{
				Version:   "2020-01-01",
				Statement: []Statement{allowStatement("s3:GetObject", "*")},
			},
			wantErr: true,
		},
		{
			caseName: "lowercase effect rejected",
			policy: Policy{
				Statement: []Statement{{Effect: "allow", Action: stringOrSlice{"s3:GetObject"}, Resource: stringOrSlice{"*"}}},
			},
			wantErr: true,
		},
		{
			caseName: "unknown effect rejected",
			policy: Policy{
				Statement: []Statement{{Effect: "Maybe", Action: stringOrSlice{"s3:GetObject"}, Resource: stringOrSlice{"*"}}},
			},
			wantErr: true,
		},
		{
			caseName: "empty action rejected",
			policy: Policy{
				Statement: []Statement{{Effect: "Allow", Resource: stringOrSlice{"*"}}},
			},
			wantErr: true,
		},
		{
			caseName: "empty resource rejected",
			policy: Policy{
				Statement: []Statement{{Effect: "Allow", Action: stringOrSlice{"s3:GetObject"}}},
			},
			wantErr: true,
		},
		{
			caseName: "non-empty condition rejected",
			policy: Policy{
				Statement: []Statement{{
					Effect:    "Allow",
					Action:    stringOrSlice{"s3:GetObject"},
					Resource:  stringOrSlice{"*"},
					Condition: json.RawMessage(`{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}`),
				}},
			},
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			err := tc.policy.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStringOrSliceUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		caseName string
		input    string
		want     stringOrSlice
	}{
		{caseName: "bare string", input: `"s3:GetObject"`, want: stringOrSlice{"s3:GetObject"}},
		{caseName: "array", input: `["s3:GetObject", "s3:ListBucket"]`, want: stringOrSlice{"s3:GetObject", "s3:ListBucket"}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			var got stringOrSlice
			require.NoError(t, json.Unmarshal([]byte(tc.input), &got))
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestStringOrSliceUnmarshalJSONRejectsNull guards against "Action": null
// (or Resource) producing stringOrSlice{""} -- a one-element slice with an
// empty string, which Validate's len(...) == 0 check wouldn't catch,
// silently making an inert Deny statement never apply.
func TestStringOrSliceUnmarshalJSONRejectsNull(t *testing.T) {
	var got stringOrSlice
	err := json.Unmarshal([]byte(`null`), &got)
	assert.Error(t, err)
}

// TestPolicyValidateRejectsNullActionOrResource is the end-to-end version
// of TestStringOrSliceUnmarshalJSONRejectsNull: a full policy document
// with "Action": null must fail to even parse (and therefore fail
// Config.LoadFile), not silently validate as an inert statement.
func TestPolicyValidateRejectsNullActionOrResource(t *testing.T) {
	testCases := []struct {
		caseName string
		input    string
	}{
		{caseName: "null action", input: `{"Statement":[{"Effect":"Deny","Action":null,"Resource":"arn:aws:s3:::secret/*"}]}`},
		{caseName: "null resource", input: `{"Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":null}]}`},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			var p Policy
			assert.Error(t, json.Unmarshal([]byte(tc.input), &p))
		})
	}
}

func TestWildcardMatch(t *testing.T) {
	testCases := []struct {
		caseName string
		pattern  string
		s        string
		want     bool
	}{
		{caseName: "exact match", pattern: "s3:GetObject", s: "s3:GetObject", want: true},
		{caseName: "exact mismatch", pattern: "s3:GetObject", s: "s3:PutObject", want: false},
		{caseName: "trailing wildcard", pattern: "s3:Get*", s: "s3:GetObject", want: true},
		{caseName: "trailing wildcard mismatch", pattern: "s3:Get*", s: "s3:PutObject", want: false},
		{caseName: "full wildcard", pattern: "*", s: "anything", want: true},
		{caseName: "middle wildcard resource", pattern: "arn:aws:s3:::bucket/*", s: "arn:aws:s3:::bucket/foo/bar.txt", want: true},
		{caseName: "wildcard does not stop at slash", pattern: "arn:aws:s3:::*", s: "arn:aws:s3:::bucket/foo/bar.txt", want: true},
		{caseName: "multiple wildcards, middle segment matches", pattern: "arn:aws:s3:::my-*-bucket/*", s: "arn:aws:s3:::my-prod-bucket/foo/bar.txt", want: true},
		{caseName: "multiple wildcards, middle segment absent", pattern: "arn:aws:s3:::my-*-bucket/*", s: "arn:aws:s3:::my-prod-other/foo.txt", want: false},
		{caseName: "multiple wildcards, three segments", pattern: "a*b*c", s: "aXXbYYc", want: true},
		{caseName: "multiple wildcards, middle segment out of order", pattern: "a*b*c", s: "acXXbYY", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, wildcardMatch(tc.pattern, tc.s))
		})
	}
}

func TestS3Action(t *testing.T) {
	testCases := []struct {
		caseName     string
		method       string
		url          string
		bucket       string
		key          string
		wantAction   string
		wantResource string
	}{
		{
			caseName: "list all my buckets",
			method:   http.MethodGet,
			url:      "/",
			// HandleListBuckets filters the result per-bucket via
			// FilterBucketNames instead of gating the whole request on a
			// single up-front resource, mirroring ConsoleAction's GET /.
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			caseName:     "create bucket",
			method:       http.MethodPut,
			url:          "/mybucket",
			bucket:       "mybucket",
			wantAction:   "s3:CreateBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "delete bucket",
			method:       http.MethodDelete,
			url:          "/mybucket",
			bucket:       "mybucket",
			wantAction:   "s3:DeleteBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "head bucket",
			method:       http.MethodHead,
			url:          "/mybucket",
			bucket:       "mybucket",
			wantAction:   "s3:ListBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			// A trailing slash matches Go ServeMux's "/{bucket}/{key...}"
			// pattern with an empty key (handleDeleteObject), not the
			// bucket-level "/{bucket}" pattern (handleDeleteBucket) --
			// even though PathValue("key") is "" either way. Classifying
			// this as DeleteBucket would authorize a different operation
			// than the one that actually runs.
			caseName:     "delete with trailing slash routes to object handler, not bucket handler",
			method:       http.MethodDelete,
			url:          "/mybucket/",
			bucket:       "mybucket",
			key:          "",
			wantAction:   "s3:DeleteObject",
			wantResource: "arn:aws:s3:::mybucket/",
		},
		{
			// Unlike DELETE/PUT, handleGetObject delegates key=="" back to
			// handleBucketGET regardless of the trailing slash, so this
			// must be checked as a bucket-level action -- classifying it
			// as GetObject would let a GetObject-only grant enumerate the
			// whole bucket via ListObjectsV2.
			caseName:     "get with trailing slash still routes to the bucket handler",
			method:       http.MethodGet,
			url:          "/mybucket/",
			bucket:       "mybucket",
			key:          "",
			wantAction:   "s3:ListBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "head with trailing slash still routes to the bucket handler",
			method:       http.MethodHead,
			url:          "/mybucket/",
			bucket:       "mybucket",
			key:          "",
			wantAction:   "s3:ListBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "get bucket location with trailing slash still routes to the bucket handler",
			method:       http.MethodGet,
			url:          "/mybucket/?location",
			bucket:       "mybucket",
			key:          "",
			wantAction:   "s3:GetBucketLocation",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "put with trailing slash routes to object handler, not bucket handler",
			method:       http.MethodPut,
			url:          "/mybucket/",
			bucket:       "mybucket",
			key:          "",
			wantAction:   "s3:PutObject",
			wantResource: "arn:aws:s3:::mybucket/",
		},
		{
			caseName:     "get bucket location",
			method:       http.MethodGet,
			url:          "/mybucket?location",
			bucket:       "mybucket",
			wantAction:   "s3:GetBucketLocation",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "list objects",
			method:       http.MethodGet,
			url:          "/mybucket",
			bucket:       "mybucket",
			wantAction:   "s3:ListBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "list multipart uploads",
			method:       http.MethodGet,
			url:          "/mybucket?uploads",
			bucket:       "mybucket",
			wantAction:   "s3:ListBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName: "batch delete objects has no single up-front resource",
			method:   http.MethodPost,
			url:      "/mybucket?delete",
			bucket:   "mybucket",
			// The affected keys are only known once the handler decodes
			// the request body, so S3Action defers entirely to
			// handleDeleteObjects' per-key AllowedS3Action checks.
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			caseName: "batch delete objects via trailing-slash bucket URL",
			method:   http.MethodPost,
			url:      "/mybucket/?delete",
			bucket:   "mybucket",
			// "POST /{bucket}/?delete" routes to "/{bucket}/{key...}" with
			// an empty key and is delegated back to handleBucketPOST /
			// handleDeleteObjects (see handleObjectPOST). It must defer
			// identically to the no-trailing-slash form above rather than
			// being checked as s3:PutObject on the empty-key resource.
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			caseName:     "get object",
			method:       http.MethodGet,
			url:          "/mybucket/key.txt",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:GetObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
		{
			caseName:     "head object",
			method:       http.MethodHead,
			url:          "/mybucket/key.txt",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:GetObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
		{
			caseName:     "put object",
			method:       http.MethodPut,
			url:          "/mybucket/key.txt",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:PutObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
		{
			caseName:     "upload part",
			method:       http.MethodPut,
			url:          "/mybucket/key.txt?uploadId=abc&partNumber=1",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:PutObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
		{
			caseName:     "delete object",
			method:       http.MethodDelete,
			url:          "/mybucket/key.txt",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:DeleteObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
		{
			caseName:     "abort multipart upload",
			method:       http.MethodDelete,
			url:          "/mybucket/key.txt?uploadId=abc",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:PutObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
		{
			caseName:     "create multipart upload",
			method:       http.MethodPost,
			url:          "/mybucket/key.txt?uploads",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:PutObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
		{
			caseName:     "complete multipart upload",
			method:       http.MethodPost,
			url:          "/mybucket/key.txt?uploadId=abc",
			bucket:       "mybucket",
			key:          "key.txt",
			wantAction:   "s3:PutObject",
			wantResource: "arn:aws:s3:::mybucket/key.txt",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.url, nil)
			action, resource := S3Action(r, tc.bucket, tc.key)
			assert.Equal(t, tc.wantAction, action)
			assert.Equal(t, tc.wantResource, resource)
		})
	}
}

func TestConsoleAction(t *testing.T) {
	testCases := []struct {
		caseName     string
		method       string
		url          string
		pathValues   map[string]string
		wantAction   string
		wantResource string
	}{
		{
			// GET / is the console's only entry point (login landing
			// page + bucket sidebar data source), so it must never be
			// blocked by a policy check -- FilterBucketNames handles
			// visibility instead. See ConsoleAction's doc comment.
			caseName:     "index bucket list has no single up-front resource",
			method:       http.MethodGet,
			url:          "/",
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			// The bucket name lives in the form body, not the URL, so
			// ConsoleAction defers to handleCreateBucket's
			// AllowedS3BucketAction check on the real name.
			caseName:     "create bucket has no single up-front resource",
			method:       http.MethodPost,
			url:          "/buckets",
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			caseName:     "delete bucket",
			method:       http.MethodDelete,
			url:          "/buckets/mybucket",
			pathValues:   map[string]string{"name": "mybucket"},
			wantAction:   "s3:DeleteBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			caseName:     "list objects",
			method:       http.MethodGet,
			url:          "/buckets/mybucket",
			pathValues:   map[string]string{"name": "mybucket"},
			wantAction:   "s3:ListBucket",
			wantResource: "arn:aws:s3:::mybucket",
		},
		{
			// The target key is only known once handleCreateFolder parses
			// the form body, so ConsoleAction defers entirely; the handler
			// checks the real key itself via AllowedS3Action.
			caseName:     "create folder has no single up-front resource",
			method:       http.MethodPost,
			url:          "/buckets/mybucket/folders",
			pathValues:   map[string]string{"name": "mybucket"},
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			// Same as create folder: the filename is only known once
			// handleUploadFile parses the multipart body.
			caseName:     "upload file has no single up-front resource",
			method:       http.MethodPost,
			url:          "/buckets/mybucket/upload",
			pathValues:   map[string]string{"name": "mybucket"},
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			caseName:     "delete object",
			method:       http.MethodDelete,
			url:          "/buckets/mybucket/objects?key=foo.txt",
			pathValues:   map[string]string{"name": "mybucket"},
			wantAction:   "s3:DeleteObject",
			wantResource: "arn:aws:s3:::mybucket/foo.txt",
		},
		{
			// A recursive folder delete's affected keys are only known
			// once handleDeleteObject lists the prefix, so ConsoleAction
			// defers entirely rather than checking the bare prefix (which
			// would miss a Deny scoped to one descendant key).
			caseName:     "delete folder has no single up-front resource",
			method:       http.MethodDelete,
			url:          "/buckets/mybucket/objects?key=folder/",
			pathValues:   map[string]string{"name": "mybucket"},
			wantAction:   actionDeferred,
			wantResource: "",
		},
		{
			caseName:     "view object",
			method:       http.MethodGet,
			url:          "/buckets/mybucket/view/foo.txt",
			pathValues:   map[string]string{"name": "mybucket", "object": "foo.txt"},
			wantAction:   "s3:GetObject",
			wantResource: "arn:aws:s3:::mybucket/foo.txt",
		},
		{
			caseName:     "meta object",
			method:       http.MethodGet,
			url:          "/buckets/mybucket/meta/foo.txt",
			pathValues:   map[string]string{"name": "mybucket", "object": "foo.txt"},
			wantAction:   "s3:GetObject",
			wantResource: "arn:aws:s3:::mybucket/foo.txt",
		},
		{
			caseName:     "preview object",
			method:       http.MethodGet,
			url:          "/buckets/mybucket/preview/foo.txt",
			pathValues:   map[string]string{"name": "mybucket", "object": "foo.txt"},
			wantAction:   "s3:GetObject",
			wantResource: "arn:aws:s3:::mybucket/foo.txt",
		},
		{
			caseName:     "static asset bypasses check",
			method:       http.MethodGet,
			url:          "/static/app.css",
			wantAction:   actionSkip,
			wantResource: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.url, nil)
			for k, v := range tc.pathValues {
				r.SetPathValue(k, v)
			}
			action, resource := ConsoleAction(r)
			assert.Equal(t, tc.wantAction, action)
			assert.Equal(t, tc.wantResource, resource)
		})
	}
}

package middleware

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/assert"
)

// presignRequest rewrites r.URL.RawQuery to embed AWS SigV4 presigned-URL parameters.
// expires is the validity window in seconds.
func presignRequest(r *http.Request, accessKey, secretKey string, expires int) {
	presignRequestAt(r, accessKey, secretKey, expires, time.Now().UTC())
}

// presignRequestAt is like presignRequest but uses the supplied time as X-Amz-Date.
func presignRequestAt(r *http.Request, accessKey, secretKey string, expires int, now time.Time) {
	date := now.Format("20060102")
	datetime := now.Format("20060102T150405Z")
	const region, service = "us-east-1", "s3"
	if r.Host == "" {
		r.Host = "localhost:9000"
	}
	signedHeaders := []string{"host"}
	scope := date + "/" + region + "/" + service + "/aws4_request"

	q := r.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", accessKey+"/"+scope)
	q.Set("X-Amz-Date", datetime)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", expires))
	q.Set("X-Amz-SignedHeaders", strings.Join(signedHeaders, ";"))
	r.URL.RawQuery = q.Encode()

	canonReq := buildCanonicalRequest(r, signedHeaders, r.URL.RawQuery, "UNSIGNED-PAYLOAD")
	stringToSign := "AWS4-HMAC-SHA256\n" + datetime + "\n" + scope + "\n" + hashSHA256(canonReq)
	signingKey := buildSigningKey(secretKey, date, region, service)
	sig := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	q.Set("X-Amz-Signature", sig)
	r.URL.RawQuery = q.Encode()
}

// signRequest adds AWS Signature V4 headers to r signed at the current time.
func signRequest(r *http.Request, accessKey, secretKey string) {
	signRequestAt(r, accessKey, secretKey, time.Now().UTC())
}

// signRequestAt adds AWS Signature V4 headers to r signed at the given time.
func signRequestAt(r *http.Request, accessKey, secretKey string, now time.Time) {
	date := now.Format("20060102")
	datetime := now.Format("20060102T150405Z")
	const region, service = "us-east-1", "s3"

	r.Header.Set("X-Amz-Date", datetime)
	r.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	if r.Host == "" {
		r.Host = "localhost:9000"
	}

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}

	canonReq := buildCanonicalRequest(r, signedHeaders, r.URL.RawQuery, emptyStringSHA256)
	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + datetime + "\n" + scope + "\n" + hashSHA256(canonReq)
	signingKey := buildSigningKey(secretKey, date, region, service)
	sig := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	r.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, strings.Join(signedHeaders, ";"), sig,
	))
}

func TestSigV4(t *testing.T) {
	testCases := []struct {
		caseName   string
		user       string
		password   string
		signFunc   func(r *http.Request) // nil = no signing
		wantStatus int
	}{
		{
			caseName:   "auth disabled",
			wantStatus: http.StatusOK,
		},
		{
			caseName:   "missing authorization header",
			user:       "minioadmin",
			password:   "minioadmin",
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "valid signature",
			user:     "minioadmin",
			password: "minioadmin",
			signFunc: func(r *http.Request) {
				signRequest(r, "minioadmin", "minioadmin")
			},
			wantStatus: http.StatusOK,
		},
		{
			caseName: "invalid secret key",
			user:     "minioadmin",
			password: "minioadmin",
			signFunc: func(r *http.Request) {
				signRequest(r, "minioadmin", "wrongsecret")
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "wrong access key",
			user:     "minioadmin",
			password: "minioadmin",
			signFunc: func(r *http.Request) {
				signRequest(r, "wrongkey", "minioadmin")
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "expired timestamp",
			user:     "minioadmin",
			password: "minioadmin",
			signFunc: func(r *http.Request) {
				signRequestAt(r, "minioadmin", "minioadmin", time.Now().UTC().Add(-16*time.Minute))
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "future timestamp",
			user:     "minioadmin",
			password: "minioadmin",
			signFunc: func(r *http.Request) {
				signRequestAt(r, "minioadmin", "minioadmin", time.Now().UTC().Add(16*time.Minute))
			},
			wantStatus: http.StatusForbidden,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			srv := &server.Server{Config: &server.Config{User: tc.user, Password: tc.password}}
			handler := SigV4(noopHandler)

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.signFunc != nil {
				tc.signFunc(r)
			}
			w := httptest.NewRecorder()
			handler(srv, w, r)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

func TestSigV4Presigned(t *testing.T) {
	const user, pass = "minioadmin", "minioadmin"
	testCases := []struct {
		caseName   string
		method     string
		setup      func(r *http.Request)
		wantStatus int
	}{
		{
			caseName: "valid presigned GET",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				presignRequest(r, user, pass, 300)
			},
			wantStatus: http.StatusOK,
		},
		{
			caseName: "valid presigned PUT",
			method:   http.MethodPut,
			setup: func(r *http.Request) {
				presignRequest(r, user, pass, 300)
			},
			wantStatus: http.StatusOK,
		},
		{
			caseName: "expired presigned URL",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				presignRequestAt(r, user, pass, 60, time.Now().UTC().Add(-2*time.Minute))
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "tampered signature",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				presignRequest(r, user, pass, 300)
				q := r.URL.Query()
				q.Set("X-Amz-Signature", strings.Repeat("0", 64))
				r.URL.RawQuery = q.Encode()
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "wrong access key",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				presignRequest(r, "wrongkey", pass, 300)
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "wrong secret key",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				presignRequest(r, user, "wrongsecret", 300)
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "missing X-Amz-Signature",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				presignRequest(r, user, pass, 300)
				q := r.URL.Query()
				q.Del("X-Amz-Signature")
				r.URL.RawQuery = q.Encode()
			},
			wantStatus: http.StatusForbidden,
		},
		{
			caseName: "header takes precedence over query",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				// Presigned query is invalid (signature missing) but a valid Authorization header is present.
				presignRequest(r, user, pass, 300)
				q := r.URL.Query()
				q.Set("X-Amz-Signature", strings.Repeat("0", 64))
				r.URL.RawQuery = q.Encode()
				signRequest(r, user, pass)
			},
			wantStatus: http.StatusOK,
		},
		{
			caseName: "extra query parameter included in signature",
			method:   http.MethodGet,
			setup: func(r *http.Request) {
				r.URL.RawQuery = "prefix=foo/bar&max-keys=10"
				presignRequest(r, user, pass, 300)
			},
			wantStatus: http.StatusOK,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			srv := &server.Server{Config: &server.Config{User: user, Password: pass}}
			handler := SigV4(noopHandler)

			r := httptest.NewRequest(tc.method, "/bucket/key", nil)
			tc.setup(r)
			w := httptest.NewRecorder()
			handler(srv, w, r)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

func TestStripQueryParam(t *testing.T) {
	testCases := []struct {
		caseName string
		raw      string
		name     string
		want     string
	}{
		{caseName: "empty", raw: "", name: "X-Amz-Signature", want: ""},
		{caseName: "only target", raw: "X-Amz-Signature=abc", name: "X-Amz-Signature", want: ""},
		{caseName: "target at end", raw: "a=1&X-Amz-Signature=abc", name: "X-Amz-Signature", want: "a=1"},
		{caseName: "target in middle", raw: "a=1&X-Amz-Signature=abc&b=2", name: "X-Amz-Signature", want: "a=1&b=2"},
		{caseName: "target absent", raw: "a=1&b=2", name: "X-Amz-Signature", want: "a=1&b=2"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, stripQueryParam(tc.raw, tc.name))
		})
	}
}

func TestCanonicalQueryString(t *testing.T) {
	testCases := []struct {
		caseName string
		input    string
		want     string
	}{
		{caseName: "empty", input: "", want: ""},
		{caseName: "sorted", input: "b=2&a=1", want: "a=1&b=2"},
		{caseName: "plus to space", input: "key=hello+world", want: "key=hello%20world"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, canonicalQueryString(tc.input))
		})
	}
}

func TestCanonicalURI(t *testing.T) {
	testCases := []struct {
		caseName string
		url      string
		want     string
	}{
		{caseName: "simple path", url: "/my-bucket/path/to/key", want: "/my-bucket/path/to/key"},
		{caseName: "encoded spaces", url: "/my-bucket/key%20with%20spaces", want: "/my-bucket/key%20with%20spaces"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tc.url, nil)
			assert.Equal(t, tc.want, canonicalURI(r))
		})
	}
}

func TestAWSURIEncode(t *testing.T) {
	testCases := []struct {
		caseName string
		input    string
		want     string
	}{
		{caseName: "plain", input: "hello", want: "hello"},
		{caseName: "space", input: "hello world", want: "hello%20world"},
		{caseName: "slash", input: "hello/world", want: "hello%2Fworld"},
		{caseName: "unreserved chars", input: "key~_.-", want: "key~_.-"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, awsURIEncode(tc.input))
		})
	}
}

func TestParseAuthHeader(t *testing.T) {
	testCases := []struct {
		caseName string
		input    string
		wantKey  string
		wantVal  string
	}{
		{
			caseName: "credential",
			input:    "Credential=AKID/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123",
			wantKey:  "Credential",
			wantVal:  "AKID/20130524/us-east-1/s3/aws4_request",
		},
		{
			caseName: "signed headers",
			input:    "Credential=AKID/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123",
			wantKey:  "SignedHeaders",
			wantVal:  "host;x-amz-date",
		},
		{
			caseName: "signature",
			input:    "Credential=AKID/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123",
			wantKey:  "Signature",
			wantVal:  "abc123",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			parts := parseAuthHeader(tc.input)
			assert.Equal(t, tc.wantVal, parts[tc.wantKey])
		})
	}
}

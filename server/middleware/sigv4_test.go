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

			r := httptest.NewRequest(http.MethodGet, "/s3api", nil)
			if tc.signFunc != nil {
				tc.signFunc(r)
			}
			w := httptest.NewRecorder()
			handler(srv, w, r)

			assert.Equal(t, tc.wantStatus, w.Code)
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
		{caseName: "simple path", url: "/s3api/my-bucket/path/to/key", want: "/s3api/my-bucket/path/to/key"},
		{caseName: "encoded spaces", url: "/s3api/my-bucket/key%20with%20spaces", want: "/s3api/my-bucket/key%20with%20spaces"},
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

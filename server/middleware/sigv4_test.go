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
	"github.com/stretchr/testify/require"
)

// signRequest adds AWS Signature V4 headers to r using the same signing logic as this package.
func signRequest(r *http.Request, accessKey, secretKey string) {
	now := time.Now().UTC()
	date := now.Format("20060102")
	datetime := now.Format("20060102T150405Z")
	region := "us-east-1"
	service := "s3"

	r.Header.Set("X-Amz-Date", datetime)
	r.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	if r.Host == "" {
		r.Host = "localhost:9000"
	}

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}

	canonReq := buildCanonicalRequest(r, signedHeaders)
	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + datetime + "\n" + scope + "\n" + hashSHA256(canonReq)
	signingKey := buildSigningKey(secretKey, date, region, service)
	sig := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	r.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, strings.Join(signedHeaders, ";"), sig,
	))
}

func TestSigV4_NoAuth(t *testing.T) {
	srv := &server.Server{Config: &server.Config{}}
	handler := SigV4(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/s3api", nil)
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestSigV4_MissingAuthorizationHeader(t *testing.T) {
	srv := &server.Server{Config: &server.Config{User: "minioadmin", Password: "minioadmin"}}
	handler := SigV4(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/s3api", nil)
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestSigV4_ValidSignature(t *testing.T) {
	srv := &server.Server{Config: &server.Config{User: "minioadmin", Password: "minioadmin"}}
	handler := SigV4(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/s3api", nil)
	signRequest(r, "minioadmin", "minioadmin")
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestSigV4_InvalidSignature(t *testing.T) {
	srv := &server.Server{Config: &server.Config{User: "minioadmin", Password: "minioadmin"}}
	handler := SigV4(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/s3api", nil)
	signRequest(r, "minioadmin", "wrongsecret")
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestSigV4_WrongAccessKey(t *testing.T) {
	srv := &server.Server{Config: &server.Config{User: "minioadmin", Password: "minioadmin"}}
	handler := SigV4(noopHandler)

	r := httptest.NewRequest(http.MethodGet, "/s3api", nil)
	signRequest(r, "wrongkey", "minioadmin")
	w := httptest.NewRecorder()
	handler(srv, w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestParseAuthHeader(t *testing.T) {
	s := "Credential=AKID/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123"
	parts := parseAuthHeader(s)
	require.Equal(t, "AKID/20130524/us-east-1/s3/aws4_request", parts["Credential"])
	require.Equal(t, "host;x-amz-date", parts["SignedHeaders"])
	require.Equal(t, "abc123", parts["Signature"])
}

func TestCanonicalQueryString(t *testing.T) {
	assert.Equal(t, "", canonicalQueryString(""))
	assert.Equal(t, "a=1&b=2", canonicalQueryString("b=2&a=1"))
	assert.Equal(t, "key=hello%20world", canonicalQueryString("key=hello+world"))
}

func TestCanonicalURI(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/s3api/my-bucket/path/to/key", nil)
	assert.Equal(t, "/s3api/my-bucket/path/to/key", canonicalURI(r))

	r2 := httptest.NewRequest(http.MethodGet, "/s3api/my-bucket/key%20with%20spaces", nil)
	assert.Equal(t, "/s3api/my-bucket/key%20with%20spaces", canonicalURI(r2))
}

func TestAWSURIEncode(t *testing.T) {
	assert.Equal(t, "hello", awsURIEncode("hello"))
	assert.Equal(t, "hello%20world", awsURIEncode("hello world"))
	assert.Equal(t, "hello%2Fworld", awsURIEncode("hello/world"))
	assert.Equal(t, "key~_.-", awsURIEncode("key~_.-"))
}

package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mojatter/s2/server"
)

const (
	sigV4MaxClockSkew = 15 * time.Minute
	// emptyStringSHA256 is the hex SHA-256 of the empty string.
	emptyStringSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// SigV4 returns a handler that enforces AWS Signature Version 4 authentication for S3 API routes.
// Authentication is skipped when no credentials are configured (AuthEnabled false).
// Requests with X-Amz-Date outside ±15 minutes of server time are rejected.
//
// After a successful signature verification, if the matched User carries a
// Policy, the request's S3 action/resource (see S3Action) is checked against
// it; a denied request gets a 403 AccessDenied response distinct from the
// SignatureDoesNotMatch used for verification failures. The matched User is
// stashed on the request context (server.WithUser) so handlers such as
// HandleListBuckets can filter their results by the same policy.
func SigV4(next server.HandlerFunc) server.HandlerFunc {
	return func(srv *server.Server, w http.ResponseWriter, r *http.Request) {
		if !srv.Config.AuthEnabled() {
			next(srv, w, r)
			return
		}

		lookup := func(accessKeyID string) (*server.User, bool) {
			u := srv.Config.LookupUser(accessKeyID)
			return u, u != nil
		}

		// AWS prefers the Authorization header when both are present.
		// Fall back to query-string (presigned URL) verification when the header is absent.
		var matched *server.User
		var err error
		if r.Header.Get("Authorization") == "" && r.URL.Query().Get("X-Amz-Algorithm") != "" {
			matched, err = verifyPresignedV4(r, lookup, time.Now().UTC())
		} else {
			matched, err = verifySignatureV4(r, lookup)
		}
		if err != nil {
			writeS3AuthError(w, r, err.Error())
			return
		}

		bucket, key := r.PathValue("bucket"), r.PathValue("key")
		action, resource := server.S3Action(r, bucket, key)
		if !server.Authorized(matched, action, resource) {
			writeS3AccessDeniedError(w, r)
			return
		}

		next(srv, w, r.WithContext(server.WithUser(r.Context(), matched)))
	}
}

func writeS3AuthError(w http.ResponseWriter, r *http.Request, message string) {
	server.WriteS3Error(w, r, "SignatureDoesNotMatch", message, http.StatusForbidden)
}

func writeS3AccessDeniedError(w http.ResponseWriter, r *http.Request) {
	server.WriteS3Error(w, r, "AccessDenied", "Access Denied", http.StatusForbidden)
}

// verifySignatureV4 verifies the AWS Signature Version 4 of an HTTP request.
// lookup resolves the request's access key ID to the matching User, or
// ok=false for an unrecognized access key. On success, the matched User is
// returned directly rather than threaded back through a captured variable.
func verifySignatureV4(r *http.Request, lookup func(accessKeyID string) (user *server.User, ok bool)) (*server.User, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return nil, fmt.Errorf("unsupported authorization scheme")
	}

	parts := parseAuthHeader(authHeader[len("AWS4-HMAC-SHA256 "):])
	credential := parts["Credential"]
	signedHeadersStr := parts["SignedHeaders"]
	signature := parts["Signature"]
	if credential == "" || signedHeadersStr == "" || signature == "" {
		return nil, fmt.Errorf("malformed Authorization header")
	}

	// Credential = <access-key>/<date>/<region>/<service>/aws4_request
	credParts := strings.SplitN(credential, "/", 5)
	if len(credParts) != 5 {
		return nil, fmt.Errorf("malformed Credential")
	}
	reqAccessKey := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]

	user, ok := lookup(reqAccessKey)
	if !ok {
		return nil, fmt.Errorf("invalid access key")
	}

	datetime := r.Header.Get("X-Amz-Date")
	if datetime == "" {
		return nil, fmt.Errorf("missing X-Amz-Date header")
	}
	reqTime, err := time.Parse("20060102T150405Z", datetime)
	if err != nil {
		return nil, fmt.Errorf("invalid X-Amz-Date: %w", err)
	}
	if diff := time.Since(reqTime).Abs(); diff > sigV4MaxClockSkew {
		return nil, fmt.Errorf("request timestamp too skewed: %v", diff.Round(time.Second))
	}

	signedHeaders := strings.Split(signedHeadersStr, ";")
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = emptyStringSHA256
	}
	canonReq := buildCanonicalRequest(r, signedHeaders, r.URL.RawQuery, payloadHash)

	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + datetime + "\n" + scope + "\n" + hashSHA256(canonReq)

	signingKey := buildSigningKey(user.SecretAccessKey, date, region, service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
		return nil, fmt.Errorf("signature mismatch")
	}
	return user, nil
}

// verifyPresignedV4 verifies an AWS Signature Version 4 presigned URL request.
// The signature is carried in query parameters (X-Amz-*) instead of the Authorization header,
// and the body is treated as UNSIGNED-PAYLOAD per the presigned-URL spec.
// lookup resolves the request's access key ID to the matching User, or
// ok=false for an unrecognized access key.
func verifyPresignedV4(r *http.Request, lookup func(accessKeyID string) (user *server.User, ok bool), now time.Time) (*server.User, error) {
	q := r.URL.Query()
	if algo := q.Get("X-Amz-Algorithm"); algo != "AWS4-HMAC-SHA256" {
		return nil, fmt.Errorf("unsupported X-Amz-Algorithm: %q", algo)
	}
	credential := q.Get("X-Amz-Credential")
	datetime := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	signedHeadersStr := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")
	if credential == "" || datetime == "" || expiresStr == "" || signedHeadersStr == "" || signature == "" {
		return nil, fmt.Errorf("missing presigned query parameters")
	}

	// Credential = <access-key>/<date>/<region>/<service>/aws4_request
	credParts := strings.SplitN(credential, "/", 5)
	if len(credParts) != 5 {
		return nil, fmt.Errorf("malformed Credential")
	}
	reqAccessKey := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]

	user, ok := lookup(reqAccessKey)
	if !ok {
		return nil, fmt.Errorf("invalid access key")
	}

	reqTime, err := time.Parse("20060102T150405Z", datetime)
	if err != nil {
		return nil, fmt.Errorf("invalid X-Amz-Date: %w", err)
	}
	expires, err := strconv.Atoi(expiresStr)
	if err != nil || expires <= 0 {
		return nil, fmt.Errorf("invalid X-Amz-Expires: %q", expiresStr)
	}
	if now.After(reqTime.Add(time.Duration(expires) * time.Second)) {
		return nil, fmt.Errorf("presigned URL expired")
	}

	signedHeaders := strings.Split(signedHeadersStr, ";")
	// X-Amz-Signature must be excluded from the canonical query string.
	filteredQuery := stripQueryParam(r.URL.RawQuery, "X-Amz-Signature")
	canonReq := buildCanonicalRequest(r, signedHeaders, filteredQuery, "UNSIGNED-PAYLOAD")

	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + datetime + "\n" + scope + "\n" + hashSHA256(canonReq)

	signingKey := buildSigningKey(user.SecretAccessKey, date, region, service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
		return nil, fmt.Errorf("signature mismatch")
	}
	return user, nil
}

// stripQueryParam removes all occurrences of name from a raw (unparsed) query string,
// preserving the order and exact encoding of the remaining parameters.
func stripQueryParam(rawQuery, name string) string {
	if rawQuery == "" {
		return ""
	}
	prefix := name + "="
	parts := strings.Split(rawQuery, "&")
	out := parts[:0]
	for _, p := range parts {
		if p == name || strings.HasPrefix(p, prefix) {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, "&")
}

func parseAuthHeader(s string) map[string]string {
	result := make(map[string]string)
	for _, part := range strings.Split(s, ", ") {
		part = strings.TrimSpace(part)
		idx := strings.IndexByte(part, '=')
		if idx > 0 {
			result[part[:idx]] = part[idx+1:]
		}
	}
	return result
}

// buildCanonicalRequest builds the AWS SigV4 canonical request string.
// rawQuery is the unprocessed query string (it will be canonicalized internally);
// callers in presigned-URL mode should pre-strip X-Amz-Signature before passing it.
// payloadHash is the hex SHA-256 of the body, or "UNSIGNED-PAYLOAD" for presigned URLs.
func buildCanonicalRequest(r *http.Request, signedHeaders []string, rawQuery, payloadHash string) string {
	sorted := make([]string, len(signedHeaders))
	copy(sorted, signedHeaders)
	sort.Strings(sorted)

	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalURI(r))
	b.WriteByte('\n')
	b.WriteString(canonicalQueryString(rawQuery))
	b.WriteByte('\n')
	for _, name := range sorted {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(getSignedHeaderValue(r, name))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(strings.Join(sorted, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String()
}

func getSignedHeaderValue(r *http.Request, name string) string {
	if name == "host" {
		if r.Host != "" {
			return r.Host
		}
		return r.URL.Host
	}
	return strings.TrimSpace(r.Header.Get(name))
}

func canonicalURI(r *http.Request) string {
	path := r.URL.Path
	if path == "" {
		return "/"
	}
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = awsURIEncode(seg)
	}
	return strings.Join(segments, "/")
}

func canonicalQueryString(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	type kv struct{ k, v string }
	var pairs []kv
	for _, part := range strings.Split(rawQuery, "&") {
		if part == "" {
			continue
		}
		idx := strings.IndexByte(part, '=')
		var k, v string
		if idx < 0 {
			k = part
		} else {
			k, v = part[:idx], part[idx+1:]
		}
		kDec, _ := url.QueryUnescape(k)
		vDec, _ := url.QueryUnescape(v)
		pairs = append(pairs, kv{awsURIEncode(kDec), awsURIEncode(vDec)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func awsURIEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUnreserved(c) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func isUnreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
		c == '-' || c == '_' || c == '.' || c == '~'
}

func hashSHA256(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func buildSigningKey(secretKey, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

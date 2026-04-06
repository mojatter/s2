package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/mojatter/s2/server"
)

const sigV4MaxClockSkew = 15 * time.Minute

// SigV4 returns a handler that enforces AWS Signature Version 4 authentication for S3 API routes.
// Authentication is skipped when User is not configured.
// Requests with X-Amz-Date outside ±15 minutes of server time are rejected.
func SigV4(next server.HandlerFunc) server.HandlerFunc {
	return func(srv *server.Server, w http.ResponseWriter, r *http.Request) {
		if srv.Config.User == "" {
			next(srv, w, r)
			return
		}
		if err := verifySignatureV4(r, srv.Config.User, srv.Config.Password); err != nil {
			writeS3AuthError(w, r, err.Error())
			return
		}
		next(srv, w, r)
	}
}

func writeS3AuthError(w http.ResponseWriter, r *http.Request, message string) {
	type ErrorResponse struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource"`
		RequestID string   `xml:"RequestId"`
	}
	resp := ErrorResponse{
		Code:      "SignatureDoesNotMatch",
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: "s2-request-id",
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusForbidden)
	_, _ = fmt.Fprint(w, xml.Header)
	enc := xml.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		slog.Error("Failed to encode XML", "error", err)
	}
}

// verifySignatureV4 verifies the AWS Signature Version 4 of an HTTP request.
func verifySignatureV4(r *http.Request, accessKeyID, secretAccessKey string) error {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return fmt.Errorf("missing Authorization header")
	}
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return fmt.Errorf("unsupported authorization scheme")
	}

	parts := parseAuthHeader(authHeader[len("AWS4-HMAC-SHA256 "):])
	credential := parts["Credential"]
	signedHeadersStr := parts["SignedHeaders"]
	signature := parts["Signature"]
	if credential == "" || signedHeadersStr == "" || signature == "" {
		return fmt.Errorf("malformed Authorization header")
	}

	// Credential = <access-key>/<date>/<region>/<service>/aws4_request
	credParts := strings.SplitN(credential, "/", 5)
	if len(credParts) != 5 {
		return fmt.Errorf("malformed Credential")
	}
	reqAccessKey := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]

	if subtle.ConstantTimeCompare([]byte(reqAccessKey), []byte(accessKeyID)) != 1 {
		return fmt.Errorf("invalid access key")
	}

	datetime := r.Header.Get("X-Amz-Date")
	if datetime == "" {
		return fmt.Errorf("missing X-Amz-Date header")
	}
	reqTime, err := time.Parse("20060102T150405Z", datetime)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Date: %w", err)
	}
	if diff := time.Since(reqTime).Abs(); diff > sigV4MaxClockSkew {
		return fmt.Errorf("request timestamp too skewed: %v", diff.Round(time.Second))
	}

	signedHeaders := strings.Split(signedHeadersStr, ";")
	canonReq := buildCanonicalRequest(r, signedHeaders)

	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + datetime + "\n" + scope + "\n" + hashSHA256(canonReq)

	signingKey := buildSigningKey(secretAccessKey, date, region, service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
		return fmt.Errorf("signature mismatch")
	}
	return nil
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

func buildCanonicalRequest(r *http.Request, signedHeaders []string) string {
	sorted := make([]string, len(signedHeaders))
	copy(sorted, signedHeaders)
	sort.Strings(sorted)

	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalURI(r))
	b.WriteByte('\n')
	b.WriteString(canonicalQueryString(r.URL.RawQuery))
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
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		// SHA256 of empty string
		payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
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

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mojatter/s2/server"
	"github.com/stretchr/testify/assert"
)

func noopHandler(_ *server.Server, w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestBasicAuth(t *testing.T) {
	testCases := []struct {
		caseName     string
		user         string
		password     string
		setBasicAuth bool
		authUser     string
		authPass     string
		wantStatus   int
		wantWWWAuth  string
	}{
		{
			caseName:   "auth disabled",
			wantStatus: http.StatusOK,
		},
		{
			caseName:    "missing credentials",
			user:        "admin",
			password:    "secret",
			wantStatus:  http.StatusUnauthorized,
			wantWWWAuth: `Basic realm="s2"`,
		},
		{
			caseName:     "wrong password",
			user:         "admin",
			password:     "secret",
			setBasicAuth: true,
			authUser:     "admin",
			authPass:     "wrong",
			wantStatus:   http.StatusUnauthorized,
			wantWWWAuth:  `Basic realm="s2"`,
		},
		{
			caseName:     "wrong user",
			user:         "admin",
			password:     "secret",
			setBasicAuth: true,
			authUser:     "other",
			authPass:     "secret",
			wantStatus:   http.StatusUnauthorized,
			wantWWWAuth:  `Basic realm="s2"`,
		},
		{
			caseName:     "correct credentials",
			user:         "admin",
			password:     "secret",
			setBasicAuth: true,
			authUser:     "admin",
			authPass:     "secret",
			wantStatus:   http.StatusOK,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			srv := &server.Server{Config: &server.Config{User: tc.user, Password: tc.password}}
			handler := BasicAuth(noopHandler)

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.setBasicAuth {
				r.SetBasicAuth(tc.authUser, tc.authPass)
			}
			w := httptest.NewRecorder()
			handler(srv, w, r)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantWWWAuth != "" {
				assert.Equal(t, tc.wantWWWAuth, w.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

func TestBasicAuthMultiUser(t *testing.T) {
	readOnlyPolicy := &server.Policy{Statement: []server.Statement{
		{Effect: "Allow", Action: []string{"s3:ListAllMyBuckets", "s3:ListBucket", "s3:GetObject"}, Resource: []string{"arn:aws:s3:::*"}},
	}}
	// noListAllBucketsPolicy deliberately omits s3:ListAllMyBuckets, unlike
	// readOnlyPolicy above -- GET / must still succeed for this user since
	// it's the console's only entry point (see ConsoleAction's doc
	// comment on the GET / case).
	noListAllBucketsPolicy := &server.Policy{Statement: []server.Statement{
		{Effect: "Allow", Action: []string{"s3:ListBucket", "s3:GetObject"}, Resource: []string{"arn:aws:s3:::visible"}},
	}}
	cfg := &server.Config{
		Users: []server.User{
			{AccessKeyID: "userA", SecretAccessKey: "secretA"},
			{AccessKeyID: "userB", SecretAccessKey: "secretB", Policy: readOnlyPolicy},
			{AccessKeyID: "userC", SecretAccessKey: "secretC", Policy: noListAllBucketsPolicy},
		},
	}

	testCases := []struct {
		caseName   string
		method     string
		url        string
		name       string
		authUser   string
		authPass   string
		wantStatus int
	}{
		{
			caseName:   "restricted user login succeeds (login is not policy-gated)",
			method:     http.MethodGet,
			url:        "/",
			authUser:   "userB",
			authPass:   "secretB",
			wantStatus: http.StatusOK,
		},
		{
			caseName:   "wrong password for valid access key rejected",
			method:     http.MethodGet,
			url:        "/",
			authUser:   "userB",
			authPass:   "wrong",
			wantStatus: http.StatusUnauthorized,
		},
		{
			caseName:   "restricted user denied console action gets 403",
			method:     http.MethodDelete,
			url:        "/buckets/mybucket",
			name:       "mybucket",
			authUser:   "userB",
			authPass:   "secretB",
			wantStatus: http.StatusForbidden,
		},
		{
			caseName:   "restricted user allowed console action passes",
			method:     http.MethodGet,
			url:        "/buckets/mybucket",
			name:       "mybucket",
			authUser:   "userB",
			authPass:   "secretB",
			wantStatus: http.StatusOK,
		},
		{
			caseName:   "unrestricted user allowed anything",
			method:     http.MethodDelete,
			url:        "/buckets/mybucket",
			name:       "mybucket",
			authUser:   "userA",
			authPass:   "secretA",
			wantStatus: http.StatusOK,
		},
		{
			caseName:   "user without s3:ListAllMyBuckets can still reach the console home page",
			method:     http.MethodGet,
			url:        "/",
			authUser:   "userC",
			authPass:   "secretC",
			wantStatus: http.StatusOK,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			srv := &server.Server{Config: cfg}
			handler := BasicAuth(noopHandler)

			r := httptest.NewRequest(tc.method, tc.url, nil)
			if tc.name != "" {
				r.SetPathValue("name", tc.name)
			}
			r.SetBasicAuth(tc.authUser, tc.authPass)
			w := httptest.NewRecorder()
			handler(srv, w, r)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

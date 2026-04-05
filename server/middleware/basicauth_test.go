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
		caseName       string
		user           string
		password       string
		setBasicAuth   bool
		authUser       string
		authPass       string
		wantStatus     int
		wantWWWAuth    string
	}{
		{
			caseName:   "auth disabled",
			wantStatus: http.StatusOK,
		},
		{
			caseName:   "missing credentials",
			user:       "admin",
			password:   "secret",
			wantStatus: http.StatusUnauthorized,
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

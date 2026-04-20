package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCORSHandler(t *testing.T) {
	testCases := []struct {
		caseName        string
		method          string
		wantCode        int
		wantHandlerHit  bool
	}{
		{
			caseName:       "GET passes through with CORS headers",
			method:         http.MethodGet,
			wantCode:       http.StatusOK,
			wantHandlerHit: true,
		},
		{
			caseName:       "PUT passes through with CORS headers",
			method:         http.MethodPut,
			wantCode:       http.StatusOK,
			wantHandlerHit: true,
		},
		{
			caseName:       "OPTIONS preflight returns 200 without hitting handler",
			method:         http.MethodOptions,
			wantCode:       http.StatusOK,
			wantHandlerHit: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			handlerHit := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				handlerHit = true
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(tc.method, "/bucket/key", nil)
			w := httptest.NewRecorder()
			corsHandler(next).ServeHTTP(w, req)

			assert.Equal(t, tc.wantCode, w.Code)
			assert.Equal(t, tc.wantHandlerHit, handlerHit)
			assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
			assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Methods"))
			assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Headers"))
		})
	}
}

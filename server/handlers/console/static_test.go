package console

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"
)

type StaticTestSuite struct{ consoleSuite }

func TestStaticTestSuite(t *testing.T) {
	suite.Run(t, &StaticTestSuite{})
}

func (s *StaticTestSuite) TestHandleStatic() {
	testCases := []struct {
		caseName    string
		path        string
		wantStatus  int
		wantType    string
		wantContain string
	}{
		{
			caseName:    "CSS file",
			path:        "style.css",
			wantStatus:  http.StatusOK,
			wantType:    "text/css",
			wantContain: ":root",
		},
		{
			caseName:   "JS file",
			path:       "htmx.min.js",
			wantStatus: http.StatusOK,
			wantType:   "application/javascript",
		},
		{
			caseName:   "not found",
			path:       "nonexistent.css",
			wantStatus: http.StatusNotFound,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			req := httptest.NewRequest("GET", "/static/"+tc.path, nil)
			req.SetPathValue("filepath", tc.path)
			w := httptest.NewRecorder()
			handleStatic(s.server, w, req)

			s.Equal(tc.wantStatus, w.Code)
			if tc.wantType != "" {
				s.Equal(tc.wantType, w.Header().Get("Content-Type"))
			}
			if tc.wantContain != "" {
				s.Contains(w.Body.String(), tc.wantContain)
			}
		})
	}
}

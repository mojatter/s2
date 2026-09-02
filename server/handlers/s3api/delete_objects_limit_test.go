package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/stretchr/testify/require"
)

// A DeleteObjects request body is parsed with xml.NewDecoder, which streams an
// unbounded number of <Object> entries into req.Objects. Without a body cap a
// client can grow that slice until the process runs out of memory. The handler
// must reject an oversized body instead of decoding it.
func (s *ObjectsTestSuite) TestDeleteObjectsRejectsOversizedBody() {
	s.createBucket("bomb")

	// Build a DeleteObjects body well over the 8 MiB cap.
	var b strings.Builder
	b.WriteString("<Delete>")
	entry := "<Object><Key>" + strings.Repeat("k", 64) + "</Key></Object>"
	for b.Len() < 12<<20 { // ~12 MiB, over maxXMLRequestBody (8 MiB)
		b.WriteString(entry)
	}
	b.WriteString("</Delete>")
	body := b.String()

	req := httptest.NewRequest("POST", "/bomb?delete", strings.NewReader(body))
	req.SetPathValue("bucket", "bomb")
	w := httptest.NewRecorder()
	handleDeleteObjects(s.server, w, req)

	// MaxBytesReader makes the decode fail, so the handler returns a 4xx
	// (MalformedXML / bad request) rather than happily building a giant slice.
	require.GreaterOrEqual(s.T(), w.Code, 400, fmt.Sprintf("expected a 4xx rejection, got %d", w.Code))
	require.Less(s.T(), w.Code, 500)
}

// A normal small DeleteObjects body must still succeed.
func (s *ObjectsTestSuite) TestDeleteObjectsSmallBodyStillWorks() {
	s.putObject("okdel", "a.txt", "1")

	body := `<Delete><Object><Key>a.txt</Key></Object></Delete>`
	req := httptest.NewRequest("POST", "/okdel?delete", strings.NewReader(body))
	req.SetPathValue("bucket", "okdel")
	w := httptest.NewRecorder()
	handleDeleteObjects(s.server, w, req)

	require.Equal(s.T(), http.StatusOK, w.Code)
}

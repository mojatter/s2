package server

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

// failWriter fails every body write with err.
type failWriter struct {
	*httptest.ResponseRecorder
	err error
}

func (w *failWriter) Write([]byte) (int, error) { return 0, w.err }

// fileSource is a source that offers sendfile, as an *os.File does.
type fileSource struct{ io.Reader }

func (fileSource) SyscallConn() (syscall.RawConn, error) { return nil, errors.ErrUnsupported }

func TestCopyBody(t *testing.T) {
	type testCase struct {
		caseName string
		src      io.Reader
		writeErr error
		wantLog  string
	}
	testCases := []testCase{
		{caseName: "ok", src: strings.NewReader("hello")},
		{caseName: "storage read fails", src: io.MultiReader(strings.NewReader("ab"), iotest.ErrReader(syscall.EIO)), wantLog: "level=ERROR"},
		{caseName: "client write fails", src: strings.NewReader("hello"), writeErr: errors.New("stream reset"), wantLog: "level=DEBUG"},
		{caseName: "body longer than its Content-Length", src: strings.NewReader("hello"), writeErr: http.ErrContentLength, wantLog: "level=ERROR"},
		{caseName: "sendfile source, other failure", src: fileSource{strings.NewReader("hello")}, writeErr: syscall.EIO, wantLog: "level=ERROR"},
		{
			caseName: "sendfile source, client gone as net reports it",
			src:      fileSource{strings.NewReader("hello")},
			writeErr: &net.OpError{Op: "readfrom", Net: "tcp", Err: os.NewSyscallError("sendfile", clientGoneErrnos[0])},
			wantLog:  "level=DEBUG",
		},
	}
	for _, errno := range clientGoneErrnos {
		testCases = append(testCases, testCase{caseName: "sendfile source, " + errno.Error(), src: fileSource{strings.NewReader("hello")}, writeErr: errno, wantLog: "level=DEBUG"})
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			buf := captureSlog(t)
			var w http.ResponseWriter = httptest.NewRecorder()
			if tc.writeErr != nil {
				w = &failWriter{ResponseRecorder: httptest.NewRecorder(), err: tc.writeErr}
			}
			CopyBody(w, httptest.NewRequest(http.MethodGet, "/b/k", nil), tc.src)
			if tc.wantLog == "" {
				assert.Empty(t, buf.String())
				return
			}
			assert.Contains(t, buf.String(), tc.wantLog+` msg="Failed to send object body" method=GET path=/b/k`)
		})
	}

	t.Run("request ID", func(t *testing.T) {
		buf := captureSlog(t)
		w := httptest.NewRecorder()
		requestLogHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			CopyBody(w, r, io.MultiReader(strings.NewReader("ab"), iotest.ErrReader(syscall.EIO)))
		})).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/b/k", nil))
		assert.Contains(t, buf.String(), `msg="Failed to send object body" request_id=`+w.Header().Get("x-amz-request-id")+" method=GET path=/b/k bytes=2 ")
	})
}

// readFromSpy records the source its ReadFrom is handed.
type readFromSpy struct {
	*httptest.ResponseRecorder
	src io.Reader
}

func (w *readFromSpy) ReadFrom(src io.Reader) (int64, error) {
	w.src = src
	return io.Copy(w.ResponseRecorder, src)
}

func TestCopyBodyKeepsSendfile(t *testing.T) {
	testCases := []struct {
		caseName string
		src      io.Reader
		wantConn bool
	}{
		{caseName: "file", src: fileSource{strings.NewReader("hello")}, wantConn: true},
		{caseName: "plain reader", src: strings.NewReader("hello")},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			w := &readFromSpy{ResponseRecorder: httptest.NewRecorder()}
			CopyBody(w, httptest.NewRequest(http.MethodGet, "/b/k", nil), tc.src)
			_, ok := w.src.(syscall.Conn)
			assert.Equal(t, tc.wantConn, ok)
			assert.Equal(t, "hello", w.Body.String())
		})
	}
}

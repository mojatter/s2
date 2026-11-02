package server

import (
	"context"
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

	"github.com/mojatter/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{caseName: "ranged file source, other failure", src: rangedSource{&io.LimitedReader{R: fileSource{strings.NewReader("hello world")}, N: 5}}, writeErr: syscall.EIO, wantLog: "level=ERROR"},
		{caseName: "ranged file source, client gone", src: rangedSource{&io.LimitedReader{R: fileSource{strings.NewReader("hello world")}, N: 5}}, writeErr: clientGoneErrnos[0], wantLog: "level=DEBUG"},
		{caseName: "ranged plain reader, client write fails", src: rangedSource{&io.LimitedReader{R: strings.NewReader("hello world"), N: 5}}, writeErr: errors.New("stream reset"), wantLog: "level=DEBUG"},
		{
			caseName: "ranged file source, storage read fails",
			src:      rangedSource{&io.LimitedReader{R: fileSource{io.MultiReader(strings.NewReader("ab"), iotest.ErrReader(syscall.EIO))}, N: 5}},
			wantLog:  "level=ERROR",
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

// rangedSource is a ranged body that exposes its file, as an fs OpenRange does.
type rangedSource struct{ lr *io.LimitedReader }

func (r rangedSource) Read(p []byte) (int, error)       { return r.lr.Read(p) }
func (r rangedSource) LimitedReader() *io.LimitedReader { return r.lr }

// osfsRange opens "hello" as a range of an osfs object, the source fs hands CopyBody.
func osfsRange(t *testing.T) io.Reader {
	ctx := context.Background()
	strg, err := s2.NewStorage(ctx, s2.Config{Type: s2.TypeOSFS, Root: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, strg.Put(ctx, s2.NewObjectBytes("k", []byte("world hello"))))
	obj, err := strg.Get(ctx, "k")
	require.NoError(t, err)
	rc, err := obj.OpenRange(6, 5)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })
	return rc
}

func TestCopyBodyKeepsSendfile(t *testing.T) {
	testCases := []struct {
		caseName string
		src      func(t *testing.T) io.Reader
		wantConn bool
	}{
		{caseName: "file", src: func(*testing.T) io.Reader { return fileSource{strings.NewReader("hello")} }, wantConn: true},
		{caseName: "plain reader", src: func(*testing.T) io.Reader { return strings.NewReader("hello") }},
		{caseName: "ranged file", src: func(*testing.T) io.Reader {
			return rangedSource{&io.LimitedReader{R: fileSource{strings.NewReader("hello world")}, N: 5}}
		}, wantConn: true},
		{caseName: "ranged plain reader", src: func(*testing.T) io.Reader {
			return rangedSource{&io.LimitedReader{R: strings.NewReader("hello world"), N: 5}}
		}},
		{caseName: "osfs range", src: osfsRange, wantConn: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			src := tc.src(t)
			var lr *io.LimitedReader
			if l, ok := src.(limitedReaderer); ok {
				lr = l.LimitedReader()
			}
			w := &readFromSpy{ResponseRecorder: httptest.NewRecorder()}
			CopyBody(w, httptest.NewRequest(http.MethodGet, "/b/k", nil), src)
			// net's sendfile takes a file, or a bare *io.LimitedReader around one.
			got := w.src
			if lr, ok := got.(*io.LimitedReader); ok {
				got = lr.R
			}
			_, ok := got.(syscall.Conn)
			assert.Equal(t, tc.wantConn, ok)
			assert.Equal(t, "hello", w.Body.String())
			if lr != nil && tc.wantConn {
				// The range's own reader is handed over and counted down, so nothing past it stays readable.
				assert.Same(t, lr, w.src)
				assert.Zero(t, lr.N)
			}
		})
	}
}

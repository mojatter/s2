package server

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"syscall"
)

// CopyBody sends src as the body of w and logs a failure: a storage read at Error, a client gone away at Debug.
func CopyBody(w http.ResponseWriter, r *http.Request, src io.Reader) {
	br := &bodyReader{r: src}
	var body io.Reader = br
	sc, sendfile := src.(syscall.Conn)
	if sendfile {
		// Keeps sendfile, which reads the file in the kernel without calling Read.
		body = &bodyFile{bodyReader: br, sc: sc}
	}
	n, err := io.Copy(w, body)
	if err == nil {
		return
	}
	level := slog.LevelDebug
	if br.err != nil || (sendfile && !clientGone(err)) {
		level = slog.LevelError
	}
	args := []any{"method", r.Method, "path", r.URL.Path, "bytes", n, "error", err}
	if id := RequestIDFromContext(r.Context()); id != "" {
		args = append([]any{"request_id", id}, args...)
	}
	slog.Log(r.Context(), level, "Failed to send object body", args...)
}

// clientGone reports an error a write to a closed connection returns.
func clientGone(err error) bool {
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ENOTCONN)
}

// bodyReader records the error its source returns.
type bodyReader struct {
	r   io.Reader
	err error
}

func (br *bodyReader) Read(p []byte) (int, error) {
	n, err := br.r.Read(p)
	if err != nil && err != io.EOF {
		br.err = err
	}
	return n, err
}

// bodyFile is a bodyReader that lets net/http find the file for sendfile.
type bodyFile struct {
	*bodyReader
	sc syscall.Conn
}

func (bf *bodyFile) SyscallConn() (syscall.RawConn, error) {
	return bf.sc.SyscallConn()
}

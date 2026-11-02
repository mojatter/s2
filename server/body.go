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
	} else if lr, sc, ok := limitedFile(src); ok {
		// net's sendfile unwraps only a bare *io.LimitedReader around the file, and counts down its N.
		br.r = lr.R
		lr.R = &bodyFile{bodyReader: br, sc: sc}
		body = lr
		sendfile = true
	}
	n, err := io.Copy(w, body)
	if err == nil {
		return
	}
	level := slog.LevelDebug
	// A body longer than its Content-Length was replaced while being sent.
	if br.err != nil || errors.Is(err, http.ErrContentLength) || (sendfile && !clientGone(err)) {
		level = slog.LevelError
	}
	args := []any{"method", r.Method, "path", r.URL.Path, "bytes", n, "error", err}
	if id := RequestIDFromContext(r.Context()); id != "" {
		args = append([]any{"request_id", id}, args...)
	}
	slog.Log(r.Context(), level, "Failed to send object body", args...)
}

// limitedReaderer is a ranged body that exposes its positioned source and the bytes left, as fs OpenRange does on osfs.
type limitedReaderer interface {
	LimitedReader() *io.LimitedReader
}

// limitedFile returns the reader and file behind a ranged body whose source is a file.
func limitedFile(src io.Reader) (*io.LimitedReader, syscall.Conn, bool) {
	l, ok := src.(limitedReaderer)
	if !ok {
		return nil, nil, false
	}
	lr := l.LimitedReader()
	sc, ok := lr.R.(syscall.Conn)
	return lr, sc, ok
}

// clientGone reports an error a write to a peer that went away returns.
func clientGone(err error) bool {
	for _, errno := range clientGoneErrnos {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
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

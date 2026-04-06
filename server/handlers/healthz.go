package handlers

import (
	"net/http"

	"github.com/mojatter/s2/server"
)

func handleHealthz(_ *server.Server, w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func init() {
	server.RegisterHandleFunc("GET /healthz", handleHealthz)
}

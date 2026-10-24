package objects

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

func handleView(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	objectName := r.PathValue("object")

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	obj, err := strg.Get(ctx, objectName)
	if err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	rc, err := obj.Open()
	if err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Type", server.ResolveContentType(obj, objectName))
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", path.Base(objectName)))
	// The type served here is the uploader's, so an object stored as
	// text/html would otherwise run on the console origin (#199).
	w.Header().Set("Content-Security-Policy", "sandbox")

	server.CopyBody(w, r, rc)
}

func handleMeta(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	objectName := r.PathValue("object")

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	obj, err := strg.Get(ctx, objectName)
	if err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	ct := server.ResolveContentType(obj, objectName)
	resp := map[string]any{
		"name":         path.Base(objectName),
		"contentType":  ct,
		"size":         obj.Length(),
		"lastModified": obj.LastModified().Format("2006-01-02 15:04:05"),
	}
	if md := obj.Metadata(); len(md) > 0 {
		resp["metadata"] = md
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func handlePreview(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	objectName := r.PathValue("object")

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	obj, err := strg.Get(ctx, objectName)
	if err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	ext := strings.ToLower(path.Ext(objectName))
	previewType := server.PreviewType(ext)

	var textContent string
	maxSize := s.Config.EffectiveMaxPreviewSize()
	tooLarge := previewType == "text" && s2.MustInt64(obj.Length()) > maxSize
	if previewType == "text" && !tooLarge {
		rc, err := obj.Open()
		if err != nil {
			server.WriteConsoleStorageError(w, r, err)
			return
		}
		defer func() { _ = rc.Close() }()

		// The limit also holds when the stored bytes outgrow Length.
		b, err := io.ReadAll(io.LimitReader(rc, maxSize+1))
		if err != nil {
			server.WriteConsoleError(w, r, http.StatusInternalServerError, err)
			return
		}
		if int64(len(b)) > maxSize {
			tooLarge = true
		} else {
			textContent = string(b)
		}
	}

	data := struct {
		Bucket       string
		Key          string
		Filename     string
		ContentType  string
		Size         uint64
		LastModified string
		Metadata     map[string]string
		PreviewType  string
		TextContent  string
		TooLarge     bool
	}{
		Bucket:       bucketName,
		Key:          objectName,
		Filename:     path.Base(objectName),
		ContentType:  server.ResolveContentType(obj, objectName),
		Size:         obj.Length(),
		LastModified: obj.LastModified().Format("2006-01-02 15:04:05"),
		PreviewType:  previewType,
		TextContent:  textContent,
		TooLarge:     tooLarge,
	}
	if md := obj.Metadata(); len(md) > 0 {
		data.Metadata = md
	}

	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "console/buckets/preview.html", data); err != nil {
		server.WriteConsoleError(w, r, http.StatusInternalServerError, err)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func init() {
	server.RegisterConsoleHandleFunc("GET /buckets/{name}/view/{object...}", middleware.BasicAuth(handleView))
	server.RegisterConsoleHandleFunc("GET /buckets/{name}/meta/{object...}", middleware.BasicAuth(handleMeta))
	server.RegisterConsoleHandleFunc("GET /buckets/{name}/preview/{object...}", middleware.BasicAuth(handlePreview))
}

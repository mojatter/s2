package buckets

import (
	"bytes"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

type Breadcrumb struct {
	Name   string
	Prefix string
}

func handleObjects(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	prefix := strings.TrimRight(r.URL.Query().Get("prefix"), "/")
	search := r.URL.Query().Get("search")

	strg, err := s.Buckets.Get(ctx, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	var (
		objs     []s2.Object
		prefixes []string
	)
	if search != "" {
		listPrefix := search
		if prefix != "" {
			listPrefix = prefix + "/" + search
		}
		res, err := strg.List(ctx, s2.ListOptions{Prefix: listPrefix, Recursive: true})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		objs = server.FilterKeep(res.Objects)
	} else {
		res, err := strg.List(ctx, s2.ListOptions{Prefix: prefix})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		objs = server.FilterKeep(res.Objects)
		prefixes = res.CommonPrefixes
	}

	// Calculate breadcrumbs
	var breadcrumbs []Breadcrumb
	parts := strings.Split(strings.Trim(prefix, "/"), "/")
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if current == "" {
			current = part
		} else {
			current = path.Join(current, part)
		}
		breadcrumbs = append(breadcrumbs, Breadcrumb{
			Name:   part,
			Prefix: current + "/",
		})
	}

	parentPrefix := ""
	if len(breadcrumbs) > 1 {
		parentPrefix = breadcrumbs[len(breadcrumbs)-2].Prefix
	} else if len(breadcrumbs) == 1 {
		parentPrefix = ""
	}

	data := struct {
		BucketName    string
		Objects       []s2.Object
		Prefixes      []string
		CurrentPrefix string
		ParentPrefix  string
		Breadcrumbs   []Breadcrumb
		HasParent     bool
		Search        string
	}{
		BucketName:    name,
		Objects:       objs,
		Prefixes:      prefixes,
		CurrentPrefix: prefix,
		ParentPrefix:  parentPrefix,
		Breadcrumbs:   breadcrumbs,
		HasParent:     prefix != "" && prefix != "/",
		Search:        search,
	}

	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "buckets/objects.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = buf.WriteTo(w)
}

func handleCreateFolder(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	prefix := r.FormValue("prefix")
	folderName := r.FormValue("folder_name")
	if folderName == "" {
		http.Error(w, "folder name is required", http.StatusBadRequest)
		return
	}

	key := path.Join(prefix, folderName)
	if err := s.Buckets.CreateFolder(ctx, name, key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Re-render objects list
	r.URL.Path = "/buckets/" + name
	qs := r.URL.Query()
	qs.Set("prefix", prefix)
	r.URL.RawQuery = qs.Encode()
	r.Header.Set("HX-Request", "true")
	handleObjects(s, w, r)
}

func handleUploadFile(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	prefix := r.FormValue("prefix")

	// Enforce upload size limit
	maxSize := s.Config.EffectiveMaxUploadSize()
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	file, header, err := r.FormFile("file")
	if err != nil {
		if err.Error() == "http: request body too large" {
			http.Error(w, fmt.Sprintf("File too large (max %d MB)", maxSize/(1<<20)), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer func() { _ = file.Close() }()

	strg, err := s.Buckets.Get(ctx, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	key := path.Join(prefix, header.Filename)
	obj := s2.NewObjectReader(key, file, s2.MustUint64(header.Size))
	if err := strg.Put(ctx, obj); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Re-render objects list
	r.URL.Path = "/buckets/" + name
	qs := r.URL.Query()
	qs.Set("prefix", prefix)
	r.URL.RawQuery = qs.Encode()
	r.Header.Set("HX-Request", "true")
	handleObjects(s, w, r)
}

func handleDeleteObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	strg, err := s.Buckets.Get(ctx, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if strings.HasSuffix(key, "/") {
		if err := strg.DeleteRecursive(ctx, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if err := strg.Delete(ctx, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Re-render objects list
	prefix := r.URL.Query().Get("prefix")
	r.URL.Path = "/buckets/" + name
	qs := r.URL.Query()
	qs.Set("prefix", prefix)
	r.URL.RawQuery = qs.Encode()
	r.Header.Set("HX-Request", "true")
	handleObjects(s, w, r)
}

func init() {
	server.RegisterConsoleHandleFunc("GET /buckets/{name}", middleware.BasicAuth(middleware.ServeIndex(handleObjects)))
	server.RegisterConsoleHandleFunc("POST /buckets/{name}/folders", middleware.BasicAuth(handleCreateFolder))
	server.RegisterConsoleHandleFunc("POST /buckets/{name}/upload", middleware.BasicAuth(handleUploadFile))
	server.RegisterConsoleHandleFunc("DELETE /buckets/{name}/objects", middleware.BasicAuth(handleDeleteObject))
	server.RegisterTemplate("buckets/objects.html")
}
